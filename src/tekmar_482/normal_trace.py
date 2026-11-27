"""Nonblocking observation of both sides of a live Normal-mode session."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from enum import IntEnum, IntFlag
from time import monotonic_ns
from typing import TYPE_CHECKING, Self

from .constants import TN4_PORTS
from .exceptions import ConnectionClosedError, ProtocolError
from .native import NativeFrame
from .packet import TYPE_NORMAL_TRACE, TYPE_TRPC
from .raw import CaptureGap, FailureReason, InvalidCapture
from .trpc import TrpcPacket

if TYPE_CHECKING:
    from collections.abc import Iterator

    from .management import BridgeManagement
    from .packet import Packet

_PREFIX = 6
_TX_DETAILS = 2
_GAP_BYTES = 10


class _Event(IntEnum):
    RX = 0
    TX = 1
    INVALID = 4
    GAP = 5


class TraceOrigin(IntEnum):
    SETTING = 0
    BROADCAST = 1
    DISCOVERY = 2
    PROBE = 3


class RxFlag(IntFlag):
    ACK = 1
    EARLY_START = 2


@dataclass(frozen=True, slots=True)
class NormalTraceState:
    port_mask: int
    gateway_drops: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class TraceRx:
    frame: NativeFrame
    flags: RxFlag


@dataclass(frozen=True, slots=True)
class TraceTx:
    frame: NativeFrame
    origin: TraceOrigin | int
    # None is a link ACK, not confirmation of an application-level change.
    failure: FailureReason | int | None


@dataclass(frozen=True, slots=True)
class GatewayTraceGap(CaptureGap):
    """Lost gateway trace records across these ports, not a per-port count."""

    port_mask: int

    def affects(self, port: int, /) -> bool:
        return bool(self.port_mask & (1 << port))

    @property
    def ports(self) -> tuple[int, ...]:
        return tuple(p for p in TN4_PORTS if self.affects(p))


def _decode_trace(
    data: bytes,
    capture_index: int = 0,
) -> TraceRx | TraceTx | InvalidCapture | GatewayTraceGap | None:
    if len(data) < _PREFIX:
        return None
    kind, port = data[:2]
    if kind == _Event.GAP:
        if len(data) != _GAP_BYTES or not 0 < port < 1 << len(TN4_PORTS):
            return None
        count = int.from_bytes(data[2:], "little")
        return GatewayTraceGap(count, port) if count >= port.bit_count() else None
    if port not in TN4_PORTS:
        return None
    stamp = int.from_bytes(data[2:_PREFIX], "little")
    body = data[_PREFIX:]
    if kind == _Event.INVALID:
        return InvalidCapture.from_detail(port, stamp, body)
    try:
        if kind == _Event.RX and body:
            frame = NativeFrame(port, stamp, body[1:], capture_index=capture_index)
            return TraceRx(frame, RxFlag(body[0]))
        if kind == _Event.TX and len(body) >= _TX_DETAILS:
            origin, outcome = body[:2]
            return TraceTx(
                NativeFrame(port, stamp, body[_TX_DETAILS:]),
                TraceOrigin(origin) if origin in TraceOrigin else origin,
                FailureReason(outcome) if outcome in FailureReason else outcome or None,
            )
    except ValueError:
        return None
    return None


@dataclass(frozen=True, slots=True)
class TracePacket:
    host_monotonic_ns: int
    outbound: bool
    packet: Packet

    @property
    def decoded(
        self,
    ) -> TraceRx | TraceTx | InvalidCapture | GatewayTraceGap | TrpcPacket | None:
        """Keep malformed and future records accessible via packet.data."""
        if self.packet.type == TYPE_TRPC:
            try:
                return TrpcPacket.from_packet(self.packet)
            except ProtocolError:
                return None
        return (
            _decode_trace(self.packet.data)
            if self.packet.type == TYPE_NORMAL_TRACE
            else None
        )


class _CaptureBuffer[T]:
    def __init__(self, backlog: int) -> None:
        if backlog <= 0:
            msg = "trace backlog must be positive"
            raise ValueError(msg)
        self._queue: deque[T] = deque(maxlen=backlog)
        self._ready = asyncio.Event()
        self._lost = 0
        self._arrived = 0
        self._consumed = 0
        self._advanced = asyncio.Event()
        self.host_drops = 0
        self.gateway_drops: tuple[int, ...] | None = None
        self.closed = False
        self._error: BaseException | None = None

    def _put(self, item: T) -> None:
        if self.closed:
            return
        self._arrived += 1
        if len(self._queue) == self._queue.maxlen:
            self._lost += 1
            self.host_drops += 1
        self._queue.append(item)
        self._ready.set()

    def finish(self, error: BaseException | None = None) -> None:
        if not self.closed:
            self.closed = True
            self._error = error
            self._ready.set()
            self._advanced.set()

    def __aiter__(self) -> Self:
        return self

    def _pop(self) -> T | CaptureGap | None:
        if self._lost:
            lost, self._lost = self._lost, 0
            self._consumed += lost
            self._advanced.set()
            return CaptureGap(lost)
        if not self._queue:
            return None
        self._consumed += 1
        self._advanced.set()
        return self._queue.popleft()

    async def wait_consumed(self) -> None:
        """Wait until the consumer receives all earlier observations or their loss."""
        target = self._arrived
        while True:
            if self._error is not None:
                raise self._error
            if self._consumed >= target:
                return
            if self.closed:
                msg = "capture ended before its observations were consumed"
                raise ConnectionClosedError(msg)
            self._advanced.clear()
            await self._advanced.wait()

    def drain(self) -> Iterator[T | CaptureGap]:
        """Return buffered observations, including those before the stop reply."""
        while (item := self._pop()) is not None:
            yield item

    async def __anext__(self) -> T | CaptureGap:
        while True:
            if (item := self._pop()) is not None:
                return item
            if self.closed:
                if self._error:
                    raise self._error
                raise StopAsyncIteration
            self._ready.clear()
            await self._ready.wait()


class NormalTrace(_CaptureBuffer[TracePacket]):
    """Single-consumer diagnostic trace of both wire directions."""

    def record(self, packet: Packet, *, outbound: bool) -> None:
        self._put(TracePacket(monotonic_ns(), outbound, packet))


class NormalPeerCapture(_CaptureBuffer[NativeFrame | InvalidCapture | GatewayTraceGap]):
    """Peer evidence only, between trace-mask acknowledgements in UART order."""

    def __init__(self, backlog: int, port_mask: int, start: Packet) -> None:
        super().__init__(backlog)
        self.port_mask = port_mask
        self._start: Packet | None = start
        self._stop: Packet | None = None
        self._index = 0

    def stop_at(self, reply: Packet) -> None:
        self._stop = reply

    def record(self, packet: Packet, *, outbound: bool) -> None:
        if self.closed or outbound:
            return
        if self._start is not None:
            if packet == self._start:
                self._start = None
            return
        if packet == self._stop:
            self.finish()
            return
        if packet.type != TYPE_NORMAL_TRACE:
            return
        sample = _decode_trace(packet.data, self._index + 1)
        if isinstance(sample, GatewayTraceGap):
            if self.port_mask & sample.port_mask:
                self._put(sample)
        elif isinstance(sample, TraceRx):
            if self.port_mask & (1 << sample.frame.port) and not (
                int(sample.flags) & ~int(RxFlag.ACK)
            ):
                self._index = sample.frame.capture_index
                self._put(sample.frame)
        elif isinstance(sample, InvalidCapture):
            if self.port_mask & (1 << sample.port):
                self._put(sample)
        elif not isinstance(sample, TraceTx):
            msg = "unrecognized Normal native trace record"
            self.finish(ProtocolError(msg))

    async def read(self) -> NativeFrame | InvalidCapture | CaptureGap:
        try:
            return await anext(self)
        except StopAsyncIteration as err:
            msg = "Normal peer capture ended"
            raise ConnectionClosedError(msg) from err


@dataclass(frozen=True, slots=True)
class NormalNativeSession:
    """A single Normal connection providing independent control and peer evidence."""

    management: BridgeManagement
    peers: NormalPeerCapture
