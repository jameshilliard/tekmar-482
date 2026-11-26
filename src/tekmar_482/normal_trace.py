"""Nonblocking observation of both sides of a live Normal-mode session."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from enum import IntEnum, IntFlag
from time import monotonic_ns
from typing import TYPE_CHECKING, Self

from .constants import TN4_PORTS
from .exceptions import ProtocolError
from .native import NativeFrame
from .packet import TYPE_NORMAL_TRACE, TYPE_TRPC
from .raw import CaptureGap, FailureReason, InvalidCapture, InvalidReason
from .trpc import TrpcPacket

if TYPE_CHECKING:
    from collections.abc import Iterator

    from .packet import Packet

_PREFIX = 6
_INVALID_PREFIX = 3
_TX_DETAILS = 2


class _Event(IntEnum):
    RX = 0
    TX = 1
    INVALID = 4


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
class TracePacket:
    host_monotonic_ns: int
    outbound: bool
    packet: Packet

    @property
    def decoded(self) -> TraceRx | TraceTx | InvalidCapture | TrpcPacket | None:
        """Keep malformed and future records accessible via packet.data."""
        if self.packet.type == TYPE_TRPC:
            try:
                return TrpcPacket.from_packet(self.packet)
            except ProtocolError:
                return None
        data = self.packet.data
        if self.packet.type != TYPE_NORMAL_TRACE or len(data) < _PREFIX:
            return None
        kind, port = data[:2]
        if port not in TN4_PORTS:
            return None
        stamp = int.from_bytes(data[2:_PREFIX], "little")
        body = data[_PREFIX:]
        try:
            if kind == _Event.RX and body:
                return TraceRx(NativeFrame(port, stamp, body[1:]), RxFlag(body[0]))
            if kind == _Event.TX and len(body) >= _TX_DETAILS:
                origin, outcome = body[:2]
                return TraceTx(
                    NativeFrame(port, stamp, body[2:]),
                    TraceOrigin(origin) if origin in TraceOrigin else origin,
                    FailureReason(outcome)
                    if outcome in FailureReason
                    else outcome or None,
                )
        except ValueError:
            return None
        if kind == _Event.INVALID and len(body) >= _INVALID_PREFIX:
            reason = body[0]
            count = int.from_bytes(body[1:_INVALID_PREFIX], "little")
            bits = body[_INVALID_PREFIX:]
            if len(bits) == (count + 7) // 8:
                return InvalidCapture(
                    port,
                    stamp,
                    InvalidReason(reason) if reason in InvalidReason else reason,
                    count,
                    bits,
                )
        return None


class NormalTrace:
    """Single-consumer async iterator; a slow debugger never blocks the reader."""

    def __init__(self, backlog: int) -> None:
        if backlog <= 0:
            msg = "trace backlog must be positive"
            raise ValueError(msg)
        self._queue: deque[TracePacket] = deque(maxlen=backlog)
        self._ready = asyncio.Event()
        self._lost = 0
        self.host_drops = 0
        self.gateway_drops: tuple[int, ...] | None = None
        self.closed = False
        self._error: BaseException | None = None

    def record(self, packet: Packet, *, outbound: bool) -> None:
        if self.closed:
            return
        if len(self._queue) == self._queue.maxlen:
            self._lost += 1
            self.host_drops += 1
        self._queue.append(TracePacket(monotonic_ns(), outbound, packet))
        self._ready.set()

    def finish(self, error: BaseException | None = None) -> None:
        if not self.closed:
            self.closed = True
            self._error = error
            self._ready.set()

    def __aiter__(self) -> Self:
        return self

    def _pop(self) -> TracePacket | CaptureGap | None:
        if self._lost:
            lost, self._lost = self._lost, 0
            return CaptureGap(lost)
        return self._queue.popleft() if self._queue else None

    def drain(self) -> Iterator[TracePacket | CaptureGap]:
        """Return buffered observations, including those before the stop reply."""
        while (item := self._pop()) is not None:
            yield item

    async def __anext__(self) -> TracePacket | CaptureGap:
        while True:
            if (item := self._pop()) is not None:
                return item
            if self.closed:
                if self._error:
                    raise self._error
                raise StopAsyncIteration
            self._ready.clear()
            await self._ready.wait()
