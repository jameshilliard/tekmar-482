"""Nonblocking observation of both sides of a live Normal-mode session."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from time import monotonic_ns
from typing import TYPE_CHECKING

from ._capture import (
    CaptureBuffer,
    CaptureEvent,
    CaptureGap,
    GatewayTraceGap,
    InvalidCapture,
    StampedRecord,
)
from .exceptions import ConnectionClosedError, ProtocolError
from .native import NativeFrame, RxFlag
from .packet import TYPE_NORMAL_TRACE, TYPE_TRPC
from .raw import FailureReason
from .trpc import TrpcPacket

if TYPE_CHECKING:
    from .management import BridgeManagement
    from .packet import Packet

_TX_DETAILS = 2


class TraceOrigin(IntEnum):
    SETTING = 0
    BROADCAST = 1
    DISCOVERY = 2
    PROBE = 3
    STAGE_COMMAND = 4
    SERVICE_FRAME = 5


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


def _decode_trace(
    data: bytes,
    capture_index: int = 0,
) -> TraceRx | TraceTx | InvalidCapture | GatewayTraceGap | None:
    if data[:1] == bytes((CaptureEvent.GAP,)):
        return GatewayTraceGap.decode(data)
    record = StampedRecord.read(data)
    if record is None:
        return None
    if record.event != CaptureEvent.TX_ATTEMPT:
        sample = record.received(capture_index)
        if isinstance(sample, NativeFrame):
            return (
                TraceRx(sample, sample.rx_flags)
                if sample.rx_flags is not None
                else None
            )
        return sample
    body = record.body
    try:
        if len(body) >= _TX_DETAILS:
            origin, outcome = body[-_TX_DETAILS:]
            return TraceTx(
                NativeFrame(record.port, record.timestamp_ms, body[:-_TX_DETAILS]),
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


class NormalTrace(CaptureBuffer[TracePacket]):
    """Single-consumer diagnostic trace of both wire directions."""

    def record(self, packet: Packet, *, outbound: bool) -> None:
        self._put(TracePacket(monotonic_ns(), outbound, packet))


class NormalPeerCapture(CaptureBuffer[NativeFrame | InvalidCapture | GatewayTraceGap]):
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
            if (
                self.port_mask & (1 << sample.frame.port)
                and sample.frame.usable_for_state
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
