"""Raw-mode tN4 captures and transmit completions."""

from __future__ import annotations

import asyncio
from collections import deque
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from enum import IntEnum
from typing import TYPE_CHECKING, Protocol

from ._capture import (
    CaptureBuffer,
    CaptureEvent,
    CaptureGap,
    GatewayTraceGap,
    InvalidCapture,
    InvalidReason,
    StampedRecord,
)
from ._native_payloads.queries import StatusRequest
from ._raw_devices import DeviceOperations, RawDeviceOperations
from .constants import TN4_PORTS
from .exceptions import ConnectionClosedError, ProtocolError, TekmarError
from .native import NativeFrame, NativeMessage, native_body_size
from .native_status import StatusCollector
from .native_write import BlockDelivery, NativeBlockEditor
from .packet import TYPE_TN4

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping

    from ._native_payloads.queries import NativeStatusQuery
    from .management import BridgeManagement, ErrorReporting
    from .native import NativeAddress, NativeBlockQuery, NativeTargets
    from .native_identity import NativeIdentity
    from .native_status import NativeStatusBatch
    from .packet import Packet
    from .traffic import TrafficCounters

_RAW_HEADER = 2
_ALL_PORTS = 0x3F
_BASE_CLOSED = "base Raw session closed"

__all__ = [
    "BaseRawStream",
    "BridgeStream",
    "CaptureGap",
    "CaptureRecord",
    "FailureReason",
    "InvalidCapture",
    "InvalidReason",
    "NativeBlockBatch",
    "NativePortBusyError",
    "NativeReader",
    "NativeTransmitError",
    "PortDiagnostic",
    "PortError",
    "PortErrorReason",
    "RawDispatcher",
    "RawLink",
    "RejectReason",
    "TxResult",
    "TxStatus",
    "decode_raw_packet",
]


class TxStatus(IntEnum):
    ACK = 0
    REJECTED = 1
    FAILED = 2


class RejectReason(IntEnum):
    INVALID_BODY = 1
    PIN_UNAVAILABLE = 2
    LOCAL_ACK_ACTIVE = 4


class FailureReason(IntEnum):
    NO_ACK = 1
    ARBITRATION_LOST = 2
    ECHO_MISMATCH = 3
    BAD_END = 4
    MISSED_TIMING = 5


class PortErrorReason(IntEnum):
    SEND_IN_PROGRESS = 1
    INVALID_PORT = 2


class _Reply(IntEnum):
    TX_RESULT = 2
    PORT_ERROR = 3


def _reason[E: IntEnum](enum: type[E], value: int) -> E | int:
    return enum(value) if value in enum else value


@dataclass(frozen=True, slots=True)
class TxResult:
    """Terminal outcome on one link, not confirmation by the application."""

    port: int
    status: TxStatus
    reason: RejectReason | FailureReason | int | None = None

    @property
    def completed_frame(self) -> bool:
        """The gateway reached the end of its own frame, with or without an ACK."""
        return self.status == TxStatus.ACK or (
            self.status == TxStatus.FAILED and self.reason == FailureReason.NO_ACK
        )

    @property
    def may_have_reached_peer(self) -> bool:
        """A write needs fresh readback unless non-delivery is known."""
        return self.status == TxStatus.ACK or (
            self.status == TxStatus.FAILED
            and self.reason != FailureReason.ARBITRATION_LOST
        )


@dataclass(frozen=True, slots=True)
class NativeBlockBatch:
    """One request and the distinct peers that reported before its deadline."""

    port: int
    query: NativeBlockQuery
    tx_result: TxResult | None
    reports: tuple[NativeFrame, ...]
    expired_unsent: bool = False
    capture_lost: bool = False

    @property
    def transmit_pending(self) -> bool:
        """Only a Raw host deadline can leave an uncompleted send in the gateway."""
        return self.tx_result is None and not self.expired_unsent

    @property
    def missing_mask(self) -> int:
        mask = self.query.target.request_mask()
        for frame in self.reports:
            mask &= ~frame.message.source.request_mask()
        return mask

    @property
    def missing_nodes(self) -> tuple[int, ...]:
        mask = self.missing_mask
        return tuple(node for node in range(1, 32) if mask & (1 << (node - 1)))

    def require_report(self) -> NativeFrame:
        """Return the result of a single-node read, or explain why it failed."""
        if self.query.target.request_mask().bit_count() != 1:
            msg = "expected a single-node block request"
            raise ValueError(msg)
        if self.tx_result is not None and not self.tx_result.completed_frame:
            raise NativeTransmitError(self.tx_result)
        if self.capture_lost:
            msg = "native block read interrupted by gateway capture loss"
            raise ProtocolError(msg)
        if self.tx_result is None or not self.reports:
            msg = (
                "no transmit completion or matching native block report before deadline"
            )
            raise TimeoutError(msg)
        return self.reports[0]


@dataclass(slots=True)
class _ReportWaiter:
    collect: Callable[[NativeFrame], bool]
    done: asyncio.Future[None]
    transmitted: bool = False
    capture_lost: bool = False

    def gap(self) -> None:
        if self.transmitted and not self.done.done():
            self.capture_lost = True
            self.done.set_result(None)

    def observe(self, frame: NativeFrame) -> None:
        if self.transmitted and not self.done.done() and self.collect(frame):
            self.done.set_result(None)


class NativeTransmitError(TekmarError):
    """The gateway rejected or failed an application request on the tN4 link."""

    def __init__(self, result: TxResult) -> None:
        self.result = result
        super().__init__(
            f"port {result.port}: {result.status.name}, reason {result.reason!r}",
        )


class NativePortBusyError(ProtocolError):
    """A previous operation owns the port. The new request was not admitted."""


@dataclass(frozen=True, slots=True)
class PortError:
    port: int
    reason: PortErrorReason | int


@dataclass(frozen=True, slots=True)
class PortDiagnostic:
    port: int
    valid_dropped: int
    invalid_dropped: int


type _Record = NativeFrame | InvalidCapture | PortError | GatewayTraceGap
type CaptureRecord = _Record | CaptureGap


def decode_raw_packet(packet: Packet, capture_index: int = 0) -> _Record | TxResult:
    """Decode a gateway-to-host Raw packet without consuming a live stream."""
    data = packet.data
    if packet.type == TYPE_TN4 and len(data) >= _RAW_HEADER:
        kind, port = data[:2]
        if (
            kind == CaptureEvent.GAP
            and (gap := GatewayTraceGap.decode(data)) is not None
        ):
            return gap
        if kind == _Reply.PORT_ERROR and len(data) == _RAW_HEADER + 1:
            # InvalidPort echoes even a port number outside the physical range.
            return PortError(port, _reason(PortErrorReason, data[2]))
        if kind == _Reply.TX_RESULT and port in TN4_PORTS:
            match tuple(data[2:]):
                case (TxStatus.ACK,):
                    return TxResult(port, TxStatus.ACK)
                case (TxStatus.REJECTED, why):
                    return TxResult(port, TxStatus.REJECTED, _reason(RejectReason, why))
                case (TxStatus.FAILED, why):
                    return TxResult(port, TxStatus.FAILED, _reason(FailureReason, why))
        elif (record := StampedRecord.read(data)) is not None and (
            capture := record.received(capture_index)
        ) is not None:
            return capture
    msg = "unexpected raw packet type, service, port or payload length"
    raise ProtocolError(msg)


class NativeReader(Protocol):
    """Read-only operations shared by Normal management and a borrowed Raw stream."""

    async def read_block_group(
        self,
        port: int,
        query: NativeBlockQuery,
        *,
        source: NativeAddress,
        timeout: float,
        wait_for_edits: bool = False,
    ) -> NativeBlockBatch: ...
    async def read_status(
        self,
        port: int,
        query: NativeStatusQuery,
        *,
        source: NativeAddress,
        peer: NativeAddress | None = None,
        timeout: float,
        wait_for_edits: bool = False,
    ) -> NativeStatusBatch: ...


def _send_record(selector: int, body: bytes | NativeMessage) -> bytes:
    if isinstance(body, NativeMessage):
        body = body.to_bytes()
    if len(body) != native_body_size(body):
        msg = "expected a complete native frame"
        raise ValueError(msg)
    return bytes((CaptureEvent.NATIVE, selector)) + bytes(4) + body


class BaseRawStream(Protocol):
    """485 base profile. Submission is not acknowledgement or confirmation."""

    async def submit(self, port: int, body: bytes | NativeMessage) -> None: ...
    async def submit_all(self, body: bytes | NativeMessage) -> None: ...
    async def read(self) -> NativeFrame | CaptureGap: ...


class BaseRawDispatcher(CaptureBuffer[NativeFrame]):
    def __init__(self, write: Callable[[bytes], Awaitable[None]], backlog: int) -> None:
        super().__init__(backlog)
        self._write = write
        self._index = 0

    def feed(self, packet: Packet) -> None:
        record = decode_raw_packet(packet, self._index + 1)
        if not isinstance(record, NativeFrame) or record.rx_flags is not None:
            msg = "unexpected extension in base Raw mode"
            raise ProtocolError(msg)
        self._index = record.capture_index
        self._put(record)

    async def _submit(self, selector: int, body: bytes | NativeMessage) -> None:
        if self.closed:
            raise ConnectionClosedError(_BASE_CLOSED)
        await self._write(_send_record(selector, body))

    async def submit(self, port: int, body: bytes | NativeMessage) -> None:
        if port not in TN4_PORTS:
            msg = f"invalid tN4 port {port}"
            raise ValueError(msg)
        await self._submit(port, body)

    async def submit_all(self, body: bytes | NativeMessage) -> None:
        await self._submit(_ALL_PORTS, body)

    async def read(self) -> NativeFrame | CaptureGap:
        try:
            return await anext(self)
        except StopAsyncIteration as err:
            raise ConnectionClosedError(_BASE_CLOSED) from err


class RawLink(Protocol):
    """Low-level extended capture and per-port transmit completion."""

    @property
    def diagnostics(self) -> Mapping[int, PortDiagnostic]: ...
    @property
    def error_reporting(self) -> ErrorReporting: ...
    @property
    def traffic(self) -> TrafficCounters: ...
    async def wait_transmit_idle(self, port: int) -> None: ...
    async def send(self, port: int, body: bytes | NativeMessage) -> TxResult: ...
    async def read(self) -> CaptureRecord: ...


class BridgeStream(RawLink, NativeReader, DeviceOperations, Protocol):
    """Extended Raw link with addressed device operations."""

    async def read_status(
        self,
        port: int,
        query: NativeStatusQuery,
        *,
        source: NativeAddress,
        peer: NativeAddress | None = None,
        timeout: float = 20,
        wait_for_edits: bool = False,
    ) -> NativeStatusBatch: ...
    async def read_block(
        self,
        port: int,
        query: NativeBlockQuery,
        *,
        source: NativeAddress,
    ) -> NativeFrame: ...
    async def read_block_group(
        self,
        port: int,
        query: NativeBlockQuery,
        *,
        source: NativeAddress,
        timeout: float = 20,
        wait_for_edits: bool = False,
    ) -> NativeBlockBatch: ...


class RawDispatcher(RawDeviceOperations):
    """One reader owns the wire. Each send owns exactly one port until TxResult."""

    def __init__(
        self,
        write: Callable[[bytes], Awaitable[None]],
        backlog: int,
    ) -> None:
        if backlog < 1:
            msg = "capture backlog must be positive"
            raise ValueError(msg)
        self._write = write
        self.diagnostics: dict[int, PortDiagnostic] = {}
        self._pending: dict[int, asyncio.Future[TxResult]] = {}
        self._tx_changed = {p: asyncio.Event() for p in TN4_PORTS}
        self._waiters: dict[int, _ReportWaiter] = {}
        self._edits = {port: asyncio.Lock() for port in TN4_PORTS}
        self._records: deque[_Record] = deque(maxlen=backlog)
        self._capture_index = 0
        self._lost = 0
        self._ready = asyncio.Event()
        self._active = False
        self._management: BridgeManagement | None = None
        self._error: Exception | None = None

    @property
    def error_reporting(self) -> ErrorReporting:
        self._check_active()
        if self._management is None:
            msg = "no management session attached to the Raw dispatcher"
            raise ProtocolError(msg)
        return self._management.error_reporting

    @property
    def traffic(self) -> TrafficCounters:
        self._check_active()
        if self._management is None:
            msg = "no management session attached to the Raw dispatcher"
            raise ProtocolError(msg)
        return self._management.traffic

    def _check_active(self) -> None:
        if self._error is not None:
            raise self._error
        if not self._active:
            msg = "raw session closed"
            raise ConnectionClosedError(msg)

    async def send(self, port: int, body: bytes | NativeMessage) -> TxResult:
        """Send once and await this port's link result; cancellation cannot undo TX."""
        self._check_no_edit(port)
        self._check_no_read(port)
        return await self._send(port, body)

    async def wait_transmit_idle(self, port: int) -> None:
        """Wait for the real terminal result after cancellation, without sending."""
        if port not in TN4_PORTS:
            msg = f"invalid tN4 port {port}"
            raise ValueError(msg)
        self._check_active()
        event = self._tx_changed[port]
        while port in self._pending:
            event.clear()
            await event.wait()
            self._check_active()

    def _check_no_read(self, port: int) -> None:
        if port in self._waiters:
            msg = f"native read already in progress on port {port}"
            raise NativePortBusyError(msg)

    def _check_no_edit(self, port: int) -> None:
        if port in self._edits and self._edits[port].locked():
            msg = f"block write in progress on port {port}"
            raise NativePortBusyError(msg)

    async def _send(self, port: int, body: bytes | NativeMessage) -> TxResult:
        self._check_active()
        if port not in TN4_PORTS:
            msg = f"invalid tN4 port {port}"
            raise ValueError(msg)
        data = _send_record(port, body)
        if port in self._pending:
            msg = f"send already in progress on port {port}"
            raise NativePortBusyError(msg)
        future: asyncio.Future[TxResult] = asyncio.get_running_loop().create_future()
        self._pending[port] = future
        try:
            await self._write(data)
            return await future
        except Exception as err:
            self.fail(err)
            raise
        finally:
            # Keep a cancelled entry until the wire result arrives. Cancellation
            # can also land during write drain, before the future was awaited.
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()

    async def read_block(
        self,
        port: int,
        query: NativeBlockQuery,
        *,
        source: NativeAddress,
    ) -> NativeFrame:
        """Send once and wait for a matching new report without consuming captures."""
        if query.target.request_mask().bit_count() != 1:
            msg = "use read_block_group for multiple nodes"
            raise ValueError(msg)
        self._check_no_edit(port)
        batch = await self._collect_blocks(port, query, source, None)
        return batch.require_report()

    async def read_block_group(
        self,
        port: int,
        query: NativeBlockQuery,
        *,
        source: NativeAddress,
        timeout: float = 20,
        wait_for_edits: bool = False,
    ) -> NativeBlockBatch:
        """Collect reports in one exchange; optionally queue with verified writes."""
        if port not in TN4_PORTS or not timeout > 0:
            msg = "expected a physical port and positive block response timeout"
            raise ValueError(msg)
        if wait_for_edits:
            async with self._edits[port]:
                await self.wait_transmit_idle(port)
                return await self._collect_blocks(port, query, source, timeout)
        self._check_no_edit(port)
        return await self._collect_blocks(port, query, source, timeout)

    @asynccontextmanager
    async def _editor(
        self,
        port: int,
        target: NativeAddress | NativeTargets,
        source: NativeAddress,
        timeout: float,
        *,
        expected_identity: NativeIdentity | None,
    ) -> AsyncIterator[NativeBlockEditor]:
        if port not in TN4_PORTS or target.request_mask().bit_count() != 1:
            msg = "expected a single destination on a physical port"
            raise ValueError(msg)
        if not timeout > 0:
            msg = "block write timeout must be positive"
            raise ValueError(msg)

        async def read(query: NativeBlockQuery) -> NativeBlockBatch:
            return await self._collect_blocks(port, query, source, timeout)

        async def update(
            _query: NativeBlockQuery,
            _before: NativeFrame,
            message: NativeMessage,
        ) -> BlockDelivery:
            return BlockDelivery(await self._send(port, message))

        async with self._edits[port]:
            self._check_active()
            if port in self._pending or port in self._waiters:
                msg = f"native operation already in progress on port {port}"
                raise NativePortBusyError(msg)
            yield NativeBlockEditor(source, read, update, timeout, expected_identity)

    async def _collect_blocks(
        self,
        port: int,
        query: NativeBlockQuery,
        source: NativeAddress,
        timeout: float | None,
    ) -> NativeBlockBatch:
        frames: dict[int, NativeFrame] = {}
        count = query.target.request_mask().bit_count()

        def collect(frame: NativeFrame) -> bool:
            if query.matches(frame.message):
                frames.setdefault(frame.node & 31, frame)
            return len(frames) == count

        result, lost = await self._exchange(
            port,
            query.request(source),
            collect,
            timeout,
        )
        return NativeBlockBatch(
            port,
            query,
            result,
            () if lost else tuple(frames[n] for n in sorted(frames)),
            capture_lost=lost,
        )

    async def read_status(
        self,
        port: int,
        query: NativeStatusQuery,
        *,
        source: NativeAddress,
        peer: NativeAddress | None = None,
        timeout: float = 20,
        wait_for_edits: bool = False,
    ) -> NativeStatusBatch:
        """Request fresh, non-block status. Captures remain on the normal stream."""
        if port not in TN4_PORTS or not timeout > 0:
            msg = "expected a physical port and positive response timeout"
            raise ValueError(msg)
        if query.kind is StatusRequest.HUMIDITY and (
            peer is None or peer.subnet != query.payload[0]
        ):
            msg = "humidity queries need the reporting peer on the requested subnet"
            raise ValueError(msg)
        if wait_for_edits:
            async with self._edits[port]:
                await self.wait_transmit_idle(port)
                return await self._collect_status(port, query, source, peer, timeout)
        self._check_no_edit(port)
        return await self._collect_status(port, query, source, peer, timeout)

    async def _collect_status(
        self,
        port: int,
        query: NativeStatusQuery,
        source: NativeAddress,
        peer: NativeAddress | None,
        timeout: float,
    ) -> NativeStatusBatch:
        collector = StatusCollector(query, peer)
        request = NativeMessage.request_status(query, source=source)
        result, lost = await self._exchange(port, request, collector.observe, timeout)
        if lost:
            return StatusCollector(query, peer).batch(port, result, capture_lost=True)
        return collector.batch(port, result)

    async def _exchange(
        self,
        port: int,
        request: NativeMessage,
        collect: Callable[[NativeFrame], bool],
        timeout: float | None,
    ) -> tuple[TxResult | None, bool]:
        self._check_active()
        self._check_no_read(port)
        waiter = _ReportWaiter(collect, asyncio.get_running_loop().create_future())
        self._waiters[port] = waiter
        result = None
        deadline = asyncio.timeout(timeout)
        try:
            async with deadline:
                result = await self._send(port, request)
                if result.completed_frame:
                    await waiter.done
        except TimeoutError:
            if not deadline.expired():
                raise
        finally:
            del self._waiters[port]
            if not waiter.done.done():
                waiter.done.cancel()
            elif not waiter.done.cancelled():
                waiter.done.exception()
        return result, waiter.capture_lost

    def feed(self, packet: Packet) -> None:
        record = decode_raw_packet(packet, self._capture_index + 1)
        if isinstance(record, TxResult):
            future = self._pending.pop(record.port, None)
            if future is None:
                msg = f"unexpected transmit result on port {record.port}"
                raise ProtocolError(msg)
            if not future.done():
                # Arm at wire dispatch, not when the sending coroutine resumes:
                # a single serial read can contain both the result and a reply.
                if (waiter := self._waiters.get(record.port)) is not None:
                    waiter.transmitted = record.completed_frame
                future.set_result(record)
            self._tx_changed[record.port].set()
        else:
            if isinstance(record, NativeFrame):
                if record.rx_flags is None:
                    msg = "extended Raw capture is missing RX flags"
                    raise ProtocolError(msg)
                self._capture_index = record.capture_index
                if record.usable_for_state and (
                    waiter := self._waiters.get(record.port)
                ):
                    waiter.observe(record)
            elif isinstance(record, GatewayTraceGap):
                for port, waiter in self._waiters.items():
                    if record.affects(port):
                        waiter.gap()
            if len(self._records) == self._records.maxlen:
                self._lost += 1
            self._records.append(record)
            self._ready.set()

    async def read(self) -> _Record | CaptureGap:
        """Get the next capture or diagnostic, independently of send completions."""
        while True:
            if not self._active:
                self._check_active()
            if self._lost:
                gap = CaptureGap(self._lost)
                self._lost = 0
                return gap
            if self._records:
                return self._records.popleft()
            self._check_active()
            self._ready.clear()
            await self._ready.wait()

    def _fail_reads(self, error: Exception) -> None:
        for waiter in self._waiters.values():
            if not waiter.done.done():
                waiter.done.set_exception(error)

    def fail(self, error: Exception) -> None:
        if self._error is not None:
            return
        self._error = error
        for event in self._tx_changed.values():
            event.set()
        for future in self._pending.values():
            if not future.done():
                future.set_exception(self._error)
        self._fail_reads(error)
        self._ready.set()

    @contextmanager
    def active(self, management: BridgeManagement | None = None) -> Iterator[None]:
        self._management = management
        self._active = True
        try:
            yield
            if self._error is not None:
                raise self._error
        finally:
            self._active = False
            self._management = None
            for event in self._tx_changed.values():
                event.set()
            self._ready.set()
            self._fail_reads(ConnectionClosedError("raw session closed"))

    @asynccontextmanager
    async def receiving(
        self,
        receive: Callable[[], Awaitable[Packet]],
    ) -> AsyncIterator[None]:
        async def pump() -> None:
            try:
                while True:
                    self.feed(await receive())
            except Exception as err:  # noqa: BLE001
                self.fail(err)

        with self.active():
            reader = asyncio.create_task(pump(), name="tekmar 482 raw reader")
            try:
                yield
            finally:
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)

    def check_drained(self) -> None:
        if self._pending:
            msg = "gateway left Raw mode without a terminal transmit result"
            error = ProtocolError(msg)
            self.fail(error)
            raise error
