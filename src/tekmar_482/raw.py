"""Raw-mode tN4 captures and transmit completions."""

from __future__ import annotations

import asyncio
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import IntEnum
from typing import TYPE_CHECKING, Protocol

from ._native_payloads.writes import BlockPatch
from .constants import TN4_PORTS
from .exceptions import ConnectionClosedError, ProtocolError, TekmarError
from .native import NativeFrame, NativeMessage
from .native_write import write_verified_block
from .packet import TYPE_TN4

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Mapping

    from ._native_payloads.writes import NativeField
    from .native import NativeAddress, NativeBlockQuery
    from .native_write import NativeWriteResult
    from .packet import Packet

_HEADER = 4
_RAW_HEADER = 2
_RX_HEADER = 6
_INVALID_HEADER = 9


class TxStatus(IntEnum):
    ACK = 0
    REJECTED = 1
    FAILED = 2


class RejectReason(IntEnum):
    INVALID_BODY = 1
    PIN_UNAVAILABLE = 2
    WRONG_MODE = 3
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


class InvalidReason(IntEnum):
    STUFFING = 1
    CRC = 2
    CRC_DELIMITER = 3
    BAD_END = 4


class _Service(IntEnum):
    RX = 0
    SEND = 1
    TX_RESULT = 2
    PORT_ERROR = 3
    RX_INVALID = 4


def _reason[E: IntEnum](enum: type[E], value: int) -> E | int:
    return enum(value) if value in enum else value


@dataclass(frozen=True, slots=True)
class TxResult:
    """Terminal outcome on one link, not confirmation by the application."""

    port: int
    status: TxStatus
    reason: RejectReason | FailureReason | int | None = None

    @property
    def may_have_reached_peer(self) -> bool:
        return self.status == TxStatus.ACK or (
            self.status == TxStatus.FAILED and self.reason == FailureReason.NO_ACK
        )


@dataclass(frozen=True, slots=True)
class NativeBlockBatch:
    """One request and the distinct peers that reported before its deadline."""

    port: int
    query: NativeBlockQuery
    tx_result: TxResult | None
    reports: tuple[NativeFrame, ...]

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
        if self.tx_result is not None and not self.tx_result.may_have_reached_peer:
            raise NativeTransmitError(self.tx_result)
        if self.tx_result is None or not self.reports:
            msg = (
                "no transmit completion or matching native block report before deadline"
            )
            raise TimeoutError(msg)
        return self.reports[0]


@dataclass(slots=True)
class _BlockWaiter:
    query: NativeBlockQuery
    done: asyncio.Future[None]
    frames: dict[int, NativeFrame] = field(default_factory=dict)

    def observe(self, frame: NativeFrame) -> None:
        if not self.done.done() and self.query.matches(frame.message):
            self.frames.setdefault(frame.node & 31, frame)
            if len(self.frames) == self.query.target.request_mask().bit_count():
                self.done.set_result(None)


class NativeTransmitError(TekmarError):
    """The gateway rejected or failed an application request on the tN4 link."""

    def __init__(self, result: TxResult) -> None:
        self.result = result
        super().__init__(
            f"port {result.port}: {result.status.name}, reason {result.reason!r}",
        )


@dataclass(frozen=True, slots=True)
class PortError:
    port: int
    reason: PortErrorReason | int


@dataclass(frozen=True, slots=True)
class PortDiagnostic:
    port: int
    valid_dropped: int
    invalid_dropped: int


@dataclass(frozen=True, slots=True)
class InvalidCapture:
    port: int
    timestamp_ms: int
    reason: InvalidReason | int
    bit_count: int
    bits: bytes


@dataclass(frozen=True, slots=True)
class CaptureGap:
    """Number of oldest records lost from the host buffer before the next record."""

    dropped: int


type _Record = NativeFrame | InvalidCapture | PortError
type CaptureRecord = _Record | CaptureGap


def _valid_body(body: bytes) -> bool:
    return len(body) >= _HEADER and body[3] & 63 == len(body) - _HEADER


def _decode(packet: Packet) -> _Record | TxResult:
    data = packet.data
    if packet.type == TYPE_TN4 and len(data) >= _RAW_HEADER:
        kind, port = data[:2]
        if kind == _Service.PORT_ERROR and len(data) == _RAW_HEADER + 1:
            # InvalidPort echoes even a port number outside the physical range.
            return PortError(port, _reason(PortErrorReason, data[2]))
        if port in TN4_PORTS:
            if kind == _Service.RX and _valid_body(data[_RX_HEADER:]):
                return NativeFrame(port, int.from_bytes(data[2:6], "little"), data[6:])
            if kind == _Service.TX_RESULT:
                match tuple(data[2:]):
                    case (TxStatus.ACK,):
                        return TxResult(port, TxStatus.ACK)
                    case (TxStatus.REJECTED, why):
                        return TxResult(
                            port,
                            TxStatus.REJECTED,
                            _reason(RejectReason, why),
                        )
                    case (TxStatus.FAILED, why):
                        return TxResult(
                            port,
                            TxStatus.FAILED,
                            _reason(FailureReason, why),
                        )
            if kind == _Service.RX_INVALID and len(data) >= _INVALID_HEADER:
                count = int.from_bytes(data[7:9], "little")
                bits = data[9:]
                if (count + 7) // 8 == len(bits) and (
                    not count % 8 or not bits[-1] & ((1 << (8 - count % 8)) - 1)
                ):
                    return InvalidCapture(
                        port,
                        int.from_bytes(data[2:6], "little"),
                        _reason(InvalidReason, data[6]),
                        count,
                        bits,
                    )
    msg = "unexpected raw packet type, service, port or payload length"
    raise ProtocolError(msg)


class BridgeStream(Protocol):
    """Capture and send interface borrowed from an active Raw session."""

    @property
    def diagnostics(self) -> Mapping[int, PortDiagnostic]: ...
    async def send(self, port: int, body: bytes | NativeMessage) -> TxResult: ...
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
    ) -> NativeBlockBatch: ...
    async def write_block(
        self,
        port: int,
        query: NativeBlockQuery,
        changes: Mapping[NativeField, int],
        *,
        source: NativeAddress,
        timeout: float = 20,
        preview: bool = False,
    ) -> NativeWriteResult: ...
    async def read(self) -> CaptureRecord: ...


class RawDispatcher:
    """One reader owns the wire. Each send owns exactly one port until TxResult."""

    def __init__(self, write: Callable[[bytes], Awaitable[None]], backlog: int) -> None:
        if backlog < 1:
            msg = "capture backlog must be positive"
            raise ValueError(msg)
        self._write = write
        self.diagnostics: dict[int, PortDiagnostic] = {}
        self._pending: dict[int, asyncio.Future[TxResult]] = {}
        self._blocks: dict[int, _BlockWaiter] = {}
        self._edits = {port: asyncio.Lock() for port in TN4_PORTS}
        self._records: deque[_Record] = deque(maxlen=backlog)
        self._lost = 0
        self._ready = asyncio.Event()
        self._active = False
        self._error: Exception | None = None

    def _check_active(self) -> None:
        if self._error is not None:
            raise self._error
        if not self._active:
            msg = "raw session closed"
            raise ConnectionClosedError(msg)

    async def send(self, port: int, body: bytes | NativeMessage) -> TxResult:
        """Send once and await this port's link result; cancellation cannot undo TX."""
        self._check_no_edit(port)
        return await self._send(port, body)

    def _check_no_edit(self, port: int) -> None:
        if port in self._edits and self._edits[port].locked():
            msg = f"block write in progress on port {port}"
            raise ProtocolError(msg)

    async def _send(self, port: int, body: bytes | NativeMessage) -> TxResult:
        self._check_active()
        if isinstance(body, NativeMessage):
            body = body.to_bytes()
        if port not in TN4_PORTS or not _valid_body(body):
            msg = "expected a physical port and complete native frame"
            raise ValueError(msg)
        if port in self._pending:
            msg = f"send already in progress on port {port}"
            raise ProtocolError(msg)
        future: asyncio.Future[TxResult] = asyncio.get_running_loop().create_future()
        self._pending[port] = future
        try:
            await self._write(bytes((_Service.SEND, port)) + body)
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
    ) -> NativeBlockBatch:
        """Collect reports in one deadline including TX. A missing node is unknown."""
        if not timeout > 0:
            msg = "block response timeout must be positive"
            raise ValueError(msg)
        self._check_no_edit(port)
        return await self._collect_blocks(port, query, source, timeout)

    async def write_block(
        self,
        port: int,
        query: NativeBlockQuery,
        changes: Mapping[NativeField, int],
        *,
        source: NativeAddress,
        timeout: float = 20,
        preview: bool = False,
    ) -> NativeWriteResult:
        """Read, optionally update once, then verify. The timeout is per exchange."""
        if port not in TN4_PORTS or query.target.request_mask().bit_count() != 1:
            msg = "expected a single destination on a physical port"
            raise ValueError(msg)
        if not timeout > 0:
            msg = "block write timeout must be positive"
            raise ValueError(msg)
        patch = BlockPatch.from_fields(query.block_id, changes)

        async def read() -> NativeBlockBatch:
            return await self._collect_blocks(port, query, source, timeout)

        async def send(message: NativeMessage) -> TxResult:
            return await self._send(port, message)

        async with self._edits[port]:
            self._check_active()
            if port in self._pending or port in self._blocks:
                msg = f"native operation already in progress on port {port}"
                raise ProtocolError(msg)
            return await write_verified_block(
                patch,
                source=source,
                read=read,
                send=send,
                timeout=timeout,
                preview=preview,
            )

    async def _collect_blocks(
        self,
        port: int,
        query: NativeBlockQuery,
        source: NativeAddress,
        timeout: float | None,
    ) -> NativeBlockBatch:
        self._check_active()
        if port in self._blocks:
            msg = f"block read already in progress on port {port}"
            raise ProtocolError(msg)
        waiter = _BlockWaiter(query, asyncio.get_running_loop().create_future())
        self._blocks[port] = waiter
        result = None
        deadline = asyncio.timeout(timeout)
        try:
            async with deadline:
                result = await self._send(port, query.request(source))
                if result.may_have_reached_peer:
                    await waiter.done
        except TimeoutError:
            if not deadline.expired():
                raise
        finally:
            del self._blocks[port]
            if not waiter.done.done():
                waiter.done.cancel()
            elif not waiter.done.cancelled():
                waiter.done.exception()
        return NativeBlockBatch(
            port,
            query,
            result,
            tuple(waiter.frames[n] for n in sorted(waiter.frames)),
        )

    def feed(self, packet: Packet) -> None:
        record = _decode(packet)
        if isinstance(record, TxResult):
            future = self._pending.pop(record.port, None)
            if future is None:
                msg = f"unexpected transmit result on port {record.port}"
                raise ProtocolError(msg)
            if not future.done():
                future.set_result(record)
        else:
            if isinstance(record, NativeFrame) and (
                waiter := self._blocks.get(record.port)
            ):
                waiter.observe(record)
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

    def _fail_blocks(self, error: Exception) -> None:
        for waiter in self._blocks.values():
            if not waiter.done.done():
                waiter.done.set_exception(error)

    def fail(self, error: Exception) -> None:
        if self._error is not None:
            return
        self._error = error
        for future in self._pending.values():
            if not future.done():
                future.set_exception(self._error)
        self._fail_blocks(error)
        self._ready.set()

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

        self._active = True
        reader = asyncio.create_task(pump(), name="tekmar 482 raw reader")
        try:
            yield
            if self._error is not None:
                raise self._error
        finally:
            self._active = False
            self._ready.set()
            self._fail_blocks(ConnectionClosedError("raw session closed"))
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)

    def check_drained(self) -> None:
        if self._pending:
            msg = "gateway left Raw mode without a terminal transmit result"
            error = ProtocolError(msg)
            self.fail(error)
            raise error
