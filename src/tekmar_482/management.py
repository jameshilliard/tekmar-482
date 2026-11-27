"""Negotiated firmware management over the ordinary vendor tRPC session."""

from __future__ import annotations

import asyncio
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import IntEnum, IntFlag
from math import isfinite
from typing import TYPE_CHECKING, Self

from ._bridge_diagnostics import ERROR_REPORTING_METHOD, TRACE_STATE, WIRE_VERSION
from ._native_payloads.writes import BlockPatch
from ._normal_block import edit_key, edit_result, read_key, read_result
from ._normal_status import status_key, status_result
from .constants import TN4_PORTS
from .error_reports import ALL_SOURCES, ErrorReports, GatewayErrorCounter, source_mask
from .exceptions import ConnectionClosedError, ProtocolError, TekmarError
from .native import NativeAddress
from .native_schedule import edit_schedule_time
from .native_status import StatusCollector, snowmelt_status_query
from .native_target_edit import edit_thermostat_target
from .native_write import NativeBlockEditor
from .normal_trace import NormalTrace, NormalTraceState
from .raw import NativePortBusyError
from .traffic import TrafficCounters
from .trpc import ResponseMatch, TrpcCommand, TrpcMethod, TrpcPacket, TrpcService

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
    from struct import Struct

    from ._bridge_diagnostics import ErrorReason, ErrorSource
    from ._native_payloads.climate import ThermostatMode
    from ._native_payloads.queries import NativeStatusQuery
    from ._native_payloads.writes import NativeField
    from .client import Tekmar482Client
    from .native import NativeBlockQuery, NativeFrame, NativeMessage
    from .native_schedule import ScheduleChange, ScheduleTarget
    from .native_state import NativePath
    from .native_status import NativeStatusBatch
    from .native_target_edit import ThermostatTargetChange
    from .native_write import BlockDelivery, NativeWriteResult
    from .raw import NativeBlockBatch

_CAPABILITY_BYTES = 4


def unpack_diagnostic(layout: Struct, data: bytes) -> tuple[int, ...]:
    if len(data) != layout.size:
        msg = (
            f"invalid diagnostic length: expected {layout.size} bytes, got {len(data)}"
        )
        raise ProtocolError(msg)
    return layout.unpack(data)


class BridgeMode(IntEnum):
    NORMAL = 0
    RAW = 1


class BridgeFeature(IntFlag):
    RAW = 1
    ROM_UPDATE = 2
    NORMAL_TRACE = 4
    ERROR_REPORTING = 8
    NORMAL_IDENTITY = 16
    TRAFFIC_COUNTS = 32
    NORMAL_STATUS_READ = 128
    BLOCK_TRANSFERS = 256


class BridgeMethod(IntEnum):
    CAPABILITIES = 0x544BF001
    MODE = 0x544BF002
    STATS = 0x544BF003
    BUILD = 0x544BF004
    ENTER_UPDATE = 0x544BF005
    ERROR = 0x544BF006
    PANEL = 0x544BF007
    LED_TEST = 0x544BF008
    ACK_MASK = 0x544BF009
    COUNTERS = 0x544BF00A
    NORMAL_TRACE = 0x544BF00B
    ERROR_REPORTING = ERROR_REPORTING_METHOD
    NORMAL_IDENTITY = 0x544BF00D
    TRAFFIC_COUNTS = 0x544BF00E
    NORMAL_BLOCK_READ = 0x544BF00F
    NORMAL_STATUS_READ = 0x544BF010
    NORMAL_BLOCK_EDIT = 0x544BF011


@dataclass(frozen=True, slots=True)
class BridgeCapabilities:
    wire_version: int
    mode: BridgeMode
    ports: int
    features: BridgeFeature

    @classmethod
    def from_payload(cls, data: bytes) -> Self:
        if len(data) >= _CAPABILITY_BYTES:
            version, mode, ports = data[:3]
            flags = int.from_bytes(data[3:], "little")
            if mode in BridgeMode and ports == len(TN4_PORTS):
                if version != WIRE_VERSION:
                    msg = (
                        f"unsupported custom bridge wire version {version}; "
                        f"expected {WIRE_VERSION}"
                    )
                    raise ProtocolError(msg)
                return cls(version, BridgeMode(mode), ports, BridgeFeature(flags))
        msg = f"invalid binary bridge capabilities: {data.hex()}"
        raise ProtocolError(msg)


@dataclass(frozen=True, slots=True)
class _ExtensionCommand(TrpcCommand):
    """Opaque custom method body; vendor field shapes are unaffected."""

    payload: bytes = b""

    def to_message(self) -> TrpcPacket:
        return TrpcPacket(self.service_id, self.method_id, extra=self.payload)


class _ManagementMatch(ResponseMatch):
    def matches(self, message: TrpcPacket) -> bool:
        if self.service_ids is None or message.service_id not in self.service_ids:
            return False
        if message.method_id == TrpcMethod.NULL_METHOD:
            return not message.extra
        if message.method_id == BridgeMethod.ERROR:
            return message.extra[:4] == self.method_id.to_bytes(4, "little")
        return super().matches(message)


class _Management:
    """Use with an open Tekmar482Client. Does not open or replace its transport."""

    def __init__(self, client: Tekmar482Client) -> None:
        self._session = client.session

    async def _exchange(self, command: TrpcCommand) -> TrpcPacket:
        exchange = (
            self._session.update_response
            if command.service == TrpcService.UPDATE
            else self._session.request_response
        )
        packet = await exchange(
            command,
            response_match=_ManagementMatch.for_command(command),
        )
        if packet is None:
            msg = f"no reply to management method {int(command.method):#x}"
            raise TimeoutError(msg)
        if packet.method_id == BridgeMethod.ERROR:
            reason = packet.extra[4:]
            if reason == b"\x01":
                msg = "gateway is busy; management operation was not accepted"
            elif reason == b"\x02":
                msg = "gateway rejected the management service or payload"
            elif reason == b"\x03":
                msg = "management operation is unavailable in the current gateway mode"
            else:
                msg = f"unexpected management error: {reason.hex()}"
            raise ProtocolError(msg)
        return packet

    async def capabilities(self) -> BridgeCapabilities | None:
        """Return None only for a stock-style NullMethod, never on timeout."""
        reply = await self._exchange(TrpcCommand.request(BridgeMethod.CAPABILITIES))
        if reply.method_id == TrpcMethod.NULL_METHOD:
            return None
        return BridgeCapabilities.from_payload(reply.extra)


class ErrorReporting(_Management):
    """Custom gateway diagnostics available in either application mode."""

    async def _require(self) -> None:
        caps = await self.capabilities()
        if caps is None or not caps.features & BridgeFeature.ERROR_REPORTING:
            msg = "gateway does not advertise error reporting"
            raise ProtocolError(msg)

    async def _command(self, service: TrpcService, data: bytes = b"") -> bytes:
        reply = await self._exchange(
            _ExtensionCommand(service, BridgeMethod.ERROR_REPORTING, payload=data),
        )
        if reply.method_id != BridgeMethod.ERROR_REPORTING:
            msg = "error reporting method was not acknowledged"
            raise ProtocolError(msg)
        return reply.extra

    async def _mask(self) -> int:
        data = await self._command(TrpcService.REQUEST)
        if len(data) != 1 or data[0] >> len(ALL_SOURCES):
            msg = "invalid error reporting mask"
            raise ProtocolError(msg)
        return data[0]

    async def sources(self) -> frozenset[ErrorSource]:
        """Read the current selection without enabling reports."""
        with self._session.connection_scope():
            await self._require()
            mask = await self._mask()
        return frozenset(source for source in ALL_SOURCES if mask & (1 << source))

    async def counter(
        self,
        source: ErrorSource,
        reason: ErrorReason,
    ) -> GatewayErrorCounter:
        if not reason.supports(source):
            msg = f"{reason.name} does not apply to {source.name}"
            raise ValueError(msg)
        with self._session.connection_scope():
            await self._require()
            body = await self._command(TrpcService.REQUEST, bytes([source, reason]))
        value = GatewayErrorCounter.decode(body)
        if (value.source, value.reason) != (source, reason):
            msg = "gateway returned a different error counter"
            raise ProtocolError(msg)
        return value

    async def _set_mask(self, mask: int) -> None:
        value = bytes([mask])
        if await self._command(TrpcService.UPDATE, value) != value:
            msg = "error reporting mask was not acknowledged"
            raise ProtocolError(msg)

    async def set_sources(self, sources: Iterable[ErrorSource]) -> None:
        """Replace the selection; use an empty iterable to stop an orphaned stream."""
        mask = source_mask(sources)
        with (
            self._session.connection_scope(),
            self._session.error_packets(ErrorReports(mask)),
        ):
            await self._require()
            await self._set_mask(mask)

    @asynccontextmanager
    async def watch(
        self,
        sources: Iterable[ErrorSource] = ALL_SOURCES,
    ) -> AsyncIterator[ErrorReports]:
        """Watch changed totals. Restore the previous selection on this connection."""
        mask = source_mask(sources)
        if not mask:
            msg = "select at least one error source"
            raise ValueError(msg)
        reports = ErrorReports(mask)
        with self._session.connection_scope(), self._session.error_packets(reports):
            generation = self._session.connection_generation
            await self._require()
            previous = await self._mask()

            async def restore() -> None:
                if (
                    self._session.connection_generation == generation
                    and self._session.is_open
                    and self._session.reader_error is None
                ):
                    await self._set_mask(previous)

            try:
                await self._set_mask(mask)
                yield reports
            except BaseException as err:
                try:
                    await restore()
                except (OSError, TimeoutError, TekmarError) as cleanup:
                    err.add_note(f"Could not restore error reporting: {cleanup!r}")
                raise
            else:
                await restore()


class BridgeManagement(_Management):
    """Management extensions on an open session; none implicitly switch modes."""

    def __init__(self, client: Tekmar482Client) -> None:
        super().__init__(client)
        self.error_reporting = ErrorReporting(client)
        self.traffic = TrafficCounters(self._traffic_request)

    async def _traffic_request(self, body: bytes) -> bytes:
        reply = await self._exchange(
            _ExtensionCommand(
                TrpcService.REQUEST,
                BridgeMethod.TRAFFIC_COUNTS,
                payload=body,
            ),
        )
        if reply.method_id != BridgeMethod.TRAFFIC_COUNTS:
            msg = "gateway does not implement traffic counters"
            raise ProtocolError(msg)
        return reply.extra

    async def build_revision(self) -> str:
        reply = await self._exchange(TrpcCommand.request(BridgeMethod.BUILD))
        if not re.fullmatch(rb"(?:[0-9a-f]{40}|unversioned)", reply.extra):
            msg = "missing or invalid custom firmware revision"
            raise ProtocolError(msg)
        return reply.extra.decode("ascii")

    async def trace_state(self) -> NormalTraceState:
        """Read the active port mask and lifetime drop counts without enabling trace."""
        with self._session.connection_scope():
            await self._require_trace()
            return await self._trace_state()

    async def request_identity(self, path: NativePath) -> None:
        """Queue a block 00 read. Only the native peer's report identifies it."""
        body = bytes((path.port, path.subnet, path.address.block_node))
        reply = await self._exchange(
            _ExtensionCommand(
                TrpcService.REQUEST,
                BridgeMethod.NORMAL_IDENTITY,
                payload=body,
            ),
        )
        if reply.method_id != BridgeMethod.NORMAL_IDENTITY or reply.extra != body:
            msg = "gateway did not accept the targeted identity read"
            raise ProtocolError(msg)

    async def read_block(
        self,
        port: int,
        query: NativeBlockQuery,
        *,
        timeout: float = 15,
    ) -> NativeFrame:
        """Read one peer in Normal mode without relying on trace or cached replies."""
        return (
            await self.read_block_result(port, query, timeout=timeout)
        ).require_report()

    def _check_edit_idle(self, port: int) -> None:
        if port in TN4_PORTS and self._session.normal_edit_locks[port].locked():
            msg = f"block edit in progress on port {port}"
            raise NativePortBusyError(msg)

    async def read_block_result(
        self,
        port: int,
        query: NativeBlockQuery,
        *,
        timeout: float = 15,
    ) -> NativeBlockBatch:
        """Return observed link and peer evidence, including an unanswered request."""
        self._check_edit_idle(port)
        return await self._read_block(port, query, timeout)

    async def _read_block(
        self,
        port: int,
        query: NativeBlockQuery,
        timeout: float,
    ) -> NativeBlockBatch:
        async with self._read_events(
            BridgeMethod.NORMAL_BLOCK_READ,
            BridgeFeature.BLOCK_TRANSFERS,
            read_key(port, query),
            timeout,
        ) as receive:
            return read_result(port, query, await receive())

    @asynccontextmanager
    async def _block_editor(
        self,
        port: int,
        timeout: float,
    ) -> AsyncIterator[NativeBlockEditor]:
        if port not in TN4_PORTS or not isfinite(timeout) or timeout <= 0:
            msg = "expected a physical port and a finite positive edit timeout"
            raise ValueError(msg)

        async def update(
            query: NativeBlockQuery,
            before: NativeFrame,
            proposed: NativeMessage,
        ) -> BlockDelivery:
            async with self._read_events(
                BridgeMethod.NORMAL_BLOCK_EDIT,
                BridgeFeature.BLOCK_TRANSFERS,
                edit_key(port, query, before, proposed),
                timeout,
                service=TrpcService.UPDATE,
            ) as receive:
                return edit_result(port, await receive())

        with self._session.connection_scope():
            async with self._session.normal_edit_locks[port]:
                yield NativeBlockEditor(
                    NativeAddress(0, 0),
                    lambda query: self._read_block(port, query, timeout),
                    update,
                    timeout,
                )

    async def write_block(
        self,
        port: int,
        query: NativeBlockQuery,
        changes: Mapping[NativeField, int],
        *,
        timeout: float = 15,
        preview: bool = True,
    ) -> NativeWriteResult:
        """Preview or conditionally edit known fields, then read back from the peer."""
        patch = BlockPatch.from_fields(query.block_id, changes)
        async with self._block_editor(port, timeout) as editor:
            return await editor.write(query, patch, preview=preview)

    async def write_thermostat_mode(
        self,
        port: int,
        target: NativeAddress,
        mode: ThermostatMode,
        *,
        timeout: float = 15,
        preview: bool = True,
    ) -> NativeWriteResult:
        async with self._block_editor(port, timeout) as editor:
            return await editor.write_mode(target, mode, preview=preview)

    async def write_thermostat_target(
        self,
        port: int,
        target: NativeAddress,
        change: ThermostatTargetChange,
        *,
        timeout: float = 15,
        preview: bool = True,
    ) -> NativeWriteResult:
        async with self._block_editor(port, timeout) as editor:
            return await edit_thermostat_target(editor, target, change, preview=preview)

    async def write_schedule_time(
        self,
        port: int,
        target: ScheduleTarget,
        change: ScheduleChange,
        *,
        timeout: float = 15,
        preview: bool = True,
    ) -> NativeWriteResult:
        async with self._block_editor(port, timeout) as editor:
            return await edit_schedule_time(editor, target, change, preview=preview)

    async def read_status(
        self,
        port: int,
        query: NativeStatusQuery,
        *,
        peer: NativeAddress | None = None,
        timeout: float = 15,
    ) -> NativeStatusBatch:
        """Read peer status without changing mode, trace or link-ACK settings."""
        self._check_edit_idle(port)
        collector = StatusCollector(query, peer)
        async with self._read_events(
            BridgeMethod.NORMAL_STATUS_READ,
            BridgeFeature.NORMAL_STATUS_READ,
            status_key(port, query, peer),
            timeout,
        ) as receive:
            while True:
                batch = status_result(port, collector, await receive())
                if batch is not None:
                    return batch

    async def read_snowmelt_status(
        self,
        port: int,
        peer: NativeAddress,
        *,
        keys: bytes | None = None,
        timeout: float = 15,
    ) -> NativeStatusBatch:
        query = await snowmelt_status_query(
            peer,
            lambda block: self.read_block(port, block, timeout=timeout),
            keys,
        )
        return await self.read_status(port, query, peer=peer, timeout=timeout)

    @asynccontextmanager
    async def _read_events(
        self,
        method: BridgeMethod,
        feature: BridgeFeature,
        key: bytes,
        timeout: float,
        *,
        service: TrpcService = TrpcService.REQUEST,
    ) -> AsyncIterator[Callable[[], Awaitable[bytes]]]:
        if not isfinite(timeout) or timeout <= 0:
            msg = "Normal read timeout must be finite and positive"
            raise ValueError(msg)
        command = _ExtensionCommand(service, method, payload=key)
        session = self._session
        with session.connection_scope():
            async with session.reports(
                include_matched=frozenset({command.method_id}),
                only_method=command.method_id,
            ) as events:
                # Drain abandoned admission replies, not outstanding native reads.
                async with session.normal_read_admission:
                    caps = await self.capabilities()
                    if (
                        caps is None
                        or caps.mode != BridgeMode.NORMAL
                        or not caps.features & feature
                    ):
                        msg = f"gateway does not support {method.name} in Normal mode"
                        raise ProtocolError(msg)
                    ack = await self._exchange(command)
                    if ack.method_id != command.method_id or ack.extra != key:
                        msg = "gateway did not acknowledge this Normal read"
                        raise ProtocolError(msg)

                async def receive() -> bytes:
                    async for packet in events:
                        if (
                            packet.service is TrpcService.REPORT
                            and packet.extra[:1] == key[:1]
                        ):
                            return packet.extra
                    msg = "connection closed before the Normal read result"
                    raise ConnectionClosedError(msg)

                async with asyncio.timeout(timeout):
                    async for packet in events:
                        if packet is ack:
                            yield receive
                            return
        msg = "connection closed before Normal read admission"
        raise ConnectionClosedError(msg)

    async def _trace_state(self) -> NormalTraceState:
        reply = await self._exchange(TrpcCommand.request(BridgeMethod.NORMAL_TRACE))
        if reply.method_id != BridgeMethod.NORMAL_TRACE:
            msg = "invalid Normal trace state"
            raise ProtocolError(msg)
        mask, *counts = unpack_diagnostic(TRACE_STATE, reply.extra)
        if mask >= 1 << len(TN4_PORTS):
            msg = "invalid Normal trace port mask"
            raise ProtocolError(msg)
        return NormalTraceState(mask, tuple(counts))

    async def _require_trace(self) -> None:
        caps = await self.capabilities()
        if (
            caps is None
            or caps.mode != BridgeMode.NORMAL
            or not caps.features & BridgeFeature.NORMAL_TRACE
        ):
            msg = "Normal trace requires capable custom firmware in Normal mode"
            raise ProtocolError(msg)

    async def stop_trace(self) -> NormalTraceState:
        """Disable an orphaned trace after a debugger disconnected."""
        with self._session.connection_scope():
            await self._require_trace()
            await self._set_trace_mask(0)
            return await self._trace_state()

    async def _set_trace_mask(self, mask: int) -> None:
        data = bytes([mask])
        reply = await self._exchange(
            _ExtensionCommand(
                TrpcService.UPDATE,
                BridgeMethod.NORMAL_TRACE,
                payload=data,
            ),
        )
        if reply.method_id != BridgeMethod.NORMAL_TRACE or reply.extra != data:
            msg = "Normal trace mask was not acknowledged"
            raise ProtocolError(msg)

    @asynccontextmanager
    async def trace(
        self,
        ports: Iterable[int] = TN4_PORTS,
        *,
        backlog: int = 512,
    ) -> AsyncIterator[NormalTrace]:
        """Trace an open Normal session without changing its operating mode."""
        mask = 0
        for port in ports:
            if port not in TN4_PORTS:
                msg = f"invalid trace port {port}"
                raise ValueError(msg)
            mask |= 1 << port
        if not mask:
            msg = "select at least one trace port"
            raise ValueError(msg)
        trace = NormalTrace(backlog)
        with self._session.connection_scope(), self._session.trace_packets(trace):
            await self._require_trace()
            before = await self._trace_state()

            async def restore() -> None:
                if self._session.is_open and self._session.reader_error is None:
                    await self._set_trace_mask(before.port_mask)
                    after = await self._trace_state()
                    deltas = tuple(
                        b - a
                        for a, b in zip(
                            before.gateway_drops,
                            after.gateway_drops,
                            strict=True,
                        )
                    )
                    if any(d < 0 for d in deltas):
                        msg = "lifetime trace drop count decreased during capture"
                        raise ProtocolError(msg)
                    trace.gateway_drops = deltas

            try:
                await self._set_trace_mask(mask)
                yield trace
            except BaseException as err:
                try:
                    await restore()
                except (OSError, TimeoutError, TekmarError) as cleanup:
                    err.add_note(f"Could not restore Normal trace: {cleanup!r}")
                raise
            else:
                await restore()

    async def enter_update_mode(self) -> None:
        """Request ROM ISP once. The caller must still verify the ROM part ID."""
        caps = await self.capabilities()
        if caps is None or not caps.features & BridgeFeature.ROM_UPDATE:
            msg = "gateway did not advertise the firmware update extension"
            raise ProtocolError(msg)
        reply = await self._exchange(TrpcCommand.update(BridgeMethod.ENTER_UPDATE))
        if reply.method_id != BridgeMethod.ENTER_UPDATE or reply.extra:
            msg = "gateway did not acknowledge ROM handoff"
            raise ProtocolError(msg)
