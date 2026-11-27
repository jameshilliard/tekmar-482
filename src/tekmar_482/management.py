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
from ._native_payloads.line import (
    CLOCK_REPORT,
    GROUP_UPDATE,
    NETWORK_INIT_REQUEST,
    ZONE_ADJUSTMENT,
)
from ._native_payloads.scenes import SCENE_REPORT
from ._native_payloads.writes import BlockPatch
from ._normal_block import (
    BlockCollector,
    block_edit_terminal,
    block_read_terminal,
    edit_key,
    edit_result,
    tx_outcome,
)
from ._normal_status import status_key, status_read_terminal, status_result
from .constants import DEFAULT_BLOCK_TIMEOUT, TN4_PORTS
from .error_reports import ALL_SOURCES, ErrorReports, GatewayErrorCounter, source_mask
from .exceptions import ConnectionClosedError, ProtocolError, TekmarError
from .native import NativeAddress, NativeTargets
from .native_schedule import edit_schedule_time
from .native_snow_control import apply_snowmelt_group, edit_snowmelt_setpoint
from .native_status import StatusCollector, snowmelt_status_query
from .native_target_edit import edit_thermostat_target
from .native_write import NativeBlockEditor
from .normal_trace import (
    NormalNativeSession,
    NormalPeerCapture,
    NormalTrace,
    NormalTraceState,
)
from .raw import NativePortBusyError
from .traffic import TrafficCounters
from .trpc import ResponseMatch, TrpcCommand, TrpcMethod, TrpcPacket, TrpcService

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
    from struct import Struct

    from ._bridge_diagnostics import ErrorReason, ErrorSource
    from ._native_payloads.climate import ThermostatMode
    from ._native_payloads.layouts import SnowmeltBlock
    from ._native_payloads.queries import NativeStatusQuery
    from ._native_payloads.snowmelt import SnowmeltApplication
    from ._native_payloads.writes import NativeField
    from .client import Tekmar482Client
    from .native import NativeBlockQuery, NativeFrame, NativeMessage
    from .native_schedule import ScheduleChange, ScheduleTarget
    from .native_snow_control import SnowmeltGroupResult, SnowmeltOperation
    from .native_state import NativePath
    from .native_status import NativeStatusBatch
    from .native_target_edit import ThermostatTargetChange
    from .native_write import BlockDelivery, NativeWriteResult
    from .packet import Packet
    from .raw import NativeBlockBatch, TxResult

_CAPABILITY_BYTES = 4
_LINK_RESULT_BYTES = 2  # physical port and link outcome
_BUSY = b"\x01"
_GATEWAY_SOURCE = NativeAddress(0, 0)


def _check_source(source: NativeAddress) -> None:
    if source != _GATEWAY_SOURCE:
        msg = "Normal reads use the gateway source address (0, 0)"
        raise ValueError(msg)


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
    NORMAL_ANNOUNCE = 512
    BLOCK_TRANSFERS = 1024
    ORDERED_TRACE_GAPS = 2048
    NORMAL_DISCOVER = 4096
    ORDERED_NATIVE_RESULTS = 8192


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
    NORMAL_ANNOUNCE = 0x544BF012
    NORMAL_DISCOVER = 0x544BF013


_PORT_OPERATIONS = frozenset(
    {
        BridgeMethod.NORMAL_BLOCK_READ,
        BridgeMethod.NORMAL_STATUS_READ,
        BridgeMethod.NORMAL_BLOCK_EDIT,
        BridgeMethod.NORMAL_ANNOUNCE,
        BridgeMethod.NORMAL_DISCOVER,
    },
)


def _port_events(port: int, packet: TrpcPacket) -> bool:
    if packet.method_id == BridgeMethod.ERROR:
        return packet.extra[4:] == _BUSY
    if packet.service in (TrpcService.RESPONSE_REQUEST, TrpcService.RESPONSE_UPDATE):
        return (
            packet.method_id in _PORT_OPERATIONS
            and bool(packet.extra)
            and packet.extra[0] == port
        )
    return _port_terminal(port, packet)


def _port_terminal(port: int, packet: TrpcPacket) -> bool:
    body = packet.extra
    if packet.service is not TrpcService.REPORT or not body or body[0] != port:
        return False
    match packet.method_id:
        case BridgeMethod.NORMAL_BLOCK_READ:
            return block_read_terminal(body)
        case BridgeMethod.NORMAL_STATUS_READ:
            return status_read_terminal(body)
        case BridgeMethod.NORMAL_BLOCK_EDIT:
            return block_edit_terminal(body)
        case BridgeMethod.NORMAL_ANNOUNCE | BridgeMethod.NORMAL_DISCOVER:
            return len(body) == _LINK_RESULT_BYTES
    return False


class _OccupiedPortError(NativePortBusyError):
    def __init__(self, rejection: TrpcPacket) -> None:
        super().__init__("gateway is busy; native operation was not accepted")
        self.reply = rejection


class _NativeResultTimeoutError(TimeoutError):
    def __init__(self, admission: TrpcPacket) -> None:
        super().__init__("timed out waiting for the admitted Normal operation result")
        self.reply = admission


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
            if reason == _BUSY:
                if command.method in _PORT_OPERATIONS:
                    raise _OccupiedPortError(packet)
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
        timeout: float = DEFAULT_BLOCK_TIMEOUT,
    ) -> NativeFrame:
        """Read one peer in Normal mode without relying on trace or cached replies."""
        if query.target.request_mask().bit_count() != 1:
            msg = "use read_block_group for multiple nodes"
            raise ValueError(msg)
        return (
            await self.read_block_group(port, query, timeout=timeout)
        ).require_report()

    def _check_edit_idle(self, port: int) -> None:
        if port in TN4_PORTS and self._session.normal_edit_locks[port].locked():
            msg = f"native operation in progress on port {port}"
            raise NativePortBusyError(msg)

    async def read_block_group(
        self,
        port: int,
        query: NativeBlockQuery,
        *,
        source: NativeAddress = _GATEWAY_SOURCE,
        timeout: float = DEFAULT_BLOCK_TIMEOUT,
        wait_for_edits: bool = False,
    ) -> NativeBlockBatch:
        """Read one native bitmap and retain distinct peer reports on gateway expiry."""
        _check_source(source)
        async with self._reader(port, wait_for_edits=wait_for_edits):
            return await self._read_block(port, query, timeout)

    @asynccontextmanager
    async def _reader(self, port: int, *, wait_for_edits: bool) -> AsyncIterator[None]:
        if port not in TN4_PORTS:
            msg = "expected a physical tN4 port"
            raise ValueError(msg)
        if wait_for_edits:
            with self._session.connection_scope():
                async with self._session.normal_edit_locks[port]:
                    yield
        else:
            self._check_edit_idle(port)
            yield

    async def _read_block(
        self,
        port: int,
        query: NativeBlockQuery,
        timeout: float,
    ) -> NativeBlockBatch:
        collector = BlockCollector(port, query)
        async with self._normal_events(
            BridgeMethod.NORMAL_BLOCK_READ,
            BridgeFeature.BLOCK_TRANSFERS,
            collector.key(),
            timeout,
            progress_timeout=True,
        ) as receive:
            while (result := collector.collect(await receive())) is None:
                pass
            return result

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
            async with self._normal_events(
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
        timeout: float = DEFAULT_BLOCK_TIMEOUT,
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
        timeout: float = DEFAULT_BLOCK_TIMEOUT,
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
        timeout: float = DEFAULT_BLOCK_TIMEOUT,
        preview: bool = True,
    ) -> NativeWriteResult:
        async with self._block_editor(port, timeout) as editor:
            return await edit_thermostat_target(editor, target, change, preview=preview)

    async def write_snowmelt_setpoint(
        self,
        port: int,
        target: NativeAddress,
        block: SnowmeltBlock,
        celsius: float | None,
        *,
        expected_application: SnowmeltApplication | None = None,
        timeout: float = DEFAULT_BLOCK_TIMEOUT,
        preview: bool = True,
    ) -> NativeWriteResult:
        """Preview or verify a stored slab target without enabling a group."""
        async with self._block_editor(port, timeout) as editor:
            return await edit_snowmelt_setpoint(
                editor,
                target,
                block,
                celsius,
                expected_application=expected_application,
                preview=preview,
            )

    async def write_schedule_time(
        self,
        port: int,
        target: ScheduleTarget,
        change: ScheduleChange,
        *,
        timeout: float = DEFAULT_BLOCK_TIMEOUT,
        preview: bool = True,
    ) -> NativeWriteResult:
        async with self._block_editor(port, timeout) as editor:
            return await edit_schedule_time(editor, target, change, preview=preview)

    async def read_status(
        self,
        port: int,
        query: NativeStatusQuery,
        *,
        source: NativeAddress = _GATEWAY_SOURCE,
        peer: NativeAddress | None = None,
        timeout: float = 15,
        wait_for_edits: bool = False,
    ) -> NativeStatusBatch:
        """Read peer status without changing mode, trace or link-ACK settings."""
        _check_source(source)
        async with self._reader(port, wait_for_edits=wait_for_edits):
            return await self._read_status(port, query, peer, timeout)

    async def _read_status(
        self,
        port: int,
        query: NativeStatusQuery,
        peer: NativeAddress | None,
        timeout: float,
    ) -> NativeStatusBatch:
        collector = StatusCollector(query, peer)
        async with self._normal_events(
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
        timeout: float = DEFAULT_BLOCK_TIMEOUT,
    ) -> NativeStatusBatch:
        query = await snowmelt_status_query(
            peer,
            lambda block: self.read_block(port, block, timeout=timeout),
            keys,
        )
        return await self.read_status(port, query, peer=peer, timeout=timeout)

    @asynccontextmanager
    async def _hold_port_until_terminal(self, port: int) -> AsyncIterator[None]:
        # Arm before sending. A terminal can arrive in the same UART burst as
        # admission or rejection. Older completions do not release this waiter.
        with self._session.connection_scope():
            async with self._session.reports(
                include_matched=_PORT_OPERATIONS | {BridgeMethod.ERROR},
                predicate=lambda packet: _port_events(port, packet),
            ) as events:
                try:
                    yield
                except (_OccupiedPortError, _NativeResultTimeoutError) as err:
                    method = err.reply.method_id
                    boundary = False
                    async for packet in events:
                        if (
                            boundary
                            and method in {BridgeMethod.ERROR, packet.method_id}
                            and _port_terminal(port, packet)
                        ):
                            break
                        boundary |= packet is err.reply
                    else:
                        msg = f"connection closed while waiting for port {port}"
                        raise ConnectionClosedError(msg) from None
                    raise

    async def send_announcement(
        self,
        port: int,
        message: NativeMessage,
        *,
        timeout: float = 15,
    ) -> TxResult | None:
        """Send one known announcement in Normal mode. None means it expired unsent."""
        self._check_edit_idle(port)
        return await self._announce(port, message, timeout)

    async def discover_subnet(
        self,
        port: int,
        subnet: int,
        *,
        timeout: float = DEFAULT_BLOCK_TIMEOUT,
    ) -> TxResult | None:
        """Attempt one identity broadcast. Only peer RX identifies respondents."""
        if port not in TN4_PORTS:
            msg = "expected a physical port for discovery"
            raise ValueError(msg)
        key = bytes((port, NativeTargets(subnet).subnet))
        self._check_edit_idle(port)
        async with self._normal_events(
            BridgeMethod.NORMAL_DISCOVER,
            BridgeFeature.NORMAL_DISCOVER,
            key,
            timeout,
        ) as receive:
            return self._link_result(port, await receive())

    async def _announce(
        self,
        port: int,
        message: NativeMessage,
        timeout: float,
    ) -> TxResult | None:
        if (
            port not in TN4_PORTS
            or message.source != NativeAddress(0, 0)
            or message.message_id
            not in (
                CLOCK_REPORT,
                GROUP_UPDATE,
                NETWORK_INIT_REQUEST,
                SCENE_REPORT,
                ZONE_ADJUSTMENT,
            )
            or message.priority != int(message.message_id == NETWORK_INIT_REQUEST)
        ):
            msg = "invalid port, message, priority or source for a Normal announcement"
            raise ValueError(msg)
        key = bytes([port]) + message.message_id.to_bytes(2, "little") + message.payload
        async with self._normal_events(
            BridgeMethod.NORMAL_ANNOUNCE,
            BridgeFeature.NORMAL_ANNOUNCE,
            key,
            timeout,
            service=TrpcService.UPDATE,
        ) as receive:
            return self._link_result(port, await receive())

    @staticmethod
    def _link_result(port: int, data: bytes) -> TxResult | None:
        if len(data) != _LINK_RESULT_BYTES or data[0] != port:
            msg = "invalid Normal transmit result"
            raise ProtocolError(msg)
        return tx_outcome(port, data[1])

    async def set_snowmelt_group(
        self,
        port: int,
        target: NativeAddress,
        operation: SnowmeltOperation,
        *,
        enabled: bool,
        expected_group: int | None = None,
        expected_zone: int | None = None,
        timeout: float = DEFAULT_BLOCK_TIMEOUT,
        preview: bool = True,
    ) -> SnowmeltGroupResult:
        """Read the live binding and query that zone's state after an explicit send."""
        async with self._block_editor(port, timeout) as editor:
            return await apply_snowmelt_group(
                editor,
                lambda message: self._announce(port, message, timeout),
                lambda query: self._read_status(port, query, target, timeout),
                target,
                operation,
                enabled=enabled,
                expected_group=expected_group,
                expected_zone=expected_zone,
                preview=preview,
            )

    @asynccontextmanager
    async def _normal_events(
        self,
        method: BridgeMethod,
        feature: BridgeFeature,
        key: bytes,
        timeout: float,
        *,
        service: TrpcService = TrpcService.REQUEST,
        progress_timeout: bool = False,
    ) -> AsyncIterator[Callable[[], Awaitable[bytes]]]:
        if not isfinite(timeout) or timeout <= 0:
            msg = "Normal operation timeout must be finite and positive"
            raise ValueError(msg)
        command = _ExtensionCommand(service, method, payload=key)
        session = self._session
        with session.connection_scope():
            async with session.reports(
                include_matched=frozenset({command.method_id}),
                predicate=lambda packet: (
                    packet.method_id == command.method_id
                    and packet.extra[:1] == key[:1]
                ),
            ) as events:
                # Fence abandoned admission replies without waiting on other ports.
                async with session.normal_operation_admission:
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
                        msg = "gateway did not acknowledge this Normal operation"
                        raise ProtocolError(msg)

                async def receive() -> bytes:
                    if progress_timeout:
                        deadline.reschedule(asyncio.get_running_loop().time() + timeout)
                    async for packet in events:
                        if (
                            packet.service is TrpcService.REPORT
                            and packet.extra[:1] == key[:1]
                        ):
                            return packet.extra
                    msg = "connection closed before the Normal operation result"
                    raise ConnectionClosedError(msg)

                deadline = asyncio.timeout(timeout)
                try:
                    async with deadline:
                        async for packet in events:
                            if packet is ack:
                                yield receive
                                return
                except TimeoutError as err:
                    if deadline.expired():
                        raise _NativeResultTimeoutError(ack) from err
                    raise
        msg = "connection closed before Normal operation admission"
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

    async def _require_trace(
        self,
        features: BridgeFeature = BridgeFeature.NORMAL_TRACE,
    ) -> None:
        caps = await self.capabilities()
        if (
            caps is None
            or caps.mode != BridgeMode.NORMAL
            or caps.features & features != features
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

    @staticmethod
    def _trace_mask(ports: Iterable[int]) -> int:
        mask = 0
        for port in ports:
            if port not in TN4_PORTS:
                msg = f"invalid trace port {port}"
                raise ValueError(msg)
            mask |= 1 << port
        if not mask:
            msg = "select at least one trace port"
            raise ValueError(msg)
        return mask

    @staticmethod
    def _trace_ack(mask: int) -> Packet:
        return (
            _ExtensionCommand(
                TrpcService.RESPONSE_UPDATE,
                BridgeMethod.NORMAL_TRACE,
                payload=bytes([mask]),
            )
            .to_message()
            .to_packet()
        )

    @asynccontextmanager
    async def trace(
        self,
        ports: Iterable[int] = TN4_PORTS,
        *,
        backlog: int = 512,
    ) -> AsyncIterator[NormalTrace]:
        """Trace an open Normal session without changing its operating mode."""
        trace = NormalTrace(backlog)
        async with self._capture(trace, self._trace_mask(ports)):
            yield trace

    @asynccontextmanager
    async def peer_capture(
        self,
        ports: Iterable[int] = TN4_PORTS,
        *,
        backlog: int = 512,
    ) -> AsyncIterator[NormalPeerCapture]:
        """Receive peers and ordered loss notices without sending on tN4."""
        mask = self._trace_mask(ports)
        peers = NormalPeerCapture(backlog, mask, self._trace_ack(mask))
        async with self._capture(peers, mask):
            yield peers

    @asynccontextmanager
    async def native_session(
        self,
        ports: Iterable[int] = TN4_PORTS,
        *,
        backlog: int = 512,
    ) -> AsyncIterator[NormalNativeSession]:
        """Borrow native reads and peer trace on this Normal connection."""
        async with self.peer_capture(ports, backlog=backlog) as peers:
            yield NormalNativeSession(self, peers)

    @asynccontextmanager
    async def _capture(
        self,
        trace: NormalTrace | NormalPeerCapture,
        mask: int,
    ) -> AsyncIterator[None]:
        with self._session.connection_scope(), self._session.trace_packets(trace):
            required = BridgeFeature.NORMAL_TRACE
            if isinstance(trace, NormalPeerCapture):
                required |= BridgeFeature.ORDERED_TRACE_GAPS
            await self._require_trace(required)
            before = await self._trace_state()

            async def restore() -> None:
                if self._session.is_open and self._session.reader_error is None:
                    if isinstance(trace, NormalPeerCapture):
                        trace.stop_at(self._trace_ack(before.port_mask))
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
                yield
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
