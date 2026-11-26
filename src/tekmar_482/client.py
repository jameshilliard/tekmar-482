"""High-level asyncio client for the tekmar 482 gateway."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from typing import TYPE_CHECKING, Self

from ._poll import RuntimePoll, filter_discovery_events, observe_runtime
from .constants import (
    DEFAULT_BAUDRATE,
    MAX_PERCENT,
    SETPOINT_GROUP_IDS,
    DeviceMode,
    ThaSetback,
    ThaValue,
    setback_event_count,
)
from .exceptions import (
    ConnectionClosedError,
    InventoryDrainTimeoutError,
    ProtocolError,
    TekmarError,
)
from .models import (
    AvailableInfo,
    DeviceAttributes,
    DeviceSnapshot,
    DiscoveredDevice,
    DiscoveryResult,
    GatewayInfo,
    GatewaySnapshot,
)
from .session import Tekmar482Session
from .transport import (
    RawTcpTpckTransport,
    SerialxTpckTransport,
)
from .trpc import (
    REJECTED_CLOCK,
    AddressFields,
    AddressSetbackFields,
    DateTimeFields,
    EnableFields,
    ModeSettingFields,
    ReportingStateFields,
    ResponseMatch,
    SetbackPercentFields,
    SetbackSetpointFields,
    SetbackTemperatureFields,
    SetpointGroupFields,
    TrpcCommand,
    TrpcFieldsInput,
    TrpcMethod,
    TrpcMethodId,
    TrpcPacket,
    TrpcService,
    TrpcServiceId,
)
from .units import celsius_to_dege, celsius_to_degh, degh_to_celsius

if TYPE_CHECKING:
    from collections.abc import (
        AsyncIterator,
        Callable,
        Hashable,
        Iterable,
        Sequence,
    )
    from types import TracebackType

    from .packet import Packet
    from .session import ReportSubscription
    from .transport import PacketTransport

_REPORTING_ACK_TIMEOUT = 5
_OUTDOOR_LIMIT = 0x0BEA  # Exclusive bound in J1196J's host sample handler.

type _SetbackPayload = (
    AddressSetbackFields
    | SetbackSetpointFields
    | SetbackPercentFields
    | SetbackTemperatureFields
)


def _require_reply(reply: TrpcPacket | None, operation: str) -> TrpcPacket:
    if reply is None:
        msg = f"No response for {operation}"
        raise TimeoutError(msg)
    return reply


class Tekmar482Client:
    """Async client for tRPC messages carried by tekmar packets."""

    def __init__(
        self,
        transport: PacketTransport,
        *,
        readback_methods: frozenset[int] = frozenset(),
    ) -> None:
        self.transport = transport
        self.session = Tekmar482Session(transport, readback_methods=readback_methods)

    @classmethod
    def serial(
        cls,
        url: str,
        *,
        baudrate: int = DEFAULT_BAUDRATE,
        **serial_options: object,
    ) -> Self:
        """Create a client for a local serial device path."""
        return cls.serial_url(url, baudrate=baudrate, **serial_options)

    @classmethod
    def serial_url(
        cls,
        url: str,
        *,
        baudrate: int = DEFAULT_BAUDRATE,
        **serial_options: object,
    ) -> Self:
        """Create a client for any serialx URL.

        Examples include `/dev/serial/by-id/...`, `socket://host:port`, and
        `rfc2217://host:port`.
        """
        return cls(
            SerialxTpckTransport(
                url,
                baudrate=baudrate,
                serial_options=serial_options,
            ),
        )

    @classmethod
    def tcp(cls, host: str, port: int) -> Self:
        """Create a client for a raw TCP stream carrying binary TPCK frames."""
        return cls(RawTcpTpckTransport(host, port))

    @property
    def is_open(self) -> bool:
        """Return whether the underlying transport is open."""
        return self.transport.is_open

    async def __aenter__(self) -> Self:
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc, traceback
        await self.close()

    async def open(self) -> None:
        """Open the underlying transport."""
        await self.session.open()

    async def close(self) -> None:
        """Close the underlying transport."""
        await self.session.close()

    async def read_packet(self, timeout: float | None = None) -> Packet | None:
        """Read one raw tekmar packet."""
        return await self.session.read_packet(timeout)

    async def write_packet(self, packet: Packet) -> None:
        """Write one raw tekmar packet."""
        await self.session.write_packet(packet)

    async def read_message(self, timeout: float | None = None) -> TrpcPacket | None:
        """Read the next tRPC message, returning None on timeout."""
        return await self.session.read_message(timeout)

    async def write_message(self, message: TrpcPacket) -> None:
        """Write one tRPC message."""
        await self.session.write_message(message)

    async def send(self, command: TrpcCommand) -> TrpcPacket:
        """Write one typed tRPC command."""
        return await self.session.send(command)

    async def request(
        self,
        method: TrpcMethodId,
        fields: TrpcFieldsInput | None = None,
    ) -> TrpcPacket:
        """Send a tRPC Request message."""
        return await self.send(TrpcCommand.request(method, fields))

    async def update(
        self,
        method: TrpcMethodId,
        fields: TrpcFieldsInput | None = None,
    ) -> TrpcPacket:
        """Send a tRPC Update message."""
        return await self.send(TrpcCommand.update(method, fields))

    async def coalesced_update(
        self,
        method: TrpcMethodId,
        *,
        coalesce_key: Hashable,
        timeout: float | None = None,
        fields: TrpcFieldsInput | None = None,
    ) -> TrpcPacket | None:
        """Send a coalescible tRPC Update message.

        Returns None when a newer pending command with the same key superseded
        this command before it was written to the gateway.
        """
        return await self.session.coalesced_update(
            TrpcCommand.update(method, fields),
            coalesce_key=coalesce_key,
            timeout=timeout,
        )

    async def coalesced_update_response(
        self,
        method: TrpcMethodId,
        *,
        coalesce_key: Hashable,
        response_method: TrpcMethodId | None = None,
        response_services: Iterable[TrpcServiceId] | None = None,
        match_address: int | None = None,
        match_fields: TrpcFieldsInput | None = None,
        timeout: float | None = 5,
        fields: TrpcFieldsInput | None = None,
    ) -> TrpcPacket | None:
        """Send a coalescible Update and wait for a matching response/report."""
        command = TrpcCommand.update(method, fields)
        return await self.session.coalesced_update_response(
            command,
            coalesce_key=coalesce_key,
            response_match=ResponseMatch.for_command(
                command,
                readback=method in self.session.readback_methods,
                response_method=response_method,
                response_services=response_services,
                address=match_address,
                fields=match_fields,
            ),
            timeout=timeout,
        )

    async def set_reporting_state(
        self,
        *,
        enabled: bool,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Enable or disable tHA reporting."""
        return await self._set_reporting_state(enabled=enabled, timeout=timeout)

    async def _set_reporting_state(
        self,
        *,
        enabled: bool,
        timeout: float | None,
        on_sending: Callable[[], None] | None = None,
    ) -> TrpcPacket | None:
        fields = ReportingStateFields(enabled)
        command = TrpcCommand.update(TrpcMethod.REPORTING_STATE, fields)
        return await self.session.update_response(
            command,
            response_match=ResponseMatch.for_command(command, fields=fields),
            timeout=timeout,
            on_sending=on_sending,
        )

    @asynccontextmanager
    async def _reporting_cleanup(
        self,
        timeout: float | None,
    ) -> AsyncIterator[Callable[[], None]]:
        generation: int | None = None
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        budget = (
            _REPORTING_ACK_TIMEOUT
            if timeout is None
            else min(timeout, _REPORTING_ACK_TIMEOUT)
        )

        def mark_sending() -> None:
            nonlocal generation
            generation = self.session.connection_generation

        async def restore(remaining: float) -> None:
            if generation is None:
                return
            if generation != self.session.connection_generation:
                msg = "Connection changed before restoring reporting"
                raise ConnectionClosedError(msg)
            async with asyncio.timeout(remaining):
                _require_reply(
                    await self.set_reporting_state(enabled=True, timeout=None),
                    "ReportingState=1",
                )

        try:
            yield mark_sending
        except BaseException as err:
            try:
                await restore(budget)
            except (OSError, TekmarError) as cleanup_err:
                err.add_note(f"Could not restore reporting: {cleanup_err!r}")
            raise
        else:
            try:
                await restore(
                    budget if deadline is None else min(budget, deadline - loop.time()),
                )
            except TimeoutError as err:
                msg = "Discovery timed out restoring reporting"
                raise TimeoutError(msg) from err

    async def request_device_inventory(self, address: int = 0) -> TrpcPacket:
        """Request device inventory, usually starting at address 0."""
        return await self.request(TrpcMethod.DEVICE_INVENTORY, AddressFields(address))

    async def request_firmware_revision(self) -> TrpcPacket:
        """Request the gateway firmware revision."""
        return await self.request(TrpcMethod.FIRMWARE_REVISION)

    async def request_protocol_version(self) -> TrpcPacket:
        """Request the tHA protocol version."""
        return await self.request(TrpcMethod.PROTOCOL_VERSION)

    async def read_until(
        self,
        predicate: Callable[[TrpcPacket], bool],
        *,
        timeout: float | None = None,
    ) -> TrpcPacket | None:
        """Read tRPC messages until `predicate` matches or timeout expires."""
        return await self.session.read_until(predicate, timeout=timeout)

    async def request_response(
        self,
        method: TrpcMethodId,
        *,
        response_method: TrpcMethodId | None = None,
        response_services: Iterable[TrpcServiceId] | None = None,
        match_address: int | None = None,
        match_fields: TrpcFieldsInput | None = None,
        publish_match: bool = False,
        timeout: float | None = 5,
        fields: TrpcFieldsInput | None = None,
    ) -> TrpcPacket | None:
        """Send a Request and wait for a matching response/report message."""
        command = TrpcCommand.request(method, fields)
        return await self.session.request_response(
            command,
            publish_match=publish_match,
            response_match=ResponseMatch.for_command(
                command,
                response_method=response_method,
                response_services=response_services,
                address=match_address,
                fields=match_fields,
            ),
            timeout=timeout,
        )

    async def update_response(
        self,
        method: TrpcMethodId,
        *,
        response_method: TrpcMethodId | None = None,
        response_services: Iterable[TrpcServiceId] | None = None,
        match_address: int | None = None,
        match_fields: TrpcFieldsInput | None = None,
        timeout: float | None = 5,
        fields: TrpcFieldsInput | None = None,
    ) -> TrpcPacket | None:
        """Send an Update and wait for a matching response/report message."""
        command = TrpcCommand.update(method, fields)
        return await self.session.update_response(
            command,
            response_match=ResponseMatch.for_command(
                command,
                readback=method in self.session.readback_methods,
                response_method=response_method,
                response_services=response_services,
                address=match_address,
                fields=match_fields,
            ),
            timeout=timeout,
        )

    def reports(
        self,
        *,
        max_queue_size: int = 0,
        replay_backlog: bool = False,
        include_matched: frozenset[TrpcMethod] = frozenset(),
    ) -> ReportSubscription:
        """Subscribe to public messages and matched replies for selected methods."""
        return self.session.reports(
            max_queue_size=max_queue_size,
            replay_backlog=replay_backlog,
            include_matched=include_matched,
        )

    async def get_gateway_info(self, *, timeout: float | None = 5) -> GatewayInfo:
        """Request gateway firmware and protocol metadata."""
        values = await self._read_metadata(
            {
                TrpcMethod.FIRMWARE_REVISION: "revision",
                TrpcMethod.PROTOCOL_VERSION: "version",
            },
            timeout=timeout,
        )
        return GatewayInfo(
            firmware_revision=values.get(TrpcMethod.FIRMWARE_REVISION),
            protocol_version=values.get(TrpcMethod.PROTOCOL_VERSION),
        )

    async def _read_metadata(
        self,
        queries: dict[TrpcMethod, str],
        *,
        timeout: float | None,
        address: int | None = None,
    ) -> dict[TrpcMethod, int]:
        """Collect replies within a shared budget, including time queued to send."""
        values: dict[TrpcMethod, int] = {}
        with self.session.connection_scope():
            timer = asyncio.timeout(timeout)
            try:
                async with timer:
                    for method, field in queries.items():
                        reply = await self.request_response(
                            method,
                            fields=None if address is None else AddressFields(address),
                            timeout=None,
                        )
                        values[method] = _require_reply(reply, method.name).require(
                            field,
                        )
            except TimeoutError:
                if not timer.expired():
                    raise
        return values

    async def get_inventory_addresses(
        self,
        *,
        start_address: int = 0,
        timeout: float | None = 10,
    ) -> tuple[int, ...]:
        """Walk the inventory at zero, or look up one exact nonzero address.

        An explicit not-found reply returns an empty tuple; a timeout raises.
        """
        return await self.session.request_inventory(start_address, timeout=timeout)

    async def get_device_attributes(
        self,
        address: int,
        *,
        timeout: float | None = 5,
    ) -> DeviceAttributes:
        """Read the gateway's current capability mask for this address."""
        reply = _require_reply(
            await self.request_response(
                TrpcMethod.DEVICE_ATTRIBUTES,
                fields=AddressFields(address),
                timeout=timeout,
            ),
            f"attributes at {address}",
        )
        return DeviceAttributes(reply.require("attributes"))

    async def get_device_info(
        self,
        address: int,
        *,
        timeout: float | None = 5,
    ) -> DiscoveredDevice:
        """Request metadata for one discovered device address."""
        values = await self._read_metadata(
            {
                TrpcMethod.DEVICE_TYPE: "type",
                TrpcMethod.DEVICE_VERSION: "j_number",
                TrpcMethod.DEVICE_ATTRIBUTES: "attributes",
                TrpcMethod.SETBACK_EVENTS: "events",
            },
            address=address,
            timeout=timeout,
        )
        raw_type_code = values.get(TrpcMethod.DEVICE_TYPE)
        raw_version = values.get(TrpcMethod.DEVICE_VERSION)
        return DiscoveredDevice(
            address=address,
            type_code=None if raw_type_code == ThaValue.NA_32 else raw_type_code,
            version=None if raw_version == ThaValue.NA_32 else raw_version,
            attributes=DeviceAttributes(values.get(TrpcMethod.DEVICE_ATTRIBUTES)),
            setback_events=setback_event_count(values.get(TrpcMethod.SETBACK_EVENTS)),
        )

    async def discover(
        self,
        *,
        timeout: float | None = 30,
        manage_reporting: bool = False,
        setback_enable: bool | None = None,
        write_delay: float = 0,
    ) -> DiscoveryResult:
        """Discover gateway metadata and all visible attached devices.

        By default this only sends request messages. Set `manage_reporting=True`
        to turn reporting off during discovery and back on afterwards. Pass
        `setback_enable` to update the gateway setback support flag. The timeout
        includes restoring reporting. If a scan fails or is cancelled after
        sending a disable, cleanup gets up to five additional seconds, capped
        by the specified timeout.
        """

        async def delay() -> None:
            if write_delay > 0:
                await asyncio.sleep(write_delay)

        phase = "gateway metadata"
        with self.session.connection_scope():
            deadline = (
                None if timeout is None else asyncio.get_running_loop().time() + timeout
            )
            async with self._reporting_cleanup(timeout) as mark_sending:
                await self.session.wait_for_inventory_drain(timeout=timeout)
                try:
                    async with asyncio.timeout_at(deadline):
                        if manage_reporting:
                            phase = "disabling reporting"
                            ack = await self._set_reporting_state(
                                enabled=False,
                                timeout=_REPORTING_ACK_TIMEOUT,
                                on_sending=mark_sending,
                            )
                            _require_reply(ack, phase)
                            await delay()

                        phase = "gateway metadata"
                        gateway = await self.get_gateway_info(timeout=None)
                        await delay()

                        if setback_enable is not None:
                            phase = f"SetbackEnable={int(setback_enable)}"
                            fields = EnableFields(setback_enable)
                            ack = await self.update_response(
                                TrpcMethod.SETBACK_ENABLE,
                                match_fields=fields,
                                timeout=None,
                                fields=fields,
                            )
                            _require_reply(ack, phase)
                            await delay()

                        phase = "complete device inventory"
                        addresses = await self.get_inventory_addresses(timeout=None)
                        devices: list[DiscoveredDevice] = []
                        for address in addresses:
                            phase = f"metadata for device {address}"
                            devices.append(
                                await self.get_device_info(address, timeout=None),
                            )
                            await delay()

                        return DiscoveryResult(gateway=gateway, devices=tuple(devices))
                except InventoryDrainTimeoutError:
                    raise
                except TimeoutError as err:
                    msg = f"Discovery timed out during {phase}"
                    raise TimeoutError(msg) from err

    async def dump_available_info(
        self,
        *,
        timeout: float | None = 2,
        inventory_timeout: float | None = 10,
        include_setbacks: bool = True,
        write_delay: float = 0,
    ) -> AvailableInfo:
        """Request a broad read-only snapshot of gateway and attached device data.

        Missing or unsupported values are returned as None. This method does not
        alter reporting or setback settings.
        """
        with observe_runtime(self.session) as events:
            discovery = await self.discover(
                timeout=inventory_timeout,
                write_delay=write_delay,
            )
            filter_discovery_events(events, discovery, None)
            poll = RuntimePoll.plan(
                discovery,
                include_setbacks=include_setbacks,
                exhaustive_setbacks=True,
            )
            await poll.execute(self.session, timeout=timeout, write_delay=write_delay)
        return poll.reduce(events)

    async def poll_runtime(
        self,
        discovery: DiscoveryResult,
        *,
        timeout: float | None = 2,
        include_setbacks: bool = False,
        include_setpoint_groups: bool = True,
        write_delay: float = 0,
    ) -> AvailableInfo:
        """Poll runtime values for an existing discovery result.

        This is the Home Assistant-friendly polling path: inventory and stable
        device metadata are expected to come from `discover()`, while this method
        refreshes values that can change at runtime.
        """
        poll = RuntimePoll.plan(
            discovery,
            include_setbacks=include_setbacks,
            include_setpoint_groups=include_setpoint_groups,
        )
        return await poll.snapshot(
            self.session,
            timeout=timeout,
            write_delay=write_delay,
        )

    async def poll_gateway(
        self,
        *,
        gateway_info: GatewayInfo | None = None,
        timeout: float | None = 2,
        include_setpoint_groups: bool = True,
        write_delay: float = 0,
    ) -> GatewaySnapshot:
        """Poll gateway runtime values without walking device inventory."""
        with observe_runtime(self.session) as events:
            if gateway_info is None:
                gateway_info = await self.get_gateway_info(timeout=timeout)
                if write_delay > 0:
                    await asyncio.sleep(write_delay)
            poll = RuntimePoll.plan(
                DiscoveryResult(gateway_info, ()),
                include_setpoint_groups=include_setpoint_groups,
            )
            await poll.execute(self.session, timeout=timeout, write_delay=write_delay)
        return poll.reduce(events).gateway

    async def poll_devices(
        self,
        devices: Sequence[DiscoveredDevice],
        *,
        protocol_version: int | None,
        timeout: float | None = 2,
        include_setbacks: bool = False,
        write_delay: float = 0,
    ) -> tuple[DeviceSnapshot, ...]:
        """Poll runtime values for known devices without rediscovering inventory."""
        discovery = DiscoveryResult(
            GatewayInfo(protocol_version=protocol_version),
            tuple(devices),
        )
        poll = RuntimePoll.plan(
            discovery,
            include_gateway=False,
            include_setbacks=include_setbacks,
        )
        data = await poll.snapshot(
            self.session,
            timeout=timeout,
            write_delay=write_delay,
        )
        return data.devices

    async def request_outdoor_temperature(self) -> TrpcPacket:
        """Request the gateway outdoor temperature."""
        return await self.request(TrpcMethod.OUTDOOR_TEMP)

    async def get_outdoor_temperature_celsius(
        self,
        *,
        timeout: float | None = 5,
    ) -> float | None:
        """Read the selected network value; None means no source, not a timeout."""
        reply = _require_reply(
            await self.request_response(TrpcMethod.OUTDOOR_TEMP, timeout=timeout),
            "OutdoorTemp",
        )
        return degh_to_celsius(reply.require("temp"))

    async def set_outdoor_temperature(
        self,
        temp: int,
        *,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Supply a four-minute local sample (degH); the reply is the network value."""
        if not 0 <= temp < _OUTDOOR_LIMIT:
            msg = f"OutdoorTemp update must be in 0..{_OUTDOOR_LIMIT - 1}: {temp}"
            raise ValueError(msg)
        return await self.coalesced_update_response(
            TrpcMethod.OUTDOOR_TEMP,
            coalesce_key=TrpcMethod.OUTDOOR_TEMP,
            fields={"temp": temp},
            timeout=timeout,
        )

    async def set_outdoor_temperature_celsius(
        self,
        celsius: float,
        *,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Supply one outdoor sample; the caller owns the refresh schedule."""
        return await self.set_outdoor_temperature(
            celsius_to_degh(celsius),
            timeout=timeout,
        )

    async def request_setpoint_group(self, group_id: int) -> TrpcPacket:
        """Request a setpoint group enable state."""
        return await self.request(
            TrpcMethod.SETPOINT_GROUP_ENABLE,
            SetpointGroupFields(group_id),
        )

    async def set_setpoint_group(
        self,
        group_id: int,
        *,
        enabled: bool,
        timeout: float | None = 5,
    ) -> TrpcPacket:
        """Set a gateway group and wait for its report (requires reporting enabled).

        A timeout does not establish whether the gateway applied the command.
        """
        if group_id not in SETPOINT_GROUP_IDS:
            msg = f"Gateway setpoint group must be in 1..12: {group_id}"
            raise ValueError(msg)
        fields = SetpointGroupFields(group_id, enabled)
        response = await self.update_response(
            TrpcMethod.SETPOINT_GROUP_ENABLE,
            response_services=(TrpcService.REPORT,),
            match_fields=fields,
            timeout=timeout,
            fields=fields,
        )
        if response is None:
            msg = (
                f"No confirmation for setpoint group {group_id}, enable={int(enabled)}"
            )
            raise TimeoutError(msg)
        return response

    async def get_setpoint_group(
        self,
        group_id: int,
        *,
        timeout: float | None = 5,
    ) -> bool | None:
        """Read cached group activity; False does not prove a receiver exists."""
        if group_id not in SETPOINT_GROUP_IDS:
            msg = f"Gateway setpoint group must be in 1..12: {group_id}"
            raise ValueError(msg)
        reply = _require_reply(
            await self.request_response(
                TrpcMethod.SETPOINT_GROUP_ENABLE,
                fields=SetpointGroupFields(group_id),
                timeout=timeout,
            ),
            f"setpoint group {group_id}",
        )
        value = reply.require("enable")
        return bool(value) if value in (0, 1) else None

    async def set_datetime(
        self,
        value: datetime | None = None,
        *,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Set the clock; rejection raises ProtocolError, timeout returns None."""
        value = value or datetime.now().astimezone()
        fields = DateTimeFields.from_datetime(value)
        command = TrpcCommand.update(TrpcMethod.DATE_TIME, fields)
        response = await self.session.update_response(
            command,
            timeout=timeout,
        )
        if response is not None and response.body == REJECTED_CLOCK:
            msg = "Gateway rejected DateTime update"
            raise ProtocolError(msg)
        return response

    async def set_heat_setpoint(
        self,
        address: int,
        setpoint: int,
        *,
        setback: int = ThaSetback.CURRENT,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Update a device heat setpoint using the raw tHA degE value."""
        return await self._update_setback_payload(
            TrpcMethod.HEAT_SETPOINT,
            SetbackSetpointFields(address, setpoint, setback),
            timeout=timeout,
        )

    async def set_heat_setpoint_celsius(
        self,
        address: int,
        celsius: float,
        *,
        setback: int = ThaSetback.CURRENT,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Update a device heat setpoint using Celsius."""
        return await self.set_heat_setpoint(
            address,
            celsius_to_dege(celsius),
            setback=setback,
            timeout=timeout,
        )

    async def set_latest_heat_setpoint(
        self,
        address: int,
        setpoint: int,
        *,
        setback: int = ThaSetback.CURRENT,
        confirm: bool = False,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Update a heat setpoint, replacing older unsent heat setpoints."""
        return await self._coalesced_setback_payload(
            TrpcMethod.HEAT_SETPOINT,
            SetbackSetpointFields(address, setpoint, setback),
            confirm=confirm,
            timeout=timeout,
        )

    async def set_latest_heat_setpoint_celsius(
        self,
        address: int,
        celsius: float,
        *,
        setback: int = ThaSetback.CURRENT,
        confirm: bool = False,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Update a heat setpoint using Celsius, replacing older unsent values."""
        return await self.set_latest_heat_setpoint(
            address,
            celsius_to_dege(celsius),
            setback=setback,
            confirm=confirm,
            timeout=timeout,
        )

    async def set_cool_setpoint(
        self,
        address: int,
        setpoint: int,
        *,
        setback: int = ThaSetback.CURRENT,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Update a device cool setpoint using the raw tHA degE value."""
        return await self._update_setback_payload(
            TrpcMethod.COOL_SETPOINT,
            SetbackSetpointFields(address, setpoint, setback),
            timeout=timeout,
        )

    async def set_cool_setpoint_celsius(
        self,
        address: int,
        celsius: float,
        *,
        setback: int = ThaSetback.CURRENT,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Update a device cool setpoint using Celsius."""
        return await self.set_cool_setpoint(
            address,
            celsius_to_dege(celsius),
            setback=setback,
            timeout=timeout,
        )

    async def set_latest_cool_setpoint(
        self,
        address: int,
        setpoint: int,
        *,
        setback: int = ThaSetback.CURRENT,
        confirm: bool = False,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Update a cool setpoint, replacing older unsent cool setpoints."""
        return await self._coalesced_setback_payload(
            TrpcMethod.COOL_SETPOINT,
            SetbackSetpointFields(address, setpoint, setback),
            confirm=confirm,
            timeout=timeout,
        )

    async def set_latest_cool_setpoint_celsius(
        self,
        address: int,
        celsius: float,
        *,
        setback: int = ThaSetback.CURRENT,
        confirm: bool = False,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Update a cool setpoint using Celsius, replacing older unsent values."""
        return await self.set_latest_cool_setpoint(
            address,
            celsius_to_dege(celsius),
            setback=setback,
            confirm=confirm,
            timeout=timeout,
        )

    async def set_slab_setpoint(
        self,
        address: int,
        setpoint: int,
        *,
        setback: int = ThaSetback.CURRENT,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Update a device slab setpoint using the raw tHA degE value."""
        return await self._update_setback_payload(
            TrpcMethod.SLAB_SETPOINT,
            SetbackSetpointFields(address, setpoint, setback),
            timeout=timeout,
        )

    async def set_slab_setpoint_celsius(
        self,
        address: int,
        celsius: float,
        *,
        setback: int = ThaSetback.CURRENT,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Update a device slab setpoint using Celsius."""
        return await self.set_slab_setpoint(
            address,
            celsius_to_dege(celsius),
            setback=setback,
            timeout=timeout,
        )

    async def set_latest_slab_setpoint(
        self,
        address: int,
        setpoint: int,
        *,
        setback: int = ThaSetback.CURRENT,
        confirm: bool = False,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Update a slab setpoint, replacing older unsent slab setpoints."""
        return await self._coalesced_setback_payload(
            TrpcMethod.SLAB_SETPOINT,
            SetbackSetpointFields(address, setpoint, setback),
            confirm=confirm,
            timeout=timeout,
        )

    async def set_latest_slab_setpoint_celsius(
        self,
        address: int,
        celsius: float,
        *,
        setback: int = ThaSetback.CURRENT,
        confirm: bool = False,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Update a slab setpoint using Celsius, replacing older unsent values."""
        return await self.set_latest_slab_setpoint(
            address,
            celsius_to_dege(celsius),
            setback=setback,
            confirm=confirm,
            timeout=timeout,
        )

    async def set_fan_percent(
        self,
        address: int,
        percent: int,
        *,
        setback: int = ThaSetback.CURRENT,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Update a device fan percentage."""
        self._validate_percent(percent)
        return await self._update_setback_payload(
            TrpcMethod.FAN_PERCENT,
            SetbackPercentFields(address, percent, setback),
            timeout=timeout,
        )

    async def set_humidity_min(
        self,
        address: int,
        percent: int,
        *,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Set the humidification target and return the accepted percentage."""
        return await self._set_humidity_limit(
            TrpcMethod.HUMIDITY_SET_MIN,
            address,
            percent,
            timeout,
        )

    async def set_humidity_max(
        self,
        address: int,
        percent: int,
        *,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Set the dehumidification target and return the accepted percentage."""
        return await self._set_humidity_limit(
            TrpcMethod.HUMIDITY_SET_MAX,
            address,
            percent,
            timeout,
        )

    async def _set_humidity_limit(
        self,
        method: TrpcMethod,
        address: int,
        percent: int,
        timeout: float | None,
    ) -> TrpcPacket | None:
        self._validate_percent(percent)
        return await self.coalesced_update_response(
            method,
            coalesce_key=(method, address),
            match_address=address,
            fields={"address": address, "percent": percent},
            timeout=timeout,
        )

    async def set_setpoint_device(
        self,
        address: int,
        temp: int,
        *,
        setback: int = ThaSetback.CURRENT,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Update a setpoint device target using the raw tHA degH value."""
        return await self._update_setback_payload(
            TrpcMethod.SETPOINT_DEVICE,
            SetbackTemperatureFields(address, temp, setback),
            timeout=timeout,
        )

    async def set_setpoint_device_celsius(
        self,
        address: int,
        celsius: float,
        *,
        setback: int = ThaSetback.CURRENT,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Update a setpoint device target using Celsius."""
        return await self.set_setpoint_device(
            address,
            celsius_to_degh(celsius),
            setback=setback,
            timeout=timeout,
        )

    async def set_latest_setpoint_device(
        self,
        address: int,
        temp: int,
        *,
        setback: int = ThaSetback.CURRENT,
        confirm: bool = False,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Update a setpoint device target, replacing older unsent targets."""
        return await self._coalesced_setback_payload(
            TrpcMethod.SETPOINT_DEVICE,
            SetbackTemperatureFields(address, temp, setback),
            confirm=confirm,
            timeout=timeout,
        )

    async def set_latest_setpoint_device_celsius(
        self,
        address: int,
        celsius: float,
        *,
        setback: int = ThaSetback.CURRENT,
        confirm: bool = False,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Update a setpoint device target using Celsius, replacing older values."""
        return await self.set_latest_setpoint_device(
            address,
            celsius_to_degh(celsius),
            setback=setback,
            confirm=confirm,
            timeout=timeout,
        )

    async def set_latest_fan_percent(
        self,
        address: int,
        percent: int,
        *,
        setback: int = ThaSetback.CURRENT,
        confirm: bool = False,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Update a fan percentage, replacing older unsent fan percentages."""
        self._validate_percent(percent)
        return await self._coalesced_setback_payload(
            TrpcMethod.FAN_PERCENT,
            SetbackPercentFields(address, percent, setback),
            confirm=confirm,
            timeout=timeout,
        )

    async def set_mode(
        self,
        address: int,
        mode: int | DeviceMode,
        *,
        confirm: bool = False,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Update a thermostat operating mode."""
        mode_value = int(mode)
        if mode_value not in {int(item) for item in DeviceMode}:
            msg = f"unsupported device mode: {mode_value}"
            raise ValueError(msg)
        payload = ModeSettingFields(address, mode_value)
        if confirm or TrpcMethod.MODE_SETTING in self.session.readback_methods:
            return await self.coalesced_update_response(
                TrpcMethod.MODE_SETTING,
                coalesce_key=payload.coalesce_key(TrpcMethod.MODE_SETTING),
                match_address=payload.address,
                timeout=timeout,
                fields=payload,
            )
        return await self.coalesced_update(
            TrpcMethod.MODE_SETTING,
            coalesce_key=payload.coalesce_key(TrpcMethod.MODE_SETTING),
            timeout=timeout,
            fields=payload,
        )

    async def _update_setback_payload(
        self,
        method: TrpcMethod,
        payload: _SetbackPayload,
        *,
        timeout: float | None,
    ) -> TrpcPacket | None:
        return await self.update_response(
            method,
            match_address=payload.address,
            timeout=timeout,
            fields=payload,
        )

    async def _coalesced_setback_payload(
        self,
        method: TrpcMethod,
        payload: _SetbackPayload,
        *,
        confirm: bool,
        timeout: float | None,
    ) -> TrpcPacket | None:
        if confirm or method in self.session.readback_methods:
            return await self.coalesced_update_response(
                method,
                coalesce_key=payload.coalesce_key(method),
                match_address=payload.address,
                timeout=timeout,
                fields=payload,
            )
        return await self.coalesced_update(
            method,
            coalesce_key=payload.coalesce_key(method),
            timeout=timeout,
            fields=payload,
        )

    @staticmethod
    def _validate_percent(value: int) -> None:
        if not 0 <= value <= MAX_PERCENT:
            msg = f"percent must be between 0 and {MAX_PERCENT}: {value}"
            raise ValueError(msg)
