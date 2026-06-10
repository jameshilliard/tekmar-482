"""High-level asyncio client for the tekmar 482 gateway."""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import TYPE_CHECKING, Self

from ._deadline import Deadline
from .constants import (
    DEFAULT_BAUDRATE,
    DeviceMode,
    ThaSetback,
    ThaValue,
)
from .models import (
    AvailableInfo,
    DeviceAttributes,
    DeviceRuntime,
    DeviceSnapshot,
    DeviceValue,
    DiscoveredDevice,
    DiscoveryResult,
    GatewayDateTime,
    GatewayInfo,
    GatewaySnapshot,
    SetpointValues,
    setback_from_value,
)
from .session import Tekmar482Session
from .transport import (
    RawTcpTpckTransport,
    SerialxTpckTransport,
)
from .trpc import (
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
from .units import celsius_to_dege, celsius_to_degh

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Hashable, Iterable, Sequence
    from types import TracebackType

    from .packet import Packet
    from .session import ReportSubscription
    from .transport import PacketTransport

_PROTOCOL_VERSION_RUNTIME_EXTENSIONS = 3
_MIN_PERCENT = 0
_MAX_PERCENT = 100

type _SetbackPayload = (
    AddressSetbackFields
    | SetbackSetpointFields
    | SetbackPercentFields
    | SetbackTemperatureFields
)


class Tekmar482Client:
    """Async client for tRPC messages carried by tekmar packets."""

    def __init__(self, transport: PacketTransport) -> None:
        self.transport = transport
        self.session = Tekmar482Session(transport)

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
        fields: TrpcFieldsInput | None = None,
    ) -> TrpcPacket | None:
        """Send a coalescible tRPC Update message.

        Returns None when a newer pending command with the same key superseded
        this command before it was written to the gateway.
        """
        return await self.session.coalesced_update(
            TrpcCommand.update(method, fields),
            coalesce_key=coalesce_key,
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
        fields = ReportingStateFields(enabled)
        return await self.update_response(
            TrpcMethod.REPORTING_STATE,
            match_fields=fields,
            timeout=timeout,
            fields=fields,
        )

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
        timeout: float | None = 5,
        fields: TrpcFieldsInput | None = None,
    ) -> TrpcPacket | None:
        """Send a Request and wait for a matching response/report message."""
        command = TrpcCommand.request(method, fields)
        return await self.session.request_response(
            command,
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
    ) -> ReportSubscription:
        """Subscribe to unmatched tRPC messages."""
        return self.session.reports(
            max_queue_size=max_queue_size,
            replay_backlog=replay_backlog,
        )

    async def get_gateway_info(self, *, timeout: float | None = 5) -> GatewayInfo:
        """Request gateway firmware and protocol metadata."""
        firmware = await self.request_response(
            TrpcMethod.FIRMWARE_REVISION,
            timeout=timeout,
        )
        protocol = await self.request_response(
            TrpcMethod.PROTOCOL_VERSION,
            timeout=timeout,
        )
        return GatewayInfo(
            firmware_revision=(
                firmware.require("revision") if firmware is not None else None
            ),
            protocol_version=(
                protocol.require("version") if protocol is not None else None
            ),
        )

    async def get_inventory_addresses(
        self,
        *,
        start_address: int = 0,
        timeout: float | None = 10,
    ) -> tuple[int, ...]:
        """Request and return all discovered tN4/tN2 device addresses."""
        await self.request_device_inventory(start_address)
        addresses: list[int] = []
        deadline = Deadline.from_timeout(timeout)

        while True:
            if deadline.expired:
                return tuple(addresses)

            message = await self.read_until(
                lambda item: (
                    item.service == TrpcService.RESPONSE_REQUEST
                    and item.method == TrpcMethod.DEVICE_INVENTORY
                ),
                timeout=deadline.remaining(),
            )
            if message is None:
                return tuple(addresses)

            address = message.require("address")
            if address in {0, ThaValue.NA_16}:
                return tuple(addresses)
            if address not in addresses:
                addresses.append(address)

    async def get_device_info(
        self,
        address: int,
        *,
        timeout: float | None = 5,
    ) -> DiscoveredDevice:
        """Request metadata for one discovered device address."""
        device_type = await self.request_response(
            TrpcMethod.DEVICE_TYPE,
            match_address=address,
            timeout=timeout,
            fields=AddressFields(address),
        )
        device_version = await self.request_response(
            TrpcMethod.DEVICE_VERSION,
            match_address=address,
            timeout=timeout,
            fields=AddressFields(address),
        )
        attributes = await self.request_response(
            TrpcMethod.DEVICE_ATTRIBUTES,
            match_address=address,
            timeout=timeout,
            fields=AddressFields(address),
        )
        setback_events = await self.request_response(
            TrpcMethod.SETBACK_EVENTS,
            match_address=address,
            timeout=timeout,
            fields=AddressFields(address),
        )

        raw_attributes = (
            attributes.require("attributes") if attributes is not None else 0
        )
        raw_type_code = device_type.require("type") if device_type is not None else None
        raw_version = (
            device_version.require("j_number") if device_version is not None else None
        )
        return DiscoveredDevice(
            address=address,
            type_code=None if raw_type_code == ThaValue.NA_32 else raw_type_code,
            version=None if raw_version == ThaValue.NA_32 else raw_version,
            attributes=DeviceAttributes(raw_attributes),
            setback_events=(
                setback_events.require("events") if setback_events is not None else None
            ),
        )

    async def discover(
        self,
        *,
        timeout: float | None = 30,
        manage_reporting: bool = False,
        setback_enable: bool | None = None,
        write_delay: float = 0.1,
    ) -> DiscoveryResult:
        """Discover gateway metadata and all visible attached devices.

        By default this only sends request messages. Set `manage_reporting=True`
        to turn reporting off during discovery and back on afterwards. Pass
        `setback_enable` to update the gateway setback support flag.
        """

        async def delay() -> None:
            if write_delay > 0:
                await asyncio.sleep(write_delay)

        deadline = Deadline.from_timeout(timeout)

        try:
            if manage_reporting:
                await self.set_reporting_state(enabled=False)
                await delay()

            gateway = await self.get_gateway_info(timeout=deadline.remaining())
            await delay()

            if setback_enable is not None:
                fields = EnableFields(setback_enable)
                await self.update_response(
                    TrpcMethod.SETBACK_ENABLE,
                    match_fields=fields,
                    timeout=deadline.remaining(),
                    fields=fields,
                )
                await delay()

            addresses = await self.get_inventory_addresses(timeout=deadline.remaining())
            devices: list[DiscoveredDevice] = []
            for address in addresses:
                devices.append(
                    await self.get_device_info(address, timeout=deadline.remaining()),
                )
                await delay()

            return DiscoveryResult(gateway=gateway, devices=tuple(devices))
        finally:
            if manage_reporting:
                await self.set_reporting_state(enabled=True)

    async def dump_available_info(
        self,
        *,
        timeout: float | None = 2,
        inventory_timeout: float | None = 10,
        include_setbacks: bool = True,
        include_datetime: bool = True,
        write_delay: float = 0.1,
    ) -> AvailableInfo:
        """Request a broad read-only snapshot of gateway and attached device data.

        Missing or unsupported values are returned as None. This method does not
        alter reporting or setback settings.
        """
        discovery = await self.discover(
            timeout=inventory_timeout,
            write_delay=write_delay,
        )
        return await self.poll_runtime(
            discovery,
            timeout=timeout,
            include_setbacks=include_setbacks,
            include_datetime=include_datetime,
            include_setpoint_groups=True,
            write_delay=write_delay,
        )

    async def poll_runtime(
        self,
        discovery: DiscoveryResult,
        *,
        timeout: float | None = 2,
        include_setbacks: bool = False,
        include_datetime: bool = False,
        include_setpoint_groups: bool = True,
        write_delay: float = 0.1,
    ) -> AvailableInfo:
        """Poll runtime values for an existing discovery result.

        This is the Home Assistant-friendly polling path: inventory and stable
        device metadata are expected to come from `discover()`, while this method
        refreshes values that can change at runtime.
        """
        gateway = await self.poll_gateway(
            gateway_info=discovery.gateway,
            timeout=timeout,
            include_datetime=include_datetime,
            include_setpoint_groups=include_setpoint_groups,
            write_delay=write_delay,
        )
        devices = await self.poll_devices(
            discovery.devices,
            protocol_version=discovery.gateway.protocol_version,
            timeout=timeout,
            include_setbacks=include_setbacks,
            write_delay=write_delay,
        )
        return AvailableInfo(gateway=gateway, devices=devices)

    async def poll_gateway(
        self,
        *,
        gateway_info: GatewayInfo | None = None,
        timeout: float | None = 2,
        include_datetime: bool = False,
        include_setpoint_groups: bool = True,
        write_delay: float = 0.1,
    ) -> GatewaySnapshot:
        """Poll gateway runtime values without walking device inventory."""

        async def delay() -> None:
            if write_delay > 0:
                await asyncio.sleep(write_delay)

        async def request_field(
            method: TrpcMethodId,
            field: str,
            *,
            match_address: int | None = None,
            match_fields: TrpcFieldsInput | None = None,
            fields: TrpcFieldsInput | None = None,
        ) -> int | None:
            message = await self.request_response(
                method,
                match_address=match_address,
                match_fields=match_fields,
                timeout=timeout,
                fields=fields,
            )
            await delay()
            return message.get(field) if message is not None else None

        info = gateway_info
        if info is None:
            info = await self.get_gateway_info(timeout=timeout)
            await delay()

        reporting = await request_field(TrpcMethod.REPORTING_STATE, "state")
        setback = await request_field(TrpcMethod.SETBACK_ENABLE, "enable")
        return GatewaySnapshot(
            info=info,
            outdoor_temp=await request_field(TrpcMethod.OUTDOOR_TEMP, "temp"),
            network_error=await request_field(TrpcMethod.NETWORK_ERROR, "error"),
            date_time=(
                await self.get_datetime(timeout=timeout, delay=delay)
                if include_datetime
                else None
            ),
            reporting_enabled=None if reporting is None else bool(reporting),
            setback_enabled=None if setback is None else bool(setback),
            setpoint_groups=(
                await self._dump_setpoint_groups(timeout=timeout, delay=delay)
                if include_setpoint_groups
                else None
            ),
        )

    async def poll_devices(
        self,
        devices: Sequence[DiscoveredDevice],
        *,
        protocol_version: int | None,
        timeout: float | None = 2,
        include_setbacks: bool = False,
        write_delay: float = 0.1,
    ) -> tuple[DeviceSnapshot, ...]:
        """Poll runtime values for known devices without rediscovering inventory."""

        async def delay() -> None:
            if write_delay > 0:
                await asyncio.sleep(write_delay)

        snapshots = [
            await self._dump_device_snapshot(
                device,
                protocol_version=protocol_version,
                include_setbacks=include_setbacks,
                timeout=timeout,
                delay=delay,
            )
            for device in devices
        ]
        return tuple(snapshots)

    async def _dump_setpoint_groups(
        self,
        *,
        timeout: float | None,
        delay: Callable[[], Awaitable[None]],
    ) -> dict[int, bool | None]:
        groups: dict[int, bool | None] = {}
        for group_id in range(1, 13):
            message = await self.request_response(
                TrpcMethod.SETPOINT_GROUP_ENABLE,
                match_fields=SetpointGroupFields(group_id).match_fields(),
                timeout=timeout,
                fields=SetpointGroupFields(group_id),
            )
            await delay()
            value = message.get("enable") if message is not None else None
            groups[group_id] = None if value is None else bool(value)
        return groups

    async def _dump_device_snapshot(
        self,
        device: DiscoveredDevice,
        *,
        protocol_version: int | None,
        include_setbacks: bool,
        timeout: float | None,
        delay: Callable[[], Awaitable[None]],
    ) -> DeviceSnapshot:
        async def request_field(
            method: TrpcMethodId,
            field: str,
            fields: TrpcFieldsInput,
        ) -> int | None:
            message = await self.request_response(
                method,
                match_address=device.address,
                timeout=timeout,
                fields=fields,
            )
            await delay()
            return message.get(field) if message is not None else None

        available_values = {
            DeviceValue.CURRENT_TEMPERATURE,
            DeviceValue.ACTIVE_DEMAND,
            DeviceValue.SETBACK_STATE,
        }
        current_temperature = await request_field(
            TrpcMethod.CURRENT_TEMPERATURE,
            "temp",
            AddressFields(device.address),
        )
        active_demand = await request_field(
            TrpcMethod.ACTIVE_DEMAND,
            "demand",
            AddressFields(device.address),
        )
        setback_state = await request_field(
            TrpcMethod.SETBACK_STATE,
            "setback",
            AddressFields(device.address),
        )
        active_setback = setback_from_value(setback_state)
        current_floor_temperature: int | None = None
        mode_setting: int | None = None
        relative_humidity: int | None = None
        humidity_setpoint_min: int | None = None
        humidity_setpoint_max: int | None = None
        heat_setpoints = SetpointValues(active_setback=active_setback)
        cool_setpoints = SetpointValues(active_setback=active_setback)
        slab_setpoints = SetpointValues(active_setback=active_setback)
        fan_percent = SetpointValues(active_setback=active_setback)
        setpoint_targets = SetpointValues(active_setback=active_setback)

        if protocol_version == _PROTOCOL_VERSION_RUNTIME_EXTENSIONS:
            available_values.add(DeviceValue.CURRENT_FLOOR_TEMPERATURE)
            current_floor_temperature = await request_field(
                TrpcMethod.CURRENT_FLOOR_TEMPERATURE,
                "temp",
                AddressFields(device.address),
            )

        if device.supports_mode_setting:
            available_values.add(DeviceValue.MODE_SETTING)
            mode_setting = await request_field(
                TrpcMethod.MODE_SETTING,
                "mode",
                AddressFields(device.address),
            )

        if device.supports_heat_setpoint:
            available_values.add(DeviceValue.HEAT_SETPOINTS)
            heat_setpoints = await self._dump_setpoints(
                TrpcMethod.HEAT_SETPOINT,
                "setpoint",
                device.address,
                active_setback=active_setback,
                include_setbacks=include_setbacks,
                timeout=timeout,
                delay=delay,
            )

        if device.supports_cool_setpoint:
            available_values.add(DeviceValue.COOL_SETPOINTS)
            cool_setpoints = await self._dump_setpoints(
                TrpcMethod.COOL_SETPOINT,
                "setpoint",
                device.address,
                active_setback=active_setback,
                include_setbacks=include_setbacks,
                timeout=timeout,
                delay=delay,
            )

        if device.supports_slab_setpoint:
            available_values.add(DeviceValue.SLAB_SETPOINTS)
            slab_setpoints = await self._dump_setpoints(
                TrpcMethod.SLAB_SETPOINT,
                "setpoint",
                device.address,
                active_setback=active_setback,
                include_setbacks=include_setbacks,
                timeout=timeout,
                delay=delay,
            )

        if device.supports_fan_percent:
            available_values.add(DeviceValue.FAN_PERCENT)
            fan_percent = await self._dump_setpoints(
                TrpcMethod.FAN_PERCENT,
                "percent",
                device.address,
                active_setback=active_setback,
                include_setbacks=include_setbacks,
                timeout=timeout,
                delay=delay,
            )

        if device.supports_humidity and protocol_version in {2, 3}:
            available_values.update(
                {
                    DeviceValue.RELATIVE_HUMIDITY,
                    DeviceValue.HUMIDITY_SETPOINT_MIN,
                    DeviceValue.HUMIDITY_SETPOINT_MAX,
                },
            )
            relative_humidity = await request_field(
                TrpcMethod.RELATIVE_HUMIDITY,
                "percent",
                AddressFields(device.address),
            )
            humidity_setpoint_min = await request_field(
                TrpcMethod.HUMIDITY_SET_MIN,
                "percent",
                AddressFields(device.address),
            )
            humidity_setpoint_max = await request_field(
                TrpcMethod.HUMIDITY_SET_MAX,
                "percent",
                AddressFields(device.address),
            )

        if device.supports_setpoint_device:
            available_values.add(DeviceValue.SETPOINT_TARGETS)
            setpoint_targets = await self._dump_setpoints(
                TrpcMethod.SETPOINT_DEVICE,
                "temp",
                device.address,
                active_setback=active_setback,
                include_setbacks=include_setbacks,
                timeout=timeout,
                delay=delay,
            )

        return DeviceSnapshot(
            DeviceRuntime.create(
                info=device,
                available_values=frozenset(available_values),
                current_temperature=current_temperature,
                current_floor_temperature=current_floor_temperature,
                active_demand=active_demand,
                setback_state=setback_state,
                mode_setting=mode_setting,
                relative_humidity=relative_humidity,
                humidity_setpoint_min=humidity_setpoint_min,
                humidity_setpoint_max=humidity_setpoint_max,
                setpoint_targets=setpoint_targets,
                heat_setpoints=heat_setpoints,
                cool_setpoints=cool_setpoints,
                slab_setpoints=slab_setpoints,
                fan_percent=fan_percent,
            ),
        )

    async def _dump_setpoints(
        self,
        method: TrpcMethodId,
        field: str,
        address: int,
        *,
        active_setback: ThaSetback | None = None,
        include_setbacks: bool,
        timeout: float | None,
        delay: Callable[[], Awaitable[None]],
    ) -> SetpointValues:
        setbacks = tuple(ThaSetback) if include_setbacks else (ThaSetback.CURRENT,)
        values: dict[ThaSetback, int | None] = {}
        observed_active_setback = active_setback
        current_observed_setback: ThaSetback | None = None
        for setback in setbacks:
            fields = AddressSetbackFields(address, setback)
            match_fields = (
                None if setback is ThaSetback.CURRENT else fields.match_fields()
            )
            message = await self.request_response(
                method,
                match_address=address,
                match_fields=match_fields,
                timeout=timeout,
                fields=fields,
            )
            await delay()
            value = message.get(field) if message is not None else None
            if setback is ThaSetback.CURRENT and message is not None:
                current_observed_setback = (
                    setback_from_value(message.get("setback"))
                    or observed_active_setback
                )
                observed_active_setback = (
                    current_observed_setback or observed_active_setback
                )
                if current_observed_setback is not None:
                    values[current_observed_setback] = value
            values[setback] = value
        return SetpointValues(
            values,
            observed_active_setback,
            current_observed_setback,
        )

    async def request_outdoor_temperature(self) -> TrpcPacket:
        """Request the gateway outdoor temperature."""
        return await self.request(TrpcMethod.OUTDOOR_TEMP)

    async def get_datetime(
        self,
        *,
        timeout: float | None = 5,
        delay: Callable[[], Awaitable[None]] | None = None,
    ) -> GatewayDateTime | None:
        """Request the gateway's current date/time fields."""
        message = await self.request_response(TrpcMethod.DATE_TIME, timeout=timeout)
        if delay is not None:
            await delay()
        if message is None:
            return None
        return GatewayDateTime(
            year=message.require("year"),
            month=message.require("month"),
            day=message.require("day"),
            weekday=message.require("weekday"),
            hour=message.require("hour"),
            minute=message.require("minute"),
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
    ) -> TrpcPacket | None:
        """Enable or disable a setpoint group."""
        fields = SetpointGroupFields(group_id, enabled)
        return await self.update_response(
            TrpcMethod.SETPOINT_GROUP_ENABLE,
            match_fields=fields,
            timeout=timeout,
            fields=fields,
        )

    async def set_datetime(
        self,
        value: datetime | None = None,
        *,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Update the gateway date/time."""
        value = value or datetime.now().astimezone()
        fields = DateTimeFields.from_datetime(value)
        return await self.update_response(
            TrpcMethod.DATE_TIME,
            match_fields=fields,
            timeout=timeout,
            fields=fields,
        )

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
        if confirm:
            return await self.coalesced_update_response(
                TrpcMethod.MODE_SETTING,
                coalesce_key=payload.coalesce_key(TrpcMethod.MODE_SETTING),
                match_address=payload.address,
                match_fields=payload,
                timeout=timeout,
                fields=payload,
            )
        return await self.coalesced_update(
            TrpcMethod.MODE_SETTING,
            coalesce_key=payload.coalesce_key(TrpcMethod.MODE_SETTING),
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
            match_fields=payload,
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
        if confirm:
            return await self.coalesced_update_response(
                method,
                coalesce_key=payload.coalesce_key(method),
                match_address=payload.address,
                match_fields=payload,
                timeout=timeout,
                fields=payload,
            )
        return await self.coalesced_update(
            method,
            coalesce_key=payload.coalesce_key(method),
            fields=payload,
        )

    @staticmethod
    def _validate_percent(value: int) -> None:
        if not _MIN_PERCENT <= value <= _MAX_PERCENT:
            msg = f"percent must be between {_MIN_PERCENT} and {_MAX_PERCENT}: {value}"
            raise ValueError(msg)
