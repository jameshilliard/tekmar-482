"""Runtime state reducer for tekmar 482 report messages."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum, auto
from typing import TYPE_CHECKING

from .constants import ThaSetback, setback_event_count
from .exceptions import ProtocolError
from .models import (
    AvailableInfo,
    DeviceAttributes,
    DeviceRuntime,
    DeviceSnapshot,
    DeviceValue,
    DiscoveredDevice,
    DiscoveryResult,
    SetpointValues,
    setback_from_value,
)
from .trpc import (
    TrpcCommand,
    TrpcMethod,
    TrpcPacket,
    TrpcService,
    method_name,
    response_service_for,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

RUNTIME_SERVICES = {
    TrpcService.REPORT,
    TrpcService.RESPONSE_REQUEST,
    TrpcService.RESPONSE_UPDATE,
}

GATEWAY_VERSION_METHODS = {TrpcMethod.FIRMWARE_REVISION, TrpcMethod.PROTOCOL_VERSION}
GATEWAY_METHODS = {
    *GATEWAY_VERSION_METHODS,
    TrpcMethod.NETWORK_ERROR,
    TrpcMethod.OUTDOOR_TEMP,
    TrpcMethod.REPORTING_STATE,
    TrpcMethod.SETBACK_ENABLE,
    TrpcMethod.SETPOINT_GROUP_ENABLE,
}

DEVICE_METADATA_FIELDS = {
    TrpcMethod.DEVICE_ATTRIBUTES: "attributes",
    TrpcMethod.SETBACK_EVENTS: "events",
}


@dataclass(frozen=True, slots=True)
class DeviceField:
    key: DeviceValue
    wire_name: str
    indexed: bool = False


# In poll order: in particular SetbackState precedes any CURRENT requests.
DEVICE_FIELDS = {
    TrpcMethod.CURRENT_TEMPERATURE: DeviceField(
        DeviceValue.CURRENT_TEMPERATURE,
        "temp",
    ),
    TrpcMethod.ACTIVE_DEMAND: DeviceField(DeviceValue.ACTIVE_DEMAND, "demand"),
    TrpcMethod.SETBACK_STATE: DeviceField(DeviceValue.SETBACK_STATE, "setback"),
    TrpcMethod.CURRENT_FLOOR_TEMPERATURE: DeviceField(
        DeviceValue.CURRENT_FLOOR_TEMPERATURE,
        "temp",
    ),
    TrpcMethod.MODE_SETTING: DeviceField(DeviceValue.MODE_SETTING, "mode"),
    TrpcMethod.HEAT_SETPOINT: DeviceField(
        DeviceValue.HEAT_SETPOINTS,
        "setpoint",
        indexed=True,
    ),
    TrpcMethod.COOL_SETPOINT: DeviceField(
        DeviceValue.COOL_SETPOINTS,
        "setpoint",
        indexed=True,
    ),
    TrpcMethod.SLAB_SETPOINT: DeviceField(
        DeviceValue.SLAB_SETPOINTS,
        "setpoint",
        indexed=True,
    ),
    TrpcMethod.FAN_PERCENT: DeviceField(
        DeviceValue.FAN_PERCENT,
        "percent",
        indexed=True,
    ),
    TrpcMethod.RELATIVE_HUMIDITY: DeviceField(DeviceValue.RELATIVE_HUMIDITY, "percent"),
    TrpcMethod.HUMIDITY_SET_MIN: DeviceField(
        DeviceValue.HUMIDITY_SETPOINT_MIN,
        "percent",
    ),
    TrpcMethod.HUMIDITY_SET_MAX: DeviceField(
        DeviceValue.HUMIDITY_SETPOINT_MAX,
        "percent",
    ),
    TrpcMethod.SETPOINT_DEVICE: DeviceField(
        DeviceValue.SETPOINT_TARGETS,
        "temp",
        indexed=True,
    ),
}
DEVICE_VALUE_METHODS = set(DEVICE_FIELDS)
SETPOINT_METHODS = {
    method: (field.key, field.wire_name)
    for method, field in DEVICE_FIELDS.items()
    if field.indexed
}


class MessageApplyResult(Enum):
    """Result of applying a tRPC message to runtime state."""

    UNSUPPORTED = auto()
    UNCHANGED = auto()
    CHANGED = auto()


def is_runtime_message(message: TrpcPacket) -> bool:
    """Return whether a message can carry runtime state."""
    return message.service in RUNTIME_SERVICES and message.method is not None


def is_topology_message(message: TrpcPacket) -> bool:
    """Return whether a message indicates device inventory may have changed."""
    return (
        message.service is TrpcService.REPORT
        and message.method is TrpcMethod.TAKING_ADDRESS
    )


def device_supports_method(device: DiscoveredDevice, method: TrpcMethod) -> bool:
    """Return whether a discovered device supports a writable/readable method."""
    if method == TrpcMethod.HEAT_SETPOINT:
        return device.supports_heat_setpoint
    if method == TrpcMethod.COOL_SETPOINT:
        return device.supports_cool_setpoint
    if method == TrpcMethod.SLAB_SETPOINT:
        return device.supports_slab_setpoint
    if method == TrpcMethod.CURRENT_FLOOR_TEMPERATURE:
        return device.attributes.slab_setpoint
    if method == TrpcMethod.FAN_PERCENT:
        return device.supports_fan_percent
    if method == TrpcMethod.RELATIVE_HUMIDITY:
        return device.supports_relative_humidity
    if method == TrpcMethod.HUMIDITY_SET_MIN:
        return device.attributes.humidity_setpoint_min
    if method == TrpcMethod.HUMIDITY_SET_MAX:
        return device.attributes.humidity_setpoint_max
    if method == TrpcMethod.SETPOINT_DEVICE:
        return device.supports_setpoint_device
    if method == TrpcMethod.MODE_SETTING:
        return device.supports_mode_setting
    return method in {
        TrpcMethod.ACTIVE_DEMAND,
        TrpcMethod.CURRENT_TEMPERATURE,
        TrpcMethod.SETBACK_STATE,
    }


def require_device_support(device: DiscoveredDevice, method: TrpcMethod) -> None:
    """Raise when a discovered device does not support a protocol method."""
    if device_supports_method(device, method):
        return
    msg = (
        f"device {device.address} does not support {method_name(method) or method.name}"
    )
    raise ProtocolError(msg)


class Tekmar482State:
    """Mutable runtime state for a discovered tekmar 482 topology."""

    def __init__(self, discovery: DiscoveryResult, data: AvailableInfo) -> None:
        self.data = data
        self.discovery = self._snapshot_metadata(discovery)

    def _snapshot_metadata(self, discovery: DiscoveryResult) -> DiscoveryResult:
        updates = {s.info.address: s.info for s in self.data.devices}
        devices = tuple(updates.get(d.address, d) for d in discovery.devices)
        return (
            discovery
            if devices == discovery.devices
            and self.data.gateway.info == discovery.gateway
            else replace(discovery, devices=devices, gateway=self.data.gateway.info)
        )

    @property
    def discovery_devices(self) -> dict[int, DiscoveredDevice]:
        """Return discovered devices keyed by tHA address."""
        return {device.address: device for device in self.discovery.devices}

    @property
    def device_snapshots(self) -> dict[int, DeviceSnapshot]:
        """Return latest device snapshots keyed by tHA address."""
        return {snapshot.info.address: snapshot for snapshot in self.data.devices}

    @property
    def device_runtime(self) -> dict[int, DeviceRuntime]:
        """Return typed device runtime views keyed by tHA address."""
        return self.data.runtime.device_map

    def device(self, address: int) -> DiscoveredDevice | None:
        """Return a discovered device by tHA address."""
        return self.discovery_devices.get(address)

    def runtime(self, address: int) -> DeviceRuntime | None:
        """Return typed runtime values for one device address."""
        return self.device_runtime.get(address)

    def require_device_support(self, address: int, method: TrpcMethod) -> None:
        """Raise when an address is unknown or does not support a method."""
        device = self.device(address)
        if device is None:
            msg = f"unknown tekmar device address: {address}"
            raise ProtocolError(msg)
        require_device_support(device, method)

    def apply_message(self, message: TrpcPacket) -> bool:
        """Apply a report/response message to state.

        Returns True when the stored snapshot changed.
        """
        return self.apply_message_result(message) is MessageApplyResult.CHANGED

    def expire_setpoint_group(self, group_id: int) -> bool:
        """Forget a previously enabled group that the gateway did not reconfirm."""
        groups = self.data.gateway.setpoint_groups
        if not groups or groups.get(group_id) is not True:
            return False
        gateway = replace(self.data.gateway, setpoint_groups={**groups, group_id: None})
        self.data = replace(self.data, gateway=gateway)
        return True

    def invalidate_devices(self, addresses: set[int]) -> bool:
        """Discard occupant-specific readings without removing discovery metadata."""
        if not addresses:
            return False
        devices = tuple(
            DeviceSnapshot(
                DeviceRuntime.create(
                    info=s.info,
                    available_values=s.runtime.available_values,
                ),
            )
            if s.info.address in addresses
            else s
            for s in self.data.devices
        )
        if self.data.devices == devices:
            return False
        self.data = replace(self.data, devices=devices)
        return True

    def apply_message_result(
        self,
        message: TrpcPacket,
        command: TrpcCommand | None = None,
    ) -> MessageApplyResult:
        """Apply a report/response message and return the reducer outcome."""
        updated = apply_message(self.discovery, self.data, message, command)
        if updated is None:
            return MessageApplyResult.UNSUPPORTED
        if updated == self.data:
            return MessageApplyResult.UNCHANGED
        self.data = updated
        if (
            message.method in DEVICE_METADATA_FIELDS
            or message.method in GATEWAY_VERSION_METHODS
        ):
            self.discovery = self._snapshot_metadata(self.discovery)
        return MessageApplyResult.CHANGED


def _retain_device(empty: DeviceRuntime, prior: DeviceRuntime | None) -> DeviceRuntime:
    if prior is None or empty.info.type_code is None or prior.info != empty.info:
        return empty
    empty = empty.with_active_setback(setback_from_value(prior.setback_state))
    retained = {}
    for key in empty.available_values & prior.available_values:
        value = getattr(prior, key.value)
        if isinstance(value, SetpointValues):
            # A broader scan can declare additional slots before any reply arrives.
            planned: SetpointValues = getattr(empty, key.value)
            value = replace(value, values={**planned.values, **value.values})
        retained[key.value] = value
    return replace(empty, **retained)


def reconcile_runtime(
    discovery: DiscoveryResult,
    empty: AvailableInfo,
    previous: AvailableInfo | None,
    observations: Iterable[tuple[TrpcPacket, TrpcCommand | None]],
) -> AvailableInfo:
    """Replay wire observations over the scan shape and any same-session values."""
    data = empty
    if previous is not None:
        groups = {
            **(empty.gateway.setpoint_groups or {}),
            **(previous.gateway.setpoint_groups or {}),
        }
        prior = {s.info.address: s.runtime for s in previous.devices}
        data = AvailableInfo(
            replace(
                previous.gateway,
                info=empty.gateway.info,
                setpoint_groups=groups or None,
            ),
            tuple(
                DeviceSnapshot(_retain_device(s.runtime, prior.get(s.info.address)))
                for s in empty.devices
            ),
        )
    for message, command in observations:
        updated = apply_message(discovery, data, message, command)
        if updated is not None:
            data = updated
    return data


def apply_message(
    discovery: DiscoveryResult,
    data: AvailableInfo,
    message: TrpcPacket,
    command: TrpcCommand | None = None,
) -> AvailableInfo | None:
    """Return updated runtime data after applying one tRPC message."""
    if not is_runtime_message(message):
        return None

    method = message.method
    if method is None:
        return None

    if method in GATEWAY_METHODS:
        return _apply_gateway_message(data, method, message)

    if method in DEVICE_METADATA_FIELDS:
        address = message.get("address")
        raw = message.get(DEVICE_METADATA_FIELDS[method])
        if raw is None or not any(s.info.address == address for s in data.devices):
            return None
        return replace(
            data,
            devices=tuple(
                _update_metadata(s, method, raw) if s.info.address == address else s
                for s in data.devices
            ),
        )

    if method not in DEVICE_VALUE_METHODS:
        return None

    address = message.get("address")
    if address is None or not any(
        device.address == address for device in discovery.devices
    ):
        return None

    return _apply_device_message(discovery, data, address, method, message, command)


def _update_metadata(
    snapshot: DeviceSnapshot,
    method: TrpcMethod,
    raw: int,
) -> DeviceSnapshot:
    runtime = snapshot.runtime
    if method is TrpcMethod.SETBACK_EVENTS:
        info = replace(snapshot.info, setback_events=setback_event_count(raw))
        return DeviceSnapshot(replace(runtime, info=info))
    if snapshot.info.attributes.raw == raw:
        return snapshot
    info = replace(snapshot.info, attributes=DeviceAttributes(raw))
    supported = {
        field.key
        for candidate, field in DEVICE_FIELDS.items()
        if device_supports_method(info, candidate)
    }
    # Capability evidence can change the runtime subtype without an address move.
    updated = DeviceRuntime.create(
        info=info,
        available_values=runtime.available_values & supported,
    )
    return DeviceSnapshot(
        replace(
            updated,
            **{key.value: getattr(runtime, key.value) for key in supported},
        ),
    )


def _apply_gateway_message(
    data: AvailableInfo,
    method: TrpcMethod,
    message: TrpcPacket,
) -> AvailableInfo | None:
    if method in GATEWAY_VERSION_METHODS:
        if message.service is TrpcService.RESPONSE_UPDATE:
            return None
        if method is TrpcMethod.FIRMWARE_REVISION:
            revision = message.get("revision")
            if revision is None:
                return None
            info = replace(data.gateway.info, firmware_revision=revision)
        else:
            version = message.get("version")
            if version is None:
                return None
            info = replace(data.gateway.info, protocol_version=version)
        return replace(data, gateway=replace(data.gateway, info=info))
    if method == TrpcMethod.REPORTING_STATE:
        state = message.get("state")
        if state not in (0, 1):
            return None
        if message.service is TrpcService.REPORT:
            # The Test task samples the physical DIP, not reporting_get_enabled().
            return replace(
                data,
                gateway=replace(data.gateway, reporting_dip_on=bool(state)),
            )
        return replace(
            data,
            gateway=replace(data.gateway, reporting_enabled=bool(state)),
        )
    if method == TrpcMethod.NETWORK_ERROR:
        return replace(
            data,
            gateway=replace(data.gateway, network_error=message.get("error")),
        )
    if method == TrpcMethod.OUTDOOR_TEMP:
        return replace(
            data,
            gateway=replace(data.gateway, outdoor_temp=message.get("temp")),
        )
    if method == TrpcMethod.SETBACK_ENABLE:
        enable = message.get("enable")
        return replace(
            data,
            gateway=replace(
                data.gateway,
                setback_enabled=bool(enable) if enable in (0, 1) else None,
            ),
        )
    if method == TrpcMethod.SETPOINT_GROUP_ENABLE:
        group_id = message.get("group_id")
        enable = message.get("enable")
        if group_id is None or group_id not in range(1, 13):
            return None
        groups = dict(data.gateway.setpoint_groups or {})
        groups[group_id] = bool(enable) if enable in (0, 1) else None
        return replace(
            data,
            gateway=replace(data.gateway, setpoint_groups=groups),
        )
    return None


def _apply_device_message(
    discovery: DiscoveryResult,
    data: AvailableInfo,
    address: int,
    method: TrpcMethod,
    message: TrpcPacket,
    command: TrpcCommand | None,
) -> AvailableInfo | None:
    snapshots = {snapshot.info.address: snapshot for snapshot in data.devices}
    snapshot = snapshots.get(address)
    if snapshot is None:
        return None

    runtime = snapshot.runtime
    if (
        method is TrpcMethod.CURRENT_FLOOR_TEMPERATURE
        and not runtime.info.attributes.slab_setpoint
    ):
        return None
    field = DEVICE_FIELDS[method]
    if field.indexed:
        updated_runtime = _apply_setpoint_report(
            runtime,
            method,
            message,
            command,
            setbacks_enabled=data.gateway.setback_enabled,
        )
        if updated_runtime is None:
            return None
        runtime = updated_runtime
    else:
        runtime = runtime.with_scalar(field.key, message.get(field.wire_name))

    snapshots[address] = DeviceSnapshot(runtime)
    return replace(
        data,
        devices=tuple(
            snapshots[device.address]
            for device in discovery.devices
            if device.address in snapshots
        ),
    )


def _apply_setpoint_report(
    runtime: DeviceRuntime,
    method: TrpcMethod,
    message: TrpcPacket,
    command: TrpcCommand | None,
    *,
    setbacks_enabled: bool | None,
) -> DeviceRuntime | None:
    field = DEVICE_FIELDS[method]
    setback = setback_from_value(message.get("setback"))
    if setback is None:
        return None
    raw = message.get(field.wire_name)
    if command is None or (
        command.method != method
        or command.fields.get("address") != message.get("address")
        or command.fields.get("setback") not in (ThaSetback.CURRENT, setback)
        or message.service != response_service_for(command.service)
    ):
        return runtime.with_setpoint(field.key, setback, raw)
    requested_current = command.fields.get("setback") == ThaSetback.CURRENT
    if requested_current and setback is not ThaSetback.CURRENT:
        # FW06 resolves CURRENT from the same device byte as SetbackState.
        runtime = runtime.with_scalar(DeviceValue.SETBACK_STATE, int(setback))
    return runtime.with_setpoint(
        field.key,
        setback,
        raw,
        requested_current=requested_current,
        confirmed_write=command.service == TrpcService.UPDATE
        and raw == command.fields.get(field.wire_name),
        setbacks_enabled=setbacks_enabled,
    )
