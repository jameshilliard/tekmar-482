"""Runtime state reducer for tekmar 482 report messages."""

from __future__ import annotations

from dataclasses import replace

from .exceptions import ProtocolError
from .models import (
    AvailableInfo,
    DeviceRuntime,
    DeviceSnapshot,
    DeviceValue,
    DiscoveredDevice,
    DiscoveryResult,
    GatewayDateTime,
    setback_from_value,
)
from .trpc import TrpcMethod, TrpcPacket, TrpcService, method_name

RUNTIME_SERVICES = {
    TrpcService.REPORT,
    TrpcService.RESPONSE_REQUEST,
    TrpcService.RESPONSE_UPDATE,
}

GATEWAY_METHODS = {
    TrpcMethod.DATE_TIME,
    TrpcMethod.NETWORK_ERROR,
    TrpcMethod.OUTDOOR_TEMP,
    TrpcMethod.REPORTING_STATE,
    TrpcMethod.SETBACK_ENABLE,
    TrpcMethod.SETPOINT_GROUP_ENABLE,
}

DEVICE_VALUE_METHODS = {
    TrpcMethod.ACTIVE_DEMAND,
    TrpcMethod.COOL_SETPOINT,
    TrpcMethod.CURRENT_FLOOR_TEMPERATURE,
    TrpcMethod.CURRENT_TEMPERATURE,
    TrpcMethod.FAN_PERCENT,
    TrpcMethod.HEAT_SETPOINT,
    TrpcMethod.HUMIDITY_SET_MAX,
    TrpcMethod.HUMIDITY_SET_MIN,
    TrpcMethod.MODE_SETTING,
    TrpcMethod.RELATIVE_HUMIDITY,
    TrpcMethod.SETBACK_STATE,
    TrpcMethod.SETPOINT_DEVICE,
    TrpcMethod.SLAB_SETPOINT,
}

SETPOINT_METHODS = {
    TrpcMethod.COOL_SETPOINT: (DeviceValue.COOL_SETPOINTS, "setpoint"),
    TrpcMethod.FAN_PERCENT: (DeviceValue.FAN_PERCENT, "percent"),
    TrpcMethod.HEAT_SETPOINT: (DeviceValue.HEAT_SETPOINTS, "setpoint"),
    TrpcMethod.SETPOINT_DEVICE: (DeviceValue.SETPOINT_TARGETS, "temp"),
    TrpcMethod.SLAB_SETPOINT: (DeviceValue.SLAB_SETPOINTS, "setpoint"),
}

SCALAR_METHODS = {
    TrpcMethod.ACTIVE_DEMAND: (DeviceValue.ACTIVE_DEMAND, "demand"),
    TrpcMethod.CURRENT_FLOOR_TEMPERATURE: (
        DeviceValue.CURRENT_FLOOR_TEMPERATURE,
        "temp",
    ),
    TrpcMethod.CURRENT_TEMPERATURE: (DeviceValue.CURRENT_TEMPERATURE, "temp"),
    TrpcMethod.HUMIDITY_SET_MAX: (DeviceValue.HUMIDITY_SETPOINT_MAX, "percent"),
    TrpcMethod.HUMIDITY_SET_MIN: (DeviceValue.HUMIDITY_SETPOINT_MIN, "percent"),
    TrpcMethod.MODE_SETTING: (DeviceValue.MODE_SETTING, "mode"),
    TrpcMethod.RELATIVE_HUMIDITY: (DeviceValue.RELATIVE_HUMIDITY, "percent"),
    TrpcMethod.SETBACK_STATE: (DeviceValue.SETBACK_STATE, "setback"),
}


def is_runtime_message(message: TrpcPacket) -> bool:
    """Return whether a message can carry runtime state."""
    return message.service in RUNTIME_SERVICES and message.method is not None


def is_topology_message(message: TrpcPacket) -> bool:
    """Return whether a message indicates device inventory may have changed."""
    return is_runtime_message(message) and message.method == TrpcMethod.TAKING_ADDRESS


def device_supports_method(device: DiscoveredDevice, method: TrpcMethod) -> bool:
    """Return whether a discovered device supports a writable/readable method."""
    if method == TrpcMethod.HEAT_SETPOINT:
        return device.supports_heat_setpoint
    if method == TrpcMethod.COOL_SETPOINT:
        return device.supports_cool_setpoint
    if method == TrpcMethod.SLAB_SETPOINT:
        return device.supports_slab_setpoint
    if method == TrpcMethod.FAN_PERCENT:
        return device.supports_fan_percent
    if method in {
        TrpcMethod.RELATIVE_HUMIDITY,
        TrpcMethod.HUMIDITY_SET_MIN,
        TrpcMethod.HUMIDITY_SET_MAX,
    }:
        return device.supports_humidity
    if method == TrpcMethod.SETPOINT_DEVICE:
        return device.supports_setpoint_device
    if method == TrpcMethod.MODE_SETTING:
        return device.supports_mode_setting
    return method in {
        TrpcMethod.ACTIVE_DEMAND,
        TrpcMethod.CURRENT_FLOOR_TEMPERATURE,
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
        self.discovery = discovery
        self.data = data

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
        updated = apply_message(self.discovery, self.data, message)
        if updated is None or updated == self.data:
            return False
        self.data = updated
        return True


def apply_message(
    discovery: DiscoveryResult,
    data: AvailableInfo,
    message: TrpcPacket,
) -> AvailableInfo | None:
    """Return updated runtime data after applying one tRPC message."""
    if not is_runtime_message(message):
        return None

    method = message.method
    if method is None:
        return None

    if method in GATEWAY_METHODS:
        return _apply_gateway_message(data, method, message)

    if method not in DEVICE_VALUE_METHODS:
        return None

    address = message.get("address")
    if address is None:
        return None

    devices = {device.address for device in discovery.devices}
    if address not in devices:
        return None

    return _apply_device_message(discovery, data, address, method, message)


def _apply_gateway_message(
    data: AvailableInfo,
    method: TrpcMethod,
    message: TrpcPacket,
) -> AvailableInfo | None:
    if method == TrpcMethod.REPORTING_STATE:
        state = message.get("state")
        return replace(
            data,
            gateway=replace(
                data.gateway,
                reporting_enabled=None if state is None else bool(state),
            ),
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
                setback_enabled=None if enable is None else bool(enable),
            ),
        )
    if method == TrpcMethod.SETPOINT_GROUP_ENABLE:
        group_id = message.get("group_id")
        enable = message.get("enable")
        if group_id is None:
            return None
        groups = dict(data.gateway.setpoint_groups or {})
        groups[group_id] = None if enable is None else bool(enable)
        return replace(
            data,
            gateway=replace(data.gateway, setpoint_groups=groups),
        )
    if method == TrpcMethod.DATE_TIME:
        date_time = _message_datetime(message)
        if date_time is None:
            return None
        return replace(
            data,
            gateway=replace(data.gateway, date_time=date_time),
        )
    return None


def _apply_device_message(
    discovery: DiscoveryResult,
    data: AvailableInfo,
    address: int,
    method: TrpcMethod,
    message: TrpcPacket,
) -> AvailableInfo | None:
    snapshots = {snapshot.info.address: snapshot for snapshot in data.devices}
    snapshot = snapshots.get(address)
    if snapshot is None:
        return None

    runtime = snapshot.runtime
    if method in SCALAR_METHODS:
        value, field = SCALAR_METHODS[method]
        runtime = runtime.with_scalar(value, message.get(field))
    elif method in SETPOINT_METHODS:
        updated_runtime = _apply_setpoint_report(runtime, method, message)
        if updated_runtime is None:
            return None
        runtime = updated_runtime
    else:
        return None

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
) -> DeviceRuntime | None:
    value_key, field = SETPOINT_METHODS[method]
    setback = setback_from_value(message.get("setback"))
    if setback is None:
        return None
    return runtime.with_setpoint(value_key, setback, message.get(field))


def _message_datetime(message: TrpcPacket) -> GatewayDateTime | None:
    year = message.get("year")
    month = message.get("month")
    day = message.get("day")
    weekday = message.get("weekday")
    hour = message.get("hour")
    minute = message.get("minute")
    if (
        year is None
        or month is None
        or day is None
        or weekday is None
        or hour is None
        or minute is None
    ):
        return None
    return GatewayDateTime(
        year=year,
        month=month,
        day=day,
        weekday=weekday,
        hour=hour,
        minute=minute,
    )
