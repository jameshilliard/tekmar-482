"""Protocol-level models for gateway and device discovery."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, Self, assert_never

from .constants import (
    DEVICE_FEATURES,
    DeviceAttributeBit,
    DeviceFeature,
    DeviceKind,
    ThaSetback,
)
from .decoding import (
    DecodedNamedValue,
    DecodedTemperature,
    decode_active_demand,
    decode_dege,
    decode_degh,
    decode_device_mode,
    decode_network_error,
    decode_percent,
    decode_setback,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping


@dataclass(frozen=True, slots=True)
class GatewayDateTime:
    """Date/time fields reported by the 482 gateway."""

    year: int
    month: int
    day: int
    weekday: int
    hour: int
    minute: int

    @classmethod
    def from_mapping(
        cls,
        value: GatewayDateTime | Mapping[str, int],
    ) -> GatewayDateTime:
        """Build an immutable gateway date/time value."""
        if isinstance(value, GatewayDateTime):
            return value
        return cls(
            year=value["year"],
            month=value["month"],
            day=value["day"],
            weekday=value["weekday"],
            hour=value["hour"],
            minute=value["minute"],
        )

    def as_dict(self) -> dict[str, int]:
        """Return a JSON-friendly dictionary."""
        return {
            "year": self.year,
            "month": self.month,
            "day": self.day,
            "weekday": self.weekday,
            "hour": self.hour,
            "minute": self.minute,
        }


class DeviceValue(StrEnum):
    """Canonical runtime value keys exposed for a discovered device."""

    CURRENT_TEMPERATURE = "current_temperature"
    CURRENT_FLOOR_TEMPERATURE = "current_floor_temperature"
    ACTIVE_DEMAND = "active_demand"
    SETBACK_STATE = "setback_state"
    MODE_SETTING = "mode_setting"
    RELATIVE_HUMIDITY = "relative_humidity"
    HUMIDITY_SETPOINT_MIN = "humidity_setpoint_min"
    HUMIDITY_SETPOINT_MAX = "humidity_setpoint_max"
    SETPOINT_TARGETS = "setpoint_targets"
    HEAT_SETPOINTS = "heat_setpoints"
    COOL_SETPOINTS = "cool_setpoints"
    SLAB_SETPOINTS = "slab_setpoints"
    FAN_PERCENT = "fan_percent"

    @classmethod
    def from_key(cls, key: str) -> DeviceValue | None:
        """Return a device value enum from a legacy raw key."""
        try:
            return cls(key)
        except ValueError:
            return None


type _RawSetpointMap = dict[str, int | None]
type _RawSnapshotValue = int | None | _RawSetpointMap


@dataclass(frozen=True, slots=True)
class DeviceAttributes:
    """Decoded DeviceAttributes bit field."""

    raw: int = 0

    def supports(self, bit: DeviceAttributeBit) -> bool:
        """Return whether the raw DeviceAttributes bit is set."""
        return bool(self.raw & bit)

    @property
    def heat_setpoint(self) -> bool:
        return self.supports(DeviceAttributeBit.HEAT_SETPOINT)

    @property
    def cool_setpoint(self) -> bool:
        return self.supports(DeviceAttributeBit.COOL_SETPOINT)

    @property
    def zone_heating(self) -> bool:
        return self.heat_setpoint

    @property
    def zone_cooling(self) -> bool:
        return self.cool_setpoint

    @property
    def slab_setpoint(self) -> bool:
        return self.supports(
            DeviceAttributeBit.SLAB_SETPOINT | DeviceAttributeBit.SLAB_SETPOINT_ALT,
        )

    @property
    def fan_percent(self) -> bool:
        return self.supports(DeviceAttributeBit.FAN_PERCENT)

    @property
    def humidity_setpoint_min(self) -> bool:
        return self.supports(DeviceAttributeBit.HUMIDITY_SET_MIN)

    @property
    def humidity_setpoint_max(self) -> bool:
        return self.supports(DeviceAttributeBit.HUMIDITY_SET_MAX)

    @property
    def humidity(self) -> bool:
        return self.humidity_setpoint_min or self.humidity_setpoint_max

    @property
    def setpoint_device(self) -> bool:
        return self.supports(DeviceAttributeBit.SETPOINT_DEVICE)


@dataclass(frozen=True, slots=True)
class GatewayInfo:
    """Static tekmar 482 gateway metadata."""

    firmware_revision: int | None = None
    protocol_version: int | None = None


@dataclass(frozen=True, slots=True)
class DiscoveredDevice:
    """Device discovered behind a tekmar 482 gateway."""

    address: int
    type_code: int | None = None
    version: int | None = None
    attributes: DeviceAttributes = DeviceAttributes()
    setback_events: int | None = None

    @property
    def feature(self) -> DeviceFeature | None:
        if self.type_code is None:
            return None
        return DEVICE_FEATURES.get(self.type_code)

    @property
    def kind(self) -> DeviceKind | None:
        feature = self.feature
        if feature is not None:
            return feature.kind
        if self.attributes.setpoint_device:
            return DeviceKind.SETPOINT
        if (
            self.supports_heat_setpoint
            or self.supports_cool_setpoint
            or self.supports_slab_setpoint
            or self.supports_fan_percent
            or self.supports_humidity
        ):
            return DeviceKind.THERMOSTAT
        return None

    @property
    def address_parts(self) -> tuple[int, int, int]:
        """Return the tHA port, bus, and device address components."""
        return (
            self.address // 1000,
            (self.address // 100) % 10,
            self.address % 100,
        )

    @property
    def model(self) -> str | None:
        feature = self.feature
        if feature is None:
            return None
        return feature.model

    @property
    def is_known_type(self) -> bool:
        return self.type_code in DEVICE_FEATURES

    @property
    def supports_heat_setpoint(self) -> bool:
        feature = self.feature
        return self.attributes.heat_setpoint or (
            feature is not None and feature.supports_heat
        )

    @property
    def supports_cool_setpoint(self) -> bool:
        feature = self.feature
        return self.attributes.cool_setpoint or (
            feature is not None and feature.supports_cool
        )

    @property
    def supports_slab_setpoint(self) -> bool:
        return self.attributes.slab_setpoint

    @property
    def supports_fan_percent(self) -> bool:
        feature = self.feature
        return self.attributes.fan_percent or (
            feature is not None and feature.supports_fan
        )

    @property
    def supports_humidity(self) -> bool:
        feature = self.feature
        return self.attributes.humidity or (
            feature is not None and feature.supports_humidity
        )

    @property
    def supports_setpoint_device(self) -> bool:
        feature = self.feature
        return self.attributes.setpoint_device or (
            feature is not None and feature.kind is DeviceKind.SETPOINT
        )

    @property
    def supports_mode_setting(self) -> bool:
        return self.kind is DeviceKind.THERMOSTAT and not self.supports_setpoint_device


@dataclass(frozen=True, slots=True)
class DiscoveryResult:
    """Gateway and device inventory returned by discovery."""

    gateway: GatewayInfo
    devices: tuple[DiscoveredDevice, ...]

    @property
    def known_devices(self) -> tuple[DiscoveredDevice, ...]:
        return tuple(device for device in self.devices if device.is_known_type)

    @property
    def unknown_devices(self) -> tuple[DiscoveredDevice, ...]:
        return tuple(device for device in self.devices if not device.is_known_type)


@dataclass(frozen=True, slots=True)
class GatewaySnapshot:
    """Read-only gateway values collected by a dump operation."""

    info: GatewayInfo
    outdoor_temp: int | None = None
    network_error: int | None = None
    date_time: GatewayDateTime | None = None
    reporting_enabled: bool | None = None
    setback_enabled: bool | None = None
    setpoint_groups: Mapping[int, bool | None] | None = None

    def __post_init__(self) -> None:
        """Freeze mutable constructor inputs."""
        if self.date_time is not None:
            object.__setattr__(
                self,
                "date_time",
                GatewayDateTime.from_mapping(self.date_time),
            )
        if self.setpoint_groups is not None:
            object.__setattr__(
                self,
                "setpoint_groups",
                _readonly_mapping(self.setpoint_groups),
            )

    @property
    def decoded_outdoor_temp(self) -> object:
        """Return decoded outdoor temperature metadata."""
        return decode_degh(self.outdoor_temp)

    @property
    def decoded_network_error(self) -> object:
        """Return decoded network error metadata."""
        return decode_network_error(self.network_error)

    @property
    def runtime(self) -> GatewayRuntime:
        """Return a typed runtime view of this gateway snapshot."""
        return GatewayRuntime.from_snapshot(self)


@dataclass(frozen=True, slots=True, init=False)
class DeviceSnapshot:
    """Read-only device values collected by a dump operation."""

    _runtime: DeviceRuntime

    def __init__(
        self,
        runtime: DeviceRuntime,
    ) -> None:
        """Create a snapshot from canonical runtime data."""
        object.__setattr__(self, "_runtime", runtime)

    @classmethod
    def from_raw_values(
        cls,
        info: DiscoveredDevice,
        values: Mapping[str, _RawSnapshotValue],
    ) -> DeviceSnapshot:
        """Create a snapshot from a legacy raw value mapping."""
        return cls(DeviceRuntime.from_raw_values(info, values))

    @property
    def info(self) -> DiscoveredDevice:
        """Return discovered metadata for this device."""
        return self._runtime.info

    @property
    def values(self) -> dict[str, _RawSnapshotValue]:
        """Return JSON-friendly raw values keyed by legacy names."""
        return self._runtime.raw_values

    @property
    def decoded(self) -> DecodedDeviceRuntime:
        """Return a typed decoded runtime view."""
        return self._runtime.decoded

    @property
    def decoded_values(self) -> dict[str, object]:
        """Return decoded values suitable for application integrations."""
        return self._runtime.decoded_values

    @property
    def runtime(self) -> DeviceRuntime:
        """Return the typed runtime view of this device snapshot."""
        return self._runtime

    def has_value(self, value: DeviceValue) -> bool:
        """Return whether this snapshot contains a value for a device field."""
        return self._runtime.has_value(value)


@dataclass(frozen=True, slots=True)
class AvailableInfo:
    """Broad read-only snapshot of information exposed by the gateway."""

    gateway: GatewaySnapshot
    devices: tuple[DeviceSnapshot, ...]

    @property
    def runtime(self) -> AvailableRuntime:
        """Return a typed runtime view of this full snapshot."""
        return AvailableRuntime.from_snapshot(self)

    def device_runtime(self, address: int) -> DeviceRuntime | None:
        """Return a typed runtime view for one device address."""
        for snapshot in self.devices:
            if snapshot.info.address == address:
                return snapshot.runtime
        return None


@dataclass(frozen=True, slots=True)
class GatewayRuntime:
    """Typed gateway runtime values."""

    metadata: GatewayInfo
    outdoor_temp: int | None = None
    network_error: int | None = None
    date_time: GatewayDateTime | None = None
    reporting_enabled: bool | None = None
    setback_enabled: bool | None = None
    setpoint_groups: Mapping[int, bool | None] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Freeze mutable constructor inputs."""
        if self.date_time is not None:
            object.__setattr__(
                self,
                "date_time",
                GatewayDateTime.from_mapping(self.date_time),
            )
        object.__setattr__(
            self,
            "setpoint_groups",
            _readonly_mapping(self.setpoint_groups),
        )

    @property
    def info(self) -> GatewayInfo:
        """Return static gateway metadata."""
        return self.metadata

    @classmethod
    def from_snapshot(cls, snapshot: GatewaySnapshot) -> GatewayRuntime:
        """Build a typed runtime gateway view from a snapshot."""
        return cls(
            metadata=snapshot.info,
            outdoor_temp=snapshot.outdoor_temp,
            network_error=snapshot.network_error,
            date_time=snapshot.date_time,
            reporting_enabled=snapshot.reporting_enabled,
            setback_enabled=snapshot.setback_enabled,
            setpoint_groups=dict(snapshot.setpoint_groups or {}),
        )

    @property
    def decoded_outdoor_temp(self) -> object:
        """Return decoded outdoor temperature metadata."""
        return decode_degh(self.outdoor_temp)

    @property
    def decoded_network_error(self) -> object:
        """Return decoded network error metadata."""
        return decode_network_error(self.network_error)


@dataclass(frozen=True, slots=True)
class SetpointValues:
    """Enum-keyed values for setback-indexed setpoint-like reports."""

    values: Mapping[ThaSetback, int | None] = field(default_factory=dict)
    active_setback: ThaSetback | None = None
    current_observed_setback: ThaSetback | None = None

    def __post_init__(self) -> None:
        """Freeze mutable constructor inputs."""
        object.__setattr__(self, "values", _readonly_mapping(self.values))

    @classmethod
    def from_mapping(
        cls,
        values: Mapping[ThaSetback, int | None],
        *,
        active_setback: ThaSetback | None = None,
        current_observed_setback: ThaSetback | None = None,
    ) -> SetpointValues:
        """Build setpoint values from a setback-keyed mapping."""
        if current_observed_setback is None and ThaSetback.CURRENT in values:
            current_observed_setback = _current_observed_setback(active_setback)
        return cls(dict(values), active_setback, current_observed_setback)

    @classmethod
    def from_raw(
        cls,
        value: _RawSnapshotValue,
        *,
        active_setback: ThaSetback | None = None,
    ) -> SetpointValues:
        """Build setpoint values from a legacy raw snapshot map."""
        if not isinstance(value, dict):
            return cls(
                active_setback=active_setback,
                current_observed_setback=_current_observed_setback(active_setback),
            )
        values: dict[ThaSetback, int | None] = {}
        for raw_key, raw_value in value.items():
            setback = _setback_from_key(raw_key)
            if setback is not None:
                values[setback] = _raw_int(raw_value)
        return cls.from_mapping(values, active_setback=active_setback)

    @classmethod
    def from_current(
        cls,
        value: _RawSnapshotValue | None,
        *,
        active_setback: ThaSetback | None = None,
    ) -> SetpointValues:
        """Build setpoint values from a legacy scalar current value."""
        return cls.from_mapping(
            {ThaSetback.CURRENT: _raw_int(value)},
            active_setback=active_setback,
        )

    @property
    def current(self) -> int | None:
        """Return the active setpoint value."""
        if (
            self.active_setback is not None
            and self.active_setback is not ThaSetback.CURRENT
        ):
            if self.current_observed_setback == self.active_setback:
                active_value = self.values.get(self.active_setback)
                return (
                    self.values.get(ThaSetback.CURRENT)
                    if active_value is None
                    else active_value
                )
            if self.active_setback in self.values:
                return self.values[self.active_setback]
            return None
        return self.values.get(ThaSetback.CURRENT)

    @property
    def wake_4(self) -> int | None:
        return self.get(ThaSetback.WAKE_4)

    @property
    def unocc_4(self) -> int | None:
        return self.get(ThaSetback.UNOCC_4)

    @property
    def occ_4(self) -> int | None:
        return self.get(ThaSetback.OCC_4)

    @property
    def sleep_4(self) -> int | None:
        return self.get(ThaSetback.SLEEP_4)

    @property
    def occ_2(self) -> int | None:
        return self.get(ThaSetback.OCC_2)

    @property
    def unocc_2(self) -> int | None:
        return self.get(ThaSetback.UNOCC_2)

    @property
    def away(self) -> int | None:
        return self.get(ThaSetback.AWAY)

    def get(self, setback: ThaSetback) -> int | None:
        """Return a value by setback enum."""
        if setback is ThaSetback.CURRENT:
            return self.current
        return self.values.get(setback)

    def with_value(self, setback: ThaSetback, value: int | None) -> SetpointValues:
        """Return a copy with one setback value updated."""
        values = dict(self.values)
        values[setback] = value
        current_observed_setback = self.current_observed_setback
        if setback is ThaSetback.CURRENT:
            current_observed_setback = _current_observed_setback(self.active_setback)
            if current_observed_setback is not None:
                values[current_observed_setback] = value
        return replace(
            self,
            values=values,
            current_observed_setback=current_observed_setback,
        )

    def with_active_setback(
        self,
        active_setback: ThaSetback | None,
    ) -> SetpointValues:
        """Return a copy with a new active setback."""
        return replace(self, active_setback=active_setback)

    def decoded[DecodedValue](
        self,
        decoder: Callable[[object], DecodedValue | None],
    ) -> DecodedSetbackValues[DecodedValue]:
        """Return typed decoded values using the provided raw-value decoder."""
        return DecodedSetbackValues.from_setpoints(self, decoder)

    def as_dict(self) -> _RawSetpointMap:
        """Return JSON-friendly raw values keyed by setback name."""
        values: _RawSetpointMap = {}
        if ThaSetback.CURRENT in self.values or (
            self.active_setback is not None
            and self.active_setback is not ThaSetback.CURRENT
            and self.active_setback in self.values
        ):
            values[ThaSetback.CURRENT.name.lower()] = self.current
        for setback in ThaSetback:
            if setback is ThaSetback.CURRENT or setback not in self.values:
                continue
            values[setback.name.lower()] = self.values[setback]
        return values


@dataclass(frozen=True, slots=True)
class DecodedSetbackValues[DecodedValue]:
    """Decoded values for setback-indexed raw data."""

    values: Mapping[ThaSetback, DecodedValue | None] = field(default_factory=dict)
    active_setback: ThaSetback | None = None
    current_observed_setback: ThaSetback | None = None

    def __post_init__(self) -> None:
        """Freeze mutable constructor inputs."""
        object.__setattr__(self, "values", _readonly_mapping(self.values))

    @classmethod
    def from_setpoints(
        cls,
        setpoints: SetpointValues,
        decoder: Callable[[object], DecodedValue | None],
    ) -> DecodedSetbackValues[DecodedValue]:
        """Decode raw setpoint values."""
        return cls(
            {setback: decoder(value) for setback, value in setpoints.values.items()},
            setpoints.active_setback,
            setpoints.current_observed_setback,
        )

    @property
    def current(self) -> DecodedValue | None:
        """Return the active decoded value."""
        if (
            self.active_setback is not None
            and self.active_setback is not ThaSetback.CURRENT
        ):
            if self.current_observed_setback == self.active_setback:
                active_value = self.values.get(self.active_setback)
                return (
                    self.values.get(ThaSetback.CURRENT)
                    if active_value is None
                    else active_value
                )
            if self.active_setback in self.values:
                return self.values[self.active_setback]
            return None
        return self.values.get(ThaSetback.CURRENT)

    def get(self, setback: ThaSetback) -> DecodedValue | None:
        """Return a decoded value by setback enum."""
        if setback is ThaSetback.CURRENT:
            return self.current
        return self.values.get(setback)

    def as_dict(self) -> dict[str, DecodedValue | None]:
        """Return JSON-friendly decoded values keyed by setback name."""
        values: dict[str, DecodedValue | None] = {}
        if ThaSetback.CURRENT in self.values or (
            self.active_setback is not None
            and self.active_setback is not ThaSetback.CURRENT
            and self.active_setback in self.values
        ):
            values[ThaSetback.CURRENT.name.lower()] = self.current
        for setback in ThaSetback:
            if setback is ThaSetback.CURRENT or setback not in self.values:
                continue
            values[setback.name.lower()] = self.values[setback]
        return values


@dataclass(frozen=True, slots=True)
class DecodedDeviceRuntime:
    """Typed decoded runtime values for one discovered device."""

    current_temperature: DecodedTemperature | None = None
    current_floor_temperature: DecodedTemperature | None = None
    active_demand: DecodedNamedValue | None = None
    setback_state: DecodedNamedValue | None = None
    mode_setting: DecodedNamedValue | None = None
    relative_humidity: int | None = None
    humidity_setpoint_min: int | None = None
    humidity_setpoint_max: int | None = None
    setpoint_targets: DecodedSetbackValues[DecodedTemperature] = field(
        default_factory=DecodedSetbackValues,
    )
    heat_setpoints: DecodedSetbackValues[DecodedTemperature] = field(
        default_factory=DecodedSetbackValues,
    )
    cool_setpoints: DecodedSetbackValues[DecodedTemperature] = field(
        default_factory=DecodedSetbackValues,
    )
    slab_setpoints: DecodedSetbackValues[DecodedTemperature] = field(
        default_factory=DecodedSetbackValues,
    )
    fan_percent: DecodedSetbackValues[int] = field(
        default_factory=DecodedSetbackValues,
    )

    @classmethod
    def from_runtime(cls, runtime: DeviceRuntime) -> DecodedDeviceRuntime:
        """Build typed decoded values from raw runtime values."""
        return cls(
            current_temperature=decode_degh(runtime.current_temperature),
            current_floor_temperature=decode_degh(runtime.current_floor_temperature),
            active_demand=decode_active_demand(runtime.active_demand),
            setback_state=decode_setback(runtime.setback_state),
            mode_setting=decode_device_mode(runtime.mode_setting),
            relative_humidity=decode_percent(runtime.relative_humidity),
            humidity_setpoint_min=decode_percent(runtime.humidity_setpoint_min),
            humidity_setpoint_max=decode_percent(runtime.humidity_setpoint_max),
            setpoint_targets=runtime.setpoint_targets.decoded(decode_degh),
            heat_setpoints=runtime.heat_setpoints.decoded(decode_dege),
            cool_setpoints=runtime.cool_setpoints.decoded(decode_dege),
            slab_setpoints=runtime.slab_setpoints.decoded(decode_dege),
            fan_percent=runtime.fan_percent.decoded(decode_percent),
        )

    def raw_value(self, value: DeviceValue) -> object:
        """Return one decoded value in the legacy shape."""
        raw = getattr(self, value.value)
        if isinstance(raw, DecodedSetbackValues):
            return raw.as_dict()
        return raw


@dataclass(frozen=True, slots=True)
class DeviceRuntime:
    """Typed runtime values for one discovered device."""

    info: DiscoveredDevice
    available_values: frozenset[DeviceValue] = field(default_factory=frozenset)
    current_temperature: int | None = None
    current_floor_temperature: int | None = None
    active_demand: int | None = None
    setback_state: int | None = None
    mode_setting: int | None = None
    relative_humidity: int | None = None
    humidity_setpoint_min: int | None = None
    humidity_setpoint_max: int | None = None
    setpoint_targets: SetpointValues = field(default_factory=SetpointValues)
    heat_setpoints: SetpointValues = field(default_factory=SetpointValues)
    cool_setpoints: SetpointValues = field(default_factory=SetpointValues)
    slab_setpoints: SetpointValues = field(default_factory=SetpointValues)
    fan_percent: SetpointValues = field(default_factory=SetpointValues)

    @classmethod
    def create(
        cls,
        *,
        info: DiscoveredDevice,
        available_values: frozenset[DeviceValue] | None = None,
        current_temperature: int | None = None,
        current_floor_temperature: int | None = None,
        active_demand: int | None = None,
        setback_state: int | None = None,
        mode_setting: int | None = None,
        relative_humidity: int | None = None,
        humidity_setpoint_min: int | None = None,
        humidity_setpoint_max: int | None = None,
        setpoint_targets: SetpointValues | None = None,
        heat_setpoints: SetpointValues | None = None,
        cool_setpoints: SetpointValues | None = None,
        slab_setpoints: SetpointValues | None = None,
        fan_percent: SetpointValues | None = None,
    ) -> DeviceRuntime:
        """Build the right runtime subtype for a discovered device."""
        runtime_cls = _runtime_class(info)
        return runtime_cls(
            info=info,
            available_values=available_values or frozenset(),
            current_temperature=current_temperature,
            current_floor_temperature=current_floor_temperature,
            active_demand=active_demand,
            setback_state=setback_state,
            mode_setting=mode_setting,
            relative_humidity=relative_humidity,
            humidity_setpoint_min=humidity_setpoint_min,
            humidity_setpoint_max=humidity_setpoint_max,
            setpoint_targets=setpoint_targets or SetpointValues(),
            heat_setpoints=heat_setpoints or SetpointValues(),
            cool_setpoints=cool_setpoints or SetpointValues(),
            slab_setpoints=slab_setpoints or SetpointValues(),
            fan_percent=fan_percent or SetpointValues(),
        )

    @classmethod
    def from_snapshot(cls, snapshot: DeviceSnapshot) -> DeviceRuntime:
        """Return the canonical typed runtime view from a snapshot."""
        return snapshot.runtime

    @classmethod
    def from_raw_values(
        cls,
        info: DiscoveredDevice,
        values: Mapping[str, _RawSnapshotValue],
    ) -> DeviceRuntime:
        """Build typed runtime values from a legacy raw value mapping."""
        available_values = {
            value for key in values if (value := DeviceValue.from_key(key)) is not None
        }
        if "setpoint_target" in values:
            available_values.add(DeviceValue.SETPOINT_TARGETS)

        setback_state = _raw_int(values.get(DeviceValue.SETBACK_STATE.value))
        active_setback = setback_from_value(setback_state)
        setpoint_targets = (
            SetpointValues.from_raw(
                values.get(DeviceValue.SETPOINT_TARGETS.value),
                active_setback=active_setback,
            )
            if DeviceValue.SETPOINT_TARGETS in available_values
            else SetpointValues()
        )
        if "setpoint_target" in values and DeviceValue.SETPOINT_TARGETS not in {
            DeviceValue.from_key(key) for key in values
        }:
            setpoint_targets = SetpointValues.from_current(
                values.get("setpoint_target"),
                active_setback=active_setback,
            )

        return cls.create(
            info=info,
            available_values=frozenset(available_values),
            current_temperature=_raw_int(
                values.get(DeviceValue.CURRENT_TEMPERATURE.value),
            ),
            current_floor_temperature=_raw_int(
                values.get(DeviceValue.CURRENT_FLOOR_TEMPERATURE.value),
            ),
            active_demand=_raw_int(values.get(DeviceValue.ACTIVE_DEMAND.value)),
            setback_state=setback_state,
            mode_setting=_raw_int(values.get(DeviceValue.MODE_SETTING.value)),
            relative_humidity=_raw_int(values.get(DeviceValue.RELATIVE_HUMIDITY.value)),
            humidity_setpoint_min=_raw_int(
                values.get(DeviceValue.HUMIDITY_SETPOINT_MIN.value),
            ),
            humidity_setpoint_max=_raw_int(
                values.get(DeviceValue.HUMIDITY_SETPOINT_MAX.value),
            ),
            setpoint_targets=setpoint_targets,
            heat_setpoints=SetpointValues.from_raw(
                values.get(DeviceValue.HEAT_SETPOINTS.value),
                active_setback=active_setback,
            ),
            cool_setpoints=SetpointValues.from_raw(
                values.get(DeviceValue.COOL_SETPOINTS.value),
                active_setback=active_setback,
            ),
            slab_setpoints=SetpointValues.from_raw(
                values.get(DeviceValue.SLAB_SETPOINTS.value),
                active_setback=active_setback,
            ),
            fan_percent=SetpointValues.from_raw(
                values.get(DeviceValue.FAN_PERCENT.value),
                active_setback=active_setback,
            ),
        )

    @property
    def active_setback(self) -> ThaSetback | None:
        """Return the typed active setback, when known."""
        return setback_from_value(self.setback_state)

    @property
    def setpoint_target(self) -> int | None:
        """Return the active setpoint-device target value."""
        return self.setpoint_targets.current

    @property
    def current_heat_setpoint(self) -> int | None:
        """Return the active heat setpoint."""
        return self.heat_setpoints.current

    @property
    def current_cool_setpoint(self) -> int | None:
        """Return the active cool setpoint."""
        return self.cool_setpoints.current

    @property
    def current_slab_setpoint(self) -> int | None:
        """Return the active slab setpoint."""
        return self.slab_setpoints.current

    @property
    def current_fan_percent(self) -> int | None:
        """Return the active fan percent."""
        return self.fan_percent.current

    @property
    def decoded(self) -> DecodedDeviceRuntime:
        """Return typed decoded runtime values."""
        return DecodedDeviceRuntime.from_runtime(self)

    @property
    def decoded_values(self) -> dict[str, object]:
        """Return decoded values in the legacy snapshot value shape."""
        decoded = self.decoded
        return {
            value.value: decoded.raw_value(value)
            for value in DeviceValue
            if value in self.available_values
        }

    @property
    def raw_values(self) -> dict[str, _RawSnapshotValue]:
        """Return this runtime view in the legacy snapshot value shape."""
        raw: dict[str, _RawSnapshotValue] = {}
        for value in DeviceValue:
            if value not in self.available_values:
                continue
            raw[value.value] = self.raw_value(value)
        return raw

    def raw_value(self, value: DeviceValue) -> _RawSnapshotValue:
        """Return one raw runtime value in the legacy shape."""
        if value is DeviceValue.CURRENT_TEMPERATURE:
            return self.current_temperature
        if value is DeviceValue.CURRENT_FLOOR_TEMPERATURE:
            return self.current_floor_temperature
        if value is DeviceValue.ACTIVE_DEMAND:
            return self.active_demand
        if value is DeviceValue.SETBACK_STATE:
            return self.setback_state
        if value is DeviceValue.MODE_SETTING:
            return self.mode_setting
        if value is DeviceValue.RELATIVE_HUMIDITY:
            return self.relative_humidity
        if value is DeviceValue.HUMIDITY_SETPOINT_MIN:
            return self.humidity_setpoint_min
        if value is DeviceValue.HUMIDITY_SETPOINT_MAX:
            return self.humidity_setpoint_max
        if value is DeviceValue.SETPOINT_TARGETS:
            return self.setpoint_targets.as_dict()
        if value is DeviceValue.HEAT_SETPOINTS:
            return self.heat_setpoints.as_dict()
        if value is DeviceValue.COOL_SETPOINTS:
            return self.cool_setpoints.as_dict()
        if value is DeviceValue.SLAB_SETPOINTS:
            return self.slab_setpoints.as_dict()
        if value is DeviceValue.FAN_PERCENT:
            return self.fan_percent.as_dict()
        assert_never(value)

    def has_value(self, value: DeviceValue) -> bool:
        """Return whether this runtime has a value slot for the field."""
        return value in self.available_values

    def with_scalar(self, value: DeviceValue, raw: int | None) -> Self:
        """Return a copy with one scalar value updated."""
        available_values = self.available_values | {value}
        if value is DeviceValue.SETBACK_STATE:
            active_setback = setback_from_value(raw)
            return replace(
                self,
                available_values=available_values,
                setback_state=raw,
                setpoint_targets=self.setpoint_targets.with_active_setback(
                    active_setback,
                ),
                heat_setpoints=self.heat_setpoints.with_active_setback(
                    active_setback,
                ),
                cool_setpoints=self.cool_setpoints.with_active_setback(
                    active_setback,
                ),
                slab_setpoints=self.slab_setpoints.with_active_setback(
                    active_setback,
                ),
                fan_percent=self.fan_percent.with_active_setback(
                    active_setback,
                ),
            )
        if value is DeviceValue.CURRENT_TEMPERATURE:
            return replace(
                self,
                available_values=available_values,
                current_temperature=raw,
            )
        if value is DeviceValue.CURRENT_FLOOR_TEMPERATURE:
            return replace(
                self,
                available_values=available_values,
                current_floor_temperature=raw,
            )
        if value is DeviceValue.ACTIVE_DEMAND:
            return replace(self, available_values=available_values, active_demand=raw)
        if value is DeviceValue.MODE_SETTING:
            return replace(self, available_values=available_values, mode_setting=raw)
        if value is DeviceValue.RELATIVE_HUMIDITY:
            return replace(
                self,
                available_values=available_values,
                relative_humidity=raw,
            )
        if value is DeviceValue.HUMIDITY_SETPOINT_MIN:
            return replace(
                self,
                available_values=available_values,
                humidity_setpoint_min=raw,
            )
        if value is DeviceValue.HUMIDITY_SETPOINT_MAX:
            return replace(
                self,
                available_values=available_values,
                humidity_setpoint_max=raw,
            )
        msg = f"{value.value} is not a scalar value"
        raise TypeError(msg)

    def with_setpoint(
        self,
        value: DeviceValue,
        setback: ThaSetback,
        raw: int | None,
    ) -> Self:
        """Return a copy with one setpoint-like value updated."""
        available_values = self.available_values | {value}
        if value is DeviceValue.SETPOINT_TARGETS:
            return replace(
                self,
                available_values=available_values,
                setpoint_targets=self.setpoint_targets.with_value(setback, raw),
            )
        if value is DeviceValue.HEAT_SETPOINTS:
            return replace(
                self,
                available_values=available_values,
                heat_setpoints=self.heat_setpoints.with_value(setback, raw),
            )
        if value is DeviceValue.COOL_SETPOINTS:
            return replace(
                self,
                available_values=available_values,
                cool_setpoints=self.cool_setpoints.with_value(setback, raw),
            )
        if value is DeviceValue.SLAB_SETPOINTS:
            return replace(
                self,
                available_values=available_values,
                slab_setpoints=self.slab_setpoints.with_value(setback, raw),
            )
        if value is DeviceValue.FAN_PERCENT:
            return replace(
                self,
                available_values=available_values,
                fan_percent=self.fan_percent.with_value(setback, raw),
            )
        msg = f"{value.value} is not a setpoint value"
        raise TypeError(msg)

    def with_active_setback(self, active_setback: ThaSetback | None) -> Self:
        """Return a copy with a new active setback on all setpoint values."""
        return replace(
            self,
            setpoint_targets=self.setpoint_targets.with_active_setback(active_setback),
            heat_setpoints=self.heat_setpoints.with_active_setback(active_setback),
            cool_setpoints=self.cool_setpoints.with_active_setback(active_setback),
            slab_setpoints=self.slab_setpoints.with_active_setback(active_setback),
            fan_percent=self.fan_percent.with_active_setback(active_setback),
        )


@dataclass(frozen=True, slots=True)
class ThermostatData:
    """Thermostat-specific runtime capabilities."""

    mode_setting: int | None
    relative_humidity: int | None
    humidity_setpoint_min: int | None
    humidity_setpoint_max: int | None
    heat_setpoints: SetpointValues
    cool_setpoints: SetpointValues
    slab_setpoints: SetpointValues
    fan_percent: SetpointValues


@dataclass(frozen=True, slots=True)
class SnowmeltData:
    """Snowmelt-specific runtime capabilities."""

    current_temperature: int | None
    current_floor_temperature: int | None
    active_demand: int | None
    setback_state: int | None
    slab_setpoints: SetpointValues
    setpoint_targets: SetpointValues


@dataclass(frozen=True, slots=True)
class SetpointDeviceData:
    """Setpoint-device-specific runtime capabilities."""

    current_temperature: int | None
    active_demand: int | None
    setback_state: int | None
    setpoint_targets: SetpointValues


@dataclass(frozen=True, slots=True)
class ThermostatRuntime(DeviceRuntime):
    """Typed runtime values for a thermostat-like device."""

    @property
    def thermostat(self) -> ThermostatData:
        """Return thermostat-specific capability values."""
        return ThermostatData(
            mode_setting=self.mode_setting,
            relative_humidity=self.relative_humidity,
            humidity_setpoint_min=self.humidity_setpoint_min,
            humidity_setpoint_max=self.humidity_setpoint_max,
            heat_setpoints=self.heat_setpoints,
            cool_setpoints=self.cool_setpoints,
            slab_setpoints=self.slab_setpoints,
            fan_percent=self.fan_percent,
        )


@dataclass(frozen=True, slots=True)
class SnowmeltRuntime(DeviceRuntime):
    """Typed runtime values for a snowmelt device."""

    @property
    def snowmelt(self) -> SnowmeltData:
        """Return snowmelt-specific capability values."""
        return SnowmeltData(
            current_temperature=self.current_temperature,
            current_floor_temperature=self.current_floor_temperature,
            active_demand=self.active_demand,
            setback_state=self.setback_state,
            slab_setpoints=self.slab_setpoints,
            setpoint_targets=self.setpoint_targets,
        )


@dataclass(frozen=True, slots=True)
class SetpointDeviceRuntime(DeviceRuntime):
    """Typed runtime values for a setpoint-only device."""

    @property
    def setpoint_device(self) -> SetpointDeviceData:
        """Return setpoint-device-specific capability values."""
        return SetpointDeviceData(
            current_temperature=self.current_temperature,
            active_demand=self.active_demand,
            setback_state=self.setback_state,
            setpoint_targets=self.setpoint_targets,
        )


@dataclass(frozen=True, slots=True)
class AvailableRuntime:
    """Typed runtime view of all gateway and device values."""

    gateway: GatewayRuntime
    devices: tuple[DeviceRuntime, ...]

    @classmethod
    def from_snapshot(cls, snapshot: AvailableInfo) -> AvailableRuntime:
        """Build a typed runtime view from a snapshot."""
        return cls(
            gateway=snapshot.gateway.runtime,
            devices=tuple(device.runtime for device in snapshot.devices),
        )

    @property
    def device_map(self) -> dict[int, DeviceRuntime]:
        """Return typed device runtime views keyed by tHA address."""
        return {device.info.address: device for device in self.devices}


def _runtime_class(device: DiscoveredDevice) -> type[DeviceRuntime]:
    if device.kind is DeviceKind.THERMOSTAT:
        return ThermostatRuntime
    if device.kind is DeviceKind.SNOWMELT:
        return SnowmeltRuntime
    if device.kind is DeviceKind.SETPOINT:
        return SetpointDeviceRuntime
    return DeviceRuntime


def _raw_int(value: _RawSnapshotValue | int | None) -> int | None:
    return value if isinstance(value, int) else None


def _readonly_mapping[MappingKey, MappingValue](
    values: Mapping[MappingKey, MappingValue],
) -> Mapping[MappingKey, MappingValue]:
    return MappingProxyType(dict(values))


def setback_from_value(value: int | ThaSetback | None) -> ThaSetback | None:
    """Return a typed setback enum from a raw protocol value."""
    if value is None:
        return None
    try:
        return ThaSetback(value)
    except ValueError:
        return None


def _setback_from_key(key: object) -> ThaSetback | None:
    if isinstance(key, ThaSetback):
        return key
    if not isinstance(key, str):
        return None
    normalized = key.upper()
    if normalized == "CURRENT":
        return ThaSetback.CURRENT
    try:
        return ThaSetback[normalized]
    except KeyError:
        return None


def _current_observed_setback(
    active_setback: ThaSetback | None,
) -> ThaSetback | None:
    if active_setback is None or active_setback is ThaSetback.CURRENT:
        return None
    return active_setback
