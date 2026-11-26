"""Thermostat output reports and stored adjustment tables from the 483 SW 11 registry."""

from collections.abc import Mapping
from dataclasses import dataclass
from enum import IntEnum

from .codec import Fields, Reader, temp
from .layouts import SETPOINT_FIELDS

FAN_STATUS = 0x25f
SLAB_STATUS = 0x26f
HUMIDITY_STATUS = 0x323
FAN_SETUP = 0x08
HUMIDITY_SETUP = 0xa3


def _integer(fields: Mapping[str, object], name: str) -> int | None:
    value = fields.get(name)
    return value if isinstance(value, int) else None


def _percent(raw: int | None) -> int | None:
    return raw if raw is not None and 0 <= raw <= 100 else None


@dataclass(frozen=True, slots=True)
class FanDuctStatus:
    flags: int
    duct_raw: int
    coil_raw: int

    @classmethod
    def read(cls, r: Reader) -> "FanDuctStatus":
        return cls(r.byte(), r.word(), r.word())

    @property
    def fan1(self) -> int:
        return self.flags & 3

    @property
    def fan2(self) -> int:
        return self.flags >> 2 & 3

    def fields(self) -> Fields:
        return {"fan1": self.fan1, "fan2": self.fan2, "optimum_start": self.flags >> 4 & 3,
                "freeze_protect": self.flags >> 6 & 1, "reserved": self.flags >> 7} | (
                temp("duct_temp", self.duct_raw) | temp("coil_temp", self.coil_raw))


@dataclass(frozen=True, slots=True)
class SlabStatus:
    flags: int
    target_raw: int
    current_raw: int

    @classmethod
    def read(cls, r: Reader) -> "SlabStatus":
        return cls(r.byte(), r.byte(), r.word())

    @property
    def primary(self) -> bool:
        # J1196J at 0x6214 tests the entire byte, not just the low status bits.
        return self.flags < 2

    def fields(self) -> Fields:
        return {"slab_status": self.flags & 7, "reserved": self.flags >> 3, "primary_target": self.primary} | (
                temp("target", self.target_raw, deg_e=True) | temp("current", self.current_raw))


@dataclass(frozen=True, slots=True)
class HumidityStatus:
    flags: int
    control_raw: int
    dewpoint_raw: int
    internal_raw: int
    sensor_bytes: bytes
    humidify_target_raw: int | None
    dehumidify_target_raw: int | None

    @classmethod
    def read(cls, r: Reader) -> "HumidityStatus":
        count, flags, control = r.take(3)
        dewpoint, internal = r.word(), r.byte()
        # SW 11's indexed reader overlaps the last sensor and first target.
        # Both layouts put the optional targets after the declared sensor region.
        return cls(flags, control, dewpoint, internal, r.take(count),
                   r.byte() if r.remaining else None, r.byte() if r.remaining else None)

    @property
    def relative_percent(self) -> int | None:
        return _percent(self.control_raw)

    def fields(self) -> Fields:
        values: Fields = {"count": len(self.sensor_bytes), "flags": self.flags, "control": self.control_raw,
                          "dewpoint_raw": self.dewpoint_raw, "internal_sensor_raw": self.internal_raw,
                          "sensor_bytes_hex": self.sensor_bytes.hex()}
        for name, value in (("humidity_current_target", self.humidify_target_raw),
                            ("dehumidity_current_target", self.dehumidify_target_raw)):
            if value is not None:
                values[name] = value
        return values


class TargetSlot(IntEnum):
    """Physical byte offsets. Air setback codes and schedule phases are not offsets."""
    WAKE = 0
    UNOCCUPIED = 1
    OCCUPIED = 2
    SLEEP = 3
    AWAY = 4


class ThermostatTarget(IntEnum):
    HEAT = 0x05
    COOL = 0x06
    FLOOR = 0x07
    FAN = 0x09
    HUMIDIFY = 0xa4
    DEHUMIDIFY = 0xa5

    @property
    def temperature(self) -> bool:
        return self in (self.HEAT, self.COOL, self.FLOOR)

    def physical_value(self, raw: int | None, model: str) -> float | int | None:
        if raw is None:
            return None
        if self.temperature:
            return raw / 2 if 0 < raw < 255 else None
        return _percent(raw * fan_step(model) if self is self.FAN else raw)


def fan_step(model: str | None) -> int:
    return 10 if model in ("544", "545", "546") else 1


@dataclass(frozen=True, slots=True)
class TargetTable:
    model: str
    target: ThermostatTarget
    slots: tuple[int | None, ...]
    maximum_raw: int | None

    @classmethod
    def from_report(cls, model: str, target: ThermostatTarget, fields: Mapping[str, object]) -> "TargetTable":
        suffix = "_raw" if target.temperature else "_fan_percent" if target is ThermostatTarget.FAN else "_target_rh_percent"
        return cls(model, target, tuple(_integer(fields, name + suffix) for name in SETPOINT_FIELDS),
                   _integer(fields, "maximum_raw") if target is ThermostatTarget.FLOOR else None)

    def raw(self, slot: TargetSlot) -> int | None:
        if not isinstance(slot, TargetSlot):
            raise TypeError("expected a physical target slot")
        return self.slots[slot]

    def value(self, slot: TargetSlot) -> float | int | None:
        """Degrees Celsius or physical percent; zero fan is Auto, not a fan output."""
        return self.target.physical_value(self.raw(slot), self.model)


@dataclass(frozen=True, slots=True)
class FanSetup:
    stages_raw: int | None
    ventilation_raw: int | None

    @classmethod
    def from_report(cls, fields: Mapping[str, object]) -> "FanSetup":
        return cls(_integer(fields, "fan_stage_flags"), _integer(fields, "fan_ventilation_flags"))

    @property
    def enabled(self) -> bool | None:
        """SW 11's adjustment eligibility, not evidence that a fan is running."""
        stages, vent = self.stages_raw, self.ventilation_raw
        if stages is not None and any((stages >> shift & 3) in (1, 2) for shift in (2, 4, 6)):
            return True
        if vent is not None:
            if (vent & 3) in (1, 2) or (vent >> 2 & 3) in (1, 2):
                return True
            if (vent >> 2 & 3) == 3 and stages is not None and (stages & 3) != 2:
                return True
        return None if stages is None or vent is None else False


@dataclass(frozen=True, slots=True)
class HumiditySetup:
    humidify_raw: int | None
    dehumidify_raw: int | None

    @classmethod
    def from_report(cls, fields: Mapping[str, object]) -> "HumiditySetup":
        return cls(_integer(fields, "humidity_mode_setup"), _integer(fields, "dehumidity_mode_setup"))

    def enabled(self, target: ThermostatTarget) -> bool | None:
        if target not in (ThermostatTarget.HUMIDIFY, ThermostatTarget.DEHUMIDIFY):
            raise ValueError("not a humidity target")
        raw = self.humidify_raw if target is ThermostatTarget.HUMIDIFY else self.dehumidify_raw
        return None if raw in (None, 255) else bool(raw)
