"""Setpoint control operating status and application-specific stored targets."""

from collections.abc import Mapping
from dataclasses import dataclass

from .codec import Fields, Reader, fahrenheit, status_flags
from .line import SetpointApplication

SETPOINT_STATUS = 0x22f
APPLICATION_SETUP = 0x26
OPERATION_SETUP = 0x27
OVERRIDE_TIME = 0x32
TARGET_BLOCKS = range(0x28, OVERRIDE_TIME)


@dataclass(frozen=True, slots=True)
class SetpointStatus:
    flags: int
    operation: int
    target_raw: int
    temp1_raw: int
    temp2_raw: int
    enable_flags: int
    excuse1: int
    excuse2: int
    extension: bytes = b""

    @classmethod
    def read(cls, r: Reader) -> "SetpointStatus":
        return cls(r.byte(), r.byte(), r.word(), r.word(), r.word(), r.byte(), r.byte(), r.byte(),
                   r.take(len(r.remaining)))

    @property
    def demand(self) -> bool:
        return bool(self.operation & 4)

    @property
    def cooling(self) -> bool:
        return bool(self.operation & 8)

    @property
    def operating_mode(self) -> int:
        return self.operation >> 4 & 3

    def fields(self) -> Fields:
        return status_flags(self.flags) | {
            "actuator_status": self.operation & 3, "demand_status": int(self.demand),
            "heat_cool_mode": int(self.cooling), "operating_mode": self.operating_mode,
            "stat_priority": self.operation >> 6, "target_temp_raw": self.target_raw,
            "temp1_raw": self.temp1_raw, "temp2_raw": self.temp2_raw,
            "aux_relay_status": self.enable_flags & 3,
            "requested_enable_status": self.enable_flags >> 2 & 3,
            "actual_enable_status": self.enable_flags >> 4 & 3, "reserved": self.enable_flags >> 6,
            "status_excuse1": self.excuse1, "status_excuse2": self.excuse2,
        } | ({"extension_hex": self.extension.hex()} if self.extension else {})

    def readings(self, application: SetpointApplication) -> "SetpointReadings":
        """The 483 uses E for live DHW and H otherwise; temp2 has no established role."""
        if not isinstance(application, SetpointApplication):
            raise TypeError("expected the device's confirmed setpoint application")
        def convert(raw: int) -> float | None:
            if application is SetpointApplication.DHW:
                return fahrenheit(raw, deg_e=True) if raw <= 0xff else None
            return fahrenheit(raw)
        return SetpointReadings(convert(self.target_raw), convert(self.temp1_raw))


@dataclass(frozen=True, slots=True)
class SetpointReadings:
    target_f: float | None
    temperature_f: float | None


@dataclass(frozen=True, slots=True)
class SetpointTargets:
    """Physical words, in degH even for DHW. None means no complete supported word."""
    wake_occupied_raw: int | None
    unoccupied_sleep_raw: int | None
    away_raw: int | None
    maximum_raw: int | None
    wake_raw: int | None
    sleep_raw: int | None

    @classmethod
    def from_reports(cls, primary: Mapping[str, object], secondary: Mapping[str, object]) -> "SetpointTargets":
        def word(fields: Mapping[str, object], name: str) -> int | None:
            raw = fields.get(name + "_raw")
            return raw if isinstance(raw, int) else None
        return cls(word(primary, "wake_occupied"), word(primary, "unoccupied_sleep"),
                   word(primary, "away"), word(primary, "maximum"),
                   word(secondary, "wake"), word(secondary, "sleep"))
