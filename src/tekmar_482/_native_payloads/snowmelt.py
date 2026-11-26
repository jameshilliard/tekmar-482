"""The 483's native snowmelt addresses, application profiles and ordered status pairs."""

from dataclasses import dataclass
from enum import IntEnum

from .codec import Fields, Reader, fahrenheit, temp

SNOW_REPORT = 0x17e
SNOW_REQUEST = 0x17f


class SnowmeltRole(IntEnum):
    ZONE = 1
    SENSOR = 2

    @property
    def maximum(self) -> int:
        return 12 if self is SnowmeltRole.ZONE else 24

    @classmethod
    def for_application(cls, application: int) -> "SnowmeltRole":
        if application not in range(len(SnowmeltApplication)):
            raise ValueError(f"unknown snowmelt application {application}")
        return SnowmeltApplication(application).role


@dataclass(frozen=True, order=True, slots=True)
class SnowmeltAddress:
    """A configured one-based zone or sensor number, not a native node or group."""
    role: SnowmeltRole
    number: int

    def __post_init__(self) -> None:
        if not isinstance(self.role, SnowmeltRole) or not 1 <= self.number <= self.role.maximum:
            raise ValueError("expected configured zone 1..12 or sensor 1..24")

    @property
    def request_byte(self) -> int:
        return (self.number - 1) << 3 | self.role

    @classmethod
    def from_report(cls, address: int) -> "SnowmeltAddress | None":
        try:
            return cls(SnowmeltRole(address & 7), address >> 3)
        except ValueError:
            return None


class SnowmeltKey(IntEnum):
    WATER_DETECTION = 0x19
    HEAT_SEQUENCE = 0x1a
    OPERATING_STATE = 0x1b
    WARM_WEATHER_SHUTDOWN = 0x1c
    COLD_WEATHER_CUTOFF = 0x1d
    ZONE_TYPE = 0x1e
    ELECTRIC_OFF_TIME = 0x1f
    ZONE_ID = 0x20
    SENSOR_FLAGS = 0x21
    SLAB_TEMPERATURE = 0x22
    SLAB_SETPOINT = 0x23
    EDGE_TEMPERATURE = 0x24
    MELT_GROUP = 0x25
    STORM_GROUP = 0x26
    BOILER_OUTPUT = 0x27
    MIX_OUTPUT = 0x28
    PWM_OUTPUT = 0x29
    SYSTEM_PUMP = 0x2a
    ALERT_RELAY = 0x2b
    RETURN_TEMPERATURE = 0x2c
    SUPPLY_TEMPERATURE = 0x2d
    SUPPLY_TARGET = 0x2e
    KEEP_ALIVE = 0x2f
    MELTING_TIME = 0x30


class SnowmeltApplication(IntEnum):
    PWM = 0
    BOILER = 1
    MIXING = 2
    ELECTRIC = 3
    SENSOR_ONLY = 4

    @property
    def role(self) -> SnowmeltRole:
        return SnowmeltRole.SENSOR if self is SnowmeltApplication.SENSOR_ONLY else SnowmeltRole.ZONE

    @property
    def status_keys(self) -> bytes:
        # SW 11 SnowmeltControl.stale_keys_to_request, when all values are absent.
        return _APPLICATION_KEYS[self]


_APPLICATION_KEYS = (
    bytes.fromhex("19 22 1b 23 2d 2a 29"), bytes.fromhex("19 22 1b 23 2d 2a 2e 27"),
    bytes.fromhex("19 22 1b 23 2d 2a 2e 28"), bytes.fromhex("19 22 1b 23 29"),
    bytes.fromhex("19 22"),
)
_WORDS = {
    0x19: "water_detect_state", 0x1a: "heat_sequence_state",
    0x1c: "warm_weather_shutdown_state", 0x1d: "cold_weather_cutoff_state",
    0x1e: "zone_type", 0x1f: "electric_off_time_15s_ticks",
    0x20: "zone_id", 0x21: "sensor_flags", 0x25: "melt_group_id", 0x26: "storm_group_id",
    0x27: "boiler_output_raw", 0x28: "mix_output_raw", 0x29: "pwm_output_raw",
    0x2a: "system_pump_contacts", 0x2b: "alert_relay_contacts",
    0x2f: "keep_alive", 0x30: "melting_time_minutes",
}
_TEMPERATURES = {
    0x22: "slab_temperature", 0x23: "slab_setpoint", 0x24: "edge_temperature",
    0x2c: "return_temperature", 0x2d: "supply_temperature", 0x2e: "supply_target",
}


class SnowmeltOperatingState(IntEnum):
    OFF = 1
    IDLE = 2
    STORM = 3
    MELTING = 4


class WaterDetection(IntEnum):
    ABSENT = 1
    DRY = 2
    WET = 3
    OPEN = 4
    SHORT = 5
    OVERHEATED = 6


@dataclass(frozen=True, slots=True)
class SnowmeltPair:
    key: int
    raw: int

    @property
    def temperature_f(self) -> float | None:
        return fahrenheit(self.raw) if self.key in _TEMPERATURES else None

    @property
    def operating_state(self) -> SnowmeltOperatingState | None:
        if self.key == SnowmeltKey.OPERATING_STATE and self.raw >> 8 in range(1, 5):
            return SnowmeltOperatingState(self.raw >> 8)
        return None

    @property
    def water_detection(self) -> WaterDetection | None:
        if self.key == SnowmeltKey.WATER_DETECTION and self.raw in range(1, 7):
            return WaterDetection(self.raw)
        return None

    @property
    def pump_on(self) -> bool | None:
        return bool(self.raw) if self.key == SnowmeltKey.SYSTEM_PUMP and self.raw in (0, 1) else None

    def fields(self) -> Fields:
        if name := _TEMPERATURES.get(self.key):
            return temp(name, self.raw)
        if self.key == SnowmeltKey.OPERATING_STATE:
            values: Fields = {"operating_state": self.raw >> 8, "operating_substate": self.raw & 0xff}
            if (state := self.operating_state) is not None:
                values["operating_state_name"] = state.name.lower()
            return values
        name = "command_status" if 1 <= self.key <= 24 else _WORDS.get(self.key, "value_raw")
        fields: Fields = {name: self.raw}
        if (detection := self.water_detection) is not None:
            fields["water_detect_state_name"] = detection.name.lower()
        if (pump := self.pump_on) is not None:
            fields["system_pump_on"] = pump
        return fields


@dataclass(frozen=True, slots=True)
class SnowmeltReport:
    source_raw: int
    pairs: tuple[SnowmeltPair, ...]

    @classmethod
    def read(cls, reader: Reader) -> "SnowmeltReport":
        address, count = reader.take(2)
        return cls(address, tuple(SnowmeltPair(reader.byte(), reader.word()) for _ in range(count)))

    @property
    def address(self) -> SnowmeltAddress | None:
        return SnowmeltAddress.from_report(self.source_raw)

    def fields(self) -> Fields:
        fields: Fields = {"source_type": self.source_raw & 7, "source_id": self.source_raw >> 3,
                          "pairs": len(self.pairs)}
        for i, pair in enumerate(self.pairs):
            fields[f"pair{i}_key"] = f"0x{pair.key:02x}"
            fields.update((f"pair{i}_{k}", v) for k, v in pair.fields().items())
        return fields
