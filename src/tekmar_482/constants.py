"""Constants and enums for the tekmar Home Automation protocol."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum, IntFlag, StrEnum

DEFAULT_BAUDRATE = 9600


class ThaSetback(IntEnum):
    """tHA setback states."""

    WAKE_4 = 0x00
    UNOCC_4 = 0x01
    OCC_4 = 0x02
    SLEEP_4 = 0x03
    OCC_2 = 0x04
    UNOCC_2 = 0x05
    AWAY = 0x06
    CURRENT = 0x07


class ThaValue(IntEnum):
    """Common tHA boolean and not-available values."""

    OFF = 0x00
    ON = 0x01
    NA_8 = 0xFF
    NA_16 = 0xFFFF
    NA_32 = 0xFFFFFFFF


class DeviceKind(StrEnum):
    """Known high-level tHA device categories."""

    THERMOSTAT = "thermostat"
    SETPOINT = "setpoint"
    SNOWMELT = "snowmelt"


class DeviceAttributeBit(IntFlag):
    """DeviceAttributes capability bits used by the 482 firmware."""

    HEAT_SETPOINT = 1 << 0
    COOL_SETPOINT = 1 << 1
    SLAB_SETPOINT = 1 << 2
    FAN_PERCENT = 1 << 3
    HUMIDITY_SET_MAX = 1 << 5
    HUMIDITY_SET_MIN = 1 << 6
    SETPOINT_DEVICE = 1 << 7
    SLAB_SETPOINT_ALT = 1 << 8


class DeviceMode(IntEnum):
    """Device operating mode."""

    OFF = 0x00
    HEAT = 0x01
    AUTO = 0x02
    COOL = 0x03
    VENT = 0x04
    EMERGENCY = 0x06


class ActiveDemand(IntEnum):
    """Current device demand."""

    IDLE = 0x00
    HEAT = 0x01
    COOL = 0x03


@dataclass(frozen=True, slots=True)
class DeviceFeature:
    """Known capability metadata for a tekmar device type code."""

    model: str
    kind: DeviceKind
    heat: int = 0
    cool: int = 0
    fan: int = 0
    humidity: int = 0
    snow: int = 0
    emergency: int = 0

    @property
    def supports_heat(self) -> bool:
        return self.heat > 0

    @property
    def supports_cool(self) -> bool:
        return self.cool > 0

    @property
    def supports_fan(self) -> bool:
        return self.fan > 0

    @property
    def supports_humidity(self) -> bool:
        return self.humidity > 0

    @property
    def supports_snowmelt(self) -> bool:
        return self.snow > 0

    @property
    def supports_emergency_heat(self) -> bool:
        return self.emergency > 0


SETBACK_NAME: dict[ThaSetback, str] = {
    ThaSetback.WAKE_4: "WAKE_4",
    ThaSetback.UNOCC_4: "UNOCC_4",
    ThaSetback.OCC_4: "OCC_4",
    ThaSetback.SLEEP_4: "SLEEP_4",
    ThaSetback.OCC_2: "OCC_2",
    ThaSetback.UNOCC_2: "UNOCC_2",
    ThaSetback.AWAY: "AWAY",
}

SETBACK_DESCRIPTION: dict[ThaSetback, str] = {
    ThaSetback.WAKE_4: "Awake",
    ThaSetback.UNOCC_4: "Sleep",
    ThaSetback.OCC_4: "Awake",
    ThaSetback.SLEEP_4: "Sleep",
    ThaSetback.OCC_2: "Awake",
    ThaSetback.UNOCC_2: "Sleep",
    ThaSetback.AWAY: "Away",
}

SETBACK_SETPOINT_MAP: dict[int, int] = {
    ThaSetback.WAKE_4: 0x00,
    ThaSetback.UNOCC_4: 0x01,
    ThaSetback.OCC_4: 0x00,
    ThaSetback.SLEEP_4: 0x01,
    ThaSetback.OCC_2: 0x00,
    ThaSetback.UNOCC_2: 0x01,
    ThaSetback.AWAY: 0x02,
}

SETBACK_FAN_MAP: dict[int, int] = {
    ThaSetback.WAKE_4: 0x00,
    ThaSetback.UNOCC_4: 0x01,
    ThaSetback.OCC_4: 0x00,
    ThaSetback.SLEEP_4: 0x01,
    ThaSetback.OCC_2: 0x00,
    ThaSetback.UNOCC_2: 0x01,
    ThaSetback.AWAY: 0x01,
}


def _feature(
    model: str,
    kind: DeviceKind,
    *,
    heat: int = 0,
    cool: int = 0,
    fan: int = 0,
    humidity: int = 0,
    snow: int = 0,
    emergency: int = 0,
) -> DeviceFeature:
    return DeviceFeature(
        model=model,
        kind=kind,
        heat=heat,
        cool=cool,
        fan=fan,
        humidity=humidity,
        snow=snow,
        emergency=emergency,
    )


DEVICE_FEATURES: dict[int, DeviceFeature] = {
    101101: _feature("161", DeviceKind.SETPOINT, heat=1),
    101102: _feature("162", DeviceKind.SETPOINT, heat=1, cool=1),
    102301: _feature("527", DeviceKind.THERMOSTAT, heat=1),
    102302: _feature("528", DeviceKind.THERMOSTAT, heat=1),
    102303: _feature("529", DeviceKind.THERMOSTAT, heat=2),
    102304: _feature("530", DeviceKind.THERMOSTAT, heat=1, cool=1, fan=1),
    100102: _feature("537", DeviceKind.THERMOSTAT, heat=1),
    100103: _feature("538", DeviceKind.THERMOSTAT, heat=1),
    100101: _feature("540", DeviceKind.THERMOSTAT, heat=1, cool=1, fan=1),
    99301: _feature("541", DeviceKind.THERMOSTAT, heat=1),
    10599181: _feature("541", DeviceKind.THERMOSTAT, heat=1),
    99302: _feature("542", DeviceKind.THERMOSTAT, heat=1),
    99401: _feature("543", DeviceKind.THERMOSTAT, heat=2),
    99203: _feature("544", DeviceKind.THERMOSTAT, heat=1, cool=1, fan=1),
    99202: _feature("545", DeviceKind.THERMOSTAT, heat=2, cool=1, fan=1),
    99201: _feature("546", DeviceKind.THERMOSTAT, heat=2, cool=2, fan=2),
    107201: _feature("532", DeviceKind.THERMOSTAT, heat=1),
    105103: _feature("552", DeviceKind.THERMOSTAT, heat=1),
    105102: _feature("553", DeviceKind.THERMOSTAT, heat=2, cool=1, fan=1, humidity=1),
    105101: _feature("554", DeviceKind.THERMOSTAT, heat=1, cool=1, fan=1),
    104401: _feature(
        "557",
        DeviceKind.THERMOSTAT,
        heat=2,
        cool=2,
        fan=1,
        humidity=1,
        emergency=1,
    ),
    105801: _feature("654", DeviceKind.SNOWMELT, snow=1),
    108401: _feature("670", DeviceKind.SNOWMELT, snow=1),
    108402: _feature("671", DeviceKind.SNOWMELT, snow=1),
}

TN_ERRORS: dict[int, str] = {
    0x00: "No Errors",
    0x01: "EEPROM Error",
    0x02: "Internal Error",
    0x03: "Address Error or Master Device Error",
    0x04: "Device Lost Error",
    0x05: "Mixing Configuration Error",
    0x06: "tN4 Bus Communications Error",
    0x07: "Schedule Master Error",
    0x08: "Cool Group Error",
    0x09: "Device Configuration Error",
    0x0A: "Alert Input Error",
    0x0B: "Primary Pump Proof Error",
    0x0C: "No Pumps Are Running, Flow Proof Demand Input is Active",
    0x0D: "Combustion Air Proof is Missing",
    0x0E: "Combustion Air Damper is Closed, Proof Demand Input is Active",
    0x0F: "Dewpoint Configuration Error",
    0x80: "Outdoor Sensor Error",
    0x81: "System Supply Sensor Error",
    0x82: "System Return Sensor Error",
    0x83: "Heating Device Supply Sensor Error",
    0x84: "Heating Device Return Sensor Error",
    0x85: "Heating Device Outlet Sensor Error",
    0x86: "Heating Device Inlet Sensor Error",
    0x87: "Cooling Device Supply Sensor Error",
    0x88: "Cooling Device Return Sensor Error",
    0x89: "Cooling Device Inlet Sensor Error",
    0x8A: "Cooling Device Outlet Sensor Error",
    0x8B: "Mixing Supply Sensor Error",
    0x8C: "Mixing Return Sensor Error",
    0x8D: "Dhw Tank Sensor Error",
    0x8E: "Room Sensor Error",
    0x8F: "Slab Sensor Error",
    0x90: "Duct Sensor Error",
    0x91: "Remote Sensor Error",
    0x92: "Coil Return Sensor Error",
    0x93: "Tank Sensor Error",
    0x94: "Humidity Sensor Error",
    0x95: "Heat Pump Error",
    0x96: "Brown/Slab Error",
    0x97: "Yellow Error",
    0x98: "Blue Error",
    0x99: "Tandem Error",
    0xC0: "Hot Room Warning",
    0xC1: "Cold Room Warning",
    0xC2: "Freeze Protect Warning",
    0xC3: "Filter Change Warning",
    0xC4: "Snow/Ice Sensor Heater Not Heating",
    0xC5: "Snow/Ice Sensor Overheating",
    0xC6: "Snow/Ice Sensor Temperature Drift",
    0xC7: "Maximum Melt Time Exceeded",
}
