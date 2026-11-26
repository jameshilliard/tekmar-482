"""Native 3D7 fault words shared by Raw reports and addressless tHA NetworkError."""

from dataclasses import dataclass
from enum import StrEnum

from .codec import Fields, Reader

NETWORK_ERROR = 0x3D7

ERROR_DESCRIPTIONS: dict[int, str] = {
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

# Descriptor tables are local to each code, not interchangeable sensor IDs.
_DETAILS = {
    0x01: ("adjust menu", "monitor menu", "schedule menu", "miscellaneous menu",
           "control-variable read", "EEPROM update", "time menu", "test menu", "scene menu"),
    0x02: ("enclosure overheated", "internal sensor open", "internal sensor shorted"),
    0x05: ("module on an unsupported bus", "required module missing", "module no longer present",
           "module not responding", "multiple modules on one bus", "module internal fault"),
    0x09: ("all thermostat sensors disabled", "bus/return sensor conflict", "stager setting conflict",
           "unexpected reset control", "DHW tank sensor and demand both selected",
           "heat/cool actuator already assigned", "configured bus energy source changed",
           "required duct sensor missing", "duplicate snowmelt zone", "duplicate snow/ice sensor",
           "no relay configured for heating"),
}


class SensorCircuit(StrEnum):
    OPEN = "open"
    SHORT = "short"


@dataclass(frozen=True, slots=True)
class NetworkFault:
    """The exact little-endian word. Priority here is not the tN4 header priority."""

    raw: int

    def __post_init__(self) -> None:
        if not 0 <= self.raw <= 0xffff:
            raise ValueError("a network fault must be a u16")

    @classmethod
    def read(cls, reader: Reader) -> "NetworkFault":
        return cls(reader.word())

    @property
    def code(self) -> int:
        return self.raw & 0xff

    @property
    def instance(self) -> int:
        return (self.raw >> 8) & 0x3f

    @property
    def priority(self) -> int:
        return self.raw >> 14

    @property
    def is_clear(self) -> bool:
        return self.code == 0

    @property
    def description(self) -> str:
        return ERROR_DESCRIPTIONS.get(self.code, "Unknown Error")

    @property
    def circuit(self) -> SensorCircuit | None:
        if 0x80 <= self.code <= 0x92 or self.code in (0x93, 0x94) and self.instance & 0x0f == 0:
            return SensorCircuit.OPEN if self.instance & 0x20 else SensorCircuit.SHORT
        return None

    @property
    def detail(self) -> str | None:
        code, descriptor = self.code, self.instance
        if code in _DETAILS:
            items = _DETAILS[code]
            return items[descriptor] if descriptor < len(items) else None
        if code == 0x03:
            return "duplicate reset controls" if descriptor == 0x1f else f"duplicate thermostat address {descriptor}"
        if code == 0x04:
            return f"missing device {descriptor & 0x1f}"
        if code in (0x07, 0x08):
            role = "schedule" if code == 0x07 else "cool group"
            state = "duplicate" if descriptor & 0x20 else "missing"
            return f"{state} master for {role} {descriptor & 0x1f}"
        if code == 0x0a:
            return f"alert input {descriptor}"
        if code == 0x0b and descriptor:
            # SW 11 displays the first asserted pump bit, numbered from one.
            return f"no flow proof for pump {(descriptor & -descriptor).bit_length()}"
        if 0x81 <= code <= 0x82:
            index = descriptor & 0x0f
            systems = ("boiler", "chiller", "heating duct", "cooling duct")
            return systems[index] if index < len(systems) else None
        if 0x83 <= code <= 0x8a:
            return f"plant device {(descriptor & 0x0f) + 1}"
        if code in (0x90, 0x92) and descriptor & 0x0f:
            return f"cool group {descriptor & 0x0f}"
        if 0x95 <= code <= 0x99:
            subject = ("heat pump", "edge sensor", "centre sensor", "water sensor", "remote sensor")[code - 0x95]
            index = descriptor & (0x1f if code == 0x99 else 0x0f)
            return f"{subject} {index + 1}" if index else subject
        return None

    def fields(self) -> Fields:
        result: Fields = {"code": self.code, "instance": self.instance, "priority": self.priority,
                          "description": self.description, "is_clear": int(self.is_clear)}
        if self.circuit is not None:
            result["circuit"] = self.circuit.value
        if self.detail is not None:
            result["detail"] = self.detail
        return result
