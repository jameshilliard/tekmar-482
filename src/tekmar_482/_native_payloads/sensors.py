"""483 SW 11 thermostat inputs. Interpret status only after identifying its source."""

from collections.abc import Mapping
from dataclasses import dataclass
from enum import IntEnum

from .codec import fahrenheit as _fahrenheit
from .layouts import H_UNAVAILABLE
from .products import NativeProduct

SETUP = 0x0d
ROOM_STATS = 0x15
SENSOR_BLOCKS = (SETUP, *range(ROOM_STATS, 0x1b))
_SWAPPED_ONBOARD = {"544": 8, "545": 9, "546": 9}  # Last affected Z.


class SensorSlot(IntEnum):
    ONBOARD = 0
    EXTERNAL_1 = 1
    EXTERNAL_2 = 2
    EXTERNAL_3 = 3
    EXTERNAL_4 = 4

    @property
    def block(self) -> int:
        return 0x16 + self

    @property
    def setup_field(self) -> str:
        return "onboard_sensor" if self is self.ONBOARD else f"sensor{self.value}"


class SensorRole(IntEnum):
    OFF = 0
    ROOM = 1
    FLOOR = 2
    REMOTE = 3
    OUTDOOR = 4
    COIL = 5
    DUCT = 6
    SETPOINT = 7
    HOT_TUB = 8
    DHW = 9
    POOL = 10


def _int(fields: Mapping[str, object], name: str) -> int | None:
    value = fields.get(name)
    return value if isinstance(value, int) else None


def _role(raw: int | None) -> SensorRole | None:
    return SensorRole(raw) if raw is not None and raw in range(len(SensorRole)) else None


def _status_role(model: str, z: int | None, slot: SensorSlot, raw: int | None,
                 current: int | None) -> SensorRole | None:
    if slot is SensorSlot.ONBOARD and model in _SWAPPED_ONBOARD and raw in (0, 1):
        if z is None:
            return None
        if z <= _SWAPPED_ONBOARD[model]:
            return SensorRole.ROOM if raw == 0 else SensorRole.OFF
    if slot is SensorSlot.EXTERNAL_1 and model == "538" and (z is None or z <= 3):
        if z is None or current is None or current in H_UNAVAILABLE:
            return None
        return SensorRole.FLOOR if current >= 270 else SensorRole.OFF
    return _role(raw)


@dataclass(frozen=True, slots=True)
class SensorInput:
    slot: SensorSlot
    setup_raw: int | None
    reported_raw: int | None
    status_role: SensorRole | None
    display_role: SensorRole | None
    current_raw: int | None
    minimum_raw: int | None
    maximum_raw: int | None

    @property
    def conflict(self) -> bool:
        return self.setup_raw is not None and self.status_role is not None and self.setup_raw != self.status_role

    @property
    def current_f(self) -> float | None:
        return _fahrenheit(self.current_raw) if self.display_role not in (None, SensorRole.OFF) else None

    @property
    def minimum_f(self) -> float | None:
        return self._historical_f(self.minimum_raw)

    @property
    def maximum_f(self) -> float | None:
        return self._historical_f(self.maximum_raw)

    def _historical_f(self, raw: int | None) -> float | None:
        # SW 11's OFF-as-floor display workaround applies only to the current sample.
        if self.status_role is SensorRole.OFF or self.display_role in (None, SensorRole.OFF):
            return None
        return _fahrenheit(raw)

    @classmethod
    def _decode(cls, slot: SensorSlot, model: str, z: int | None, setup: int | None,
                status: Mapping[str, object]) -> "SensorInput":
        raw = _int(status, "sensor_configuration")
        current, minimum, maximum = (_int(status, k + "_value_raw") for k in ("current", "minimum", "maximum"))
        role = _status_role(model, z, slot, raw, current)
        display = role
        if setup is not None:
            # A separate setup observation can interpret a status without a
            # role, but never override an unknown code or conflicting assignment.
            if role is not None and setup != role:
                display = None
            elif raw is None and role is None:
                display = _role(setup)
        elif (role is SensorRole.OFF and slot is SensorSlot.EXTERNAL_1
              and z is not None and z <= {"528": 1, "538": 4}.get(model, -1)
              and _fahrenheit(current) is not None):
            display = SensorRole.FLOOR  # SW 11 presentation hint, not a setup change.
        return cls(slot, setup, raw, role, display, current, minimum, maximum)


@dataclass(frozen=True, slots=True)
class RoomTemperatures:
    average_raw: int | None
    minimum_raw: int | None
    maximum_raw: int | None

    @property
    def average_f(self) -> float | None:
        return _fahrenheit(self.average_raw)

    @property
    def minimum_f(self) -> float | None:
        return _fahrenheit(self.minimum_raw)

    @property
    def maximum_f(self) -> float | None:
        return _fahrenheit(self.maximum_raw)


@dataclass(frozen=True, slots=True)
class ThermostatSensors:
    product: NativeProduct
    z_number: int | None
    inputs: tuple[SensorInput, ...]
    room: RoomTemperatures | None

    @classmethod
    def from_blocks(cls, product: NativeProduct, z: int | None,
                    blocks: Mapping[int, Mapping[str, object]]) -> "ThermostatSensors":
        """Use only complete decoded fields from reports belonging to one thermostat."""
        if product.family != "thermostat":
            raise ValueError("thermostat sensor semantics require an identified thermostat")
        z = z if z is not None and z in range(26) else None
        setup = blocks.get(SETUP, {})
        inputs = tuple(SensorInput._decode(slot, product.model, z, _int(setup, slot.setup_field),
                                           blocks.get(slot.block, {})) for slot in SensorSlot
                       if slot.setup_field in setup or slot.block in blocks)
        room = blocks.get(ROOM_STATS)
        return cls(product, z, inputs, None if room is None else RoomTemperatures(
            _int(room, "average_room_temp_raw"), _int(room, "minimum_room_temp_raw"),
            _int(room, "maximum_room_temp_raw")))

    @property
    def average_floor_f(self) -> float | None:
        """Mean of the assigned floor inputs, only when every contributor has a reading."""
        if self.product.model in ("528", "538"):
            return next((s.current_f for s in self.inputs if s.slot is SensorSlot.EXTERNAL_1
                         and s.status_role is SensorRole.FLOOR), None)
        assigned = [s for s in self.inputs if s.setup_raw == SensorRole.FLOOR]
        if not assigned or any(s.current_f is None for s in assigned):
            return None
        return _fahrenheit(sum(s.current_raw for s in assigned if s.current_raw is not None) // len(assigned))
