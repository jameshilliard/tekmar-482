"""Addressed 2DF field edits for known vendor writers, not advertised capabilities."""

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum, IntEnum, auto

from .layouts import H_UNAVAILABLE, MULTI_WEEK
from .scenes import SCENE_RESPONSE_FIELDS, SceneResponse


class WritableBlock(IntEnum):
    HEATING_SETUP = 0x03
    HEATING_SETPOINTS = 0x05
    COOLING_SETPOINTS = 0x06
    FLOOR_SETPOINTS = 0x07
    VENTILATION = 0x09
    SCENE_SETUP = 0x0e
    SCHEDULE_SETUP = 0x0f
    WAKE_EVENT = 0x10
    UNOCCUPIED_EVENT = 0x11
    OCCUPIED_EVENT = 0x12
    SLEEP_EVENT = 0x13
    HEAT_STAGE1_RUNTIME = 0x1c
    HEAT_STAGE2_RUNTIME = 0x1d
    COOL_STAGE1_RUNTIME = 0x1e
    COOL_STAGE2_RUNTIME = 0x1f
    FAN1_RUNTIME = 0x20
    FAN2_RUNTIME = 0x21
    DHW_SETUP = 0x24
    SETPOINT_SETUP = 0x25
    APPLICATION_SETUP = 0x26
    OPERATION_SETUP = 0x27
    SETPOINT_TARGETS = 0x28
    SETPOINT_WAKE_SLEEP = 0x29
    HOT_TUB_TARGETS = 0x2a
    HOT_TUB_WAKE_SLEEP = 0x2b
    DHW_TARGETS = 0x2c
    DHW_WAKE_SLEEP = 0x2d
    FLOOR_TARGETS = 0x2e
    FLOOR_WAKE_SLEEP = 0x2f
    POOL_TARGETS = 0x30
    POOL_WAKE_SLEEP = 0x31
    MULTI_WAKE_EVENT = 0x38
    MULTI_UNOCCUPIED_EVENT = 0x39
    MULTI_OCCUPIED_EVENT = 0x3a
    MULTI_SLEEP_EVENT = 0x3b
    BOILER_RUNTIME = 0x43
    PUMP_RUNTIME = 0x44
    ENERGY_SAVINGS = 0x4c
    TANK_RUNTIME = 0x5a
    MELT_SETPOINT = 0x71
    IDLE_SETPOINT = 0x72
    STORM_SETPOINT = 0x78
    HEAT_PUMP_RUNTIME = 0x98
    W1_HEAT_RUNTIME = 0x99
    W1_COOL_RUNTIME = 0x9a
    W2_HEAT_RUNTIME = 0x9b
    W2_COOL_RUNTIME = 0x9c
    Y1_HEAT_RUNTIME = 0x9d
    Y1_COOL_RUNTIME = 0x9e
    Y2_HEAT_RUNTIME = 0x9f
    Y2_COOL_RUNTIME = 0xa0
    HUMIDITY_RUNTIME = 0xa1
    DEHUMIDITY_RUNTIME = 0xa2
    HUMIDITY_SETPOINTS = 0xa4
    DEHUMIDITY_SETPOINTS = 0xa5

    @property
    def fields(self) -> tuple["NativeField", ...]:
        return tuple(slot.field for slot in _LAYOUTS[self].slots)


class NativeField(Enum):
    MODE_SETTING = auto()
    HEATING_CPH = auto()
    AIR_GROUP_ID = auto()
    WAKE = auto()
    UNOCCUPIED = auto()
    OCCUPIED = auto()
    SLEEP = auto()
    AWAY = auto()
    MAXIMUM = auto()
    AWAY_PERIOD = auto()
    HOLD_PERIOD = auto()
    PERMANENT_SCENE_1 = auto()
    PERMANENT_SCENE_4 = auto()
    PERMANENT_SCENE_5 = auto()
    TEMPORARY_SCENE_7 = auto()
    TEMPORARY_SCENE_8 = auto()
    SCENES_ENABLED = auto()
    HEATING_SCHEDULE_ID = auto()
    COOLING_SCHEDULE_ID = auto()
    SCHEDULE_CYCLING_RATE = auto()
    NUM_DAILY_EVENTS = auto()
    OPTIMUM_START_OPERATION = auto()
    MONDAY = auto()
    TUESDAY = auto()
    WEDNESDAY = auto()
    THURSDAY = auto()
    FRIDAY = auto()
    SATURDAY = auto()
    SUNDAY = auto()
    MASTER_FLAG = auto()
    HOURS = auto()
    MODE = auto()
    DHW_SENSOR_USAGE = auto()
    DHW_BOILER_COUNT = auto()
    APPLICATION_MODE = auto()
    MODE_CONFIGURATION = auto()
    RUN_MODE_CONFIGURATION = auto()
    RELAY_TWO_SAMPLING = auto()
    RELAY_TWO_START_TIME = auto()
    RELAY_TWO_DURATION = auto()
    SETPOINT_ENABLE_ADDRESS = auto()
    OUTDOOR_TEMP_ENABLE = auto()
    EXCHANGE_SUPPLY_TEMP = auto()
    MAXIMUM_SUPPLY_TEMP = auto()
    CYCLE_TIME = auto()
    DIFFERENTIAL = auto()
    MINIMUM_RELAY_ON_TIME = auto()
    MINIMUM_RELAY_OFF_TIME = auto()
    HEAT_SOURCE_TYPE = auto()
    PRIORITY = auto()
    WAKE_OCCUPIED = auto()
    UNOCCUPIED_SLEEP = auto()
    SLAB_TARGET = auto()

    @property
    def label(self) -> str:
        return self.name.lower()


@dataclass(frozen=True, slots=True)
class _Slot:
    field: NativeField
    offset: int
    width: int
    mask: int
    shift: int = 0

    @property
    def support(self) -> int:
        return ((1 << self.width) - 1) << self.offset

    def put(self, data: bytearray, value: int) -> None:
        area = slice(self.offset, self.offset + self.width)
        previous = int.from_bytes(data[area], "little")
        data[area] = ((previous & ~(self.mask << self.shift)) | value << self.shift).to_bytes(self.width, "little")


@dataclass(frozen=True, slots=True)
class _Layout:
    size: int
    slots: tuple[_Slot, ...]


def _slots(fields: tuple[NativeField, ...], *, width: int = 1, offset: int = 0) -> tuple[_Slot, ...]:
    return tuple(_Slot(f, offset + i * width, width, (1 << (8 * width)) - 1) for i, f in enumerate(fields))


def _layout(*fields: NativeField, width: int = 1, offset: int = 0) -> _Layout:
    return _Layout(offset + len(fields) * width, _slots(fields, width=width, offset=offset))


F = NativeField
_SETBACKS = (F.WAKE, F.UNOCCUPIED, F.OCCUPIED, F.SLEEP, F.AWAY)
_DAYS = (F.MONDAY, F.TUESDAY, F.WEDNESDAY, F.THURSDAY, F.FRIDAY, F.SATURDAY, F.SUNDAY)
_SCHEDULE = (F.HEATING_SCHEDULE_ID, F.COOLING_SCHEDULE_ID, F.SCHEDULE_CYCLING_RATE,
             F.NUM_DAILY_EVENTS, F.OPTIMUM_START_OPERATION)
_LAYOUTS = {
    0x03: _layout(F.MODE_SETTING, F.HEATING_CPH, F.AIR_GROUP_ID),
    **dict.fromkeys((0x05, 0x06, 0x09, 0xa4, 0xa5), _layout(*_SETBACKS)),
    0x07: _layout(*_SETBACKS, F.MAXIMUM),
    0x0e: _layout(F.AWAY_PERIOD, F.HOLD_PERIOD, F.PERMANENT_SCENE_1, F.PERMANENT_SCENE_4, F.PERMANENT_SCENE_5,
                  F.TEMPORARY_SCENE_7, F.TEMPORARY_SCENE_8, F.SCENES_ENABLED),
    0x0f: _Layout(6, _slots(_SCHEDULE)),  # Writer emits zero for the sixth byte.
    **dict.fromkeys(range(0x10, 0x14), _layout(*_DAYS)),
    **dict.fromkeys((*range(0x1c, 0x22), 0x4c, *range(0x99, 0xa3)), _layout(F.HOURS, width=2)),
    0x24: _layout(F.MODE, *_SETBACKS, F.DHW_SENSOR_USAGE, F.DHW_BOILER_COUNT),
    0x25: _layout(F.MODE, *_SETBACKS),
    0x26: _layout(F.APPLICATION_MODE, F.MODE_CONFIGURATION, F.RUN_MODE_CONFIGURATION, F.RELAY_TWO_SAMPLING,
                  F.RELAY_TWO_START_TIME, F.RELAY_TWO_DURATION, F.SETPOINT_ENABLE_ADDRESS, F.OUTDOOR_TEMP_ENABLE),
    0x27: _layout(F.EXCHANGE_SUPPLY_TEMP, F.MAXIMUM_SUPPLY_TEMP, F.CYCLE_TIME, F.DIFFERENTIAL,
                  F.MINIMUM_RELAY_ON_TIME, F.MINIMUM_RELAY_OFF_TIME, F.HEAT_SOURCE_TYPE, F.PRIORITY),
    **dict.fromkeys(range(0x28, 0x32, 2), _layout(F.WAKE_OCCUPIED, F.UNOCCUPIED_SLEEP, F.AWAY, F.MAXIMUM, width=2)),
    **dict.fromkeys(range(0x29, 0x32, 2), _layout(F.WAKE, F.SLEEP, width=2)),
    **dict.fromkeys(MULTI_WEEK, _Layout(8, (
        _Slot(F.MASTER_FLAG, 0, 1, 1, 3), _Slot(F.SCHEDULE_CYCLING_RATE, 0, 1, 3, 4),
        _Slot(F.NUM_DAILY_EVENTS, 0, 1, 3, 6), *_slots(_DAYS, offset=1)))),
    **dict.fromkeys((0x43, 0x44, 0x5a, 0x98), _layout(F.HOURS, width=2, offset=1)),
    **dict.fromkeys((0x71, 0x72, 0x78), _layout(F.SLAB_TARGET, width=2)),
}


@dataclass(frozen=True, slots=True)
class BlockPatch:
    """Validated host intent; only apply to a fresh report from the selected peer."""

    block: WritableBlock
    edits: tuple[tuple[_Slot, int], ...]

    @classmethod
    def from_fields(cls, block: int, values: Mapping[NativeField, int]) -> "BlockPatch":
        if block not in _LAYOUTS:
            raise ValueError(f"no documented native writer for block {block:#x}")
        if not values or any(not isinstance(field, NativeField) for field in values):
            raise ValueError("expected at least one typed native field")
        slots = {slot.field: slot for slot in _LAYOUTS[block].slots}
        edits = []
        for field, value in values.items():
            slot = slots.get(field)
            if slot is None:
                raise ValueError(f"{field.label} is not an editable field of block {block:#x}")
            if not isinstance(value, int) or not 0 <= value <= slot.mask:
                raise ValueError(f"{field.label} does not fit its wire field")
            if field in _DAYS and value > 144:
                raise ValueError("a schedule day must be a ten-minute slot 0..143 or disabled (144)")
            if field.label in SCENE_RESPONSE_FIELDS and value not in range(len(SceneResponse)):
                raise ValueError("unknown scene response setting")
            if field is F.SCENES_ENABLED and value not in (0, 1):
                raise ValueError("scenes_enabled must be zero or one")
            if block == 0x5a and value:
                raise ValueError("only a reset to zero is documented for tank runtime")
            if block in (0x05, 0x06, 0x07) and value == 0xff:
                raise ValueError("cannot set an unavailable temperature")
            if (0x28 <= block <= 0x31 or field is F.SLAB_TARGET) and value in H_UNAVAILABLE:
                raise ValueError("cannot set an unavailable temperature")
            if block == 0x71 and value == 0:
                raise ValueError("the melt slab target has no Off setting")
            edits.append((slot, value))
        return cls(WritableBlock(block), tuple(edits))

    def apply(self, support: int, body: bytes) -> bytes:
        """Produce positional update data without changing or synthesizing siblings."""
        width = _LAYOUTS[self.block].size
        if self.block == 0x2e and len(body) == 6 and not support & 0xc0:
            width = 6  # Some floor controls have no fourth word.
        if len(body) != width or support >> width:
            raise ValueError("a native update requires the complete positional block")
        if self.block == 0x0f and body[5] != 0:
            raise ValueError("the schedule writer cannot preserve the nonzero RTC byte")
        data = bytearray(body)
        for slot, value in self.edits:
            if support & slot.support != slot.support:
                raise ValueError(f"device does not report all bytes of {slot.field.label}")
            slot.put(data, value)
        return bytes(data)

    def matches(self, support: int, body: bytes) -> bool | None:
        """Compare only edited bits. Missing or ambiguous readback is not a mismatch."""
        count, span = support.bit_count(), support.bit_length()
        if span > _LAYOUTS[self.block].size or len(body) > _LAYOUTS[self.block].size:
            return None
        positional = len(body) >= span
        if not positional and len(body) != count:
            return None
        if any(support & slot.support != slot.support for slot, _ in self.edits):
            return None
        for slot, expected in self.edits:
            offset = slot.offset if positional else (support & ((1 << slot.offset) - 1)).bit_count()
            value = int.from_bytes(body[offset:offset + slot.width], "little")
            if (value >> slot.shift) & slot.mask != expected:
                return False
        return True
