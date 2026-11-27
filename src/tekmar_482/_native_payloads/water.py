"""Hydronic status and loop assignments from the 483 SW 11 native layouts."""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import Enum, IntEnum

from .codec import Fields, Reader, fahrenheit, status_flags, temp

MASTER_PRESENT = 0x3ff
MASTER_STATUS = 0x3cf
PLANT_LOOP_STATUS = 0x3ce
DHW_STATUS = 0x23f
TANK_STATUS = 0x173
BOILER_MAXIMUM = 0x337
GENERAL_DEMAND = 0x363
STAGE_DEMAND = 0x367
WATER_CYCLE = 0x37f
RELAY_HEARTBEAT = 0x3c7
SETUP_SLOTS = {0x33: range(1, 5), 0x34: range(5, 8)}


def subnet_fields(value: int) -> Fields:
    return {"water_subnet": value, "water_bus": value & 7, "water_control": value >> 3}


class StageCommand(IntEnum):
    STAGE_HEAT_DEMAND_REQUEST = 0x35f
    END_PURGE = 0x397
    START_PURGE = 0x39f
    START_OVERRIDE_ON = 0x3a7
    END_OVERRIDE_OFF = 0x3af
    END_OVERRIDE_ON = 0x3b7
    START_OVERRIDE_OFF = 0x3bf


@dataclass(frozen=True, slots=True)
class StageSelection:
    """483 NodeMaskIn is 24 bits, unlike a BlockRequest's NodeMask32In."""

    subnet: int
    stage: int
    output_level: int
    nodes_mask: int

    def __post_init__(self) -> None:
        for name, value, width in (("subnet", self.subnet, 8), ("stage", self.stage, 1),
                                   ("output_level", self.output_level, 7), ("nodes_mask", self.nodes_mask, 24)):
            if not isinstance(value, int) or not 0 <= value < 1 << width:
                raise ValueError(f"{name} must be a {width}-bit unsigned integer")

    @classmethod
    def from_nodes(
        cls, subnet: int, nodes: Iterable[int], *, stage: int, output_level: int
    ) -> "StageSelection":
        """Select at least one native node by its one-based number."""
        mask = 0
        for node in nodes:
            if not isinstance(node, int) or not 1 <= node <= 24:
                raise ValueError("stage commands select native nodes 1..24")
            mask |= 1 << (node - 1)
        if not mask:
            raise ValueError("stage commands require at least one native node")
        return cls(subnet, stage, output_level, mask)

    @classmethod
    def read(cls, r: Reader) -> "StageSelection":
        subnet, flags = r.take(2)
        return cls(subnet, flags >> 7, flags & 0x7f, int.from_bytes(r.take(3), "little"))

    @property
    def nodes(self) -> tuple[int, ...]:
        return tuple(i + 1 for i in range(24) if self.nodes_mask & (1 << i))

    @property
    def payload(self) -> bytes:
        return bytes((self.subnet, self.stage << 7 | self.output_level)) + self.nodes_mask.to_bytes(3, "little")

    def fields(self) -> Fields:
        return {"target_subnet": self.subnet, "output_level": self.output_level, "stage": self.stage,
                "target_nodes_mask": f"0x{self.nodes_mask:06x}"}


@dataclass(frozen=True, slots=True)
class StageDemand:
    subnet: int
    flags: int
    output_flags: int
    target_raw: int | None

    @classmethod
    def read(cls, r: Reader) -> "StageDemand":
        subnet, flags, output = r.take(3)
        return cls(subnet, flags, output, r.word() if output & 0x80 else None)

    @property
    def stage(self) -> int:
        return self.flags & 1

    @property
    def output_on(self) -> bool:
        return bool(self.output_flags & 0x80)

    def fields(self) -> Fields:
        b, p = self.flags, self.output_flags
        result = subnet_fields(self.subnet)
        result.update(stage=self.stage, override_status=b >> 1 & 3, first_report=b >> 3 & 1,
                      setpoint_control=b >> 4 & 1, dhw_control=b >> 5 & 1, priority_level=b >> 6,
                      pump_delay=p & 63, pump_not_required=p >> 6 & 1, output_on=int(self.output_on))
        return result | temp("target_temp", self.target_raw) if self.target_raw is not None else result


@dataclass(frozen=True, slots=True)
class GeneralDemand:
    subnet: int
    flags: int

    @classmethod
    def read(cls, r: Reader) -> "GeneralDemand":
        return cls(*r.take(2))

    def fields(self) -> Fields:
        return subnet_fields(self.subnet) | {"cooling": self.flags >> 6 & 1,
                "demand": self.flags >> 7, "reserved": self.flags & 63}


@dataclass(frozen=True, slots=True)
class BoilerMaximum:
    raw: int

    @property
    def percent(self) -> int | None:
        return self.raw if self.raw <= 100 else None

    def fields(self) -> Fields:
        return {"max_value": self.raw}


@dataclass(frozen=True, slots=True)
class WaterCycle:
    """WaterCycleStart plant fields; neither one is a host read deadline."""

    flags: int

    @property
    def cycle_length(self) -> int:
        return self.flags & 0x7F

    @property
    def hold_override(self) -> bool:
        return bool(self.flags & 0x80)

    def fields(self) -> Fields:
        return {"cycle_length": self.cycle_length, "hold_override": int(self.hold_override)}


@dataclass(frozen=True, slots=True)
class RelayHeartbeat:
    subnet: int

    def fields(self) -> Fields:
        return subnet_fields(self.subnet)


class LoopKind(str, Enum):
    BOILER = "boiler"
    MIX = "mix"
    TANK = "tank"
    UNLINKED = "unlinked"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class HeatSource:
    raw: int

    @property
    def kind(self) -> LoopKind:
        if not self.raw:
            return LoopKind.UNLINKED
        return (LoopKind.MIX, LoopKind.TANK, LoopKind.UNKNOWN, LoopKind.BOILER)[self.raw >> 4 & 3]

    @property
    def loop(self) -> int:
        return self.raw & 15

    @property
    def mode(self) -> int:
        return self.raw >> 6


@dataclass(frozen=True, slots=True)
class LoopAssignment:
    slot: int
    subnet: int
    heat_source: HeatSource | None = None  # None for the positional MasterPresent layout.

    @property
    def kind(self) -> LoopKind:
        if self.heat_source is not None:
            return self.heat_source.kind
        return LoopKind.MIX if self.slot else LoopKind.BOILER

    @property
    def mix_number(self) -> int | None:
        if self.kind is not LoopKind.MIX:
            return None
        return self.heat_source.loop if self.heat_source is not None else self.slot

@dataclass(frozen=True, slots=True)
class LoopSetupSlot:
    slot: int
    subnet: int | None
    heat_source: HeatSource | None

    @classmethod
    def from_block(cls, block: int, fields: Mapping[str, object]) -> tuple["LoopSetupSlot", ...]:
        slots = []
        for n in SETUP_SLOTS[block]:
            subnet, source = (fields.get(f"bus{n}_{k}") for k in ("subnet", "heat_source_id"))
            if isinstance(subnet, int) or isinstance(source, int):
                slots.append(cls(n, subnet if isinstance(subnet, int) else None,
                                 HeatSource(source) if isinstance(source, int) else None))
        return tuple(slots)


@dataclass(frozen=True, slots=True)
class LoopTopology:
    announced: tuple[int, ...] = ()  # Zero slots retain their position but do not advertise a loop.
    setup1: tuple[LoopSetupSlot, ...] | None = None
    setup2: tuple[LoopSetupSlot, ...] | None = None

    def assignment_for(self, subnet: int) -> LoopAssignment | None:
        """Only label the sender's own bus, never every subnet advertised on a port."""
        if setup := (self.setup1 or ()) + (self.setup2 or ()):
            exact_setup = tuple(s for s in setup if s.subnet == subnet)
            candidates = exact_setup or tuple(s for s in setup if s.subnet is not None and s.subnet & 7 == subnet & 7)
            if len(candidates) == 1:
                slot = candidates[0]
                if slot.subnet is not None and slot.heat_source is not None:
                    return LoopAssignment(slot.slot, slot.subnet, slot.heat_source)
            return None
        slots = tuple(LoopAssignment(i, s) for i, s in enumerate(self.announced) if s)
        exact = tuple(s for s in slots if s.subnet == subnet)
        matches = exact or tuple(s for s in slots if s.subnet & 7 == subnet & 7)
        return matches[0] if len(matches) == 1 else None


@dataclass(frozen=True, slots=True)
class LoopStatus:
    supply_raw: int
    return_raw: int
    target_raw: int
    mode: int
    loop_output_pct: int
    pump_output_pct: int
    flags: int
    dhw_supply_raw: int | None

    @classmethod
    def read(cls, r: Reader, *, master: bool) -> "LoopStatus":
        supply, ret, packed = r.word(), r.word(), r.word()
        loop, pump, flags = r.take(3)
        dhw = r.word() if master and len(r.remaining) >= 2 else None
        return cls(supply, ret, packed & 0x1fff, packed >> 13, loop, pump, flags, dhw)

    @property
    def supply_f(self) -> float | None:
        return fahrenheit(self.supply_raw)

    @property
    def return_f(self) -> float | None:
        return fahrenheit(self.return_raw)

    @property
    def target_f(self) -> float | None:
        return fahrenheit(self.target_raw)

    @property
    def dhw_supply_f(self) -> float | None:
        return fahrenheit(self.dhw_supply_raw)

    def fields(self) -> Fields:
        modes = ("neither", "heating", "heating_cooling_disallowed", "cooling", "cooling_heating_disallowed")
        b = self.flags
        result = temp("supply_temp", self.supply_raw) | temp("return_temp", self.return_raw) | temp("target_temp", self.target_raw)
        result.update(heat_cool_mode=self.mode, mode_name=modes[self.mode] if self.mode < len(modes) else "unknown",
                      loop_output_pct=self.loop_output_pct, pump_output_pct=self.pump_output_pct,
                      setback_status=b & 7, weather_shutdown=b >> 3 & 1, operating_on_max=b >> 4 & 1,
                      operating_on_min=b >> 5 & 1, output_limited=b >> 6 & 1, reserved=b >> 7)
        if self.dhw_supply_raw is not None:
            result.update(temp("dhw_supply_temp", self.dhw_supply_raw))
        return result


@dataclass(frozen=True, slots=True)
class DhwStatus:
    flags: int
    output: int
    target_raw: int
    tank_raw: int

    @classmethod
    def read(cls, r: Reader) -> "DhwStatus":
        flags, output, target, tank = r.take(4)
        return cls(flags, output, target, tank)

    @property
    def target_f(self) -> float | None:
        return fahrenheit(self.target_raw, deg_e=True)

    @property
    def tank_f(self) -> float | None:
        return fahrenheit(self.tank_raw, deg_e=True)

    @property
    def demand(self) -> bool:
        return bool(self.output & 4)

    def fields(self) -> Fields:
        b = self.output
        return (status_flags(self.flags) | {"actuator_status": b & 3, "demand_status": int(self.demand),
                "heat_cool_mode": b >> 3 & 3, "operating_mode": b >> 5}
                | temp("target_temp", self.target_raw, deg_e=True) | temp("tank_temp", self.tank_raw, deg_e=True))


@dataclass(frozen=True, slots=True)
class TankStatus:
    tank_id: int
    pump_output_pct: int
    tank_raw: int
    target_raw: int

    @classmethod
    def read(cls, r: Reader) -> "TankStatus":
        tank_id, pump, tank, target = r.take(4)
        return cls(tank_id, pump, tank, target)

    def fields(self) -> Fields:
        return {"tank_id": self.tank_id, "pump_output_pct": self.pump_output_pct,
                "tank_temp_raw": self.tank_raw, "target_temp_raw": self.target_raw}
