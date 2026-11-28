"""Native thermostat configuration and air-group telemetry (483 SW 11)."""

from collections.abc import Mapping
from dataclasses import dataclass
from enum import IntEnum

from .codec import SETBACK, Fields, Reader, fahrenheit, integer_field, temp
from .products import NativeProduct
from .thermostat import ThermostatTarget

AIR_GROUP_CONFIG = 0x36a
AIR_MEMBER = 0x36f
AIR_MASTER = 0x377
STAGE_SETUP = 0x01
HEATING_SETUP = 0x03
COOLING_SETUP = 0x04
FLOOR_COOLING_ENABLE = 0x58
HEAT_PUMP_CONFIG = 0x97
MODE_BLOCKS = (HEATING_SETUP, COOLING_SETUP, HEAT_PUMP_CONFIG)
EXTRA_CLIMATE_SETUP = (STAGE_SETUP, FLOOR_COOLING_ENABLE)


class ThermostatMode(IntEnum):
    OFF = 0
    HEAT = 1
    AUTO = 2
    COOL = 3
    VENT = 4
    HUMIDITY = 5
    EMERGENCY = 6
    BACKUP = 7

    @classmethod
    def decode(cls, raw: int | None) -> "ThermostatMode | None":
        return cls(raw) if raw is not None and raw in range(len(cls)) else None

    def setup_dependencies(self, product: NativeProduct) -> tuple[int, ...]:
        if self in (self.AUTO, self.COOL):
            return tuple(b for b in (COOLING_SETUP, HEAT_PUMP_CONFIG) if b in product.candidate_blocks)
        if self is self.EMERGENCY and product.model == "553":
            return (HEAT_PUMP_CONFIG,)
        return ()


# SW 11 decodes Humidity and Backup but omits both from the remote selector.
REMOTE_MODES = (ThermostatMode.AUTO, ThermostatMode.HEAT, ThermostatMode.COOL,
                ThermostatMode.VENT, ThermostatMode.OFF, ThermostatMode.EMERGENCY)


@dataclass(frozen=True, slots=True)
class HeatingStageSetup:
    terminal_raw: int | None
    pump_delay_raw: int | None
    pump_required: bool | None


@dataclass(frozen=True, slots=True)
class StageSetup:
    heating_stage1: HeatingStageSetup
    heating_stage2: HeatingStageSetup
    stage2_heat_source_raw: int | None

    @classmethod
    def from_report(cls, fields: Mapping[str, object]) -> "StageSetup":
        def stage(n: int) -> HeatingStageSetup:
            prefix = f"stage{n}_"
            pump = integer_field(fields, prefix + "pump_required")
            return HeatingStageSetup(integer_field(fields, prefix + "terminal_raw"),
                                     integer_field(fields, prefix + "pump_delay"),
                                     None if pump is None else bool(pump))

        return cls(stage(1), stage(2), integer_field(fields, "stage2_heat_source_raw"))


@dataclass(frozen=True, slots=True)
class CoolingSetup:
    stages: int | None
    minimum_run_time_raw: int | None
    minimum_off_time_raw: int | None
    cycles_per_hour: int | None
    heat_cool_interlock_raw: int | None
    heat_cool_priority_raw: int | None

    @classmethod
    def from_report(cls, fields: Mapping[str, object]) -> "CoolingSetup":
        return cls(integer_field(fields, "supported_cooling_stages"),
                   integer_field(fields, "minimum_cooling_run_time"),
                   integer_field(fields, "minimum_cooling_off_time"),
                   integer_field(fields, "cooling_cph"),
                   integer_field(fields, "heat_cool_interlock"),
                   integer_field(fields, "heat_cool_priority"))


@dataclass(frozen=True, slots=True)
class HeatPumpSetup:
    available_mask: int | None
    enabled_mask: int | None

    @classmethod
    def from_report(cls, fields: Mapping[str, object]) -> "HeatPumpSetup":
        return cls(integer_field(fields, "available_heat_pumps"),
                   integer_field(fields, "enabled_heat_pumps"))


@dataclass(frozen=True, slots=True)
class ThermostatConfiguration:
    product: NativeProduct
    z_number: int | None
    mode_raw: int | None
    heating_cph: int | None
    air_group_id: int | None
    stage_setup: StageSetup | None
    cooling_setup: CoolingSetup | None
    heat_pump_setup: HeatPumpSetup | None
    floor_cooling_raw: int | None

    @classmethod
    def from_blocks(cls, product: NativeProduct, z: int | None,
                    blocks: Mapping[int, Mapping[str, object]]) -> "ThermostatConfiguration":
        if product.family != "thermostat":
            raise ValueError("thermostat mode requires a thermostat identity")

        heat = blocks.get(HEATING_SETUP, {})
        stage, cool, pumps = (blocks.get(b) for b in (STAGE_SETUP, COOLING_SETUP, HEAT_PUMP_CONFIG))
        return cls(product, z, integer_field(heat, "mode_setting"),
                   integer_field(heat, "heating_cph"), integer_field(heat, "air_group_id"),
                   StageSetup.from_report(stage) if stage is not None else None,
                   CoolingSetup.from_report(cool) if cool is not None else None,
                   HeatPumpSetup.from_report(pumps) if pumps is not None else None,
                   integer_field(blocks.get(FLOOR_COOLING_ENABLE, {}), "floor_cooling_enable"))

    @property
    def cooling_stages(self) -> int | None:
        return self.cooling_setup.stages if self.cooling_setup is not None else None

    @property
    def enabled_heat_pumps(self) -> int | None:
        return self.heat_pump_setup.enabled_mask if self.heat_pump_setup is not None else None

    @property
    def floor_cooling_enabled(self) -> bool | None:
        """Observed option; neither a cooling stage nor a current demand."""
        return bool(self.floor_cooling_raw) if self.floor_cooling_raw in (0, 1) else None

    @property
    def mode(self) -> ThermostatMode | None:
        return ThermostatMode.decode(self.mode_raw)

    def offers(self, mode: ThermostatMode) -> bool | None:
        """SW 11's remote selector, not a list of every mode a unit can report."""
        if not isinstance(mode, ThermostatMode):
            raise TypeError("expected a native thermostat mode")
        if mode not in REMOTE_MODES:
            return False
        model = self.product.model
        if model == "538":
            if self.z_number is None or self.z_number not in range(26):
                return None
            if self.z_number >= 6:  # G and later have no remote mode adjustment.
                return False
        if self.mode is None:
            return None
        if mode in (ThermostatMode.AUTO, ThermostatMode.COOL):
            counts = ((COOLING_SETUP, self.cooling_stages), (HEAT_PUMP_CONFIG, self.enabled_heat_pumps))
            if any(v is not None and v > 0 for _, v in counts):
                return True
            return None if any(b in self.product.candidate_blocks and v is None for b, v in counts) else False
        if mode is ThermostatMode.EMERGENCY and model == "553":
            return None if self.enabled_heat_pumps is None else self.enabled_heat_pumps > 0
        return (mode is ThermostatMode.HEAT
                or mode is ThermostatMode.OFF and model not in ("527", "537")
                or mode is ThermostatMode.VENT and model in ("530", "540")
                or mode is ThermostatMode.EMERGENCY and model == "557")


@dataclass(frozen=True, slots=True)
class AirGroupConfiguration:
    """Current and previous air-group roles in one peer announcement."""

    flags: int
    previous_flags: int

    @classmethod
    def read(cls, r: Reader) -> "AirGroupConfiguration":
        return cls(r.byte(), r.byte())

    @property
    def role(self) -> tuple[int, bool]:
        return self.flags & 31, bool(self.flags & 128)

    @property
    def previous_role(self) -> tuple[int, bool]:
        return self.previous_flags & 31, bool(self.previous_flags & 128)

    @property
    def no_response_required(self) -> bool:
        return bool(self.flags & 64)

    def role_changed(self, previous: "AirGroupConfiguration | None") -> bool:
        before = previous.role if previous is not None else self.previous_role
        return self.role != before

    def fields(self) -> Fields:
        group, master = self.role
        previous_group, previous_master = self.previous_role
        return {"group": group, "master": int(master), "previous_group": previous_group,
                "previous_master": int(previous_master),
                "no_response_required": int(self.no_response_required),
                "reserved0": self.flags >> 5 & 1, "reserved1": self.previous_flags >> 5 & 3}


@dataclass(frozen=True, slots=True)
class AirReading:
    flags: int
    target_raw: int
    temperature_raw: int

    @classmethod
    def read(cls, r: Reader) -> "AirReading":
        return cls(r.byte(), r.byte(), r.word())

    @property
    def enabled(self) -> bool:
        return bool(self.flags & 1)

    @property
    def demand(self) -> bool:
        return bool(self.flags & 2)

    @property
    def calling(self) -> bool:
        return self.enabled and self.demand

    @property
    def setback(self) -> int:
        return self.flags >> 2 & 7

    @property
    def target_f(self) -> float | None:
        return fahrenheit(self.target_raw, deg_e=True)

    @property
    def temperature_f(self) -> float | None:
        return fahrenheit(self.temperature_raw)

    def fields(self, prefix: str) -> Fields:
        return {prefix + "enabled": int(self.enabled), prefix + "demand": int(self.demand),
                prefix + "schedule": self.setback, prefix + "optimum_start": self.flags >> 5 & 3,
                prefix + "weather_shutdown": self.flags >> 7} | temp(prefix + "target", self.target_raw, deg_e=True) | temp(prefix + "temperature", self.temperature_raw)


@dataclass(frozen=True, slots=True)
class AirStatus:
    group_flags: int
    cycle: int | None
    heat: AirReading | None
    cool: AirReading | None
    outputs: int
    remote_raw: int

    @classmethod
    def read(cls, r: Reader, *, master: bool) -> "AirStatus":
        group = r.byte()
        heat: AirReading | None
        cool: AirReading | None
        if master:
            cycle = r.byte()
            heat, cool = AirReading.read(r), AirReading.read(r)
        else:
            cycle = None
            member = AirReading.read(r)
            heat, cool = (None, member) if group & 32 else (member, None)
        return cls(group, cycle, heat, cool, r.byte(), r.word())

    @property
    def group(self) -> int:
        return self.group_flags & 31

    @property
    def remote_f(self) -> float | None:
        return fahrenheit(self.remote_raw)

    @property
    def operating_flags(self) -> tuple[int, int | None, int | None]:
        """A change justifies checking setup, but is not itself the knob setting."""
        return (self.group, self.heat.flags & 3 if self.heat else None,
                self.cool.flags & 3 if self.cool else None)

    def fields(self) -> Fields:
        fields = temp("remote_temperature", self.remote_raw)
        fields["air_group_id"] = self.group
        if self.cycle is None:
            channel = self.heat or self.cool
            if channel is None:
                raise ValueError("missing air-group member channel")
            fields.update(mode="heat" if self.heat else "cool",
                          request_for_master_report=self.group_flags >> 6 & 1, reserved0=self.group_flags >> 7,
                          heat_cool_enabled=int(channel.enabled), heat_cool_demand=int(channel.demand),
                          schedule_status=channel.setback, schedule_name=SETBACK[channel.setback],
                          optimum_start=channel.flags >> 5 & 3, weather_shutdown=channel.flags >> 7,
                          output_1_status=self.outputs & 3, output_2_status=self.outputs >> 2 & 3,
                          reserved5=self.outputs >> 4)
            return fields | temp("target", channel.target_raw, deg_e=True) | temp("temperature", channel.temperature_raw)
        fields.update(inhibit_heat=self.group_flags >> 5 & 1, inhibit_cool=self.group_flags >> 6 & 1,
                      reserved0=self.group_flags >> 7, cycle_length=self.cycle & 127, new_cycle=self.cycle >> 7)
        for name, side in (("heat", self.heat), ("cool", self.cool)):
            if side is not None:
                fields.update(side.fields(name + "_"))
        for i, name in enumerate(("heat1", "heat2", "cool1", "cool2")):
            fields[name + "_status"] = self.outputs >> (i * 2) & 3
        return fields


@dataclass(frozen=True, slots=True)
class AirTarget:
    """Operating target in this report, not a stored five-slot adjustment."""
    kind: ThermostatTarget
    setback: int
    raw: int

    @property
    def celsius(self) -> float | None:
        return None if self.raw == 255 else self.raw / 2


class AirGroupTargets:
    """J1196J shares the last master code for groups 0..16 across physical ports."""

    def __init__(self) -> None:
        self._inhibit = bytearray(17)

    def observe(self, status: AirStatus) -> AirTarget | None:
        heat, cool = status.heat, status.cool
        gate = 0
        if status.group < len(self._inhibit):
            if status.cycle is not None:
                self._inhibit[status.group] = (status.group_flags >> 5) & 3
            gate = self._inhibit[status.group]
        if heat and heat.enabled and (not cool or not cool.enabled or heat.calling):
            kind, chosen, denied = ThermostatTarget.HEAT, heat, 1
        elif cool and cool.enabled and (not heat or not heat.enabled or cool.calling):
            kind, chosen, denied = ThermostatTarget.COOL, cool, 2
        else:
            return None
        if gate == denied or chosen.flags & 0xc0 or chosen.target_raw == 0:
            return None
        return AirTarget(kind, chosen.setback, chosen.target_raw)
