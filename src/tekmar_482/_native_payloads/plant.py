"""483 SW 11 plant output variants and mixing module reports."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum

from .codec import Fields, Reader

PLANT_OUTPUT = 0x24f
MIXING_PRESENT = 0x347
MIXING_REPORT = 0x34f
MIXING_UPDATE = 0x357


class PlantKind(IntEnum):
    HEAT_ENABLE = 0
    COOL_ENABLE = 1
    MODULATING_HEAT = 2
    MODULATING_COOL = 3
    STAGED_BOILER = 4
    STAGED_CHILLER = 5
    MODULATING_BOILER = 6
    MODULATING_CHILLER = 7
    AIR_TO_AIR_HEAT_PUMP = 8
    WATER_TO_AIR_HEAT_PUMP = 9
    WATER_TO_WATER_HEAT_PUMP = 10
    AIR_TO_WATER_HEAT_PUMP = 11
    MODULATING_AIR_TO_AIR_HEAT_PUMP = 12
    MODULATING_WATER_TO_AIR_HEAT_PUMP = 13
    MODULATING_WATER_TO_WATER_HEAT_PUMP = 14
    MODULATING_AIR_TO_WATER_HEAT_PUMP = 15
    STAGED_ELECTRIC = 16
    ON_OFF_INJECTION_PUMP = 17
    VARIABLE_SPEED_PUMP = 18


@dataclass(frozen=True, slots=True)
class StageBits:
    available_mask: int
    operating_mask: int

    @staticmethod
    def _stages(mask: int) -> tuple[int, ...]:
        return tuple(i + 1 for i in range(4) if mask & (1 << i))

    @property
    def available(self) -> tuple[int, ...]:
        return self._stages(self.available_mask)

    @property
    def operating(self) -> tuple[int, ...]:
        # Preserve operating bits even when the separate availability mask disagrees.
        return self._stages(self.operating_mask)

    def fields(self) -> Fields:
        return {"stages_available_mask": self.available_mask, "stages_operating_mask": self.operating_mask}


@dataclass(frozen=True, slots=True)
class StageCounts:
    on: int
    available: int

    @property
    def percent_on(self) -> float | None:
        """Fraction of stages on, not actual heat output or compressor modulation."""
        return 100 * self.on / self.available if self.available and self.on <= self.available else None

    def fields(self) -> Fields:
        return {"number_stages_on": self.on, "number_stages_available": self.available}


@dataclass(frozen=True, slots=True)
class Modulation:
    current: int
    minimum: int
    maximum: int

    def fields(self) -> Fields:
        return {"current_modulation_level": self.current, "minimum_modulation_level": self.minimum,
                "maximum_modulation_level": self.maximum}


@dataclass(frozen=True, slots=True)
class PlantEnable:
    heating: bool
    raw: int

    def fields(self) -> Fields:
        return {"heat_enabled" if self.heating else "cool_enabled": self.raw}


@dataclass(frozen=True, slots=True)
class ModulatingSignal:
    signal_type: int
    output_level: int

    def fields(self) -> Fields:
        return {"signal_type": self.signal_type, "output_level": self.output_level}


@dataclass(frozen=True, slots=True)
class BoilerChillerOutput:
    flags: int
    stages: StageBits | Modulation
    input_raw: int
    outlet_raw: int
    output_flags: int | None

    @property
    def pump_status(self) -> int:
        return self.flags >> 6

    def fields(self) -> Fields:
        result: Fields = {"state": self.flags & 15, "reserved": self.flags >> 4 & 3,
                          "pump_status": self.pump_status, "input_temp_raw": self.input_raw,
                          "outlet_temp_raw": self.outlet_raw}
        if self.output_flags is not None:
            result.update(output=self.output_flags & 31, return_protection=self.output_flags >> 5)
        return result | self.stages.fields()


@dataclass(frozen=True, slots=True)
class HeatPumpOutput:
    flags: int
    stages: StageCounts | Modulation
    pump_flags: int | None
    temperatures: tuple[int, int] | None
    ambiguous_tail: bytes = b""

    @property
    def pump_status(self) -> int | None:
        return None if self.pump_flags is None else self.pump_flags & 3

    @classmethod
    def read(cls, r: Reader, kind: PlantKind) -> HeatPumpOutput:
        flags = r.byte()
        stages = StageCounts(*r.take(2)) if kind < 12 else Modulation(*r.take(3))
        pump = r.byte() if kind in (10, 11, 14, 15) else None
        temps = None
        tail = b""
        if kind in (9, 10, 11, 14, 15):
            # The indexed 14/15 reader skips a byte before the temperatures;
            # a longer tail also fits the packed layout followed by an extension.
            if kind in (14, 15) and len(r.remaining) > 4:
                tail = r.take(len(r.remaining))
            else:
                temps = (r.word(), r.word())
        return cls(flags, stages, pump, temps, tail)

    def fields(self) -> Fields:
        result: Fields = {"state": self.flags & 15, "ob_config": self.flags >> 4 & 1,
                          "cooling": self.flags >> 5 & 1, "reserved": self.flags >> 6}
        if self.pump_flags is not None:
            result.update(pump_status=self.pump_flags & 3, pump_reserved=self.pump_flags >> 2)
        if self.temperatures is not None:
            result.update(input_temp_raw=self.temperatures[0], outlet_temp_raw=self.temperatures[1])
        if self.ambiguous_tail:
            result["temperature_tail_hex"] = self.ambiguous_tail.hex()
        return result | self.stages.fields()


@dataclass(frozen=True, slots=True)
class PumpElectricOutput:
    state: int
    sensor_raw: int
    stages: StageCounts | None

    def fields(self) -> Fields:
        result: Fields = {"state": self.state, "sensor_temp_raw": self.sensor_raw}
        return result | self.stages.fields() if self.stages is not None else result


PlantBody = PlantEnable | ModulatingSignal | BoilerChillerOutput | HeatPumpOutput | PumpElectricOutput


@dataclass(frozen=True, slots=True)
class PlantOutput:
    device_total: int
    device_id: int
    type_id: int
    body: PlantBody | None

    @property
    def kind(self) -> PlantKind | None:
        return PlantKind(self.type_id) if self.type_id in range(len(PlantKind)) else None

    @property
    def key(self) -> tuple[int, int]:
        return (self.type_id, self.device_id)

    @classmethod
    def read(cls, r: Reader) -> PlantOutput:
        device, kind = r.take(2)
        body: PlantBody | None = None
        if kind in (0, 1):
            body = PlantEnable(kind == 0, r.byte())
        elif kind in (2, 3):
            body = ModulatingSignal(r.byte(), r.word())
        elif kind in (4, 5, 6):
            flags = r.byte()
            stages: StageBits | Modulation
            if kind == 6:
                stages = Modulation(*r.take(3))
            else:
                packed = r.byte()
                stages = StageBits(packed & 15, packed >> 4)
            body = BoilerChillerOutput(flags, stages, r.byte(), r.byte(), None if kind == 5 else r.byte())
        elif 8 <= kind <= 15:
            body = HeatPumpOutput.read(r, PlantKind(kind))
        elif kind in (16, 17, 18):
            state = r.byte()
            counts = StageCounts(*r.take(2)) if kind == 16 else None
            body = PumpElectricOutput(state, r.word(), counts)
        # No known body for modulating chiller (7) or an unknown subtype.
        return cls(device & 15, device >> 4, kind, body)

    def fields(self) -> Fields:
        fields: Fields = {"device_total": self.device_total, "device_id": self.device_id,
                          "device_type": self.type_id}
        if self.kind is not None:
            fields["plant"] = self.kind.name.lower()
        return fields | self.body.fields() if self.body is not None else fields


@dataclass(frozen=True, slots=True)
class MixingPresent:
    flags: int
    motor_speed: int

    @classmethod
    def read(cls, r: Reader) -> MixingPresent:
        return cls(*r.take(2))

    @property
    def actuator_type(self) -> int:
        return self.flags & 15

    def fields(self) -> Fields:
        return {"actuator_type": self.actuator_type, "reserved": self.flags >> 4 & 3,
                "adjustable_speed": self.flags >> 6 & 1, "system_pump_controllable": self.flags >> 7,
                "motor_speed": self.motor_speed}


@dataclass(frozen=True, slots=True)
class MixingReport:
    flags: int
    pump_output: int
    actuator: int
    sensor_flags: int
    target_raw: int
    supply_raw: int
    return_raw: int

    @classmethod
    def read(cls, r: Reader) -> MixingReport:
        mode, pump, actuator, high, target, supply, ret = r.take(7)
        return cls(mode, pump, actuator, high >> 6, (high & 3) << 8 | target,
                   (high >> 2 & 3) << 8 | supply, (high >> 4 & 3) << 8 | ret)

    def fields(self) -> Fields:
        return {"output_mode": self.flags & 3, "cooling_logic": self.flags >> 2 & 1,
                "reserved": self.flags >> 3 & 1, "external_demand": self.flags >> 4 & 1,
                "adjustment_latched": self.flags >> 5 & 1, "output_limited": self.flags >> 6 & 1,
                "fault": self.flags >> 7, "pump_output": self.pump_output, "actuator_percent": self.actuator & 63,
                "decreasing": self.actuator >> 6 & 1, "increasing": self.actuator >> 7,
                "target_raw": self.target_raw, "supply_raw": self.supply_raw, "return_raw": self.return_raw,
                "return_sensor_fault": self.sensor_flags & 1, "supply_sensor_fault": self.sensor_flags >> 1}


@dataclass(frozen=True, slots=True)
class MixingUpdate:
    flags: int
    target_raw: int
    target_pump_output: int

    @classmethod
    def read(cls, r: Reader) -> MixingUpdate:
        flags, target, pump = r.take(3)
        return cls(flags, (flags >> 6) << 8 | target, pump)

    def fields(self) -> Fields:
        return {"output_mode": self.flags & 3, "cooling_logic": self.flags >> 2 & 1,
                "clear_error": self.flags >> 3 & 1, "reserved": self.flags >> 4 & 3,
                "target_raw": self.target_raw, "target_pump_output": self.target_pump_output}
