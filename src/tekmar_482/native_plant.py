"""Source-local plant outputs and hydronic demand; commands are not measurements."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Protocol

from ._native_payloads.codec import Reader
from ._native_payloads.plant import (
    MIXING_PRESENT,
    MIXING_REPORT,
    PLANT_OUTPUT,
    MixingPresent,
    MixingReport,
    PlantOutput,
)
from ._native_payloads.water import (
    BOILER_MAXIMUM,
    GENERAL_DEMAND,
    RELAY_HEARTBEAT,
    STAGE_DEMAND,
    WATER_CYCLE,
    BoilerMaximum,
    GeneralDemand,
    RelayHeartbeat,
    StageDemand,
    WaterCycle,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from ._native_payloads.codec import Fields
    from .native import NativeFrame

PLANT_REPORTS = frozenset(
    (
        MIXING_PRESENT,
        MIXING_REPORT,
        PLANT_OUTPUT,
        STAGE_DEMAND,
        GENERAL_DEMAND,
        BOILER_MAXIMUM,
        WATER_CYCLE,
        RELAY_HEARTBEAT,
    ),
)


class WaterValue(Protocol):
    def fields(self) -> Fields: ...


@dataclass(frozen=True, slots=True)
class WaterSample[T: WaterValue]:
    timestamp_ms: int = field(compare=False)
    value: T
    extension: bytes


def read_sample[T: WaterValue](
    frame: NativeFrame,
    decode: Callable[[Reader], T],
) -> WaterSample[T]:
    reader = Reader(frame.message.payload)
    return WaterSample(frame.timestamp_ms, decode(reader), reader.remaining)


def _store[T: WaterValue, K: (int, tuple[int, int])](
    samples: tuple[WaterSample[T], ...],
    new: WaterSample[T],
    key: Callable[[T], K],
) -> tuple[WaterSample[T], ...]:
    latest = {key(s.value): s for s in samples}
    latest[key(new.value)] = new
    return tuple(latest[k] for k in sorted(latest))


@dataclass(frozen=True, slots=True)
class PlantEquipment:
    outputs: tuple[WaterSample[PlantOutput], ...] = ()
    mixing_present: WaterSample[MixingPresent] | None = None
    mixing_report: WaterSample[MixingReport] | None = None
    stage_demands: tuple[WaterSample[StageDemand], ...] = ()
    general_demands: tuple[WaterSample[GeneralDemand], ...] = ()
    relay_heartbeats: tuple[WaterSample[RelayHeartbeat], ...] = ()
    boiler_maximum: WaterSample[BoilerMaximum] | None = None
    cycle: WaterSample[WaterCycle] | None = None

    def observe(self, frame: NativeFrame) -> PlantEquipment:
        mid = frame.message_id
        if mid == PLANT_OUTPUT:
            output = read_sample(frame, PlantOutput.read)
            return replace(self, outputs=_store(self.outputs, output, lambda v: v.key))
        if mid == MIXING_PRESENT:
            setup = read_sample(frame, MixingPresent.read)
            prior = self.mixing_present
            changed = (
                prior is not None
                and prior.value.actuator_type != setup.value.actuator_type
            )
            return replace(
                self,
                mixing_present=setup,
                mixing_report=None if changed else self.mixing_report,
            )
        if mid == MIXING_REPORT:
            return replace(self, mixing_report=read_sample(frame, MixingReport.read))
        if mid == STAGE_DEMAND:
            demand = read_sample(frame, StageDemand.read)
            return replace(
                self,
                stage_demands=_store(
                    self.stage_demands,
                    demand,
                    lambda d: (d.subnet, d.stage),
                ),
            )
        if mid == GENERAL_DEMAND:
            general = read_sample(frame, GeneralDemand.read)
            return replace(
                self,
                general_demands=_store(
                    self.general_demands,
                    general,
                    lambda g: g.subnet,
                ),
            )
        if mid == RELAY_HEARTBEAT:
            pulse = read_sample(frame, lambda r: RelayHeartbeat(r.byte()))
            return replace(
                self,
                relay_heartbeats=_store(
                    self.relay_heartbeats,
                    pulse,
                    lambda h: h.subnet,
                ),
            )
        if mid == BOILER_MAXIMUM:
            return replace(
                self,
                boiler_maximum=read_sample(frame, lambda r: BoilerMaximum(r.byte())),
            )
        if mid == WATER_CYCLE:
            return replace(
                self,
                cycle=read_sample(frame, lambda r: WaterCycle(r.byte())),
            )
        msg = "not a reported plant measurement or capability"
        raise ValueError(msg)
