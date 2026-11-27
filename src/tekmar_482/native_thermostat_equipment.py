"""Addressed thermostat relay, filter and runtime reports."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Self

from ._native_payloads.codec import integer_field
from ._native_payloads.layouts import (
    FILTER_DATA,
    FIXED_BLOCKS,
    RELAY_RUNTIME_BLOCKS,
    RELAY_STATUS,
    TERMINAL_RUNTIME_BLOCKS,
)
from ._native_payloads.writes import WritableBlock
from .native_targets import OutputSample

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from .native import BlockKey, NativeFrame
    from .native_plant_registers import PlantRegisterReport

OUTPUT_RUNTIME_BLOCKS = frozenset((*RELAY_RUNTIME_BLOCKS, *TERMINAL_RUNTIME_BLOCKS))


@dataclass(frozen=True, slots=True)
class ThermostatRelayStatus:
    """Raw reported output octets, not wire-terminal assignments or demand."""

    heat_stage1: int | None
    heat_stage2: int | None
    cool_stage1: int | None
    cool_stage2: int | None
    fan1: int | None
    fan2: int | None

    @classmethod
    def from_report(cls, fields: Mapping[str, object]) -> Self:
        return cls(
            integer_field(fields, "heat_stage1_output"),
            integer_field(fields, "heat_stage2_output"),
            integer_field(fields, "cool_stage1_output"),
            integer_field(fields, "cool_stage2_output"),
            integer_field(fields, "fan1_output"),
            integer_field(fields, "fan2_output"),
        )


@dataclass(frozen=True, slots=True)
class AirFilterUsage:
    """Two separate words: notification interval and time since replacement."""

    interval_raw: int | None
    elapsed_raw: int | None

    @classmethod
    def from_report(cls, fields: Mapping[str, object]) -> Self:
        return cls(
            integer_field(fields, "filter_replacement_interval"),
            integer_field(fields, "time_since_filter_replacement"),
        )


@dataclass(frozen=True, slots=True)
class ThermostatOutputRuntime:
    block: WritableBlock
    selector: int | None
    raw: int | None
    frame: NativeFrame = field(compare=False, repr=False)

    @property
    def hours(self) -> int | None:
        """The 483 names terminal and indexed heat-pump counters in hours."""
        if (
            self.block in TERMINAL_RUNTIME_BLOCKS
            or self.block is WritableBlock.HEAT_PUMP_RUNTIME
        ):
            return self.raw
        return None


def _runtime(
    block: int,
    selector: int | None,
    frame: NativeFrame,
) -> ThermostatOutputRuntime:
    field_name = FIXED_BLOCKS[block][-1][0]
    return ThermostatOutputRuntime(
        WritableBlock(block),
        selector,
        integer_field(frame.decoded, field_name),
        frame,
    )


@dataclass(frozen=True, slots=True)
class ThermostatEquipment:
    relay_status: OutputSample[ThermostatRelayStatus] | None
    filter_usage: OutputSample[AirFilterUsage] | None
    runtimes: tuple[ThermostatOutputRuntime, ...]

    @classmethod
    def from_reports(
        cls,
        blocks: Mapping[BlockKey, NativeFrame],
        plant: Iterable[PlantRegisterReport],
    ) -> Self:
        relays = blocks.get((RELAY_STATUS, None))
        air_filter = blocks.get((FILTER_DATA, None))
        runtimes = [
            _runtime(b, selector, frame)
            for (b, selector), frame in blocks.items()
            if b in OUTPUT_RUNTIME_BLOCKS
        ]
        runtimes.extend(
            _runtime(r.block, r.selector, r.frame)
            for r in plant
            if r.block == WritableBlock.HEAT_PUMP_RUNTIME
        )
        return cls(
            OutputSample(ThermostatRelayStatus.from_report(relays.decoded), relays)
            if relays is not None
            else None,
            OutputSample(AirFilterUsage.from_report(air_filter.decoded), air_filter)
            if air_filter is not None
            else None,
            tuple(sorted(runtimes, key=lambda r: (r.block, r.selector or 0))),
        )
