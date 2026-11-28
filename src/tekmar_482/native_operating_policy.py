"""Addressed weather, heat-pump and humidity settings from one native peer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Self

from ._native_payloads.codec import integer_field
from ._native_payloads.layouts import FIXED_BLOCKS, OperatingPolicyBlock
from ._native_payloads.thermostat import TargetSlot
from .native import require_enum

if TYPE_CHECKING:
    from collections.abc import Mapping

    from .native import BlockKey, NativeFrame


@dataclass(frozen=True, slots=True)
class SetbackThresholds:
    """Configured octets in TargetSlot order; unsupported slots are None."""

    slots: tuple[int | None, ...]

    def raw(self, slot: TargetSlot) -> int | None:
        require_enum(slot, TargetSlot)
        return self.slots[slot]


@dataclass(frozen=True, slots=True)
class NativeOperatingPolicy:
    """Reported settings, distinct from live shutdown or running equipment."""

    warm_shutdown: SetbackThresholds | None
    cold_shutdown: SetbackThresholds | None
    balance_point: SetbackThresholds | None
    shutdown_source_raw: int | None
    heat_pump_cycles_raw: int | None
    cool_purge_minutes: int | None
    balance_point_schedule_raw: int | None
    w1_warm_shutdown_raw: int | None
    w2_lockout_raw: int | None
    compressor_during_backup_raw: int | None
    baseload_raw: int | None
    unoccupied_humidity_raw: int | None
    humidity_differential_raw: int | None

    @classmethod
    def from_reports(cls, blocks: Mapping[BlockKey, NativeFrame]) -> Self | None:
        """Project validated reports keyed by block and selector."""
        reports: dict[OperatingPolicyBlock, tuple[int | None, ...]] = {}
        for block in OperatingPolicyBlock:
            if (frame := blocks.get((block, None))) is not None:
                fields = frame.decoded
                values = tuple(
                    integer_field(fields, name) for name, _ in FIXED_BLOCKS[block]
                )
                if any(value is not None for value in values):
                    reports[block] = values
        if not reports:
            return None

        def first(block: OperatingPolicyBlock) -> int | None:
            return values[0] if (values := reports.get(block)) is not None else None

        def table(block: OperatingPolicyBlock) -> SetbackThresholds | None:
            # Warm shutdown begins with a separate source octet.
            slots = reports.get(block, ())[-len(TargetSlot) :]
            return (
                SetbackThresholds(slots) if any(v is not None for v in slots) else None
            )

        return cls(
            warm_shutdown=table(OperatingPolicyBlock.WARM_SHUTDOWN),
            cold_shutdown=table(OperatingPolicyBlock.COLD_SHUTDOWN),
            balance_point=table(OperatingPolicyBlock.BALANCE_POINT),
            shutdown_source_raw=first(OperatingPolicyBlock.WARM_SHUTDOWN),
            heat_pump_cycles_raw=first(OperatingPolicyBlock.HEAT_PUMP_CYCLES),
            cool_purge_minutes=first(OperatingPolicyBlock.COOL_PURGE),
            balance_point_schedule_raw=first(
                OperatingPolicyBlock.BALANCE_POINT_SCHEDULE,
            ),
            w1_warm_shutdown_raw=first(OperatingPolicyBlock.W1_WARM_SHUTDOWN),
            w2_lockout_raw=first(OperatingPolicyBlock.W2_LOCKOUT),
            compressor_during_backup_raw=first(
                OperatingPolicyBlock.COMPRESSOR_DURING_BACKUP,
            ),
            baseload_raw=first(OperatingPolicyBlock.BASELOAD),
            unoccupied_humidity_raw=first(OperatingPolicyBlock.UNOCCUPIED_HUMIDITY),
            humidity_differential_raw=first(OperatingPolicyBlock.HUMIDITY_DIFFERENTIAL),
        )
