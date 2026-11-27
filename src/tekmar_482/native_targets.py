"""Reported thermostat adjustments and operating samples for one receive path."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from typing import TYPE_CHECKING

from ._native_payloads.layouts import SETPOINT_FIELDS
from ._native_payloads.sensors import (
    ROOM_STATS,
    SensorRole,
    SensorSlot,
    ThermostatSensors,
)
from ._native_payloads.sensors import SETUP as SENSOR_SETUP
from ._native_payloads.thermostat import (
    FAN_SETUP,
    HUMIDITY_SETUP,
    FanDuctStatus,
    FanSetup,
    HumiditySetup,
    HumidityStatus,
    SlabStatus,
    TargetSlot,
    TargetTable,
    ThermostatTarget,
)
from .native import require_enum

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from ._native_payloads.climate import AirTarget
    from ._native_payloads.products import NativeProduct
    from .native import NativeFrame

type _Blocks = dict[tuple[int, int | None], NativeFrame]
_INPUT_ONE = SensorSlot.EXTERNAL_1.block
_SOURCE_GATES = (FAN_SETUP, HUMIDITY_SETUP, SENSOR_SETUP, _INPUT_ONE)
_RH = (ThermostatTarget.HUMIDIFY, ThermostatTarget.DEHUMIDIFY)
_LEGACY_FLOOR = frozenset(("528", "538"))


class RoomLimit(IntEnum):
    MAXIMUM_ROOM_HEAT = 0x53
    MINIMUM_ROOM_COOL = 0x54
    MAXIMUM_ROOM_COOL = 0x55


ADJUSTMENT_BLOCKS = frozenset((*ThermostatTarget, *RoomLimit, *_SOURCE_GATES))


@dataclass(frozen=True, slots=True)
class RoomLimitTable:
    limit: RoomLimit
    slots: tuple[int | None, ...]

    @classmethod
    def from_report(
        cls,
        limit: RoomLimit,
        fields: Mapping[str, object],
    ) -> RoomLimitTable:
        values = (fields.get(f"{s}_{limit.name.lower()}_raw") for s in SETPOINT_FIELDS)
        return cls(limit, tuple(v if isinstance(v, int) else None for v in values))

    def raw(self, slot: TargetSlot) -> int | None:
        require_enum(slot, TargetSlot)
        return self.slots[slot]


@dataclass(frozen=True, slots=True)
class OutputSample[T]:
    value: T
    frame: NativeFrame = field(compare=False)


@dataclass(frozen=True, slots=True)
class ThermostatAdjustments:
    tables: tuple[TargetTable, ...]
    limits: tuple[RoomLimitTable, ...]
    floor_configured: bool | None
    fan_setup: FanSetup | None
    humidity_setup: HumiditySetup | None
    fan: OutputSample[FanDuctStatus] | None
    slab: OutputSample[SlabStatus] | None
    humidity: OutputSample[HumidityStatus] | None
    missing_blocks: tuple[int, ...]

    def table(self, target: ThermostatTarget) -> TargetTable | None:
        require_enum(target, ThermostatTarget)
        return next((t for t in self.tables if t.target is target), None)


def floor_configured(sensors: ThermostatSensors) -> bool | None:
    if sensors.product.model in _LEGACY_FLOOR:
        for sensor in sensors.inputs:
            if (
                sensor.slot is SensorSlot.EXTERNAL_1
                and sensor.status_role is not None
                and not sensor.conflict
            ):
                return sensor.status_role is SensorRole.FLOOR
        return None
    roles = [s.setup_raw for s in sensors.inputs if s.setup_raw is not None]
    if SensorRole.FLOOR in roles:
        return True
    return (
        False
        if len(roles) == len(SensorSlot) and all(v in SensorRole for v in roles)
        else None
    )


class AdjustmentState:
    def __init__(self) -> None:
        self.fan: OutputSample[FanDuctStatus] | None = None
        self.slab: OutputSample[SlabStatus] | None = None
        self.humidity: OutputSample[HumidityStatus] | None = None
        self.revisions: dict[int, int] = {}
        self._hints: dict[tuple[ThermostatTarget, int | None], int] = {}

    def _expire(self, block: int, blocks: _Blocks) -> None:
        self.revisions[block] = self.revisions.get(block, 0) + 1
        blocks.pop((block, None), None)

    def _expire_inputs(self, slots: Iterable[SensorSlot], blocks: _Blocks) -> None:
        for slot in slots:
            self._expire(slot.block, blocks)
        self._expire(ROOM_STATS, blocks)
        self._expire(ThermostatTarget.FLOOR, blocks)

    def update(self, block: int, blocks: _Blocks) -> None:
        self._expire(block, blocks)
        if block == HUMIDITY_SETUP:
            for target in _RH:
                self._expire(target, blocks)
        elif block == FAN_SETUP:
            self._expire(ThermostatTarget.FAN, blocks)
        elif block == SENSOR_SETUP:
            self._expire_inputs(SensorSlot, blocks)
        elif block == _INPUT_ONE:
            self._expire(ThermostatTarget.FLOOR, blocks)

    def reconfigured(
        self,
        block: int,
        old: NativeFrame,
        new: NativeFrame,
        blocks: _Blocks,
        product: NativeProduct | None,
        z: int | None,
    ) -> None:
        if block == HUMIDITY_SETUP:
            before, after = (HumiditySetup.from_report(f.decoded) for f in (old, new))
            for target in _RH:
                if before.enabled(target) != after.enabled(target):
                    self._expire(target, blocks)
        elif block == FAN_SETUP:
            if FanSetup.from_report(old.decoded) != FanSetup.from_report(new.decoded):
                self._expire(ThermostatTarget.FAN, blocks)
        elif block == SENSOR_SETUP:
            a, b = old.decoded, new.decoded
            slots = tuple(
                s for s in SensorSlot if a.get(s.setup_field) != b.get(s.setup_field)
            )
            if slots:
                self._expire_inputs(slots, blocks)
        elif block == _INPUT_ONE and product and product.model in _LEGACY_FLOOR:
            setup = blocks.get((SENSOR_SETUP, None))
            roles = tuple(
                floor_configured(
                    ThermostatSensors.from_blocks(
                        product,
                        z,
                        {
                            SENSOR_SETUP: setup.decoded if setup else {},
                            _INPUT_ONE: f.decoded,
                        },
                    ),
                )
                for f in (old, new)
            )
            if roles[0] != roles[1]:
                self._expire(ThermostatTarget.FLOOR, blocks)

    def _target_hint(
        self,
        target: ThermostatTarget,
        slot: int | None,
        raw: int | None,
        blocks: _Blocks,
    ) -> None:
        if raw is None:
            return
        key = target, slot
        old = self._hints.get(key)
        self._hints[key] = raw
        if old != raw and (old is not None or (target, None) not in blocks):
            self._expire(target, blocks)

    def air(self, target: AirTarget, blocks: _Blocks) -> None:
        self._target_hint(target.kind, target.setback, target.raw, blocks)

    def observe(
        self,
        sample: FanDuctStatus | SlabStatus | HumidityStatus,
        frame: NativeFrame,
        blocks: _Blocks,
    ) -> bool:
        if isinstance(sample, FanDuctStatus):
            old_fan = self.fan
            self.fan = OutputSample(sample, frame)
            if old_fan is None and (ThermostatTarget.FAN, None) not in blocks:
                self._expire(ThermostatTarget.FAN, blocks)
            return (
                old_fan is None
                or old_fan.frame.message.payload != frame.message.payload
            )
        if isinstance(sample, SlabStatus):
            old_slab = self.slab
            self.slab = OutputSample(sample, frame)
            if sample.primary:
                self._target_hint(
                    ThermostatTarget.FLOOR,
                    None,
                    sample.target_raw,
                    blocks,
                )
            return (
                old_slab is None
                or old_slab.frame.message.payload != frame.message.payload
            )
        old_rh = self.humidity
        self.humidity = OutputSample(sample, frame)
        self._target_hint(
            ThermostatTarget.HUMIDIFY,
            None,
            sample.humidify_target_raw,
            blocks,
        )
        self._target_hint(
            ThermostatTarget.DEHUMIDIFY,
            None,
            sample.dehumidify_target_raw,
            blocks,
        )
        return old_rh is None or old_rh.frame.message.payload != frame.message.payload

    def snapshot(
        self,
        product: NativeProduct,
        sensors: ThermostatSensors,
        fields: Mapping[int, Mapping[str, object]],
    ) -> ThermostatAdjustments:
        candidates = product.candidate_blocks
        fan = FanSetup.from_report(fields[FAN_SETUP]) if FAN_SETUP in fields else None
        rh = (
            HumiditySetup.from_report(fields[HUMIDITY_SETUP])
            if HUMIDITY_SETUP in fields
            else None
        )
        floor = floor_configured(sensors)
        needed = {
            b
            for b in (FAN_SETUP, HUMIDITY_SETUP, SENSOR_SETUP, *RoomLimit)
            if b in candidates
        }
        if product.model in _LEGACY_FLOOR and _INPUT_ONE in candidates:
            needed.add(_INPUT_ONE)
        if self.humidity:
            needed.add(HUMIDITY_SETUP)
        for target in ThermostatTarget:
            if target in _RH:
                if rh is not None and rh.enabled(target) is True:
                    needed.add(target)
            elif target is ThermostatTarget.FLOOR:
                if self.slab or (floor is True and target in candidates):
                    needed.add(target)
            elif target is ThermostatTarget.FAN:
                if (
                    self.fan
                    or (fan and fan.enabled is True)
                    or (target in candidates and FAN_SETUP not in candidates)
                ):
                    needed.add(target)
            elif target in candidates:
                needed.add(target)
        return ThermostatAdjustments(
            tuple(
                TargetTable.from_report(product.model, t, fields[t])
                for t in ThermostatTarget
                if t in fields
            ),
            tuple(
                RoomLimitTable.from_report(limit, fields[limit])
                for limit in RoomLimit
                if limit in fields
            ),
            floor,
            fan,
            rh,
            self.fan,
            self.slab,
            self.humidity,
            tuple(sorted(needed.difference(fields))),
        )
