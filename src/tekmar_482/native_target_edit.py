"""Read, edit and verify one physical slot of a native thermostat adjustment."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ._native_payloads.sensors import SETUP as SENSOR_SETUP
from ._native_payloads.sensors import SensorSlot, ThermostatSensors
from ._native_payloads.thermostat import (
    FAN_SETUP,
    HUMIDITY_SETUP,
    FanSetup,
    HumiditySetup,
    TargetSlot,
    TargetTable,
    ThermostatTarget,
    fan_step,
)
from ._native_payloads.writes import BlockPatch, NativeField
from .constants import MAX_PERCENT
from .native import NativeBlockQuery, require_enum
from .native_identity import NativeIdentity
from .native_targets import floor_configured

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ._native_payloads.products import NativeProduct
    from .native import NativeAddress
    from .native_write import NativeBlockEditor, NativeWriteResult

_UNAVAILABLE = 0xFF
_HUMIDITY_RANGE = range(20, 81)


@dataclass(frozen=True, slots=True)
class ThermostatTargetChange:
    """Celsius for heat/cool/floor; physical percent for fan and humidity."""

    target: ThermostatTarget
    slot: TargetSlot
    value: float

    def __post_init__(self) -> None:
        require_enum(self.target, ThermostatTarget)
        require_enum(self.slot, TargetSlot)
        if isinstance(self.value, bool) or not math.isfinite(self.value):
            msg = "expected a finite thermostat adjustment"
            raise ValueError(msg)

    def wire_value(self, model: str) -> int:
        v = self.value
        if self.target.temperature:
            if not 0 < v * 2 < _UNAVAILABLE or v * 2 != int(v * 2):
                msg = "expected a representable positive half-degree Celsius target"
                raise ValueError(msg)
            return int(v * 2)
        step = fan_step(model) if self.target is ThermostatTarget.FAN else 1
        if not 0 <= v <= MAX_PERCENT or v % step:
            msg = f"expected a percentage in increments of {step} for {model}"
            raise ValueError(msg)
        if (
            self.target is not ThermostatTarget.FAN
            and model in {"553", "557"}
            and v
            and v not in _HUMIDITY_RANGE
        ):
            msg = "expected Off (0) or a humidity target of 20 to 80 percent"
            raise ValueError(msg)
        return int(v / step)


async def _eligible(
    editor: NativeBlockEditor,
    address: NativeAddress,
    product: NativeProduct,
    z: int | None,
    target: ThermostatTarget,
) -> bool:
    async def read(b: int) -> Mapping[str, object]:
        return (
            (await editor.read(NativeBlockQuery(address, b))).require_report().decoded
        )

    if target not in product.candidate_blocks:
        return False
    if target is ThermostatTarget.FAN:
        return FanSetup.from_report(await read(FAN_SETUP)).enabled is True
    if target in (ThermostatTarget.HUMIDIFY, ThermostatTarget.DEHUMIDIFY):
        return (
            HumiditySetup.from_report(await read(HUMIDITY_SETUP)).enabled(target)
            is True
        )
    if target is ThermostatTarget.FLOOR:
        b = (
            SensorSlot.EXTERNAL_1.block
            if product.model in ("528", "538")
            else SENSOR_SETUP
        )
        return (
            b in product.candidate_blocks
            and floor_configured(
                ThermostatSensors.from_blocks(product, z, {b: await read(b)}),
            )
            is True
        )
    return True


async def edit_thermostat_target(
    editor: NativeBlockEditor,
    address: NativeAddress,
    change: ThermostatTargetChange,
    *,
    preview: bool,
) -> NativeWriteResult:
    report = (await editor.read(NativeBlockQuery(address, 0))).require_report()
    identity = NativeIdentity.from_report(report.decoded)
    if (
        identity is None
        or not (product := identity.product)
        or product.family != "thermostat"
    ):
        msg = "target adjustment requires an identified thermostat"
        raise ValueError(msg)
    raw = change.wire_value(product.model)
    if not await _eligible(editor, address, product, identity.z_number, change.target):
        msg = f"{product.model}: {change.target.name.lower()} adjustment not available"
        raise ValueError(msg)
    query = NativeBlockQuery(address, change.target)
    before = (await editor.read(query)).require_report()
    table = TargetTable.from_report(product.model, change.target, before.decoded)
    if (
        table.maximum_raw is not None
        and 0 < table.maximum_raw < _UNAVAILABLE
        and raw > table.maximum_raw
    ):
        msg = "floor target exceeds the reported maximum"
        raise ValueError(msg)
    patch = BlockPatch.from_fields(change.target, {NativeField[change.slot.name]: raw})
    return await editor.write(query, patch, before=before, preview=preview)
