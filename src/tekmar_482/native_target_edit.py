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
)
from ._native_payloads.writes import BlockPatch, NativeField
from .native import NativeBlockQuery, require_enum
from .native_identity import NativeIdentity
from .native_targets import floor_configured

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ._native_payloads.products import NativeProduct
    from .native import NativeAddress
    from .native_write import NativeBlockEditor, NativeWriteResult

_UNAVAILABLE = 0xFF


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
        return self.target.encode_value(self.value, model)


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
    identity = NativeIdentity.from_report(await editor.identification(address))
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
