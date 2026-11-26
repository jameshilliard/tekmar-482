"""Addressed plant equipment setup and counters, independent of live output IDs."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum, StrEnum
from typing import TYPE_CHECKING

from ._native_payloads.products import (
    AVAILABILITY_FIELDS,
    INSTANCE_AVAILABILITY,
    block_selectors,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ._native_payloads.products import NativeProduct
    from .native import BlockKey, NativeFrame

_PUMP_SETUP = 0x41
_MASK_BITS = 8
PLANT_REGISTERS = frozenset(
    (
        0x22,
        0x23,
        0x33,
        0x34,
        0x35,
        0x36,
        *range(0x3C, 0x48),
        *range(0x4A, 0x51),
        0x5A,
        0x5B,
        *range(0x83, 0x99),
    ),
)


class EquipmentBank(StrEnum):
    BOILERS = "boilers"
    PRIMARY_PUMPS = "primary_pumps"
    DHW_PUMPS = "dhw_pumps"
    MIX_PUMPS = "mix_pumps"
    TANK_PUMPS = "tank_pumps"
    HEAT_PUMPS = "heat_pumps"


_BANKS = {
    0x35: (EquipmentBank.BOILERS,),
    _PUMP_SETUP: (EquipmentBank.PRIMARY_PUMPS, EquipmentBank.DHW_PUMPS),
    0x50: (EquipmentBank.MIX_PUMPS,),
    0x5B: (EquipmentBank.TANK_PUMPS,),
    0x97: (EquipmentBank.HEAT_PUMPS,),
}


class RuntimeRegister(IntEnum):
    BOILER = 0x43
    PUMP = 0x44
    MIX_PUMP = 0x4D
    TANK_PUMP = 0x5A
    HEAT_PUMP = 0x98

    @property
    def hours_field(self) -> str:
        return self.name.lower() + "_hours"


def _integer(fields: Mapping[str, object], name: str) -> int | None:
    v = fields.get(name)
    return v if isinstance(v, int) else None


@dataclass(frozen=True, slots=True)
class EquipmentAvailability:
    bank: EquipmentBank
    available_mask: int | None
    enabled_mask: int | None

    @property
    def available_indices(self) -> tuple[int, ...] | None:
        return (
            None
            if self.available_mask is None
            else tuple(i for i in range(_MASK_BITS) if self.available_mask & (1 << i))
        )


@dataclass(frozen=True, slots=True)
class EquipmentRuntime:
    register: RuntimeRegister
    selector: int
    hours: int | None


@dataclass(frozen=True, slots=True)
class PlantRegisterReport:
    block: int
    selector: int | None
    payload: bytes
    frame: NativeFrame = field(compare=False)


@dataclass(frozen=True, slots=True)
class PlantRegisters:
    reports: tuple[PlantRegisterReport, ...]
    missing: tuple[BlockKey, ...]

    @property
    def banks(self) -> tuple[EquipmentAvailability, ...]:
        return tuple(
            EquipmentAvailability(
                bank,
                _integer(fields, "available_" + bank.value),
                _integer(fields, "enabled_" + bank.value),
            )
            for report in self.reports
            if report.block in _BANKS
            for fields in (report.frame.decoded,)
            for bank in _BANKS[report.block]
        )

    @property
    def runtimes(self) -> tuple[EquipmentRuntime, ...]:
        return tuple(
            EquipmentRuntime(
                kind,
                r.selector,
                _integer(r.frame.decoded, kind.hours_field),
            )
            for r in self.reports
            if r.selector is not None and r.block in RuntimeRegister
            for kind in (RuntimeRegister(r.block),)
        )


def has_plant_registers(product: NativeProduct | None) -> bool:
    return product is not None and product.family in ("reset_control", "mixing_module")


class PlantRegisterBook:
    def __init__(self) -> None:
        self.reports: dict[BlockKey, PlantRegisterReport] = {}
        self.revisions: dict[BlockKey, int] = {}

    def _expire(self, key: BlockKey) -> None:
        self.reports.pop(key, None)
        self.revisions[key] = self.revisions.get(key, 0) + 1

    def _selection_changed(
        self,
        block: int,
        before: int | None,
        after: int | None,
        *,
        invalidate: bool = False,
    ) -> None:
        if before == after and not invalidate:
            return
        changed = (
            ((1 << _MASK_BITS) - 1)
            if before is None or after is None
            else before ^ after
        )
        for child, parent in INSTANCE_AVAILABILITY.items():
            if parent == block:
                for index in range(_MASK_BITS):
                    if changed & (1 << index):
                        self._expire((child, index))

    def invalidate(self, key: BlockKey) -> None:
        if key[0] in AVAILABILITY_FIELDS:
            before = self._availability().get(key[0])
            self._selection_changed(key[0], before, None, invalidate=True)
        self._expire(key)

    def report(self, key: BlockKey, frame: NativeFrame) -> bool:
        block, selector = key
        old = self.reports.get(key)
        if block in AVAILABILITY_FIELDS:
            name = AVAILABILITY_FIELDS[block]
            self._selection_changed(
                block,
                _integer(old.frame.decoded, name) if old else None,
                _integer(frame.decoded, name),
            )
        self.reports[key] = sample = PlantRegisterReport(
            block,
            selector,
            frame.message.payload,
            frame,
        )
        return sample != old

    def _availability(self) -> dict[int, int]:
        return {
            block: value
            for block, name in AVAILABILITY_FIELDS.items()
            if (r := self.reports.get((block, None)))
            and (value := _integer(r.frame.decoded, name)) is not None
        }

    def missing(
        self,
        product: NativeProduct | None,
        subnet: int,
    ) -> dict[BlockKey, int]:
        if product is None or not has_plant_registers(product):
            return {}
        candidates = PLANT_REGISTERS.intersection(product.candidate_blocks)
        availability = self._availability()
        missing = {}
        for block in sorted(candidates, key=lambda b: (b not in _BANKS, b)):
            parent = INSTANCE_AVAILABILITY.get(block)
            if (
                parent is not None
                and parent in candidates
                and (parent, None) not in self.reports
            ):
                continue  # Await the bank before choosing an instance.
            for selector in block_selectors(block, product, subnet, availability):
                key = (block, selector)
                if key not in self.reports:
                    missing[key] = self.revisions.get(key, 0)
        return missing

    def snapshot(
        self,
        product: NativeProduct | None,
        subnet: int,
        *,
        addressable: bool,
    ) -> PlantRegisters:
        return PlantRegisters(
            tuple(self.reports[key] for key in sorted(self.reports)),
            tuple(self.missing(product, subnet)) if addressable else (),
        )
