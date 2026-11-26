"""Native identification from supported bytes of block zero."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ._native_payloads.products import NativeProduct, identify_product

if TYPE_CHECKING:
    from collections.abc import Mapping


def _number(fields: Mapping[str, object], name: str) -> int | None:
    value = fields.get(name)
    return value if isinstance(value, int) else None


@dataclass(frozen=True, slots=True)
class NativeIdentity:
    """Supported fields from block zero, not a device-unique serial number."""

    product_number: int
    j_number: int | None
    z_number: int | None
    max_tn4_version: int | None

    @property
    def product(self) -> NativeProduct | None:
        return identify_product(self.product_number, self.j_number)

    @classmethod
    def from_report(
        cls,
        fields: Mapping[str, object],
        previous: NativeIdentity | None = None,
    ) -> NativeIdentity | None:
        """Merge fields; a changed revision alone cannot identify a replacement."""
        number = _number(fields, "product_number")
        if number is None:
            if previous is None:
                return None
            product_number = previous.product_number
        else:
            product_number = number
        current = cls(
            product_number,
            _number(fields, "j_number"),
            _number(fields, "z_number"),
            _number(fields, "max_tn4_version"),
        )
        if previous is not None:
            merged = current.merge(previous)
            if merged is not None or number is None:
                return merged
        return current

    def _consistent_with(self, previous: NativeIdentity) -> bool:
        return self.product_number == previous.product_number and all(
            a is None or b is None or a == b
            for a, b in (
                (self.j_number, previous.j_number),
                (self.z_number, previous.z_number),
                (self.max_tn4_version, previous.max_tn4_version),
            )
        )

    def merge(self, previous: NativeIdentity) -> NativeIdentity | None:
        """Combine compatible observations without inventing an identity on conflict."""
        if not self._consistent_with(previous):
            return None
        return NativeIdentity(
            self.product_number,
            previous.j_number if self.j_number is None else self.j_number,
            previous.z_number if self.z_number is None else self.z_number,
            previous.max_tn4_version
            if self.max_tn4_version is None
            else self.max_tn4_version,
        )
