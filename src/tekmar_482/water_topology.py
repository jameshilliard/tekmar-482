"""Logical water systems without merging physical tN4 endpoints."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ._native_payloads.water import LoopAssignment
from .constants import TN4_PORTS

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ._water_monitor import WaterSource


@dataclass(frozen=True, slots=True)
class WaterLoop:
    """One advertised loop and the exact receive paths that claim it."""

    advertised: LoopAssignment
    candidates: tuple[WaterSource, ...]
    other_claims: tuple[WaterSource, ...]
    repeated_slot: bool

    @property
    def source(self) -> WaterSource | None:
        """Return only an unopposed, exact assignment in the selected network."""
        if self.repeated_slot or self.other_claims or len(self.candidates) != 1:
            return None
        source = self.candidates[0]
        assignment = source.assignment
        return (
            source
            if assignment is not None and assignment.subnet == self.advertised.subnet
            else None
        )


@dataclass(frozen=True, slots=True)
class WaterSystem:
    """A shared MasterPresent map, not a unique physical controller ID."""

    subnets: tuple[int, ...]
    loops: tuple[WaterLoop, ...]


def water_systems(
    sources: Iterable[WaterSource],
    *,
    ports: Iterable[int],
) -> tuple[WaterSystem, ...]:
    """Project the slot maps in one physical network scope chosen by the caller."""
    scope = frozenset(ports)
    if not scope or not scope.issubset(TN4_PORTS):
        msg = "select at least one valid physical tN4 port"
        raise ValueError(msg)

    maps: set[tuple[int, ...]] = set()
    claims: defaultdict[int, list[tuple[tuple[int, ...], WaterSource]]] = defaultdict(
        list,
    )
    for source in sorted(
        sources,
        key=lambda s: (s.port, s.address.subnet, s.address.node),
    ):
        if source.port not in scope:
            continue
        subnets = tuple(bytes(source.topology.announced).rstrip(b"\x00"))
        if subnets:
            maps.add(subnets)
        assignment = source.assignment
        subnet = source.address.subnet
        if source.announces_own_subnet or (
            assignment is not None and assignment.subnet == subnet
        ):
            claims[subnet].append((subnets, source))

    return tuple(
        WaterSystem(
            subnets,
            tuple(
                WaterLoop(
                    LoopAssignment(slot, subnet),
                    tuple(s for slots, s in claims[subnet] if slots == subnets),
                    tuple(s for slots, s in claims[subnet] if slots != subnets),
                    subnets.count(subnet) > 1,
                )
                for slot, subnet in enumerate(subnets)
                if subnet
            ),
        )
        for subnets in sorted(maps)
    )
