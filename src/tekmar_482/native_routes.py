"""Project native receive paths into logical addresses within a known network."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from .native import NativeAddress
    from .native_identity import NativeIdentity
    from .native_state import NativePath


@dataclass(frozen=True, slots=True)
class NativeDeviceRoutes:
    """Observed paths to one address, not a device-unique hardware identifier."""

    address: NativeAddress
    paths: tuple[NativePath, ...]
    claiming_ports: tuple[int, ...]
    preferred: NativePath | None
    identity_conflict: bool


def _conflict(identities: Iterable[NativeIdentity | None]) -> bool:
    combined = None
    for identity in identities:
        if identity is None:
            continue
        combined = identity if combined is None else identity.merge(combined)
        if combined is None:
            return True
    return False


def _preferred_port(
    claims: Mapping[int, tuple[int, ...]],
    subnet: int,
    claimed: tuple[int, ...],
) -> int | None:
    if len(claimed) == 1 and claims[claimed[0]] == (subnet,):
        return claimed[0]
    if not claimed and len(claims) == 1:
        port, announcements = next(iter(claims.items()))
        if not announcements:
            return port  # A caller explicitly scoped a direct line with no master.
    return None


def device_routes(
    observed: Iterable[tuple[NativePath, NativeIdentity | None]],
    claims: Mapping[int, tuple[int, ...]],
) -> tuple[NativeDeviceRoutes, ...]:
    grouped: defaultdict[NativeAddress, dict[NativePath, NativeIdentity | None]] = (
        defaultdict(dict)
    )
    for path, identity in observed:
        if path.port in claims and path.node:
            grouped[path.address][path] = identity
    result = []
    for address in sorted(grouped, key=lambda addr: (addr.subnet, addr.node)):
        identities = grouped[address]
        if not any(identities.values()):
            continue
        claiming = tuple(
            sorted(
                port for port, subnets in claims.items() if address.subnet in subnets
            ),
        )
        conflicting = _conflict(identities.values())
        port = (
            None if conflicting else _preferred_port(claims, address.subnet, claiming)
        )
        paths = tuple(sorted(identities))
        primary = next(
            (p for p in paths if p.port == port and identities[p] is not None),
            None,
        )
        result.append(
            NativeDeviceRoutes(address, paths, claiming, primary, conflicting),
        )
    return tuple(result)
