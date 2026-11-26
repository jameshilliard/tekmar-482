"""Source-scoped native block reports and thermostat state for one Raw session."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Self

from ._native_payloads import BLOCK_SELECTORS
from ._native_payloads.climate import (
    AIR_MASTER,
    AIR_MEMBER,
    MODE_BLOCKS,
    AirStatus,
    ThermostatConfiguration,
)
from ._native_payloads.codec import Reader, ShortPayload
from ._native_payloads.sensors import SENSOR_BLOCKS, ThermostatSensors
from .constants import TN4_PORTS
from .native import NativeAddress, NativeFrame
from .native_identity import NativeIdentity
from .native_water import WaterMonitor
from .raw import CaptureGap

if TYPE_CHECKING:
    from collections.abc import Iterable

    from .native_water import WaterSource
    from .raw import CaptureRecord

BLOCK_REPORT = 0x2E7
TAKING_ADDRESS = 0x3E7
_BLOCK_HEADER = 3
_OLD_ADDRESS = 2
_NODE = 31
type _BlockKey = tuple[int, int | None]
_CLIMATE_BLOCKS = frozenset((*MODE_BLOCKS, *SENSOR_BLOCKS))


@dataclass(frozen=True, order=True, slots=True)
class NativePath:
    port: int
    subnet: int
    node: int

    def __post_init__(self) -> None:
        if self.port not in TN4_PORTS:
            msg = f"invalid tN4 port {self.port}"
            raise ValueError(msg)
        address = NativeAddress(self.subnet, self.node)
        object.__setattr__(self, "node", address.node & _NODE)

    @classmethod
    def from_frame(cls, frame: NativeFrame) -> Self:
        return cls(frame.port, frame.subnet, frame.node)

    @property
    def address(self) -> NativeAddress:
        return NativeAddress(self.subnet, self.node)


@dataclass(frozen=True, slots=True)
class NativeClimate:
    path: NativePath
    identity: NativeIdentity
    setup: ThermostatConfiguration
    air: AirStatus | None
    sensors: ThermostatSensors
    air_timestamp_ms: int | None = field(compare=False)


@dataclass(slots=True)
class _Peer:
    identity: NativeIdentity | None = None
    blocks: dict[_BlockKey, NativeFrame] = field(default_factory=dict)
    air: AirStatus | None = None
    air_frame: NativeFrame | None = None

    def climate(self, path: NativePath) -> NativeClimate | None:
        identity = self.identity
        if (
            identity is None
            or (product := identity.product) is None
            or product.family != "thermostat"
        ):
            return None
        fields = {
            b: f.decoded
            for (b, s), f in self.blocks.items()
            if s is None and b in _CLIMATE_BLOCKS
        }
        return NativeClimate(
            path,
            identity,
            ThermostatConfiguration.from_blocks(product, identity.z_number, fields),
            self.air,
            ThermostatSensors.from_blocks(product, identity.z_number, fields),
            self.air_frame.timestamp_ms if self.air_frame else None,
        )


class NativeState:
    """Retain reported values, not outbound commands, indexed by full receive path."""

    def __init__(self, paths: Iterable[NativePath] | None = None) -> None:
        self._selected = None if paths is None else frozenset(paths)
        self._peers: dict[NativePath, _Peer] = {}
        self._moves: dict[NativePath, NativePath] = {}
        self._reidentify: set[NativePath] = set()
        self._water = WaterMonitor()

    @property
    def dropped(self) -> int:
        return self._water.dropped

    @property
    def water_sources(self) -> tuple[WaterSource, ...]:
        return self._water.sources

    @property
    def paths(self) -> tuple[NativePath, ...]:
        water = (
            NativePath(w.port, w.address.subnet, w.address.node)
            for w in self.water_sources
        )
        return tuple(sorted(self._peers.keys() | set(water)))

    @property
    def thermostats(self) -> tuple[NativeClimate, ...]:
        return tuple(t for p in self.paths if (t := self.climate(p)) is not None)

    def climate(self, path: NativePath) -> NativeClimate | None:
        return peer.climate(path) if (peer := self._peers.get(path)) else None

    def block(
        self,
        path: NativePath,
        block: int,
        selector: int | None = None,
    ) -> NativeFrame | None:
        peer = self._peers.get(path)
        return peer.blocks.get((block, selector)) if peer else None

    def air_frame(self, path: NativePath) -> NativeFrame | None:
        return peer.air_frame if (peer := self._peers.get(path)) else None

    def air_status(self, path: NativePath) -> AirStatus | None:
        return peer.air if (peer := self._peers.get(path)) else None

    def identity(self, path: NativePath) -> NativeIdentity | None:
        return peer.identity if (peer := self._peers.get(path)) else None

    def _move(self, path: NativePath, data: bytes) -> tuple[NativePath, ...]:
        if len(data) < _OLD_ADDRESS:
            return ()
        previous = NativePath(path.port, data[0], data[1] & _NODE)
        if previous == path or (
            self._moves.get(path) == previous and previous not in self.paths
        ):
            return ()
        changed = tuple(
            p for p in (previous, path) if self._selected is None or p in self._selected
        )
        if changed:
            self._reidentify.update(changed)
            for p in changed:
                self._peers.pop(p, None)
                self._moves.pop(p, None)
            self._moves[path] = previous
        return changed

    def _block(self, path: NativePath, frame: NativeFrame) -> bool:
        data = frame.message.payload
        if len(data) < _BLOCK_HEADER:
            return False
        block = int.from_bytes(data[:2], "little")
        fields = frame.decoded
        if "truncated" in fields or fields.get("block_layout") == "ambiguous":
            return False
        selector = None
        if spec := BLOCK_SELECTORS.get(block):
            if not data[2] & 1 or len(data) <= _BLOCK_HEADER:
                return False
            selector = data[_BLOCK_HEADER] & spec[1]
        peer = self._peers.setdefault(path, _Peer())
        if not block:
            old = peer.identity
            identity = NativeIdentity.from_report(fields, old)
            if (old and (identity is None or identity.merge(old) is None)) or (
                identity is not None and path in self._reidentify
            ):
                peer = self._peers[path] = _Peer()
            if identity is not None:
                self._reidentify.discard(path)
            elif old is not None:
                self._reidentify.add(path)
            peer.identity = identity
        key = (block, selector)
        before = peer.blocks.get(key)
        peer.blocks[key] = frame
        return before is None or before.message.payload != data

    def observe(self, record: CaptureRecord) -> tuple[NativePath, ...]:
        """Return changed paths. Equal samples refresh timestamps without notifying."""
        if isinstance(record, CaptureGap):
            changed = self.paths
            self._reidentify.update(self._selected or changed)
            self._peers.clear()
            self._moves.clear()
            self._water.observe(record)
            return changed
        if not isinstance(record, NativeFrame):
            return ()
        path = NativePath.from_frame(record)
        data = record.message.payload
        if record.message_id == TAKING_ADDRESS:
            moved = self._move(path, data)
            if moved:
                self._water.observe(record)
            return moved
        if self._selected is not None and path not in self._selected:
            return ()
        changed_water = self._water.observe(record)
        if record.message_id == BLOCK_REPORT:
            return (path,) if self._block(path, record) or changed_water else ()
        if record.message_id not in (AIR_MASTER, AIR_MEMBER):
            return (path,) if changed_water else ()
        try:
            air = AirStatus.read(Reader(data), master=record.message_id == AIR_MASTER)
        except ShortPayload:
            return ()
        peer = self._peers.setdefault(path, _Peer())
        changed_air = peer.air != air
        peer.air, peer.air_frame = air, record
        return (path,) if changed_air else ()
