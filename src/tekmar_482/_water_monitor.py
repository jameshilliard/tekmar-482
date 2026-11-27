"""Observe native boiler, mixing and tank telemetry without assuming port ownership."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from ._native_payloads.codec import ShortPayload
from ._native_payloads.water import (
    DHW_STATUS,
    MASTER_PRESENT,
    MASTER_STATUS,
    PLANT_LOOP_STATUS,
    SETUP_SLOTS,
    TANK_STATUS,
    DhwStatus,
    LoopAssignment,
    LoopSetupSlot,
    LoopStatus,
    LoopTopology,
    TankStatus,
)
from .native import NativeAddress, NativeFrame
from .native_identity import NativeIdentity
from .native_plant import PLANT_REPORTS, PlantEquipment, WaterSample, read_sample
from .raw import CaptureGap

if TYPE_CHECKING:
    from .raw import CaptureRecord

_BLOCK = 0x2E7
_UPDATE = 0x2DF
_MOVE = 0x3E7
_NODE = 31
_NO_TOPOLOGY = LoopTopology()
_NO_EQUIPMENT = PlantEquipment()
_BLOCK_HEADER = 3
_UPDATE_HEADER = 4
_PREVIOUS_ADDRESS_BYTES = 2
_ANNOUNCED_SLOTS = 8
_SETUP1 = 0x33
_STATUS = {MASTER_STATUS, PLANT_LOOP_STATUS, DHW_STATUS, TANK_STATUS, *PLANT_REPORTS}
_TRACKED = _STATUS | {MASTER_PRESENT, _BLOCK, _MOVE, _UPDATE}
type _Path = tuple[int, NativeAddress]


@dataclass(frozen=True, slots=True)
class WaterSource:
    port: int
    address: NativeAddress
    identity: NativeIdentity | None = None
    topology: LoopTopology = _NO_TOPOLOGY
    master: WaterSample[LoopStatus] | None = None
    plant: WaterSample[LoopStatus] | None = None
    dhw: WaterSample[DhwStatus] | None = None
    tanks: tuple[WaterSample[TankStatus], ...] = ()
    equipment: PlantEquipment = _NO_EQUIPMENT
    pending_setup: frozenset[int] = frozenset()

    @property
    def assignment(self) -> LoopAssignment | None:
        if self.pending_setup:
            return None
        return self.topology.assignment_for(self.address.subnet)

    @property
    def has_measurements(self) -> bool:
        return bool(
            self.master
            or self.plant
            or self.dhw
            or self.tanks
            or self.equipment != _NO_EQUIPMENT,
        )

    @property
    def announces_own_subnet(self) -> bool:
        """A topology claim, not proof of a unique device or physical wiring."""
        return (
            self.address.subnet != 0 and self.address.subnet in self.topology.announced
        )

    def with_topology(
        self,
        topology: LoopTopology,
        *,
        pending: frozenset[int] | None = None,
    ) -> WaterSource:
        updated = replace(
            self,
            topology=topology,
            pending_setup=self.pending_setup if pending is None else pending,
        )
        if (
            self.topology != _NO_TOPOLOGY and self.assignment != updated.assignment
        ) or self.pending_setup != updated.pending_setup:
            # Do not relabel a previous loop's temperatures after reassignment.
            return replace(updated, master=None, plant=None, equipment=_NO_EQUIPMENT)
        return updated

    def with_status(self, frame: NativeFrame) -> WaterSource:
        if frame.message_id in PLANT_REPORTS:
            return replace(self, equipment=self.equipment.observe(frame))
        if frame.message_id in (MASTER_STATUS, PLANT_LOOP_STATUS):
            master = frame.message_id == MASTER_STATUS
            loop = read_sample(frame, lambda r: LoopStatus.read(r, master=master))
            return replace(self, master=loop) if master else replace(self, plant=loop)
        if frame.message_id == DHW_STATUS:
            return replace(self, dhw=read_sample(frame, DhwStatus.read))
        if frame.message_id != TANK_STATUS:
            msg = "not a water status report"
            raise ValueError(msg)
        tank = read_sample(frame, TankStatus.read)
        tanks = {s.value.tank_id: s for s in self.tanks}
        tanks[tank.value.tank_id] = tank
        return replace(self, tanks=tuple(tanks[k] for k in sorted(tanks)))


class WaterMonitor:
    """Observe one uninterrupted stream. Sources include non-addressable masters."""

    def __init__(self) -> None:
        self._sources: dict[_Path, WaterSource] = {}
        self._identities: dict[_Path, NativeIdentity] = {}
        self._moves: dict[_Path, _Path] = {}
        self._reidentify: set[_Path] = set()
        self.dropped = 0

    @property
    def sources(self) -> tuple[WaterSource, ...]:
        return tuple(
            sorted(
                self._sources.values(),
                key=lambda s: (s.port, s.address.subnet, s.address.node),
            ),
        )

    def source(self, port: int, address: NativeAddress) -> WaterSource | None:
        return self._sources.get(
            (port, NativeAddress(address.subnet, address.node & _NODE)),
        )

    def has_master_announcement(self, port: int) -> bool:
        return any(
            s.port == port and s.announces_own_subnet for s in self._sources.values()
        )

    def forget(self, port: int, address: NativeAddress) -> bool:
        """Discard measurements and identity for a single interrupted path."""
        return self._forget((port, NativeAddress(address.subnet, address.node & _NODE)))

    def _forget(self, path: _Path) -> bool:
        self._reidentify.add(path)
        self._moves.pop(path, None)
        self._identities.pop(path, None)
        return self._sources.pop(path, None) is not None

    def _identity(self, path: _Path, frame: NativeFrame) -> bool:
        fields = frame.decoded
        if "truncated" in fields or fields.get("block_layout") == "ambiguous":
            return False
        old = self._identities.get(path)
        identity = NativeIdentity.from_report(fields, old)
        if identity == old:
            return False
        if identity is None:
            self._identities.pop(path, None)
        else:
            self._identities[path] = identity
        discard = path in self._reidentify or (
            old is not None and (identity is None or identity.merge(old) is None)
        )
        if identity is not None:
            self._reidentify.discard(path)
        elif old is not None:
            self._reidentify.add(path)
        if path not in self._sources:
            return False
        if discard:
            del self._sources[path]
        else:
            self._sources[path] = replace(self._sources[path], identity=identity)
        return True

    def _moved(self, path: _Path, data: bytes) -> bool:
        if len(data) < _PREVIOUS_ADDRESS_BYTES:
            return False
        previous = (path[0], NativeAddress(data[0], data[1] & _NODE))
        if previous == path or (
            self._moves.get(path) == previous
            and previous not in self._sources
            and previous not in self._identities
        ):
            return False
        changed = False
        for site in (previous, path):
            changed |= self._forget(site)
        self._moves[path] = previous
        return changed

    @staticmethod
    def _water(source: WaterSource, frame: NativeFrame) -> WaterSource | None:
        data = frame.message.payload
        if frame.message_id == MASTER_PRESENT:
            if not data:
                return source
            return source.with_topology(
                replace(source.topology, announced=tuple(data[:_ANNOUNCED_SLOTS])),
            )
        if frame.message_id == _BLOCK and len(data) >= _BLOCK_HEADER:
            block = int.from_bytes(data[:2], "little")
            if block not in SETUP_SLOTS:
                return None
            fields = frame.decoded
            if "truncated" in fields or fields.get("block_layout") == "ambiguous":
                return None
            slots = LoopSetupSlot.from_block(block, fields)
            top = source.topology
            if (
                not slots
                and top == _NO_TOPOLOGY
                and not source.has_measurements
                and not source.pending_setup
            ):
                return None
            return source.with_topology(
                replace(top, setup1=slots)
                if block == _SETUP1
                else replace(top, setup2=slots),
                pending=source.pending_setup - {block},
            )
        if frame.message_id in _STATUS:
            try:
                return source.with_status(frame)
            except ShortPayload:
                return None
        return None

    def observe(self, record: CaptureRecord) -> bool:
        """Return whether visible water sources changed; never combine input paths."""
        if isinstance(record, CaptureGap):
            self.dropped += record.dropped
            lost = {
                p
                for p in self._sources.keys() | self._identities.keys()
                if record.affects(p[0])
            }
            changed = False
            for path in lost:
                changed |= self._forget(path)
            self._moves = {
                path: old
                for path, old in self._moves.items()
                if not record.affects(path[0])
            }
            return changed
        if not isinstance(record, NativeFrame) or record.message_id not in _TRACKED:
            return False
        path = (record.port, NativeAddress(record.subnet, record.node & _NODE))
        data = record.message.payload
        if record.message_id == _MOVE:
            return self._moved(path, data)
        if record.message_id == _UPDATE:
            if (
                len(data) <= _UPDATE_HEADER
                or (block := int.from_bytes(data[2:4], "little")) not in SETUP_SLOTS
                or "truncated" in record.decoded
            ):
                return False
            target = (record.port, NativeAddress(data[0], data[1] & _NODE))
            if not target[1].node or (source := self._sources.get(target)) is None:
                return False
            if block in source.pending_setup:
                return False
            top = source.topology
            self._sources[target] = source.with_topology(
                replace(top, setup1=None)
                if block == _SETUP1
                else replace(top, setup2=None),
                pending=source.pending_setup | {block},
            )
            return True
        if record.message_id == _BLOCK and data.startswith(b"\0\0"):
            return self._identity(path, record)
        old = self._sources.get(path)
        source = old or WaterSource(*path, identity=self._identities.get(path))
        updated = self._water(source, record)
        if updated is None:
            return False
        self._sources[path] = updated
        return updated != old
