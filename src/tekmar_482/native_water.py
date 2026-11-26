"""Observe native boiler, mixing and tank telemetry without assuming port ownership."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from math import isfinite
from typing import TYPE_CHECKING

from ._native_payloads.codec import Reader, ShortPayload
from ._native_payloads.queries import NativeStatusQuery, StatusRequest
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
from .constants import TN4_PORTS
from .native import NativeAddress, NativeBlockQuery, NativeFrame
from .native_identity import NativeIdentity
from .raw import CaptureGap

if TYPE_CHECKING:
    from collections.abc import Iterable

    from .native_status import NativeStatusBatch
    from .raw import BridgeStream, CaptureRecord, NativeBlockBatch

_BLOCK = 0x2E7
_MOVE = 0x3E7
_NODE = 31
_HOST = NativeAddress(0, 0)
_NO_TOPOLOGY = LoopTopology()
_BLOCK_HEADER = 3
_PREVIOUS_ADDRESS_BYTES = 2
_ANNOUNCED_SLOTS = 8
_SETUP1 = 0x33
_STATUS = {MASTER_STATUS, PLANT_LOOP_STATUS, DHW_STATUS, TANK_STATUS}
_TRACKED = _STATUS | {MASTER_PRESENT, _BLOCK, _MOVE}
type _Path = tuple[int, NativeAddress]


@dataclass(frozen=True, slots=True)
class WaterSample[T: (LoopStatus, DhwStatus, TankStatus)]:
    timestamp_ms: int
    value: T
    extension: bytes


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

    @property
    def assignment(self) -> LoopAssignment | None:
        return self.topology.assignment_for(self.address.subnet)

    @property
    def has_measurements(self) -> bool:
        return bool(self.master or self.plant or self.dhw or self.tanks)

    def with_topology(self, topology: LoopTopology) -> WaterSource:
        updated = replace(self, topology=topology)
        if self.topology != _NO_TOPOLOGY and self.assignment != updated.assignment:
            # Do not relabel a previous loop's temperatures after reassignment.
            return replace(updated, master=None, plant=None)
        return updated

    def with_status(self, frame: NativeFrame) -> WaterSource:
        reader = Reader(frame.message.payload)
        stamp = frame.timestamp_ms
        if frame.message_id in (MASTER_STATUS, PLANT_LOOP_STATUS):
            master = frame.message_id == MASTER_STATUS
            value = LoopStatus.read(reader, master=master)
            loop = WaterSample(stamp, value, reader.remaining)
            return replace(self, master=loop) if master else replace(self, plant=loop)
        if frame.message_id == DHW_STATUS:
            dhw = DhwStatus.read(reader)
            return replace(self, dhw=WaterSample(stamp, dhw, reader.remaining))
        if frame.message_id != TANK_STATUS:
            msg = "not a water status report"
            raise ValueError(msg)
        tank = TankStatus.read(reader)
        tanks = {s.value.tank_id: s for s in self.tanks}
        tanks[tank.tank_id] = WaterSample(stamp, tank, reader.remaining)
        return replace(self, tanks=tuple(tanks[k] for k in sorted(tanks)))


class WaterMonitor:
    """Observe one uninterrupted stream. Sources include non-addressable masters."""

    def __init__(self) -> None:
        self._sources: dict[_Path, WaterSource] = {}
        self._identities: dict[_Path, NativeIdentity] = {}
        self.dropped = 0

    @property
    def sources(self) -> tuple[WaterSource, ...]:
        return tuple(
            sorted(
                self._sources.values(),
                key=lambda s: (s.port, s.address.subnet, s.address.node),
            ),
        )

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
        if path not in self._sources:
            return False
        if old is not None and (identity is None or identity.merge(old) is None):
            del self._sources[path]
        else:
            self._sources[path] = replace(self._sources[path], identity=identity)
        return True

    def _moved(self, path: _Path, data: bytes) -> bool:
        if len(data) < _PREVIOUS_ADDRESS_BYTES:
            return False
        previous = (path[0], NativeAddress(data[0], data[1] & _NODE))
        if previous == path:
            return False
        changed = False
        for site in (previous, path):
            self._identities.pop(site, None)
            changed |= self._sources.pop(site, None) is not None
        return changed

    @staticmethod
    def _water(source: WaterSource, frame: NativeFrame) -> WaterSource | None:
        data = frame.message.payload
        if frame.message_id == MASTER_PRESENT and data:
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
            if not slots and top == _NO_TOPOLOGY and not source.has_measurements:
                return None
            return source.with_topology(
                replace(top, setup1=slots)
                if block == _SETUP1
                else replace(top, setup2=slots),
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
            changed = bool(self._sources)
            self._sources.clear()
            self._identities.clear()
            return changed
        if not isinstance(record, NativeFrame) or record.message_id not in _TRACKED:
            return False
        path = (record.port, NativeAddress(record.subnet, record.node & _NODE))
        data = record.message.payload
        if record.message_id == _MOVE:
            return self._moved(path, data)
        if record.message_id == _BLOCK and data.startswith(b"\0\0"):
            return self._identity(path, record)
        old = self._sources.get(path)
        source = old or WaterSource(*path, identity=self._identities.get(path))
        updated = self._water(source, record)
        if updated is None or updated == old:
            return False
        self._sources[path] = updated
        return True


@dataclass(frozen=True, slots=True)
class WaterRead:
    sources: tuple[WaterSource, ...]
    master_queries: tuple[NativeStatusBatch, ...]
    block_reads: tuple[NativeBlockBatch, ...]
    dropped: int


async def _probe_water_port(
    stream: BridgeStream,
    port: int,
    source: NativeAddress,
    duration: float,
    timeout: float,
) -> tuple[NativeStatusBatch, tuple[NativeBlockBatch, ...]]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + duration
    master = await stream.read_status(
        port,
        NativeStatusQuery(StatusRequest.MASTER),
        source=source,
        timeout=min(timeout, duration),
    )
    peer = master.peer
    if not master.answered or peer is None or not peer.node & _NODE:
        return master, ()

    async def block(block_id: int) -> NativeBlockBatch | None:
        remaining = deadline - loop.time()
        if remaining <= 0:
            return None
        return await stream.read_block_group(
            port,
            NativeBlockQuery(peer, block_id),
            source=source,
            timeout=min(timeout, remaining),
        )

    identity = await block(0)
    if identity is None:
        return master, ()
    batches = [identity]
    if (
        identity.reports
        and identity.tx_result
        and identity.tx_result.may_have_reached_peer
    ):
        info = NativeIdentity.from_report(identity.reports[0].decoded)
        product = info.product if info else None
        if product is not None:
            for block_id in SETUP_SLOTS:
                if block_id not in product.candidate_blocks:
                    continue
                setup = await block(block_id)
                if setup is None:
                    break
                batches.append(setup)
                if setup.tx_result is None:
                    break
    return master, tuple(batches)


async def read_water(
    stream: BridgeStream,
    ports: Iterable[int],
    *,
    duration: float,
    source: NativeAddress | None = _HOST,
    timeout: float = 20,
) -> WaterRead:
    """Own the capture reader for a finite window; source=None sends no queries."""
    selected = tuple(sorted(set(ports)))
    if not selected or any(p not in TN4_PORTS for p in selected):
        msg = "select at least one valid physical tN4 port"
        raise ValueError(msg)
    if not all(isfinite(v) and v > 0 for v in (duration, timeout)):
        msg = "duration and response timeout must be finite and positive"
        raise ValueError(msg)
    monitor = WaterMonitor()

    async def collect() -> None:
        window = asyncio.timeout(duration)
        try:
            async with window:
                while True:
                    record = await stream.read()
                    if isinstance(record, CaptureGap) or record.port in selected:
                        monitor.observe(record)
        except TimeoutError:
            if not window.expired():
                raise

    reader = asyncio.create_task(collect())
    queries = [
        asyncio.create_task(_probe_water_port(stream, p, source, duration, timeout))
        for p in selected
        if source is not None
    ]
    tasks = (reader, *queries)
    try:
        await asyncio.gather(*tasks)
        results = [t.result() for t in queries]
        return WaterRead(
            monitor.sources,
            tuple(master for master, _ in results),
            tuple(b for _, batches in results for b in batches),
            monitor.dropped,
        )
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
