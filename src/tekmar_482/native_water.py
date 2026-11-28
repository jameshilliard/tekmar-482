"""Finite read-only hydronic capture and equipment setup reads."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from math import isfinite
from typing import TYPE_CHECKING

from ._capture import GatewayTraceGap
from ._configuration_watch import ConfigurationReader
from ._native_payloads.queries import NativeStatusQuery, StatusRequest
from ._native_payloads.water import MASTER_PRESENT, SETUP_SLOTS
from ._observation import DeferredReadError, ExpiredReadError, observation
from ._water_monitor import WaterMonitor, WaterSource
from .constants import TN4_PORTS
from .native import NativeAddress, NativeFrame
from .native_plant import WaterSample
from .native_plant_registers import has_plant_registers
from .native_state import BLOCK_REPORT, TAKING_ADDRESS, NativePath, NativeState
from .raw import CaptureGap
from .water_topology import water_systems

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ._observation import ObservationStream
    from .native import BlockKey
    from .native_plant_registers import PlantRegisters
    from .native_status import NativeStatusBatch
    from .raw import CaptureRecord, NativeBlockBatch, NativeReader
    from .water_topology import WaterSystem

_HOST = NativeAddress(0, 0)
_MASTER = NativeStatusQuery(StatusRequest.MASTER)

__all__ = ["WaterMonitor", "WaterRead", "WaterSample", "WaterSource", "read_water"]


@dataclass(frozen=True, slots=True)
class WaterRead:
    sources: tuple[WaterSource, ...]
    master_queries: tuple[NativeStatusBatch, ...]
    block_reads: tuple[NativeBlockBatch, ...]
    host_records_dropped: int
    plant_configurations: tuple[tuple[NativePath, PlantRegisters], ...] = ()
    gateway_trace_records_dropped: int = 0
    unfinished_ports: tuple[int, ...] = ()

    @property
    def dropped(self) -> int:
        return self.host_records_dropped + self.gateway_trace_records_dropped

    def water_systems(self, *, ports: Iterable[int]) -> tuple[WaterSystem, ...]:
        """Resolve advertised loops without modifying the observed source paths."""
        return water_systems(self.sources, ports=ports)


class _PortScan:
    """A master claim is local to one wire, not a physical device identity."""

    def __init__(self, port: int) -> None:
        self.port = port
        self.state = NativeState()
        self.reader = ConfigurationReader(self.state)
        self.master: NativeStatusBatch | None = None
        self.blocks: list[NativeBlockBatch] = []
        self.unfinished = False
        self._lost = False
        self._retired: set[NativePath] = set()
        self._claims: dict[NativePath, tuple[int, ...]] = {}
        self._index = 0
        self._received = asyncio.Event()

    def observe(self, record: CaptureRecord) -> None:
        if isinstance(record, CaptureGap):
            self._lost = True
            self._received.set()
        if not isinstance(record, NativeFrame) or not record.usable_for_state:
            self.reader.observe(record)
            return
        self._index = record.capture_index
        self._received.set()
        path = NativePath.from_frame(record)
        data = record.message.payload
        identity = (
            self.state.identity(path)
            if (record.message_id == BLOCK_REPORT and data.startswith(b"\0\0"))
            else None
        )
        changed = self.reader.observe(record)
        if record.message_id == TAKING_ADDRESS:
            self._retired.update(changed or ())
        elif identity is not None:
            current = self.state.identity(path)
            if current is None or current.merge(identity) is None:
                self._retired.add(path)
        elif (
            record.message_id == MASTER_PRESENT
            and (water := self.state.water_source(path)) is not None
        ):
            # An empty presence beacon does not withdraw an earlier slot list.
            announced = water.topology.announced
            if (previous := self._claims.get(path)) and previous != announced:
                self._retired.add(path)
                self.reader.discard(path)
            self._claims[path] = announced

    async def probe(
        self,
        reads: NativeReader,
        source: NativeAddress,
        timeout: float,
        deadline: float,
        *,
        plant_settings: bool,
    ) -> None:
        window = asyncio.timeout_at(deadline)
        try:
            async with window:
                while True:
                    try:
                        self.master = await reads.read_status(
                            self.port,
                            _MASTER,
                            source=source,
                            timeout=timeout,
                            wait_for_edits=True,
                        )
                    except DeferredReadError:
                        continue
                    break
                if not self.master.answered:
                    return
                answer = self.master.reports[-1]
                path = NativePath.from_frame(answer)
                # Raw collectors may finish before capture sees the same frame.
                # Normal completions are already ordered after their trace.
                while answer.capture_index > self._index and not self._lost:
                    self._received.clear()
                    await self._received.wait()
                if (
                    not path.node
                    or self._lost
                    or path in self._retired
                    or path not in self._claims
                ):
                    return
                await self._equipment(
                    reads,
                    path,
                    source,
                    timeout,
                    plant_settings=plant_settings,
                )
        except ExpiredReadError:
            self.unfinished = True
        except TimeoutError:
            if not window.expired():
                raise
            self.unfinished = True

    async def _equipment(
        self,
        reads: NativeReader,
        path: NativePath,
        source: NativeAddress,
        timeout: float,
        *,
        plant_settings: bool,
    ) -> None:
        async def block(
            key: BlockKey,
            revision: int,
        ) -> tuple[NativeBlockBatch, bool] | None:
            while (
                not self._lost
                and path not in self._retired
                and self.state.block_revision(path, key) == revision
            ):
                try:
                    batch, current = await self.reader.read(
                        reads,
                        path,
                        key,
                        revision,
                        source=source,
                        timeout=timeout,
                    )
                except DeferredReadError:
                    continue
                self.blocks.append(batch)
                return batch, current
            return None

        identified = await block(
            (0, None),
            self.state.block_revision(path, (0, None)),
        )
        if identified is None:
            return
        batch, current = identified
        identity = self.state.identity(path)
        if (
            not current
            or not batch.reports
            or not batch.tx_result
            or not batch.tx_result.completed_frame
            or identity is None
        ):
            return
        product = identity.product
        if product is None:
            return
        attempted: set[tuple[BlockKey, int]] = set()
        while not self._lost and path not in self._retired:
            jobs: dict[BlockKey, int] = (
                self.state.missing_plant_reads(path)
                if plant_settings and has_plant_registers(product)
                else {
                    (b, None): self.state.block_revision(path, (b, None))
                    for b in SETUP_SLOTS
                    if b in product.candidate_blocks
                }
            )
            job = next((j for j in jobs.items() if j not in attempted), None)
            if job is None:
                return
            attempted.add(job)
            response = await block(*job)
            if response is not None and response[0].tx_result is None:
                return


async def read_water(
    stream: ObservationStream,
    ports: Iterable[int],
    *,
    duration: float,
    source: NativeAddress | None = _HOST,
    timeout: float = 20,
    plant_settings: bool = True,
) -> WaterRead:
    """Observe every selected wire for one window; a passive scan never transmits."""
    selected = tuple(sorted(set(ports)))
    if not selected or any(p not in TN4_PORTS for p in selected):
        msg = "select at least one valid physical tN4 port"
        raise ValueError(msg)
    if not all(isfinite(v) and v > 0 for v in (duration, timeout)):
        msg = "duration and response timeout must be finite and positive"
        raise ValueError(msg)
    deadline = asyncio.get_running_loop().time() + duration
    async with asyncio.timeout_at(deadline):
        capture, reads = await observation(stream, selected, source)
    scans = {p: _PortScan(p) for p in selected}
    host_drops = gateway_drops = 0

    async def collect() -> None:
        nonlocal host_drops, gateway_drops
        window = asyncio.timeout_at(deadline)
        try:
            async with window:
                while True:
                    record = await capture.read()
                    if isinstance(record, GatewayTraceGap):
                        affected = scans.keys() & record.ports
                        if affected:
                            gateway_drops += record.dropped
                        for port in affected:
                            scans[port].observe(record)
                    elif isinstance(record, CaptureGap):
                        host_drops += record.dropped
                        for scan in scans.values():
                            scan.observe(record)
                    elif record.port in scans:
                        scans[record.port].observe(record)
        except TimeoutError:
            if not window.expired():
                raise

    tasks = [asyncio.create_task(collect())]
    if source is not None and reads is not None:
        tasks.extend(
            asyncio.create_task(
                scan.probe(
                    reads,
                    source,
                    timeout,
                    deadline,
                    plant_settings=plant_settings,
                ),
            )
            for scan in scans.values()
        )
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return WaterRead(
        sources=tuple(s for scan in scans.values() for s in scan.state.water_sources),
        master_queries=tuple(
            scan.master for scan in scans.values() if scan.master is not None
        ),
        block_reads=tuple(b for scan in scans.values() for b in scan.blocks),
        host_records_dropped=host_drops,
        gateway_trace_records_dropped=gateway_drops,
        unfinished_ports=tuple(scan.port for scan in scans.values() if scan.unfinished),
        plant_configurations=tuple(
            p for scan in scans.values() for p in scan.state.plant_configurations
        ),
    )
