"""Finite read-only hydronic capture and equipment setup reads."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from math import isfinite
from typing import TYPE_CHECKING

from ._configuration_watch import ConfigurationReader
from ._native_payloads.queries import NativeStatusQuery, StatusRequest
from ._native_payloads.water import SETUP_SLOTS
from ._water_monitor import WaterMonitor, WaterSource
from .constants import TN4_PORTS
from .native import NativeAddress, NativeBlockQuery
from .native_identity import NativeIdentity
from .native_plant import WaterSample
from .native_plant_registers import has_plant_registers
from .native_state import NativePath, NativeState
from .raw import CaptureGap

if TYPE_CHECKING:
    from collections.abc import Iterable

    from .native import BlockKey
    from .native_plant_registers import PlantRegisters
    from .native_status import NativeStatusBatch
    from .raw import BridgeStream, NativeBlockBatch

_HOST = NativeAddress(0, 0)
_NODE = 31

__all__ = ["WaterMonitor", "WaterRead", "WaterSample", "WaterSource", "read_water"]


@dataclass(frozen=True, slots=True)
class WaterRead:
    sources: tuple[WaterSource, ...]
    master_queries: tuple[NativeStatusBatch, ...]
    block_reads: tuple[NativeBlockBatch, ...]
    dropped: int
    plant_configurations: tuple[tuple[NativePath, PlantRegisters], ...] = ()


async def _read_plant(
    stream: BridgeStream,
    reader: ConfigurationReader,
    path: NativePath,
    identity: NativeIdentity,
    received: asyncio.Event,
    *,
    source: NativeAddress,
    timeout: float,
    deadline: float,
) -> list[NativeBlockBatch]:
    batches = []
    attempted: set[tuple[BlockKey, int]] = set()
    window = asyncio.timeout_at(deadline)
    try:
        async with window:
            # The request collector can finish just before capture sees block 0.
            while reader.state.identity(path) is None:
                received.clear()
                await received.wait()
            while (
                observed := reader.state.identity(path)
            ) is not None and observed.merge(identity) is not None:
                job = next(
                    (
                        j
                        for j in reader.state.missing_plant_reads(path).items()
                        if j not in attempted
                    ),
                    None,
                )
                if job is None:
                    break
                attempted.add(job)
                # The enclosing window also bounds waiting for an edit or occupied port.
                batch, _ = await reader.read(
                    stream,
                    path,
                    *job,
                    source=source,
                    timeout=timeout,
                )
                batches.append(batch)
                if batch.tx_result is None:
                    break
    except TimeoutError:
        if not window.expired():
            raise
    return batches


async def _probe_water_port(
    stream: BridgeStream,
    port: int,
    source: NativeAddress,
    duration: float,
    timeout: float,
    *,
    plant_settings: bool,
    reader: ConfigurationReader,
    received: asyncio.Event,
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

    async def block(
        block_id: int,
        selector: int | None = None,
    ) -> NativeBlockBatch | None:
        remaining = deadline - loop.time()
        if remaining <= 0:
            return None
        return await stream.read_block_group(
            port,
            NativeBlockQuery(peer, block_id, selector),
            source=source,
            timeout=min(timeout, remaining),
        )

    identity = await block(0)
    if identity is None:
        return master, ()
    batches = [identity]
    if identity.reports and identity.tx_result and identity.tx_result.completed_frame:
        info = NativeIdentity.from_report(identity.reports[0].decoded)
        product = info.product if info else None
        if plant_settings and info is not None and has_plant_registers(product):
            batches.extend(
                await _read_plant(
                    stream,
                    reader,
                    NativePath(port, peer.subnet, peer.node),
                    info,
                    received,
                    source=source,
                    timeout=timeout,
                    deadline=deadline,
                ),
            )
        elif product is not None:
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
    plant_settings: bool = True,
) -> WaterRead:
    """Own the capture reader for a finite window; source=None sends no queries."""
    selected = tuple(sorted(set(ports)))
    if not selected or any(p not in TN4_PORTS for p in selected):
        msg = "select at least one valid physical tN4 port"
        raise ValueError(msg)
    if not all(isfinite(v) and v > 0 for v in (duration, timeout)):
        msg = "duration and response timeout must be finite and positive"
        raise ValueError(msg)
    state = NativeState()
    observer = ConfigurationReader(state)
    received = {port: asyncio.Event() for port in selected}

    async def collect() -> None:
        window = asyncio.timeout(duration)
        try:
            async with window:
                while True:
                    record = await stream.read()
                    if isinstance(record, CaptureGap) or record.port in selected:
                        observer.observe(record)
                        if not isinstance(record, CaptureGap):
                            received[record.port].set()
        except TimeoutError:
            if not window.expired():
                raise
        finally:
            observer.discard_pending()

    reader = asyncio.create_task(collect())
    queries = [
        asyncio.create_task(
            _probe_water_port(
                stream,
                p,
                source,
                duration,
                timeout,
                plant_settings=plant_settings,
                reader=observer,
                received=received[p],
            ),
        )
        for p in selected
        if source is not None
    ]
    tasks = (reader, *queries)
    try:
        await asyncio.gather(*tasks)
        results = [t.result() for t in queries]
        return WaterRead(
            state.water_sources,
            tuple(master for master, _ in results),
            tuple(b for _, batches in results for b in batches),
            state.dropped,
            state.plant_configurations,
        )
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
