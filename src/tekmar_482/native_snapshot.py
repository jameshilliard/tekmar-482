"""Read cataloged native blocks from devices sharing a physical tN4 network."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from ._native_payloads import SnowmeltBlock
from ._native_payloads.io import IoEndpoint
from ._native_payloads.products import AVAILABILITY_FIELDS, block_selectors
from ._native_payloads.queries import NativeStatusQuery, StatusRequest
from .constants import DEFAULT_BLOCK_TIMEOUT, TN4_PORTS
from .native import NativeAddress, NativeBlockQuery, NativeTargets
from .native_discovery import NativeInventory
from .native_schedule import ScheduleDirectory
from .native_sensors import ThermostatSensorMonitor
from .native_state import NativePath, NativeState
from .raw import RejectReason, TxStatus

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterable

    from ._native_payloads.products import NativeProduct
    from .native_discovery import NativeDevice
    from .native_io import IoDirectory
    from .native_plant_registers import PlantRegisters
    from .native_sensors import SensorObservation
    from .native_setpoint import NativeSetpointControl
    from .native_snowmelt import NativeSnowmelt
    from .native_state import NativeClimate
    from .native_status import NativeStatusBatch
    from .raw import NativeBlockBatch, NativeReader

_HOST = NativeAddress(0, 0)


@dataclass(frozen=True, slots=True)
class NativeSnapshot:
    blocks: tuple[NativeBlockBatch, ...]
    statuses: tuple[NativeStatusBatch, ...]

    @property
    def schedule_directory(self) -> ScheduleDirectory:
        """Build calendar tables and system membership without additional I/O."""
        return ScheduleDirectory.from_batches(self.blocks)

    @property
    def thermostat_sensors(self) -> tuple[SensorObservation, ...]:
        """Interpret each identified thermostat's sensor replies without more I/O."""
        return ThermostatSensorMonitor.from_batches(self.blocks).devices

    @property
    def snowmelt_devices(self) -> tuple[NativeSnowmelt, ...]:
        return self.observed_state().snowmelt_devices

    @property
    def thermostats(self) -> tuple[NativeClimate, ...]:
        return self.observed_state().thermostats

    @property
    def setpoint_controls(self) -> tuple[NativeSetpointControl, ...]:
        """Configuration from the scan; a block request does not solicit live status."""
        return self.observed_state().setpoint_controls

    @property
    def plant_configurations(self) -> tuple[tuple[NativePath, PlantRegisters], ...]:
        return self.observed_state().plant_configurations

    @property
    def io_directories(self) -> tuple[tuple[NativePath, IoDirectory], ...]:
        return self.observed_state().io_directories

    def observed_state(self) -> NativeState:
        """Project the reports from the scan without issuing further requests."""
        state = NativeState()
        for batch in self.blocks:
            for frame in batch.reports:
                state.observe(frame)
        for status in self.statuses:
            if status.query.kind is StatusRequest.IO_DISCOVERY and status.peer:
                path = NativePath(status.port, status.peer.subnet, status.peer.node)
                before = state.io_directory(path, IoEndpoint(*status.query.payload))
                state.reconcile_io(status, before=before)
                continue
            for frame in status.reports:
                state.observe(frame)
        return state


@dataclass(slots=True)
class _Peer:
    device: NativeDevice
    product: NativeProduct | None = None
    availability: dict[int, int] = field(default_factory=dict)

    @property
    def blocks(self) -> frozenset[int]:
        if self.product is None:
            return self.device.reported_blocks
        return self.device.reported_blocks.union(self.product.candidate_blocks)

    def selectors(self, block: int) -> set[int | None]:
        selectors = set(
            block_selectors(
                block,
                self.product,
                self.device.address.subnet,
                self.availability,
            ),
        )
        selectors.update(s for b, s in self.device.reported_selectors if b == block)
        return selectors


def _queries(
    peers: Iterable[_Peer],
    subnet: int,
    *,
    availability_only: bool,
) -> Iterable[NativeBlockQuery]:
    masks: dict[tuple[int, int | None], int] = {}
    for peer in peers:
        for block in peer.blocks:
            if not block or (block in AVAILABILITY_FIELDS) != availability_only:
                continue
            for selector in peer.selectors(block):
                key = (block, selector)
                masks[key] = masks.get(key, 0) | peer.device.address.request_mask()
    for block, selector in sorted(masks, key=lambda k: (k[0], k[1] or 0)):
        yield NativeBlockQuery(
            NativeTargets(subnet, masks[block, selector]),
            block,
            selector,
        )


@dataclass(frozen=True, slots=True)
class _Reader:
    stream: NativeReader
    source: NativeAddress
    timeout: float
    attempts: int

    async def read(
        self,
        port: int,
        query: NativeBlockQuery,
    ) -> AsyncIterator[NativeBlockBatch]:
        for _ in range(self.attempts):
            batch = await self.stream.read_block_group(
                port,
                query,
                source=self.source,
                timeout=self.timeout,
            )
            yield batch
            result = batch.tx_result
            if (
                result is None
                or not batch.missing_mask
                or (
                    result.status is TxStatus.REJECTED
                    and result.reason != RejectReason.LOCAL_ACK_ACTIVE
                )
            ):
                return
            query = replace(
                query,
                target=NativeTargets(query.target.subnet, batch.missing_mask),
            )


async def _read_subnet(
    reader: _Reader,
    port: int,
    subnet: int,
    devices: list[NativeDevice],
) -> AsyncIterator[NativeBlockBatch]:
    inventory = NativeInventory(devices)
    peers = {d.address.node & 31: _Peer(d) for d in inventory.devices}
    mask = sum(p.device.address.request_mask() for p in peers.values())
    async for identity in reader.read(
        port,
        NativeBlockQuery(NativeTargets(subnet, mask), 0),
    ):
        yield identity
        for frame in identity.reports:
            device = inventory.observe(frame)
            if (
                device is not None
                and device.identity
                and "product_number" in frame.decoded
            ):
                peers[frame.node & 31] = _Peer(device, device.identity.product)

    for availability_only in (True, False):
        for query in _queries(
            peers.values(),
            subnet,
            availability_only=availability_only,
        ):
            async for batch in reader.read(port, query):
                yield batch
                if availability_only:
                    for frame in batch.reports:
                        value = frame.decoded.get(AVAILABILITY_FIELDS[query.block_id])
                        if isinstance(value, int):
                            peers[frame.node & 31].availability[query.block_id] = value


async def read_native_devices(
    stream: NativeReader,
    devices: Iterable[NativeDevice],
    *,
    source: NativeAddress = _HOST,
    timeout: float = DEFAULT_BLOCK_TIMEOUT,
    attempts: int = 2,
    on_batch: Callable[[NativeBlockBatch], None] | None = None,
) -> tuple[NativeBlockBatch, ...]:
    """Read known and observed blocks; report missing nodes, never infer support.

    Use Normal management or an idle Raw stream. Requests are serialized per
    port, grouped by subnet and selector, with independent ports served
    concurrently. A Raw deadline without a TX result ends that port's scan:
    its send slot is still owned. An explicit Normal expiry releases the port.
    Only missing nodes are retried, at most once by default. Each attempt
    yields a separate batch. Captures remain available to the normal reader.
    """
    if not timeout > 0:
        msg = "block response timeout must be positive"
        raise ValueError(msg)
    if attempts < 1:
        msg = "block read needs at least one attempt"
        raise ValueError(msg)
    reader = _Reader(stream, source, timeout, attempts)
    ports: dict[int, dict[int, list[NativeDevice]]] = {}
    for device in devices:
        if device.port not in TN4_PORTS:
            msg = f"no physical tN4 port {device.port}"
            raise ValueError(msg)
        if not (device.address.node & 31) and device.io_endpoints:
            continue  # A status-only source is not selectable by a block bitmap.
        device.address.request_mask()
        ports.setdefault(device.port, {}).setdefault(device.address.subnet, []).append(
            device,
        )

    async def sample(port: int) -> list[NativeBlockBatch]:
        batches = []
        for subnet, group in sorted(ports[port].items()):
            async for batch in _read_subnet(
                reader,
                port,
                subnet,
                group,
            ):
                batches.append(batch)
                if on_batch is not None:
                    on_batch(batch)
                if batch.transmit_pending:
                    return batches
        return batches

    tasks = [asyncio.create_task(sample(port)) for port in sorted(ports)]
    try:
        return tuple(
            batch for batches in await asyncio.gather(*tasks) for batch in batches
        )
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def _status_queries(
    batches: Iterable[NativeBlockBatch],
    devices: Iterable[NativeDevice],
    *,
    include_line_status: bool,
) -> dict[int, list[tuple[NativeAddress | None, NativeStatusQuery]]]:
    state = NativeState()
    inventory = NativeInventory(devices)
    busy: set[int] = set()
    active: set[int] = set()
    for batch in batches:
        if batch.transmit_pending:
            # No terminal link result: the gateway still owns its send slot.
            busy.add(batch.port)
            continue
        if (
            not batch.tx_result
            or not batch.tx_result.completed_frame
            or not batch.reports
        ):
            continue
        active.add(batch.port)
        if batch.query.block_id in {
            0,
            SnowmeltBlock.APPLICATION_MODE,
            SnowmeltBlock.ZONE_ID,
            SnowmeltBlock.SENSOR_ID,
        }:
            for frame in batch.reports:
                state.observe(frame)
                if batch.query.block_id == 0:
                    inventory.observe(frame)
    ports: dict[int, list[tuple[NativeAddress | None, NativeStatusQuery]]] = {}
    for device in state.snowmelt_devices:
        if (
            device.path.port not in busy
            and (query := device.status_query()) is not None
        ):
            ports.setdefault(device.path.port, []).append((device.path.address, query))
    for peer in inventory.devices:
        if peer.io_endpoints and peer.port not in busy:
            ports.setdefault(peer.port, []).extend(
                (peer.address, NativeStatusQuery.io(e.subnet, e.node))
                for e in sorted(peer.io_endpoints)
            )
    if include_line_status:
        for port in active - busy:
            ports.setdefault(port, []).extend(
                (None, NativeStatusQuery(kind))
                for kind in (
                    StatusRequest.MASTER,
                    StatusRequest.OUTDOOR,
                    StatusRequest.CLOCK,
                    StatusRequest.WWSD,
                    StatusRequest.ZONE_SYNC,
                )
            )
            ports[port].append((None, NativeStatusQuery.gateway_access()))
    return ports


async def read_native_snapshot(
    stream: NativeReader,
    devices: Iterable[NativeDevice],
    *,
    source: NativeAddress = _HOST,
    timeout: float = DEFAULT_BLOCK_TIMEOUT,
    attempts: int = 2,
    include_line_status: bool = False,
    on_block: Callable[[NativeBlockBatch], None] | None = None,
    on_status: Callable[[NativeStatusBatch], None] | None = None,
) -> NativeSnapshot:
    """Read blocks, then application-specific telemetry from identified devices."""
    devices = tuple(devices)
    blocks = await read_native_devices(
        stream,
        devices,
        source=source,
        timeout=timeout,
        attempts=attempts,
        on_batch=on_block,
    )

    async def sample(
        port: int,
        queries: list[tuple[NativeAddress | None, NativeStatusQuery]],
    ) -> list[NativeStatusBatch]:
        batches = []
        for peer, query in queries:
            batch = await stream.read_status(
                port,
                query,
                source=source,
                peer=peer,
                timeout=timeout,
            )
            batches.append(batch)
            if on_status is not None:
                on_status(batch)
            if batch.transmit_pending:
                break
        return batches

    ports = _status_queries(blocks, devices, include_line_status=include_line_status)
    tasks = [
        asyncio.create_task(sample(p, queries)) for p, queries in sorted(ports.items())
    ]
    try:
        return NativeSnapshot(
            blocks,
            tuple(b for answers in await asyncio.gather(*tasks) for b in answers),
        )
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
