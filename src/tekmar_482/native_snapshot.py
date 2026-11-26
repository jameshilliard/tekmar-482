"""Read cataloged native blocks from devices sharing a physical tN4 network."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from ._native_payloads.products import AVAILABILITY_FIELDS, block_selectors
from .constants import TN4_PORTS
from .native import NativeAddress, NativeBlockQuery, NativeTargets
from .native_discovery import NativeInventory
from .raw import RejectReason, TxStatus

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterable

    from ._native_payloads.products import NativeProduct
    from .native_discovery import NativeDevice
    from .raw import BridgeStream, NativeBlockBatch

_HOST = NativeAddress(0, 0)


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
    stream: BridgeStream
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
    stream: BridgeStream,
    devices: Iterable[NativeDevice],
    *,
    source: NativeAddress = _HOST,
    timeout: float = 20,
    attempts: int = 2,
    on_batch: Callable[[NativeBlockBatch], None] | None = None,
) -> tuple[NativeBlockBatch, ...]:
    """Read known and observed blocks; report missing nodes, never infer support.

    Use an idle Raw stream. Requests are serialized per port, grouped by subnet
    and selector, with independent physical ports served concurrently. A deadline
    without a TX result ends that port's scan: its send slot is still owned by
    the outstanding request. A missing device reply alone does not stop a scan.
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
                if batch.tx_result is None:
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
