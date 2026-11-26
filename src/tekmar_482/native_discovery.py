"""Discover native tN4 devices by receive path, independently of tHA inventory."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from ._native_payloads.products import NativeProduct, identify_product
from .constants import TN4_PORTS
from .native import NativeAddress, NativeFrame, NativeMessage, NativeTargets
from .raw import CaptureGap

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from .raw import BridgeStream, CaptureRecord, TxResult

_REPORT = 0x2E7
_MOVE = 0x3E7
_MOVE_HEADER = 2
_BLOCK_HEADER = 3
_NODE_BITS = 31
_HOST = NativeAddress(0, 0)


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


@dataclass(frozen=True, slots=True)
class NativeDevice:
    """One observed receive path, retaining the latest wire node lock flag."""

    port: int
    address: NativeAddress
    identity: NativeIdentity | None = None
    reported_blocks: frozenset[int] = frozenset()


class NativeInventory:
    """Accumulate valid native captures in one connection or discovery window."""

    def __init__(self) -> None:
        self._nodes: dict[tuple[int, int, int], NativeDevice] = {}
        self._moved_from: dict[tuple[int, int, int], tuple[int, int, int]] = {}

    @property
    def devices(self) -> tuple[NativeDevice, ...]:
        return tuple(self._nodes[key] for key in sorted(self._nodes))

    def observe(self, frame: NativeFrame) -> NativeDevice | None:
        node = frame.node & _NODE_BITS
        if not node:
            return None  # Not selectable by a block request.
        key = (frame.port, frame.subnet, node)
        data = frame.message.payload
        last = self._nodes.get(key)
        if frame.message_id == _MOVE and len(data) >= _MOVE_HEADER:
            old = (frame.port, data[0], data[1] & _NODE_BITS)
            if old != key:
                previous = self._nodes.pop(old, None)
                self._moved_from.pop(old, None)
                if previous is not None or self._moved_from.get(key) != old:
                    last = previous  # Never inherit a different occupant's identity.
                self._moved_from[key] = old
        device = (
            replace(last, address=frame.message.source)
            if last is not None
            else NativeDevice(frame.port, frame.message.source)
        )
        if frame.message_id == _REPORT and len(data) >= _BLOCK_HEADER and data[2]:
            fields = frame.decoded
            if "truncated" not in fields and fields.get("block_layout") != "ambiguous":
                block = int.from_bytes(data[:2], "little")
                current = device.identity
                blocks = device.reported_blocks
                if (
                    block == 0
                    and (number := _number(fields, "product_number")) is not None
                ):
                    identity = NativeIdentity(
                        number,
                        _number(fields, "j_number"),
                        _number(fields, "z_number"),
                        _number(fields, "max_tn4_version"),
                    )
                    merged = identity.merge(current) if current is not None else None
                    if current is not None and merged is None:
                        blocks = frozenset()
                    current = merged or identity
                device = replace(
                    device,
                    identity=current,
                    reported_blocks=blocks | {block},
                )
        self._nodes[key] = device
        return device


@dataclass(frozen=True, slots=True)
class NativeProbe:
    port: int
    subnet: int
    result: TxResult | None


@dataclass(frozen=True, slots=True)
class NativeDiscovery:
    devices: tuple[NativeDevice, ...]
    probes: tuple[NativeProbe, ...]
    host_records_dropped: int


async def discover_native(
    stream: BridgeStream,
    *,
    duration: float = 30,
    source: NativeAddress | None = _HOST,
    subnets: Iterable[tuple[int, int]] = (),
    on_record: Callable[[CaptureRecord], None] | None = None,
) -> NativeDiscovery:
    """Own the capture consumer for one window; source None sends no requests."""
    if not duration > 0:
        msg = "discovery duration must be positive"
        raise ValueError(msg)
    inventory = NativeInventory()
    pending: dict[int, asyncio.Queue[int]] = {p: asyncio.Queue() for p in TN4_PORTS}
    seen: set[tuple[int, int]] = set()
    probes: dict[tuple[int, int], TxResult | None] = {}
    dropped = 0

    def enqueue(port: int, subnet: int) -> None:
        key = (port, subnet)
        if key not in seen:
            seen.add(key)
            pending[port].put_nowait(subnet)

    for port, subnet in subnets:
        if source is None or port not in TN4_PORTS:
            msg = "an explicit discovery subnet needs active discovery and a valid port"
            raise ValueError(msg)
        enqueue(port, NativeTargets(subnet).subnet)

    async def receive() -> None:
        nonlocal dropped
        while True:
            record = await stream.read()
            if on_record is not None:
                on_record(record)
            if isinstance(record, CaptureGap):
                dropped += record.dropped
            elif (
                isinstance(record, NativeFrame)
                and inventory.observe(record) is not None
                and source is not None
            ):
                enqueue(record.port, record.subnet)

    async def probe(port: int, sender: NativeAddress) -> None:
        while True:
            subnet = await pending[port].get()
            key = (port, subnet)
            probes[key] = None
            request = NativeMessage.request_block(
                0,
                source=sender,
                target=NativeTargets(subnet),
                argument=0,
            )
            probes[key] = await stream.send(port, request)

    tasks = [asyncio.create_task(receive())]
    if source is not None:
        tasks.extend(asyncio.create_task(probe(port, source)) for port in TN4_PORTS)
    try:
        done, _ = await asyncio.wait(
            tasks,
            timeout=duration,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in done:
            task.result()  # A transport error is not an empty inventory.
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return NativeDiscovery(
        inventory.devices,
        tuple(NativeProbe(*key, probes[key]) for key in sorted(probes)),
        dropped,
    )
