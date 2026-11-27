"""Discover native tN4 devices by receive path, independently of tHA inventory."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from ._native_payloads import BLOCK_SELECTORS
from .constants import TN4_PORTS
from .native import NativeAddress, NativeFrame, NativeMessage, NativeTargets
from .native_identity import NativeIdentity
from .normal_trace import GatewayTraceGap, NormalPeerCapture
from .raw import CaptureGap

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from ._native_payloads.io import IoEndpoint
    from .raw import BridgeStream, CaptureRecord, TxResult

_REPORT = 0x2E7
_MOVE = 0x3E7
_MOVE_HEADER = 2
_BLOCK_HEADER = 3
_NODE_BITS = 31
_HOST = NativeAddress(0, 0)


@dataclass(frozen=True, slots=True)
class NativeDevice:
    """One observed receive path, retaining the latest wire node lock flag."""

    port: int
    address: NativeAddress
    identity: NativeIdentity | None = None
    reported_blocks: frozenset[int] = frozenset()
    reported_selectors: frozenset[tuple[int, int]] = frozenset()
    io_endpoints: frozenset[IoEndpoint] = frozenset()


class NativeInventory:
    """Accumulate valid native captures in one connection or discovery window."""

    def __init__(self, devices: Iterable[NativeDevice] = ()) -> None:
        self._nodes = {
            (d.port, d.address.subnet, d.address.node & _NODE_BITS): d for d in devices
        }
        self._moved_from: dict[tuple[int, int, int], tuple[int, int, int]] = {}
        self.host_records_dropped = 0
        self.gateway_trace_records_dropped = 0

    @property
    def devices(self) -> tuple[NativeDevice, ...]:
        return tuple(self._nodes[key] for key in sorted(self._nodes))

    def observe(self, frame: CaptureRecord) -> NativeDevice | None:
        if isinstance(frame, CaptureGap):
            if isinstance(frame, GatewayTraceGap):
                self.gateway_trace_records_dropped += frame.dropped
                for routes in (self._nodes, self._moved_from):
                    for key in tuple(routes):
                        if frame.port_mask & (1 << key[0]):
                            del routes[key]
            else:
                self.host_records_dropped += frame.dropped
                self._nodes.clear()
                self._moved_from.clear()
            return None
        if not isinstance(frame, NativeFrame):
            return None
        node = frame.node & _NODE_BITS
        key = (frame.port, frame.subnet, node)
        data = frame.message.payload
        io = frame.io_report
        last = self._nodes.get(key)
        if frame.message_id == _MOVE and len(data) >= _MOVE_HEADER:
            old = (frame.port, data[0], data[1] & _NODE_BITS)
            if old != key:
                previous = self._nodes.pop(old, None)
                self._moved_from.pop(old, None)
                if previous is not None or self._moved_from.get(key) != old:
                    # A known moving device keeps its identity, not its old I/O routes.
                    self._nodes.pop(key, None)
                    last = (
                        replace(previous, io_endpoints=frozenset())
                        if previous
                        else None
                    )
                self._moved_from[key] = old
        if not node and io is None:
            # A reported I/O endpoint is selectable without a block bitmap.
            return None
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
                selectors = device.reported_selectors
                if block == 0:
                    identity = NativeIdentity.from_report(fields, current)
                    if current is not None and (
                        identity is None or identity.merge(current) is None
                    ):
                        blocks = frozenset()
                        selectors = frozenset()
                        device = replace(device, io_endpoints=frozenset())
                    current = identity
                spec = BLOCK_SELECTORS.get(block)
                if spec and data[2] & 1 and len(data) > _BLOCK_HEADER:
                    selectors |= {(block, data[_BLOCK_HEADER] & spec[1])}
                device = replace(
                    device,
                    identity=current,
                    reported_blocks=blocks | {block},
                    reported_selectors=selectors,
                )
        elif io is not None:
            device = replace(device, io_endpoints=device.io_endpoints | {io.endpoint})
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
    gateway_trace_records_dropped: int = 0


async def discover_native(
    stream: BridgeStream | NormalPeerCapture,
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
    if isinstance(stream, NormalPeerCapture) and source is not None:
        msg = "a Normal peer capture is passive; use source=None"
        raise ValueError(msg)

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
        while True:
            record = await stream.read()
            if on_record is not None:
                on_record(record)
            device = inventory.observe(record)
            if isinstance(record, CaptureGap):
                # Keep queued or in-flight identity probes; fresh traffic can
                # request a previously completed probe again after the gap.
                seen.difference_update(k for k, v in probes.items() if v is not None)
            elif (
                isinstance(record, NativeFrame)
                and device is not None
                and source is not None
                and record.node & _NODE_BITS
            ):
                enqueue(record.port, record.subnet)

    async def probe(raw: BridgeStream, port: int, sender: NativeAddress) -> None:
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
            probes[key] = await raw.send(port, request)

    tasks = [asyncio.create_task(receive())]
    if source is not None and not isinstance(stream, NormalPeerCapture):
        tasks.extend(
            asyncio.create_task(probe(stream, port, source)) for port in TN4_PORTS
        )
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
        inventory.host_records_dropped,
        inventory.gateway_trace_records_dropped,
    )
