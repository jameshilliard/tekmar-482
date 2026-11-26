"""Read and observe the separately addressable inputs of native thermostats."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ._native_payloads.sensors import (
    ROOM_STATS,
    SENSOR_BLOCKS,
    SETUP,
    ThermostatSensors,
)
from .native import NativeAddress, NativeBlockQuery, NativeFrame
from .native_identity import NativeIdentity
from .raw import CaptureGap

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ._native_payloads.products import NativeProduct
    from .raw import BridgeStream, CaptureRecord, NativeBlockBatch

_REPORT = 0x2E7
_MOVE = 0x3E7
_HEADER = 3
_MOVE_BYTES = 2
_NODE_MASK = 31
_HOST = NativeAddress(0, 0)
_SUPPORTED = {**dict.fromkeys(SENSOR_BLOCKS, 0x7F), SETUP: 0x1F, ROOM_STATS: 0x3F}
type _Path = tuple[int, int, int]


@dataclass(frozen=True, slots=True)
class SensorObservation:
    port: int
    address: NativeAddress
    sensors: ThermostatSensors


@dataclass(slots=True)
class _Inputs:
    identity: NativeIdentity
    blocks: dict[int, dict[str, int | str | float]] = field(default_factory=dict)

    @property
    def thermostat(self) -> NativeProduct | None:
        product = self.identity.product
        return product if product and product.family == "thermostat" else None

    def snapshot(self, path: _Path) -> SensorObservation | None:
        if (product := self.thermostat) is None:
            return None
        port, subnet, node = path
        return SensorObservation(
            port,
            NativeAddress(subnet, node),
            ThermostatSensors.from_blocks(
                product,
                self.identity.z_number,
                self.blocks,
            ),
        )


class ThermostatSensorMonitor:
    """One uninterrupted Raw session; discard snapshots on a gap or address move."""

    def __init__(self) -> None:
        self._inputs: dict[_Path, _Inputs] = {}

    @property
    def devices(self) -> tuple[SensorObservation, ...]:
        return tuple(
            device
            for k in sorted(self._inputs)
            if (device := self._inputs[k].snapshot(k)) is not None
        )

    def _identity(self, path: _Path, fields: dict[str, int | str | float]) -> bool:
        old = self._inputs.get(path)
        identity = NativeIdentity.from_report(fields, old.identity if old else None)
        if identity is None:
            return self._inputs.pop(path, None) is not None
        if old is not None and identity == old.identity:
            return False
        # Reinterpret an incomplete identity once its revision arrives, but
        # never transfer readings between two incompatible occupants.
        if old is not None and identity.merge(old.identity) is not None:
            old.identity = identity
        else:
            self._inputs[path] = _Inputs(identity)
        return True

    def observe(self, record: CaptureRecord) -> bool:
        """Return whether observations changed, without I/O or combining half words."""
        if isinstance(record, CaptureGap):
            changed = bool(self._inputs)
            self._inputs.clear()
            return changed
        if not isinstance(record, NativeFrame):
            return False
        path = (record.port, record.subnet, record.node & _NODE_MASK)
        data = record.message.payload
        if record.message_id == _MOVE and len(data) >= _MOVE_BYTES:
            old_path = (record.port, data[0], data[1] & _NODE_MASK)
            if old_path != path:
                old = self._inputs.pop(old_path, None)
                new = self._inputs.pop(path, None)
                return old is not None or new is not None
        if record.message_id != _REPORT or not path[2] or len(data) < _HEADER:
            return False
        block = int.from_bytes(data[:2], "little")
        if block and not data[2] & _SUPPORTED.get(block, 0):
            return False
        fields = record.decoded
        if "truncated" in fields or fields.get("block_layout") == "ambiguous":
            return False
        if not block:
            return self._identity(path, fields)
        peer = self._inputs.get(path)
        if peer is None or peer.thermostat is None or peer.blocks.get(block) == fields:
            return False
        peer.blocks[block] = fields
        return True

    @classmethod
    def from_batches(
        cls,
        batches: Iterable[NativeBlockBatch],
    ) -> ThermostatSensorMonitor:
        monitor = cls()
        for batch in batches:
            if batch.tx_result is not None and batch.tx_result.may_have_reached_peer:
                for frame in batch.reports:
                    monitor.observe(frame)
        return monitor


@dataclass(frozen=True, slots=True)
class SensorRead:
    device: SensorObservation
    candidates: tuple[int, ...]
    batches: tuple[NativeBlockBatch, ...]

    @property
    def unanswered_blocks(self) -> tuple[int, ...]:
        """Include unattempted candidates if the preceding send still owns the port."""
        answered = {
            batch.query.block_id
            for batch in self.batches
            if batch.tx_result is not None
            and batch.tx_result.may_have_reached_peer
            and batch.reports
        }
        return tuple(b for b in self.candidates if b not in answered)


async def read_thermostat_sensors(
    stream: BridgeStream,
    port: int,
    address: NativeAddress,
    *,
    source: NativeAddress = _HOST,
    timeout: float = 20,
) -> SensorRead:
    """Identify one thermostat, then read only its cataloged input blocks."""

    async def read(block: int) -> NativeBlockBatch:
        return await stream.read_block_group(
            port,
            NativeBlockQuery(address, block),
            source=source,
            timeout=timeout,
        )

    first = await read(0)
    monitor = ThermostatSensorMonitor()
    monitor.observe(first.require_report())
    devices = monitor.devices
    if not devices:
        msg = "native sensor interpretation requires an identified thermostat"
        raise ValueError(msg)
    supported = devices[0].sensors.product.candidate_blocks
    candidates = tuple(b for b in SENSOR_BLOCKS if b in supported)
    batches = [first]
    for block in candidates:
        batch = await read(block)
        batches.append(batch)
        if batch.tx_result is None:
            break
        if batch.tx_result.may_have_reached_peer:
            for frame in batch.reports:
                monitor.observe(frame)
    return SensorRead(monitor.devices[0], candidates, tuple(batches))
