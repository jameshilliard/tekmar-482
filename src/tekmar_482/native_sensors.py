"""Read and observe the separately addressable inputs of native thermostats."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ._native_payloads.sensors import (
    SENSOR_BLOCKS,
    SETUP,
    SensorSlot,
    ThermostatSensors,
)
from .constants import DEFAULT_BLOCK_TIMEOUT
from .native import NativeAddress, NativeBlockQuery, NativeFrame
from .native_identity import NativeIdentity
from .native_state import (
    BLOCK_REPORT,
    BLOCK_UPDATE,
    TAKING_ADDRESS,
    NativePath,
    NativeState,
)
from .raw import CaptureGap

if TYPE_CHECKING:
    from collections.abc import Iterable

    from .raw import CaptureRecord, NativeBlockBatch, NativeReader

_HEADER = 3
_UPDATE_HEADER = 4
_MOVE_BYTES = 2
_NODE_MASK = 31
_HOST = NativeAddress(0, 0)
_SENSOR_UPDATES = frozenset((SETUP, SensorSlot.EXTERNAL_1.block))


@dataclass(frozen=True, slots=True)
class SensorObservation:
    port: int
    address: NativeAddress
    sensors: ThermostatSensors


class ThermostatSensorMonitor:
    """Sensor-only projection with the same freshness rules as NativeState."""

    def __init__(self) -> None:
        self._state = NativeState()

    @property
    def devices(self) -> tuple[SensorObservation, ...]:
        return tuple(
            SensorObservation(climate.path.port, climate.path.address, climate.sensors)
            for climate in self._state.thermostats
        )

    def observe(self, record: CaptureRecord) -> bool:
        """Return whether observations changed, without I/O or combining half words."""
        if isinstance(record, CaptureGap):
            return bool(self._state.observe(record))
        if not isinstance(record, NativeFrame):
            return False
        path = NativePath.from_frame(record)
        data = record.message.payload
        if record.message_id == TAKING_ADDRESS and len(data) >= _MOVE_BYTES:
            old_path = NativePath(record.port, data[0], data[1] & _NODE_MASK)
            if path != old_path and ({path, old_path} & set(self._state.paths)):
                return bool(self._state.observe(record))
            return False
        if record.message_id == BLOCK_UPDATE and len(data) > _UPDATE_HEADER:
            target = NativePath(record.port, data[0], data[1] & _NODE_MASK)
            block = int.from_bytes(data[2:4], "little")
            if block in _SENSOR_UPDATES and self._state.climate(target) is not None:
                return bool(self._state.observe(record))
            return False
        if record.message_id != BLOCK_REPORT or not path.node or len(data) < _HEADER:
            return False
        block = int.from_bytes(data[:2], "little")
        if block and (block not in SENSOR_BLOCKS or self._state.climate(path) is None):
            return False
        if not block:
            before = self._state.identity(path)
            if before is None and NativeIdentity.from_report(record.decoded) is None:
                return False
            self._state.observe(record)
            return before != self._state.identity(path)
        return bool(self._state.observe(record))

    @classmethod
    def from_batches(
        cls,
        batches: Iterable[NativeBlockBatch],
    ) -> ThermostatSensorMonitor:
        monitor = cls()
        for batch in batches:
            if batch.tx_result is not None and batch.tx_result.completed_frame:
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
            and batch.tx_result.completed_frame
            and batch.reports
        }
        return tuple(b for b in self.candidates if b not in answered)


async def read_thermostat_sensors(
    stream: NativeReader,
    port: int,
    address: NativeAddress,
    *,
    source: NativeAddress = _HOST,
    timeout: float = DEFAULT_BLOCK_TIMEOUT,
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
        if batch.transmit_pending:
            break
        if batch.tx_result and batch.tx_result.completed_frame:
            for frame in batch.reports:
                monitor.observe(frame)
    return SensorRead(monitor.devices[0], candidates, tuple(batches))
