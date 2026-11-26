"""Source-scoped native configuration and live telemetry for one Raw session."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Self

from ._native_payloads import BLOCK_SELECTORS
from ._native_payloads.climate import (
    AIR_MASTER,
    AIR_MEMBER,
    MODE_BLOCKS,
    AirStatus,
    ThermostatConfiguration,
)
from ._native_payloads.codec import Reader, ShortPayload
from ._native_payloads.layouts import MULTI_WEEK
from ._native_payloads.schedules import SCHEDULE_STATUS
from ._native_payloads.sensors import SENSOR_BLOCKS, ThermostatSensors
from ._native_payloads.snowmelt import SNOW_REPORT
from ._native_payloads.thermostat import FAN_STATUS, HUMIDITY_STATUS, SLAB_STATUS
from ._water_monitor import WaterMonitor
from .constants import TN4_PORTS
from .native import BlockKey, NativeAddress, NativeFrame
from .native_calendar import (
    SCHEDULE_BLOCKS,
    NativeCalendar,
    ScheduleBook,
    schedule_profile,
)
from .native_identity import NativeIdentity
from .native_plant_registers import (
    PLANT_REGISTERS,
    PlantRegisterBook,
    PlantRegisters,
    has_plant_registers,
)
from .native_schedule import ScheduleDirectory
from .native_snowmelt import NativeSnowmelt, SnowmeltChannels, SnowmeltConfiguration
from .native_targets import ADJUSTMENT_BLOCKS, AdjustmentState, ThermostatAdjustments
from .raw import CaptureGap

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ._water_monitor import WaterSource
    from .native_snowmelt import SnowmeltChannel
    from .raw import CaptureRecord

BLOCK_REPORT = 0x2E7
BLOCK_UPDATE = 0x2DF
TAKING_ADDRESS = 0x3E7
_BLOCK_HEADER = 3
_UPDATE_HEADER = 4
_OLD_ADDRESS = 2
_NODE = 31
_CLIMATE_BLOCKS = frozenset((*MODE_BLOCKS, *SENSOR_BLOCKS, *ADJUSTMENT_BLOCKS))


@dataclass(frozen=True, order=True, slots=True)
class NativePath:
    port: int
    subnet: int
    node: int

    def __post_init__(self) -> None:
        if self.port not in TN4_PORTS:
            msg = f"invalid tN4 port {self.port}"
            raise ValueError(msg)
        address = NativeAddress(self.subnet, self.node)
        object.__setattr__(self, "node", address.node & _NODE)

    @classmethod
    def from_frame(cls, frame: NativeFrame) -> Self:
        return cls(frame.port, frame.subnet, frame.node)

    @property
    def address(self) -> NativeAddress:
        return NativeAddress(self.subnet, self.node)


@dataclass(frozen=True, slots=True)
class NativeClimate:
    path: NativePath
    identity: NativeIdentity
    setup: ThermostatConfiguration
    air: AirStatus | None
    sensors: ThermostatSensors
    adjustments: ThermostatAdjustments
    air_timestamp_ms: int | None = field(compare=False)


@dataclass(slots=True)
class _Peer:
    identity: NativeIdentity | None = None
    blocks: dict[BlockKey, NativeFrame] = field(default_factory=dict)
    air: AirStatus | None = None
    air_frame: NativeFrame | None = None
    snow: SnowmeltChannels = field(default_factory=SnowmeltChannels)
    calendar: ScheduleBook = field(default_factory=ScheduleBook)
    outputs: AdjustmentState = field(default_factory=AdjustmentState)
    plant: PlantRegisterBook = field(default_factory=PlantRegisterBook)

    def climate(self, path: NativePath) -> NativeClimate | None:
        identity = self.identity
        if (
            identity is None
            or (product := identity.product) is None
            or product.family != "thermostat"
        ):
            return None
        fields = {
            b: f.decoded
            for (b, s), f in self.blocks.items()
            if s is None and b in _CLIMATE_BLOCKS
        }
        sensors = ThermostatSensors.from_blocks(product, identity.z_number, fields)
        return NativeClimate(
            path,
            identity,
            ThermostatConfiguration.from_blocks(product, identity.z_number, fields),
            self.air,
            sensors,
            self.outputs.snapshot(product, sensors, fields),
            self.air_frame.timestamp_ms if self.air_frame else None,
        )


class NativeState:
    """Retain reported values, not outbound commands, indexed by full receive path."""

    def __init__(self, paths: Iterable[NativePath] | None = None) -> None:
        self._selected = None if paths is None else frozenset(paths)
        self._peers: dict[NativePath, _Peer] = {}
        self._moves: dict[NativePath, NativePath] = {}
        self._reidentify: set[NativePath] = set()
        self._water = WaterMonitor()

    @property
    def dropped(self) -> int:
        return self._water.dropped

    @property
    def water_sources(self) -> tuple[WaterSource, ...]:
        return self._water.sources

    def water_source(self, path: NativePath) -> WaterSource | None:
        return self._water.source(path.port, path.address)

    def plant_registers(self, path: NativePath) -> PlantRegisters | None:
        peer = self._peers.get(path)
        if peer is None:
            return None
        product = peer.identity.product if peer.identity else None
        if not has_plant_registers(product) and (
            not peer.plant.reports or (product and product.family == "thermostat")
        ):
            return None
        return peer.plant.snapshot(product, path.subnet, addressable=bool(path.node))

    def missing_plant_reads(self, path: NativePath) -> dict[BlockKey, int]:
        peer = self._peers.get(path)
        if peer is None or not path.node or peer.identity is None:
            return {}
        return peer.plant.missing(peer.identity.product, path.subnet)

    @property
    def plant_configurations(self) -> tuple[tuple[NativePath, PlantRegisters], ...]:
        return tuple(
            (p, v) for p in self.paths if (v := self.plant_registers(p)) is not None
        )

    @property
    def paths(self) -> tuple[NativePath, ...]:
        water = (
            NativePath(w.port, w.address.subnet, w.address.node)
            for w in self.water_sources
        )
        return tuple(sorted(self._peers.keys() | set(water)))

    @property
    def thermostats(self) -> tuple[NativeClimate, ...]:
        return tuple(t for p in self.paths if (t := self.climate(p)) is not None)

    def climate(self, path: NativePath) -> NativeClimate | None:
        return peer.climate(path) if (peer := self._peers.get(path)) else None

    @property
    def snowmelt_devices(self) -> tuple[NativeSnowmelt, ...]:
        return tuple(s for p in self.paths if (s := self.snowmelt(p)) is not None)

    def snowmelt_channels(self, path: NativePath) -> tuple[SnowmeltChannel, ...]:
        """Return valid reports even without a known product or application."""
        return peer.snow.channels if (peer := self._peers.get(path)) else ()

    def snowmelt(self, path: NativePath) -> NativeSnowmelt | None:
        peer = self._peers.get(path)
        if peer is None or (identity := peer.identity) is None:
            return None
        if (
            product := identity.product
        ) is None or product.family != "snowmelt_control":
            return None
        return NativeSnowmelt(
            path,
            identity,
            SnowmeltConfiguration.from_blocks(peer.blocks),
            peer.snow.channels,
        )

    def schedule(self, path: NativePath) -> NativeCalendar | None:
        if peer := self._peers.get(path):
            return peer.calendar.snapshot(peer.identity, addressable=bool(path.node))
        return None

    @property
    def schedule_directory(self) -> ScheduleDirectory:
        return ScheduleDirectory(
            tuple(
                w
                for p in self.paths
                if (calendar := self.schedule(p)) is not None
                for w in calendar.schedules
            ),
        )

    def missing_schedule_reads(self, path: NativePath) -> dict[BlockKey, int]:
        peer = self._peers.get(path)
        if peer is None or not path.node:
            return {}
        return {
            k: peer.calendar.revisions.get(k, 0)
            for k in peer.calendar.missing(schedule_profile(peer.identity))
        }

    def block_revision(self, path: NativePath, key: BlockKey) -> int:
        peer = self._peers.get(path)
        if peer is None:
            return 0
        if key[0] in ADJUSTMENT_BLOCKS:
            return peer.outputs.revisions.get(key[0], 0)
        if key[0] in PLANT_REGISTERS:
            return peer.plant.revisions.get(key, 0)
        return peer.calendar.revisions.get(key, 0)

    def block(
        self,
        path: NativePath,
        block: int,
        selector: int | None = None,
    ) -> NativeFrame | None:
        peer = self._peers.get(path)
        if peer and block in SCHEDULE_BLOCKS:
            return peer.calendar.blocks.get((block, selector))
        if peer and block in PLANT_REGISTERS:
            sample = peer.plant.reports.get((block, selector))
            return sample.frame if sample else None
        return peer.blocks.get((block, selector)) if peer else None

    def air_frame(self, path: NativePath) -> NativeFrame | None:
        return peer.air_frame if (peer := self._peers.get(path)) else None

    def air_status(self, path: NativePath) -> AirStatus | None:
        return peer.air if (peer := self._peers.get(path)) else None

    def identity(self, path: NativePath) -> NativeIdentity | None:
        return peer.identity if (peer := self._peers.get(path)) else None

    def _move(self, path: NativePath, data: bytes) -> tuple[NativePath, ...]:
        if len(data) < _OLD_ADDRESS:
            return ()
        previous = NativePath(path.port, data[0], data[1] & _NODE)
        if previous == path or (
            self._moves.get(path) == previous and previous not in self.paths
        ):
            return ()
        changed = tuple(
            p for p in (previous, path) if self._selected is None or p in self._selected
        )
        if changed:
            self._reidentify.update(changed)
            for p in changed:
                self._peers.pop(p, None)
                self._moves.pop(p, None)
            self._moves[path] = previous
        return changed

    def _block(self, path: NativePath, frame: NativeFrame) -> bool:
        data = frame.message.payload
        if len(data) < _BLOCK_HEADER:
            return False
        block = int.from_bytes(data[:2], "little")
        fields = frame.decoded
        if "truncated" in fields or fields.get("block_layout") == "ambiguous":
            return False
        selector = None
        if spec := BLOCK_SELECTORS.get(block):
            if not data[2] & 1 or len(data) <= _BLOCK_HEADER:
                return False
            selector = data[_BLOCK_HEADER] & spec[1]
        peer = self._peers.setdefault(path, _Peer())
        if not block:
            old = peer.identity
            identity = NativeIdentity.from_report(fields, old)
            if (old and (identity is None or identity.merge(old) is None)) or (
                identity is not None and path in self._reidentify
            ):
                peer = self._peers[path] = _Peer()
            if identity is not None:
                self._reidentify.discard(path)
            elif old is not None:
                self._reidentify.add(path)
            peer.identity = identity
        key = (block, selector)
        if block in SCHEDULE_BLOCKS:
            return peer.calendar.report(key, frame)
        if block in PLANT_REGISTERS:
            changed = peer.plant.report(key, frame)
            if block not in _CLIMATE_BLOCKS:
                return changed
        before = peer.blocks.get(key)
        peer.blocks[key] = frame
        if before is not None:
            peer.snow.invalidate(block, before, frame)
            if before.message.payload != data and block in ADJUSTMENT_BLOCKS:
                identity = peer.identity
                peer.outputs.reconfigured(
                    block,
                    before,
                    frame,
                    peer.blocks,
                    identity.product if identity else None,
                    identity.z_number if identity else None,
                )
        return before is None or before.message.payload != data

    def _configuration_update(self, frame: NativeFrame) -> tuple[NativePath, ...]:
        data = frame.message.payload
        if len(data) <= _UPDATE_HEADER:
            return ()
        block = int.from_bytes(data[2:4], "little")
        if (
            block not in SCHEDULE_BLOCKS
            and block not in ADJUSTMENT_BLOCKS
            and block not in PLANT_REGISTERS
        ) or "truncated" in frame.decoded:
            return ()
        target = NativePath(frame.port, data[0], data[1] & _NODE)
        changed_water = self._water.observe(frame)
        peer = self._peers.get(target)
        if peer is None or not target.node:
            return (target,) if changed_water else ()
        if block in PLANT_REGISTERS:
            spec = BLOCK_SELECTORS.get(block)
            selector = data[_UPDATE_HEADER] & spec[1] if spec else None
            peer.plant.invalidate((block, selector))
            peer.blocks.pop((block, selector), None)
            return (target,)
        if block in ADJUSTMENT_BLOCKS:
            if (
                not peer.identity
                or not peer.identity.product
                or peer.identity.product.family != "thermostat"
            ):
                return ()
            peer.outputs.update(block, peer.blocks)
            return (target,)
        selector = (
            (data[_UPDATE_HEADER] & BLOCK_SELECTORS[block][1])
            if block in MULTI_WEEK
            else None
        )
        peer.calendar.invalidate_update((block, selector))
        return (target,)

    def observe(self, record: CaptureRecord) -> tuple[NativePath, ...]:
        """Return changed paths. Equal samples refresh timestamps without notifying."""
        if isinstance(record, CaptureGap):
            changed = self.paths
            self._reidentify.update(self._selected or changed)
            self._peers.clear()
            self._moves.clear()
            self._water.observe(record)
            return changed
        if not isinstance(record, NativeFrame):
            return ()
        if record.message_id == BLOCK_UPDATE:
            return self._configuration_update(record)
        path = NativePath.from_frame(record)
        data = record.message.payload
        if record.message_id == TAKING_ADDRESS:
            moved = self._move(path, data)
            if moved:
                self._water.observe(record)
            return moved
        if self._selected is not None and path not in self._selected:
            return ()
        changed_water = self._water.observe(record)
        if record.message_id == BLOCK_REPORT:
            return (path,) if self._block(path, record) or changed_water else ()
        if record.message_id in (FAN_STATUS, SLAB_STATUS, HUMIDITY_STATUS):
            output = record.thermostat_output
            if output is None:
                return ()
            peer = self._peers.setdefault(path, _Peer())
            return (path,) if peer.outputs.observe(output, record, peer.blocks) else ()
        if record.message_id == SCHEDULE_STATUS:
            status = record.schedule_status
            if status is None:
                return ()
            peer = self._peers.setdefault(path, _Peer())
            return (
                (path,)
                if peer.calendar.announce(
                    status,
                    record,
                    schedule_profile(peer.identity),
                )
                else ()
            )
        if record.message_id == SNOW_REPORT:
            report = record.snowmelt_report
            if report is None or (address := report.address) is None:
                return ()
            snow = self._peers.setdefault(path, _Peer()).snow
            return (path,) if snow.update(record, address, report) else ()
        if record.message_id not in (AIR_MASTER, AIR_MEMBER):
            return (path,) if changed_water else ()
        try:
            air = AirStatus.read(Reader(data), master=record.message_id == AIR_MASTER)
        except ShortPayload:
            return ()
        peer = self._peers.setdefault(path, _Peer())
        peer.outputs.air(air, peer.blocks)
        changed_air = peer.air != air
        peer.air, peer.air_frame = air, record
        return (path,) if changed_air else ()
