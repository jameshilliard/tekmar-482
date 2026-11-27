"""Source-scoped native configuration and telemetry for one capture session."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Self

from ._native_payloads import BLOCK_SELECTORS
from ._native_payloads.climate import (
    AIR_GROUP_CONFIG,
    AIR_MASTER,
    AIR_MEMBER,
    HEATING_SETUP,
    MODE_BLOCKS,
    AirGroupTargets,
    AirStatus,
    AirTarget,
    ThermostatConfiguration,
)
from ._native_payloads.codec import Reader, ShortPayload
from ._native_payloads.io import IoEndpoint
from ._native_payloads.layouts import MULTI_WEEK
from ._native_payloads.line import GroupReport, LineReportKind
from ._native_payloads.queries import StatusRequest
from ._native_payloads.schedules import SCHEDULE_STATUS
from ._native_payloads.sensors import SENSOR_BLOCKS, ThermostatSensors
from ._native_payloads.setpoint import SETPOINT_STATUS
from ._native_payloads.snowmelt import SNOW_REPORT
from ._native_payloads.thermostat import FAN_STATUS, HUMIDITY_STATUS, SLAB_STATUS
from ._native_payloads.water import MASTER_PRESENT
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
from .native_io import IoBook, IoDirectory, IoDirectoryRead
from .native_peer_link import PeerLinkCounters
from .native_plant_registers import (
    PLANT_REGISTERS,
    PlantRegisterBook,
    PlantRegisters,
    has_plant_registers,
)
from .native_routes import device_routes
from .native_schedule import ScheduleDirectory, SystemSchedule
from .native_setpoint import SETPOINT_BLOCKS, NativeSetpointControl, SetpointBook
from .native_snowmelt import NativeSnowmelt, SnowmeltChannels, SnowmeltConfiguration
from .native_targets import ADJUSTMENT_BLOCKS, AdjustmentState, ThermostatAdjustments
from .raw import CaptureGap
from .water_topology import water_systems

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ._native_payloads.climate import AirGroupConfiguration
    from ._native_payloads.faults import NetworkFault
    from ._native_payloads.line import LineValue
    from ._water_monitor import WaterSource
    from .native_routes import NativeDeviceRoutes
    from .native_schedule import NativeWeeklySchedule
    from .native_snowmelt import SnowmeltChannel
    from .native_status import NativeStatusBatch
    from .raw import CaptureRecord
    from .water_topology import WaterSystem

BLOCK_REPORT = 0x2E7
BLOCK_UPDATE = 0x2DF
TAKING_ADDRESS = 0x3E7
_BLOCK_HEADER = 3
_UPDATE_HEADER = 4
_OLD_ADDRESS = 2
_FAULT_WORD_BYTES = 2
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
class NativeFaultReport:
    path: NativePath
    fault: NetworkFault
    extension: bytes
    frame: NativeFrame = field(compare=False, repr=False)


@dataclass(frozen=True, slots=True)
class NativeLineReport:
    """Last announcement from this source, not a consensus or a device setting."""

    path: NativePath
    kind: LineReportKind
    value: LineValue
    frame: NativeFrame = field(compare=False, repr=False)
    payload: bytes = field(init=False, repr=False)

    def __post_init__(self) -> None:
        # Compare every payload byte, including unknown extensions, but not time.
        object.__setattr__(self, "payload", self.frame.message.payload)

    @property
    def group(self) -> int | None:
        return self.value.group if isinstance(self.value, GroupReport) else None


@dataclass(frozen=True, slots=True)
class NativeClimate:
    path: NativePath
    identity: NativeIdentity
    setup: ThermostatConfiguration
    air: AirStatus | None
    active_air_target: AirTarget | None
    sensors: ThermostatSensors
    adjustments: ThermostatAdjustments
    air_timestamp_ms: int | None = field(compare=False)
    air_group_configuration: AirGroupConfiguration | None = None


@dataclass(slots=True)
class _Peer:
    identity: NativeIdentity | None = None
    fault: NativeFaultReport | None = None
    line: dict[tuple[LineReportKind, int | None], NativeLineReport] = field(
        default_factory=dict,
    )
    blocks: dict[BlockKey, NativeFrame] = field(default_factory=dict)
    air: AirStatus | None = None
    active_air_target: AirTarget | None = None
    air_frame: NativeFrame | None = None
    air_group_configuration: AirGroupConfiguration | None = None
    heating_setup_revision: int = 0
    snow: SnowmeltChannels = field(default_factory=SnowmeltChannels)
    calendar: ScheduleBook = field(default_factory=ScheduleBook)
    outputs: AdjustmentState = field(default_factory=AdjustmentState)
    setpoint: SetpointBook = field(default_factory=SetpointBook)
    plant: PlantRegisterBook = field(default_factory=PlantRegisterBook)
    io: dict[IoEndpoint, IoBook] = field(default_factory=dict)

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
            self.active_air_target,
            sensors,
            self.outputs.snapshot(product, sensors, fields),
            self.air_frame.timestamp_ms if self.air_frame else None,
            self.air_group_configuration,
        )


class NativeState:
    """Retain reported values, not outbound commands, indexed by full receive path."""

    def __init__(self, paths: Iterable[NativePath] | None = None) -> None:
        self._selected = None if paths is None else frozenset(paths)
        self._peers: defaultdict[NativePath, _Peer] = defaultdict(_Peer)
        self._moves: dict[NativePath, NativePath] = {}
        self._reidentify: set[NativePath] = set()
        self._unanswered_master_ports: set[int] = set()
        self._water = WaterMonitor()
        self._air_groups = AirGroupTargets()

    @property
    def dropped(self) -> int:
        return self._water.dropped

    @property
    def water_sources(self) -> tuple[WaterSource, ...]:
        return self._water.sources

    def water_source(self, path: NativePath) -> WaterSource | None:
        return self._water.source(path.port, path.address)

    def has_master_announcement(self, port: int) -> bool:
        """Check for a MasterPresent map that includes the sender's full subnet."""
        return self._water.has_master_announcement(port)

    def master_subnets(self, port: int) -> tuple[int, ...]:
        """List distinct MasterPresent header subnets observed on a physical port."""
        return self._water.master_subnets(port)

    def port_subnets_checked(self, port: int) -> bool:
        """Return whether this capture has a header or a completed master probe."""
        return port in self._unanswered_master_ports or bool(self.master_subnets(port))

    def record_master_query(self, batch: NativeStatusBatch) -> bool:
        """Record a transmitted, unanswered query once its earlier trace is consumed."""
        port = batch.port
        if batch.query.kind is not StatusRequest.MASTER or port not in TN4_PORTS:
            msg = "expected a MasterQuery batch on a physical port"
            raise ValueError(msg)
        if (
            batch.reports
            or batch.tx_result is None
            or not batch.tx_result.completed_frame
            or self.port_subnets_checked(port)
        ):
            return False
        self._unanswered_master_ports.add(port)
        return True

    def device_routes(self, *, ports: Iterable[int]) -> tuple[NativeDeviceRoutes, ...]:
        """Resolve identified addresses in a caller-confirmed network scope."""
        scope = frozenset(ports)
        if not scope or not scope.issubset(TN4_PORTS):
            msg = "select at least one valid physical tN4 port"
            raise ValueError(msg)
        return device_routes(
            ((path, self.identity(path)) for path in self.paths),
            {port: self.master_subnets(port) for port in scope},
        )

    def water_systems(self, *, ports: Iterable[int]) -> tuple[WaterSystem, ...]:
        """Resolve advertised loops within a known shared physical network."""
        return water_systems(self.water_sources, ports=ports)

    def io_directory(
        self,
        path: NativePath,
        endpoint: IoEndpoint,
    ) -> IoDirectory | None:
        peer = self._peers.get(path)
        book = peer.io.get(endpoint) if peer else None
        return book.snapshot() if book else None

    def io_for(self, path: NativePath) -> tuple[IoDirectory, ...]:
        peer = self._peers.get(path)
        return tuple(peer.io[k].snapshot() for k in sorted(peer.io)) if peer else ()

    @property
    def io_directories(self) -> tuple[tuple[NativePath, IoDirectory], ...]:
        return tuple((p, d) for p in sorted(self._peers) for d in self.io_for(p))

    def reconcile_io(
        self,
        batch: NativeStatusBatch,
        *,
        before: IoDirectory | None,
        supplemental: tuple[NativeStatusBatch, ...] = (),
    ) -> bool:
        """Merge a completed exchange against the snapshot taken before sending."""
        if batch.query.kind is not StatusRequest.IO_DISCOVERY:
            msg = "expected an I/O discovery batch"
            raise ValueError(msg)
        if batch.peer is None or not batch.reports:
            return False
        path = NativePath(batch.port, batch.peer.subnet, batch.peer.node)
        if self._selected is not None and path not in self._selected:
            return False
        endpoint = IoEndpoint(*batch.query.payload)
        if before is not None and before.endpoint != endpoint:
            msg = "I/O discovery baseline belongs to a different endpoint"
            raise ValueError(msg)
        books = self._peers[path].io
        book = books.setdefault(endpoint, IoBook(endpoint))
        old = book.snapshot()
        book.reconcile(IoDirectoryRead(batch, supplemental), before)
        return book.snapshot() != old

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

    def peer_link_counters(self, path: NativePath) -> PeerLinkCounters | None:
        """Project observed counter blocks without assuming peer support."""
        if peer := self._peers.get(path):
            return PeerLinkCounters.from_blocks(peer.blocks)
        return None

    @property
    def peer_links(self) -> tuple[tuple[NativePath, PeerLinkCounters], ...]:
        return tuple(
            (p, counters)
            for p in sorted(self._peers)
            if (counters := self.peer_link_counters(p)) is not None
        )

    @property
    def paths(self) -> tuple[NativePath, ...]:
        water = (
            NativePath(w.port, w.address.subnet, w.address.node)
            for w in self.water_sources
        )
        return tuple(sorted(self._peers.keys() | set(water)))

    def forget(self, path: NativePath) -> bool:
        """Invalidate one path without recording a capture loss."""
        self._reidentify.add(path)
        self._moves.pop(path, None)
        peer = self._peers.pop(path, None)
        if peer is not None and peer.air is not None and peer.air.cycle is not None:
            self._air_groups = AirGroupTargets()
        water = self._water.forget(path.port, path.address)
        if path.port in self._unanswered_master_ports and not any(
            p.port == path.port for p in self.paths
        ):
            self._unanswered_master_ports.remove(path.port)
        return peer is not None or water

    @property
    def thermostats(self) -> tuple[NativeClimate, ...]:
        return tuple(t for p in self.paths if (t := self.climate(p)) is not None)

    def climate(self, path: NativePath) -> NativeClimate | None:
        return peer.climate(path) if (peer := self._peers.get(path)) else None

    def missing_sensor_reads(self, path: NativePath) -> dict[BlockKey, int]:
        """Cataloged thermostat input blocks not yet observed for this assignment."""
        peer = self._peers.get(path)
        if not path.node or not peer or not peer.identity:
            return {}
        product = peer.identity.product
        if product is None or product.family != "thermostat":
            return {}
        return {
            (block, None): peer.outputs.revisions.get(block, 0)
            for block in SENSOR_BLOCKS
            if block in product.candidate_blocks and (block, None) not in peer.blocks
        }

    def setpoint_control(self, path: NativePath) -> NativeSetpointControl | None:
        peer = self._peers.get(path)
        return peer.setpoint.snapshot(path, peer.identity) if peer else None

    @property
    def setpoint_controls(self) -> tuple[NativeSetpointControl, ...]:
        return tuple(
            s for p in self.paths if (s := self.setpoint_control(p)) is not None
        )

    def missing_setpoint_reads(self, path: NativePath) -> dict[BlockKey, int]:
        peer = self._peers.get(path)
        if not path.node or not peer or not peer.identity:
            return {}
        return peer.setpoint.missing(peer.identity.product)

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

    def system_schedule(self, number: int, *, ports: Iterable[int]) -> SystemSchedule:
        """Resolve schedule ownership on identified paths in one known network."""
        scope = frozenset(ports)
        physical = self.schedule_directory.system(number, ports=scope)
        preferred = {r.address: r.preferred for r in self.device_routes(ports=scope)}

        def local(week: NativeWeeklySchedule) -> bool:
            path = NativePath.from_frame(week.config.setup)
            return path == preferred.get(path.address)

        masters = tuple(w for w in physical.masters if local(w))
        confirmed = {NativePath.from_frame(w.config.setup).address for w in masters}
        return SystemSchedule(
            number,
            masters,
            tuple(w for w in physical.members if local(w)),
            tuple(
                w
                for w in physical.masters
                if NativePath.from_frame(w.config.setup).address not in confirmed
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
        if key == (HEATING_SETUP, None):
            return peer.heating_setup_revision
        if key[0] in SETPOINT_BLOCKS:
            return peer.setpoint.revisions.get(key[0], 0)
        if key[0] in ADJUSTMENT_BLOCKS or key[0] in SENSOR_BLOCKS:
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
        if peer and block in SETPOINT_BLOCKS:
            return peer.setpoint.reports.get(block) if selector is None else None
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

    def air_group_configuration(self, path: NativePath) -> AirGroupConfiguration | None:
        """Return the peer's latest role announcement, not its stored assignment."""
        return peer.air_group_configuration if (peer := self._peers.get(path)) else None

    def identity(self, path: NativePath) -> NativeIdentity | None:
        return peer.identity if (peer := self._peers.get(path)) else None

    def fault_report(self, path: NativePath) -> NativeFaultReport | None:
        """Last fault announcement, including explicit clears; None is unknown."""
        return peer.fault if (peer := self._peers.get(path)) else None

    @property
    def fault_reports(self) -> tuple[NativeFaultReport, ...]:
        return tuple(f for p in sorted(self._peers) if (f := self._peers[p].fault))

    def line_report(
        self,
        path: NativePath,
        kind: LineReportKind,
        *,
        group: int | None = None,
    ) -> NativeLineReport | None:
        """Return the last observed report. SETPOINT_GROUP requires its wire group."""
        return peer.line.get((kind, group)) if (peer := self._peers.get(path)) else None

    def line_reports_for(self, path: NativePath) -> tuple[NativeLineReport, ...]:
        if peer := self._peers.get(path):
            return tuple(peer.line[key] for key in sorted(peer.line))
        return ()

    @property
    def line_reports(self) -> tuple[NativeLineReport, ...]:
        return tuple(r for p in sorted(self._peers) for r in self.line_reports_for(p))

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
        peer = self._peers[path]
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
        if block in SETPOINT_BLOCKS:
            return peer.setpoint.report(block, frame)
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
            and block not in SETPOINT_BLOCKS
        ) or "truncated" in frame.decoded:
            return ()
        target = NativePath(frame.port, data[0], data[1] & _NODE)
        changed_water = self._water.observe(frame)
        peer = self._peers.get(target)
        if peer is None or not target.node:
            return (target,) if changed_water else ()
        if block in SETPOINT_BLOCKS:
            peer.setpoint.invalidate(block)
            return (target,)
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

    def _source_report(self, path: NativePath, record: NativeFrame) -> bool | None:
        if (value := record.line_value) is not None:
            peer = self._peers[path]
            report = NativeLineReport(
                path,
                LineReportKind(record.message_id),
                value,
                record,
            )
            key = report.kind, report.group
            previous = peer.line.get(key)
            peer.line[key] = report
            return previous != report
        if (fault := record.network_fault) is not None:
            peer = self._peers[path]
            before = peer.fault
            peer.fault = NativeFaultReport(
                path,
                fault,
                record.message.payload[_FAULT_WORD_BYTES:],
                record,
            )
            return before != peer.fault
        if (sample := record.io_report) is not None:
            peer = self._peers[path]
            book = peer.io.setdefault(sample.endpoint, IoBook(sample.endpoint))
            return book.observe(sample, record)
        return None

    def observe(self, record: CaptureRecord) -> tuple[NativePath, ...]:
        """Return changed paths. Equal samples refresh timestamps without notifying."""
        if isinstance(record, CaptureGap):
            changed = tuple(p for p in self.paths if record.affects(p.port))
            self._unanswered_master_ports = {
                p for p in self._unanswered_master_ports if not record.affects(p)
            }
            self._reidentify.update(
                p for p in self._selected or changed if record.affects(p.port)
            )
            for path in changed:
                self._peers.pop(path, None)
            self._moves = {
                path: old
                for path, old in self._moves.items()
                if not record.affects(path.port)
            }
            # A lost master report could change a group shared across wires.
            self._air_groups = AirGroupTargets()
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
        if record.message_id in (AIR_MASTER, AIR_MEMBER):
            return self._observe_air(path, record)
        if self._selected is not None and path not in self._selected:
            return ()
        if record.message_id == AIR_GROUP_CONFIG:
            return self._observe_air_group_configuration(path, record)
        changed_water = self._water.observe(record)
        if changed_water and record.message_id == MASTER_PRESENT:
            self._unanswered_master_ports.discard(path.port)
        if (source_changed := self._source_report(path, record)) is not None:
            return (path,) if source_changed or changed_water else ()
        if record.message_id == BLOCK_REPORT:
            return (path,) if self._block(path, record) or changed_water else ()
        if record.message_id == SETPOINT_STATUS:
            setpoint = record.setpoint_status
            if setpoint is None:
                return ()
            peer = self._peers[path]
            product = peer.identity.product if peer.identity else None
            return (path,) if peer.setpoint.observe(setpoint, record, product) else ()
        if record.message_id in (FAN_STATUS, SLAB_STATUS, HUMIDITY_STATUS):
            output = record.thermostat_output
            if output is None:
                return ()
            peer = self._peers[path]
            return (path,) if peer.outputs.observe(output, record, peer.blocks) else ()
        if record.message_id == SCHEDULE_STATUS:
            status = record.schedule_status
            if status is None:
                return ()
            peer = self._peers[path]
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
            snow = self._peers[path].snow
            return (path,) if snow.update(record, address, report) else ()
        return (path,) if changed_water else ()

    def _observe_air_group_configuration(
        self,
        path: NativePath,
        record: NativeFrame,
    ) -> tuple[NativePath, ...]:
        config = record.air_group_configuration
        if config is None:
            return ()
        peer = self._peers[path]
        previous = peer.air_group_configuration
        peer.air_group_configuration = config
        if config.role_changed(previous):
            peer.heating_setup_revision += 1
        return (path,) if config != previous else ()

    def _observe_air(
        self,
        path: NativePath,
        record: NativeFrame,
    ) -> tuple[NativePath, ...]:
        try:
            air = AirStatus.read(
                Reader(record.message.payload),
                master=record.message_id == AIR_MASTER,
            )
        except ShortPayload:
            return ()
        target = self._air_groups.observe(air)
        # Unselected masters still supply the group code, not a public peer.
        if self._selected is not None and path not in self._selected:
            return ()
        peer = self._peers[path]
        if target is not None:
            peer.outputs.air(target, peer.blocks)
        changed_air = peer.air != air or peer.active_air_target != target
        peer.air, peer.air_frame = air, record
        peer.active_air_target = target
        return (path,) if changed_air else ()
