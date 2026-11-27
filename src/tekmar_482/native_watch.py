"""One native capture reader with event-driven reads on independent physical ports."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from dataclasses import dataclass
from enum import IntEnum
from math import isfinite
from typing import TYPE_CHECKING

from ._configuration_watch import ConfigurationReader, ConfigurationWatch
from ._native_payloads.climate import AIR_MASTER, AIR_MEMBER, HEATING_SETUP, MODE_BLOCKS
from ._native_payloads.io import IoEndpoint
from ._native_payloads.layouts import MULTI_WEEK
from ._native_payloads.queries import NativeStatusQuery, StatusRequest
from ._native_payloads.schedules import SCHEDULE_STATUS, SETUP_BLOCK
from ._native_payloads.sensors import SENSOR_BLOCKS
from ._native_payloads.water import MASTER_PRESENT, MASTER_STATUS, PLANT_LOOP_STATUS
from ._observation import (
    DeferredReadError,
    ExpiredReadError,
    OrderedNormalReads,
    observation,
)
from ._snow_watch import SnowRead, SnowWatch
from .constants import TN4_PORTS
from .management import BridgeFeature
from .native import NativeAddress, NativeFrame
from .native_discovery import NativeProbe
from .native_io import local_io_endpoint, recover_io_directory
from .native_plant_registers import PLANT_REGISTERS
from .native_setpoint import SETPOINT_BLOCKS
from .native_state import (
    BLOCK_REPORT,
    BLOCK_UPDATE,
    TAKING_ADDRESS,
    NativePath,
    NativeState,
)
from .native_targets import ADJUSTMENT_BLOCKS
from .normal_trace import NormalNativeSession, NormalPeerCapture
from .raw import CaptureGap

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from ._native_payloads.climate import AirReading, AirStatus
    from ._observation import ObservationStream
    from .native import BlockKey
    from .native_identity import NativeIdentity
    from .native_io import IoDirectory, IoDirectoryRead
    from .native_state import NativeClimate
    from .native_status import NativeStatusBatch
    from .raw import BridgeStream, CaptureRecord, NativeBlockBatch, TxResult

_HOST = NativeAddress(0, 0)
_MASTER_QUERY = NativeStatusQuery(StatusRequest.MASTER)
_UPDATE_HEADER = 4
type _Read = int | SnowRead | BlockKey | IoEndpoint
type _DeviceJob = tuple[NativePath, _Read]
type _Job = _DeviceJob | _MasterProbe | _SubnetProbe
type _ChannelHint = tuple[int, int] | None
type _ModeHint = tuple[int, _ChannelHint, _ChannelHint]


def _mode_hint(air: AirStatus) -> _ModeHint:
    def side(reading: AirReading | None) -> _ChannelHint:
        # Enable, demand, setback and active target can change with a local
        # adjustment. Temperature drift and the master's cycle counter cannot
        # establish a new setting and would turn a watcher into a poller.
        return (reading.flags & 31, reading.target_raw) if reading is not None else None

    return air.group, side(air.heat), side(air.cool)


def _listen_ports(
    stream: ObservationStream,
    ports: Iterable[int] | None,
) -> frozenset[int]:
    if ports is None:
        capture = stream.peers if isinstance(stream, NormalNativeSession) else stream
        ports = (
            (p for p in TN4_PORTS if capture.port_mask & (1 << p))
            if isinstance(capture, NormalPeerCapture)
            else TN4_PORTS
        )
    selected = frozenset(ports)
    if not selected or not selected.issubset(TN4_PORTS):
        msg = "select at least one valid physical tN4 port"
        raise ValueError(msg)
    return selected


@dataclass(frozen=True, slots=True)
class ClimateChange:
    path: NativePath
    current: NativeClimate | None


@dataclass(frozen=True, slots=True)
class ClimateWatchResult:
    thermostats: tuple[NativeClimate, ...]
    reads: int
    latest_reads: tuple[NativeBlockBatch, ...]
    missing_mode_reports: tuple[NativePath, ...]
    dropped: int


@dataclass(frozen=True, slots=True)
class NativeWatchResult:
    state: NativeState
    reads: int
    latest_blocks: tuple[NativeBlockBatch, ...]
    latest_statuses: tuple[NativeStatusBatch, ...]
    subnet_probes: tuple[NativeProbe, ...]


@dataclass(frozen=True, eq=False, slots=True)
class _MasterProbe:
    port: int


@dataclass(frozen=True, eq=False, slots=True)
class _SubnetProbe:
    port: int
    subnet: int


class _Priority(IntEnum):
    URGENT = 0
    LIVE = 1
    BULK = 2


class _PortQueue:
    def __init__(self, port: int) -> None:
        self.port = port
        self.jobs: OrderedDict[_Job, _Priority] = OrderedDict()
        self.ready = asyncio.Event()
        self.master_probe: _MasterProbe | None = None
        self.master_answer: NativeStatusBatch | None = None
        self.has_master_map = False
        self.master_subnets: tuple[int, ...] = ()
        self.subnet_jobs: dict[int, _SubnetProbe] = {}
        self.subnet_results: dict[int, NativeProbe] = {}

    def add(
        self,
        job: _Job,
        *,
        priority: _Priority,
    ) -> None:
        self.jobs[job] = priority
        if priority is _Priority.URGENT:
            self.jobs.move_to_end(job, last=False)
        self.ready.set()

    def request_master(self, *, urgent: bool = False) -> None:
        if self.master_probe is None and not self.has_master_map:
            self.master_probe = _MasterProbe(self.port)
            self.add(
                self.master_probe,
                priority=_Priority.URGENT if urgent else _Priority.BULK,
            )

    def reset_master(self) -> None:
        if self.master_probe is not None:
            self.jobs.pop(self.master_probe, None)
        self.master_probe = None
        self.master_answer = None
        self.has_master_map = False
        self.master_subnets = ()

    def request_subnet(self, subnet: int) -> None:
        if subnet not in self.subnet_jobs:
            job = self.subnet_jobs[subnet] = _SubnetProbe(self.port, subnet)
            self.add(job, priority=_Priority.URGENT)

    def reset_subnets(self) -> None:
        for job in self.subnet_jobs.values():
            self.jobs.pop(job, None)
        self.subnet_jobs.clear()
        self.subnet_results.clear()

    def discard(self, path: NativePath) -> None:
        self.jobs = OrderedDict(
            (job, rank)
            for job, rank in self.jobs.items()
            if not isinstance(job, tuple) or job[0] != path
        )

    async def take(self) -> _Job:
        while not self.jobs:
            self.ready.clear()
            await self.ready.wait()
        # Equal-priority work retains queue order.
        job = min(self.jobs, key=self.jobs.__getitem__)
        del self.jobs[job]
        return job


class _Watch:
    def __init__(
        self,
        paths: frozenset[NativePath] | None,
        ports: frozenset[int],
        *,
        active: bool,
        stale_after: float | None = None,
        on_change: Callable[[ClimateChange], None] | None,
        include_snow: bool = False,
        schedules: bool = False,
        thermostat_targets: bool = False,
        thermostat_sensors: bool = False,
        plant_settings: bool = False,
        setpoint_settings: bool = False,
        io_discovery: bool = False,
        io_endpoints: Mapping[NativePath, Iterable[IoEndpoint]] | None = None,
        check_port_subnets: bool = False,
        on_native: Callable[[NativePath, NativeState], None] | None = None,
    ) -> None:
        self.state = NativeState(paths)
        self.current: dict[NativePath, NativeClimate] = {}
        self.identities: dict[NativePath, NativeIdentity] = {}
        self.latest: dict[tuple[NativePath, int], NativeBlockBatch] = {}
        self.statuses: dict[tuple[NativePath, SnowRead], NativeStatusBatch] = {}
        self.io_reads: dict[tuple[NativePath, IoEndpoint], IoDirectoryRead] = {}
        self.io_discovery = io_discovery and active
        self.io_pending: set[tuple[NativePath, IoEndpoint]] = set()
        self.io_attempted: set[NativePath] = set()
        self.io_seeds = {
            path: frozenset(endpoints)
            for path, endpoints in (io_endpoints or {}).items()
        }
        self.capture_index = 0
        self.capture_advanced = asyncio.Event()
        self.reading_status: set[NativePath] = set()
        self.snow = SnowWatch(self.state, self._queue) if include_snow else None
        self.configuration = (
            ConfigurationWatch(
                self.state,
                self._queue,
                schedules=schedules,
                targets=thermostat_targets,
                sensors=thermostat_sensors,
                plants=plant_settings,
                setpoints=setpoint_settings,
            )
            if (
                schedules
                or thermostat_targets
                or thermostat_sensors
                or plant_settings
                or setpoint_settings
            )
            and active
            else None
        )
        self.reader = (
            self.configuration.reader
            if self.configuration is not None
            else ConfigurationReader(self.state)
            if active
            else None
        )
        self.on_native = on_native
        self.count = 0
        self.epochs: dict[NativePath, int] = {}
        self.retry_hints: dict[_DeviceJob, _ModeHint] = {}
        self.on_change = on_change
        self.queues = {p: _PortQueue(p) for p in sorted(ports)} if active else {}
        self.paths = paths
        self.ports = ports
        self.discover = active and paths is None
        self.discover_plant = self.discover and plant_settings
        self.check_port_subnets = self.discover and check_port_subnets
        self.discover_subnets = False
        self.enrolled: set[NativePath] = set()
        self.stale_after = stale_after
        self.deadlines: OrderedDict[NativePath, float] = OrderedDict()
        for p in sorted(paths or ()):
            if active:
                self._enroll(p)
        for path, endpoints in self.io_seeds.items():
            for endpoint in sorted(endpoints):
                self._queue_io(path, endpoint)

    def _queue(self, path: NativePath, block: _Read) -> None:
        if not path.node and not isinstance(block, IoEndpoint):
            return
        if (q := self.queues.get(path.port)) is not None:
            urgent = (
                block == HEATING_SETUP
                or isinstance(block, SnowRead)
                or (
                    isinstance(block, tuple)
                    and (
                        block[0] in (SETUP_BLOCK, MULTI_WEEK.start)
                        or block[0] in ADJUSTMENT_BLOCKS
                        or block[0] in SENSOR_BLOCKS
                        or block[0] in PLANT_REGISTERS
                        or block[0] in SETPOINT_BLOCKS
                    )
                    and self.state.block_revision(path, block) > 0
                )
            )
            if urgent:
                priority = _Priority.URGENT
            elif isinstance(block, IoEndpoint) or (
                isinstance(block, int) and (not block or block in MODE_BLOCKS)
            ):
                priority = _Priority.LIVE
            else:
                priority = _Priority.BULK
            q.add((path, block), priority=priority)

    def _invalidate(self, path: NativePath) -> None:
        self.identities.pop(path, None)
        if self.configuration is not None:
            self.configuration.discard(path)
        elif self.reader is not None:
            self.reader.discard(path)
        if self.snow is not None:
            self.snow.discard(path)
        if queue := self.queues.get(path.port):
            queue.discard(path)
            self.epochs[path] = self.epochs.get(path, 0) + 1
            for b in (0, HEATING_SETUP):
                self.retry_hints.pop((path, b), None)
            self.latest = {
                job: batch for job, batch in self.latest.items() if job[0] != path
            }
            self.io_attempted.discard(path)
            self.io_seeds.pop(path, None)
            self.io_pending = {k for k in self.io_pending if k[0] != path}
            self.statuses = {k: b for k, b in self.statuses.items() if k[0] != path}
            self.io_reads = {k: r for k, r in self.io_reads.items() if k[0] != path}

    def _enroll(self, path: NativePath) -> None:
        if path not in self.enrolled:
            self.enrolled.add(path)
            if (
                self.check_port_subnets
                and path.node
                and not self.state.port_subnets_checked(path.port)
            ):
                self.queues[path.port].request_master(urgent=True)
            if self.state.identity(path) is None:
                self._queue(path, 0)

    @property
    def next_expiration(self) -> float | None:
        return next(iter(self.deadlines.values()), None)

    def expire(self, now: float) -> None:
        while (deadline := self.next_expiration) is not None and deadline <= now:
            path = next(iter(self.deadlines))
            seen = self.state.forget(path)
            self._update_master_map(path.port)
            self._retire(path, notify=seen)
            if self.check_port_subnets and not any(
                p.port == path.port for p in self.state.paths
            ):
                self.queues[path.port].reset_master()

    def _update_master_map(self, port: int) -> None:
        if (self.discover_plant or self.check_port_subnets) and (
            queue := self.queues.get(port)
        ) is not None:
            present = self.state.has_master_announcement(port)
            subnets = self.state.master_subnets(port)
            lost_header = bool(queue.master_subnets) and not subnets
            if (queue.has_master_map and not present) or lost_header:
                queue.reset_master()
            queue.has_master_map = present
            queue.master_subnets = subnets
            if (
                self.check_port_subnets
                and lost_header
                and any(p.port == port and p.node for p in self.state.paths)
            ):
                queue.request_master(urgent=True)

    def _observe_plant(self, frame: NativeFrame) -> None:
        if self.discover_subnets and frame.message_id == MASTER_PRESENT:
            subnets = self.state.master_subnets(frame.port)
            if len(subnets) == 1:
                self.queues[frame.port].request_subnet(subnets[0])
        if not (self.discover_plant or self.check_port_subnets):
            return
        if frame.message_id in (MASTER_PRESENT, TAKING_ADDRESS) or (
            frame.message_id == BLOCK_REPORT
            and frame.message.payload.startswith(b"\0\0")
        ):
            self._update_master_map(frame.port)
        elif self.discover_plant and frame.message_id in (
            MASTER_STATUS,
            PLANT_LOOP_STATUS,
        ):
            queue = self.queues[frame.port]
            if (
                queue.master_probe is None
                and not queue.has_master_map
                and "truncated" not in frame.decoded
            ):
                queue.request_master(urgent=self.check_port_subnets)

    def _fresh(self, path: NativePath, now: float) -> None:
        if self.stale_after is not None:
            self.deadlines[path] = now + self.stale_after
            self.deadlines.move_to_end(path)

    def _retire(
        self,
        path: NativePath,
        *,
        reidentify: bool = False,
        notify: bool,
    ) -> None:
        self.enrolled.discard(path)
        self.deadlines.pop(path, None)
        self._invalidate(path)
        if self.current.pop(path, None) is not None and self.on_change is not None:
            self.on_change(ClimateChange(path, None))
        if reidentify and self.paths is not None:
            self._enroll(path)
        if notify and self.on_native is not None:
            self.on_native(path, self.state)

    def _updated(self, path: NativePath) -> None:
        before = self.current.get(path)
        current = self.state.climate(path)
        previous = self.identities.get(path)
        identity = self.state.identity(path)
        if previous is not None and (
            identity is None or identity.merge(previous) is None
        ):
            self._invalidate(path)
            if identity is None:
                self._queue(path, 0)
        if identity is not None:
            self.identities[path] = identity
        if current is not None:
            self.current[path] = current
            if before is None or before.identity != current.identity:
                for block in MODE_BLOCKS:
                    if (
                        block in current.setup.product.candidate_blocks
                        and self.state.block(path, block) is None
                    ):
                        self._queue(path, block)
            elif current.air is not None and (
                before.air is None or _mode_hint(before.air) != _mode_hint(current.air)
            ):
                self._queue(path, HEATING_SETUP)
        elif before is not None:
            self.current.pop(path, None)
        if self.on_change is not None and current != before:
            self.on_change(ClimateChange(path, current))
        if self.snow is not None and self.snow.changed(path):
            for read in SnowRead:
                self.statuses.pop((path, read), None)
        if self.configuration is not None:
            self.configuration.reconcile(path, current)
        if self.on_native is not None:
            self.on_native(path, self.state)

    def _local_io_candidate(self, path: NativePath) -> IoEndpoint | None:
        if path in self.io_attempted or self.io_seeds.get(path):
            return None
        endpoint = local_io_endpoint(self.state.block(path, 0))
        if endpoint is None or self.state.io_for(path):
            return None
        if not self.discover:
            return endpoint  # The caller selected this physical path.
        for route in self.state.device_routes(ports=self.ports):
            if route.address == path.address and (
                route.preferred == path
                or (
                    not route.claiming_ports
                    and route.paths == (path,)
                    and not self.state.master_subnets(path.port)
                )
            ):
                return endpoint
        return None

    def _queue_io(self, path: NativePath, endpoint: IoEndpoint) -> None:
        key = (path, endpoint)
        if key not in self.io_pending:
            self.io_pending.add(key)
            self._queue(path, endpoint)

    def _discover_local_io(self, sender: NativePath, frame: NativeFrame) -> None:
        if not self.io_discovery:
            return
        if self.discover and frame.message_id == MASTER_PRESENT:
            paths: Iterable[NativePath] = self.identities
        elif frame.message_id == BLOCK_REPORT and self.state.block(sender, 0) is frame:
            paths = (sender,)
        else:
            return
        for path in paths:
            if (endpoint := self._local_io_candidate(path)) is not None:
                self._queue_io(path, endpoint)

    def _capture_gap(
        self,
        record: CaptureGap,
        changed: tuple[NativePath, ...],
    ) -> None:
        if self.snow is not None:
            self.snow.capture_gap(record)
        for port, queue in self.queues.items():
            if record.affects(port):
                queue.reset_master()
                queue.reset_subnets()
        known = set(changed)
        for path in sorted(known | (self.paths or set()) | self.enrolled):
            if record.affects(path.port):
                self._retire(path, reidentify=True, notify=path in known)

    def observe(self, record: CaptureRecord, now: float) -> None:
        if isinstance(record, NativeFrame):
            self.capture_index = record.capture_index
            self.capture_advanced.set()
        elif isinstance(record, CaptureGap):
            self.capture_advanced.set()
        io = (
            record.io_report
            if self.io_discovery and isinstance(record, NativeFrame)
            else None
        )
        old_io = (
            self.state.io_directory(NativePath.from_frame(record), io.endpoint)
            if io is not None and isinstance(record, NativeFrame)
            else None
        )
        moved = isinstance(record, NativeFrame) and record.message_id == TAKING_ADDRESS
        prior_paths = set(self.state.paths) if moved else set()
        changed = (
            self.reader.observe(record)
            if self.reader is not None
            else self.state.observe(record)
        )
        if changed is None:
            return
        if isinstance(record, CaptureGap):
            self._capture_gap(record, changed)
            return
        if not isinstance(record, NativeFrame):
            return
        sender = NativePath.from_frame(record)
        self._observe_plant(record)
        if moved:
            for path in changed:
                self._retire(path, reidentify=True, notify=path in prior_paths)
            if self.discover and changed:
                self._enroll(sender)
                self._fresh(sender, now)
            return
        if sender in self.deadlines or (
            record.message_id != BLOCK_UPDATE and sender in changed
        ):
            self._fresh(sender, now)
        if record.message_id == BLOCK_UPDATE:
            # A block update names its destination, not its author. Only a
            # subsequent report can confirm that the destination applied it.
            data = record.message.payload
            if len(data) > _UPDATE_HEADER:
                block = int.from_bytes(data[2:4], "little")
                target = NativePath(record.port, data[0], data[1] & 31)
                if "truncated" not in record.decoded:
                    if target in self.current and block in MODE_BLOCKS:
                        self._queue(target, block)
                    if self.snow is not None:
                        self.snow.updated_block(target, block)
            for path in changed:
                self._updated(path)
            return
        for path in changed:
            if self.reader is not None:
                self._enroll(path)
            self._updated(path)
            if io is not None and (
                (path, io.endpoint) not in self.io_reads
                or old_io is None
                or not old_io.describes(io)
            ):
                self._queue_io(path, io.endpoint)
        self._discover_local_io(sender, record)
        if (
            self.configuration is not None
            and self.configuration.schedules
            and record.message_id == SCHEDULE_STATUS
            and sender.port in self.queues
            and (self.paths is None or sender in self.paths)
            and (status := record.schedule_status) is not None
        ):
            self.configuration.schedule_announced(
                sender,
                status,
                self.latest.get((sender, 0)),
            )
        if record.message_id in (AIR_MEMBER, AIR_MASTER):
            self._retry_on_air(record)
        if self.snow is not None and record.port in self.queues:
            idle_status = sender not in self.reading_status
            groups = self.statuses.get((sender, SnowRead.GROUPS))
            self.snow.observe(
                record,
                self.latest,
                attempted=idle_status and (sender, SnowRead.MISSING) in self.statuses,
                group_failed=idle_status
                and groups is not None
                and not groups.answered
                and not groups.transmit_pending,
            )

    def _retry_on_air(self, frame: NativeFrame) -> None:
        path = NativePath.from_frame(frame)
        if path.port not in self.queues or self.state.air_frame(path) is not frame:
            return
        block = 0 if self.state.identity(path) is None else HEATING_SETUP
        batch = self.latest.get((path, block))
        if batch is None or batch.reports or (block and path not in self.current):
            return
        if (air := self.state.air_status(path)) is not None:
            hint = _mode_hint(air)
            if self.retry_hints.get((path, block)) != hint:
                self.retry_hints[path, block] = hint
                self._queue(path, block)

    async def _wait_io_evidence(
        self,
        path: NativePath,
        epoch: int,
        answer: NativeStatusBatch,
    ) -> bool:
        last = max((f.capture_index for f in answer.reports), default=0)
        while last > self.capture_index and epoch == self.epochs.get(path, 0):
            self.capture_advanced.clear()
            await self.capture_advanced.wait()
        return epoch == self.epochs.get(path, 0)

    async def _read_io(
        self,
        stream: BridgeStream | OrderedNormalReads,
        path: NativePath,
        endpoint: IoEndpoint,
        before: IoDirectory | None,
        epoch: int,
        source: NativeAddress,
        timeout: float,
    ) -> TxResult | None:
        async def request(query: NativeStatusQuery) -> NativeStatusBatch:
            return await stream.read_status(
                path.port,
                query,
                source=source,
                peer=path.address,
                timeout=timeout,
                wait_for_edits=True,
            )

        async def supplemental(query: NativeStatusQuery) -> NativeStatusBatch | None:
            if epoch != self.epochs.get(path, 0):
                return None
            try:
                batch = await request(query)
            except DeferredReadError:
                return None  # Don't replay the already admitted directory.
            except ExpiredReadError:
                self.count += 1
                return None
            self.count += 1
            await self._wait_io_evidence(path, epoch, batch)
            return batch

        answer = await request(NativeStatusQuery.io(endpoint.subnet, endpoint.node))
        if not await self._wait_io_evidence(path, epoch, answer):
            return answer.tx_result
        self.io_attempted.add(path)
        previous = self.io_reads.get((path, endpoint))
        read = await recover_io_directory(
            answer,
            supplemental,
            before=previous.directory if previous else before,
            identity=self.state.identity(path),
        )
        if epoch == self.epochs.get(path, 0):
            self.io_pending.discard((path, endpoint))
            self.io_reads[path, endpoint] = read
            changed = self.state.reconcile_io(
                answer,
                before=before,
                supplemental=read.supplemental,
            )
            if changed and self.on_native:
                self.on_native(path, self.state)
        return (
            read.supplemental[-1].tx_result if read.supplemental else answer.tx_result
        )

    async def probe(
        self,
        stream: BridgeStream | OrderedNormalReads,
        queue: _PortQueue,
        source: NativeAddress,
        timeout: float,
    ) -> None:
        if (reader := self.reader) is None:
            msg = "a passive watcher cannot probe a port"
            raise RuntimeError(msg)
        while True:
            job = await queue.take()
            if isinstance(job, _MasterProbe):
                await self._probe_master(stream, queue, job, source, timeout)
                continue
            if isinstance(job, _SubnetProbe):
                if isinstance(stream, OrderedNormalReads):
                    await self._probe_subnet(stream, queue, job, timeout)
                continue
            path, block = job
            epoch = self.epochs.get(path, 0)
            try:
                await self._probe_one(
                    stream,
                    reader,
                    path,
                    block,
                    epoch,
                    source,
                    timeout,
                )
            except DeferredReadError:
                if epoch == self.epochs.get(path, 0):
                    self._queue(path, block)
            except ExpiredReadError:
                self.count += 1
                if isinstance(block, IoEndpoint) and epoch == self.epochs.get(path, 0):
                    self.io_attempted.add(path)
                    self.io_pending.discard((path, block))

    async def _probe_master(
        self,
        stream: BridgeStream | OrderedNormalReads,
        queue: _PortQueue,
        job: _MasterProbe,
        source: NativeAddress,
        timeout: float,
    ) -> None:
        if queue.master_probe is not job or queue.has_master_map:
            return
        try:
            answer = await stream.read_status(
                job.port,
                _MASTER_QUERY,
                source=source,
                timeout=timeout,
                wait_for_edits=True,
            )
        except DeferredReadError:
            if queue.master_probe is job:
                queue.add(
                    job,
                    priority=(
                        _Priority.URGENT if self.check_port_subnets else _Priority.BULK
                    ),
                )
            return
        except ExpiredReadError:
            self.count += 1
            return
        self.count += 1
        if queue.master_probe is job:
            queue.master_answer = answer
            # Only the peer capture establishes a subnet, not an inline reply.
            if (
                self.check_port_subnets
                and self.state.record_master_query(answer)
                and self.on_native is not None
            ):
                for path in self.state.paths:
                    if path.port == job.port:
                        self.on_native(path, self.state)
        if answer.tx_result is None and not isinstance(stream, OrderedNormalReads):
            await stream.wait_transmit_idle(job.port)

    async def _probe_subnet(
        self,
        stream: OrderedNormalReads,
        queue: _PortQueue,
        job: _SubnetProbe,
        timeout: float,
    ) -> None:
        if queue.subnet_jobs.get(job.subnet) is not job:
            return
        if self.state.master_subnets(job.port) != (job.subnet,):
            del queue.subnet_jobs[job.subnet]
            return
        try:
            outcome = await stream.discover_subnet(
                job.port,
                job.subnet,
                timeout=timeout,
            )
        except DeferredReadError:
            if queue.subnet_jobs.get(job.subnet) is job:
                queue.add(job, priority=_Priority.URGENT)
            return
        except ExpiredReadError:
            outcome = None
        self.count += 1
        if queue.subnet_jobs.get(job.subnet) is job:
            # A link result cannot add any devices to observed state.
            queue.subnet_results[job.subnet] = NativeProbe(
                job.port,
                job.subnet,
                outcome,
            )

    async def _probe_one(
        self,
        stream: BridgeStream | OrderedNormalReads,
        reader: ConfigurationReader,
        path: NativePath,
        block: _Read,
        epoch: int,
        source: NativeAddress,
        timeout: float,
    ) -> None:
        if isinstance(block, tuple):
            if self.configuration is None:
                return
            observation = await self.configuration.read(
                stream,
                path,
                block,
                source=source,
                timeout=timeout,
            )
            if observation is None:
                return
            result = observation.tx_result
        elif isinstance(block, IoEndpoint):
            before = self.state.io_directory(path, block)
            if (
                before is None
                and block not in self.io_seeds.get(path, ())
                and self._local_io_candidate(path) != block
            ):
                self.io_pending.discard((path, block))
                return
            result = await self._read_io(
                stream,
                path,
                block,
                before,
                epoch,
                source,
                timeout,
            )
        elif isinstance(block, SnowRead):
            if self.snow is None or (query := self.snow.query(path, block)) is None:
                return
            route = self.snow.routes.get(path)
            self.reading_status.add(path)
            try:
                answer = await stream.read_status(
                    path.port,
                    query,
                    source=source,
                    peer=path.address,
                    timeout=timeout,
                    wait_for_edits=True,
                )
            finally:
                self.reading_status.discard(path)
            if epoch == self.epochs.get(path, 0) and route == self.snow.routes.get(
                path,
            ):
                self.statuses[path, block] = answer
            result = answer.tx_result
        else:
            if not block and self.discover and self.state.identity(path) is not None:
                return  # Already identified before the queued read got the line.
            if (
                block
                and path not in self.current
                and (self.snow is None or path not in self.snow.current)
            ):
                return
            if self.snow is not None and not self.snow.needs_block(path, block):
                return
            key = (block, None)
            batch, current = await reader.read(
                stream,
                path,
                key,
                self.state.block_revision(path, key),
                source=source,
                timeout=timeout,
            )
            if current and epoch == self.epochs.get(path, 0):
                self.latest[path, block] = batch
            result = batch.tx_result
        self.count += 1
        if result is None and not isinstance(stream, OrderedNormalReads):
            await stream.wait_transmit_idle(path.port)


async def _run_watch(
    stream: ObservationStream,
    watch: _Watch,
    *,
    source: NativeAddress | None,
    duration: float | None,
    timeout: float,
    on_record: Callable[[CaptureRecord], None] | None,
) -> None:
    selected = watch.paths
    if (
        source is not None
        and not watch.discover
        and (
            not selected
            or (not watch.io_discovery and any(not p.node for p in selected))
        )
    ):
        msg = "active observation requires explicit addressable device paths"
        raise ValueError(msg)
    if (
        not isfinite(timeout)
        or timeout <= 0
        or (duration is not None and (not isfinite(duration) or duration <= 0))
    ):
        msg = "expected finite positive response timeout and observation duration"
        raise ValueError(msg)

    capture, reader = await observation(
        stream,
        watch.ports,
        source,
    )
    watch.discover_subnets = (
        watch.discover
        and isinstance(reader, OrderedNormalReads)
        and BridgeFeature.NORMAL_DISCOVER in reader.features
    )
    watch.check_port_subnets &= isinstance(reader, OrderedNormalReads)

    async def receive() -> None:
        loop = asyncio.get_running_loop()
        while True:
            expiry = asyncio.timeout_at(watch.next_expiration)
            try:
                async with expiry:
                    record = await capture.read()
            except TimeoutError:
                if not expiry.expired():
                    raise
                watch.expire(loop.time())
                continue
            now = loop.time()
            watch.expire(now)
            if (
                watch.paths is None
                and not isinstance(record, CaptureGap)
                and (record.port not in watch.ports)
            ):
                continue
            if on_record is not None:
                on_record(record)
            watch.observe(record, now)

    tasks = [asyncio.create_task(receive())]
    if source is not None and reader is not None:
        tasks.extend(
            asyncio.create_task(watch.probe(reader, q, source, timeout))
            for q in watch.queues.values()
        )
    try:
        done, _ = await asyncio.wait(
            tasks,
            timeout=duration,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in done:
            task.result()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def watch_native(
    stream: ObservationStream,
    paths: Iterable[NativePath] | None = None,
    *,
    ports: Iterable[int] | None = None,
    source: NativeAddress | None = _HOST,
    duration: float | None = None,
    timeout: float = 20,
    stale_after: float | None = None,
    schedules: bool = True,
    thermostat_targets: bool = True,
    thermostat_sensors: bool = True,
    plant_settings: bool = True,
    setpoint_settings: bool = True,
    io_discovery: bool = True,
    io_endpoints: Mapping[NativePath, Iterable[IoEndpoint]] | None = None,
    check_port_subnets: bool = False,
    on_change: Callable[[NativePath, NativeState], None] | None = None,
    on_record: Callable[[CaptureRecord], None] | None = None,
) -> NativeWatchResult:
    """Discover reporting peers and read their configuration, or follow explicit paths.

    Without explicit paths, observe ports selected by the caller or capture.
    Synchronous callbacks may inspect immutable snapshots in the current state.
    Passive mode (source=None) never sends. Use a new watch on each connection.
    Active Normal watches without explicit paths also probe each observed
    unambiguous master subnet once when the gateway supports it.
    With check_port_subnets, query the master once per observed port that
    has addressable peers but no subnet evidence yet.
    Known I/O endpoints for explicit paths can seed one directory read each
    at startup, including for sources with node zero. An address move or
    capture gap invalidates the old observation instead of replaying the hint.
    An optional stale_after timeout marks a silent path unknown without sending.
    Each Normal worker waits for its own occupied slot; an admitted read is
    never replayed after a timeout.
    """
    selected = None if paths is None else frozenset(paths)
    if selected is not None and ports is not None:
        msg = "select device paths or discovery ports, not both"
        raise ValueError(msg)
    if io_endpoints and (
        source is None
        or not io_discovery
        or selected is None
        or not io_endpoints.keys() <= selected
    ):
        msg = "initial I/O endpoints require an active watch of their device paths"
        raise ValueError(msg)
    if stale_after is not None and (not isfinite(stale_after) or stale_after <= 0):
        msg = "expected a finite positive inactivity interval"
        raise ValueError(msg)
    watch = _Watch(
        selected,
        _listen_ports(stream, ports)
        if selected is None
        else frozenset(p.port for p in selected),
        active=source is not None,
        stale_after=stale_after,
        on_change=None,
        include_snow=True,
        schedules=schedules,
        thermostat_targets=thermostat_targets,
        thermostat_sensors=thermostat_sensors,
        plant_settings=plant_settings,
        setpoint_settings=setpoint_settings,
        io_discovery=io_discovery,
        io_endpoints=io_endpoints,
        check_port_subnets=check_port_subnets,
        on_native=on_change,
    )
    await _run_watch(
        stream,
        watch,
        source=source,
        duration=duration,
        timeout=timeout,
        on_record=on_record,
    )
    return NativeWatchResult(
        watch.state,
        watch.count,
        (
            *watch.latest.values(),
            *(watch.configuration.latest.values() if watch.configuration else ()),
        ),
        (
            *watch.statuses.values(),
            *(
                b
                for read in watch.io_reads.values()
                for b in (read.discovery, *read.supplemental)
            ),
            *(
                q.master_answer
                for q in watch.queues.values()
                if q.master_answer is not None
            ),
        ),
        tuple(
            probe for q in watch.queues.values() for probe in q.subnet_results.values()
        ),
    )


async def watch_climate(
    stream: ObservationStream,
    paths: Iterable[NativePath] | None = None,
    *,
    source: NativeAddress | None = _HOST,
    duration: float | None = None,
    timeout: float = 20,
    on_change: Callable[[ClimateChange], None] | None = None,
    on_record: Callable[[CaptureRecord], None] | None = None,
) -> ClimateWatchResult:
    """Own capture until deadline or cancellation; coordinate reads with verified edits.

    Only explicit active paths are probed. A passive watcher sends nothing.
    Callbacks run in arrival order and must not block. Only the last read per
    path/block is retained; no retry is triggered by silence or a link ACK.
    """
    selected = None if paths is None else frozenset(paths)
    watch = _Watch(
        selected,
        _listen_ports(stream, None)
        if selected is None
        else frozenset(p.port for p in selected),
        active=source is not None,
        on_change=on_change,
    )
    await _run_watch(
        stream,
        watch,
        source=source,
        duration=duration,
        timeout=timeout,
        on_record=on_record,
    )
    thermostats = watch.state.thermostats
    expected = set(selected) if selected is not None else {t.path for t in thermostats}
    confirmed = {t.path for t in thermostats if t.setup.mode_raw is not None}
    return ClimateWatchResult(
        thermostats,
        watch.count,
        tuple(watch.latest.values()),
        tuple(sorted(expected - confirmed)),
        watch.state.dropped,
    )
