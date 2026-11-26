"""One native capture reader with event-driven reads on independent physical ports."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from dataclasses import dataclass
from math import isfinite
from typing import TYPE_CHECKING

from ._configuration_watch import ConfigurationWatch
from ._native_payloads.climate import AIR_MASTER, AIR_MEMBER, HEATING_SETUP, MODE_BLOCKS
from ._native_payloads.io import IoEndpoint
from ._native_payloads.layouts import MULTI_WEEK
from ._native_payloads.queries import NativeStatusQuery
from ._native_payloads.schedules import SCHEDULE_STATUS, SETUP_BLOCK
from ._snow_watch import SnowRead, SnowWatch
from .native import NativeAddress, NativeBlockQuery, NativeFrame
from .native_plant_registers import PLANT_REGISTERS
from .native_setpoint import SETPOINT_BLOCKS
from .native_state import BLOCK_UPDATE, TAKING_ADDRESS, NativePath, NativeState
from .native_targets import ADJUSTMENT_BLOCKS
from .raw import CaptureGap

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from ._native_payloads.climate import AirReading, AirStatus
    from .native import BlockKey
    from .native_identity import NativeIdentity
    from .native_state import NativeClimate
    from .native_status import NativeStatusBatch
    from .raw import BridgeStream, CaptureRecord, NativeBlockBatch

_HOST = NativeAddress(0, 0)
_UPDATE_HEADER = 4
type _Read = int | SnowRead | BlockKey | IoEndpoint
type _Job = tuple[NativePath, _Read]
type _ChannelHint = tuple[int, int] | None
type _ModeHint = tuple[int, _ChannelHint, _ChannelHint]


def _mode_hint(air: AirStatus) -> _ModeHint:
    def side(reading: AirReading | None) -> _ChannelHint:
        # Enable, demand, setback and active target can change with a local
        # adjustment. Temperature drift and the master's cycle counter cannot
        # establish a new setting and would turn a watcher into a poller.
        return (reading.flags & 31, reading.target_raw) if reading is not None else None

    return air.group, side(air.heat), side(air.cool)


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


class _PortQueue:
    def __init__(self) -> None:
        self.jobs: OrderedDict[_Job, None] = OrderedDict()
        self.ready = asyncio.Event()

    def add(
        self,
        path: NativePath,
        block: _Read,
        *,
        urgent: bool,
    ) -> None:
        self.jobs[path, block] = None
        if urgent:
            self.jobs.move_to_end((path, block), last=False)
        self.ready.set()

    def discard(self, path: NativePath) -> None:
        self.jobs = OrderedDict.fromkeys(job for job in self.jobs if job[0] != path)

    async def take(self) -> _Job:
        while not self.jobs:
            self.ready.clear()
            await self.ready.wait()
        return self.jobs.popitem(last=False)[0]


class _Watch:
    def __init__(
        self,
        paths: frozenset[NativePath] | None,
        *,
        active: bool,
        on_change: Callable[[ClimateChange], None] | None,
        include_snow: bool = False,
        schedules: bool = False,
        thermostat_targets: bool = False,
        plant_settings: bool = False,
        setpoint_settings: bool = False,
        io_discovery: bool = False,
        on_native: Callable[[NativePath, NativeState], None] | None = None,
    ) -> None:
        self.state = NativeState(paths)
        self.current: dict[NativePath, NativeClimate] = {}
        self.identities: dict[NativePath, NativeIdentity] = {}
        self.latest: dict[tuple[NativePath, int], NativeBlockBatch] = {}
        self.statuses: dict[
            tuple[NativePath, SnowRead | IoEndpoint],
            NativeStatusBatch,
        ] = {}
        self.io_discovery = io_discovery and active
        self.io_pending: set[tuple[NativePath, IoEndpoint]] = set()
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
                plants=plant_settings,
                setpoints=setpoint_settings,
            )
            if (schedules or thermostat_targets or plant_settings or setpoint_settings)
            and active
            else None
        )
        self.on_native = on_native
        self.count = 0
        self.epochs: dict[NativePath, int] = {}
        self.retry_hints: dict[_Job, _ModeHint] = {}
        self.on_change = on_change
        self.queues = {p.port: _PortQueue() for p in paths or ()} if active else {}
        self.paths = paths
        for p in sorted(paths or ()):
            self._queue(p, 0)

    def _queue(self, path: NativePath, block: _Read) -> None:
        if not path.node and not isinstance(block, IoEndpoint):
            return
        if (q := self.queues.get(path.port)) is not None:
            urgent = isinstance(block, SnowRead) or (
                isinstance(block, tuple)
                and (
                    block[0] in (SETUP_BLOCK, MULTI_WEEK.start)
                    or block[0] in ADJUSTMENT_BLOCKS
                    or block[0] in PLANT_REGISTERS
                    or block[0] in SETPOINT_BLOCKS
                )
                and self.state.block_revision(path, block) > 0
            )
            q.add(path, block, urgent=urgent)

    def _invalidate(self, path: NativePath) -> None:
        self.identities.pop(path, None)
        if self.configuration is not None:
            self.configuration.discard(path)
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
            self.io_pending = {k for k in self.io_pending if k[0] != path}
            self.statuses = {k: b for k, b in self.statuses.items() if k[0] != path}

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

    def observe(self, record: CaptureRecord) -> None:
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
        changed = (
            self.configuration.reader.observe(record)
            if self.configuration is not None
            else self.state.observe(record)
        )
        if changed is None:
            return
        if isinstance(record, CaptureGap) or (
            isinstance(record, NativeFrame) and record.message_id == TAKING_ADDRESS
        ):
            paths = (
                self.paths if isinstance(record, CaptureGap) and self.paths else changed
            )
            if self.snow is not None and (paths or isinstance(record, CaptureGap)):
                self.snow.clear_hints()
            for path in paths:
                self._invalidate(path)
                if (
                    self.current.pop(path, None) is not None
                    and self.on_change is not None
                ):
                    self.on_change(ClimateChange(path, None))
                self._queue(path, 0)
                if self.on_native is not None:
                    self.on_native(path, self.state)
            return
        if isinstance(record, NativeFrame) and record.message_id == BLOCK_UPDATE:
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
            self._updated(path)
            if io is not None:
                key = (path, io.endpoint)
                if key not in self.io_pending and (
                    key not in self.statuses
                    or old_io is None
                    or not old_io.describes(io)
                ):
                    self.io_pending.add(key)
                    self._queue(path, io.endpoint)
        if (
            self.configuration is not None
            and self.configuration.schedules
            and isinstance(record, NativeFrame)
            and record.message_id == SCHEDULE_STATUS
            and self.paths
            and (path := NativePath.from_frame(record)) in self.paths
            and (status := record.schedule_status) is not None
        ):
            self.configuration.schedule_announced(
                path,
                status,
                self.latest.get((path, 0)),
            )
        if isinstance(record, NativeFrame) and record.message_id in (
            AIR_MEMBER,
            AIR_MASTER,
        ):
            self._retry_on_air(record)
        if (
            self.snow is not None
            and isinstance(record, NativeFrame)
            and record.port in self.queues
        ):
            sender = NativePath.from_frame(record)
            self.snow.observe(
                record,
                self.latest,
                attempted=sender not in self.reading_status
                and (sender, SnowRead.MISSING) in self.statuses,
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

    async def probe(
        self,
        stream: BridgeStream,
        queue: _PortQueue,
        source: NativeAddress,
        timeout: float,
    ) -> None:
        while True:
            path, block = await queue.take()
            epoch = self.epochs.get(path, 0)
            if isinstance(block, tuple):
                if self.configuration is None:
                    continue
                observation = await self.configuration.read(
                    stream,
                    path,
                    block,
                    source=source,
                    timeout=timeout,
                )
                if observation is None:
                    continue
                result = observation.tx_result
            elif isinstance(block, IoEndpoint):
                before = self.state.io_directory(path, block)
                if before is None:
                    self.io_pending.discard((path, block))
                    continue
                answer = await stream.read_status(
                    path.port,
                    NativeStatusQuery.io(block.subnet, block.node),
                    source=source,
                    peer=path.address,
                    timeout=timeout,
                    wait_for_edits=True,
                )
                # The collector can wake before our sole capture consumer.
                # Apply membership only after all its evidence passed that
                # consumer, or discard the result on a gap or reassignment.
                last = max((f.capture_index for f in answer.reports), default=0)
                while last > self.capture_index and epoch == self.epochs.get(path, 0):
                    self.capture_advanced.clear()
                    await self.capture_advanced.wait()
                if epoch == self.epochs.get(path, 0):
                    self.io_pending.discard((path, block))
                    self.statuses[path, block] = answer
                    if (
                        self.state.reconcile_io(answer, before=before)
                        and self.on_native
                    ):
                        self.on_native(path, self.state)
                result = answer.tx_result
            elif isinstance(block, SnowRead):
                if self.snow is None or (query := self.snow.query(path, block)) is None:
                    continue
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
                if (
                    block
                    and path not in self.current
                    and (self.snow is None or path not in self.snow.current)
                ):
                    continue
                batch = await stream.read_block_group(
                    path.port,
                    NativeBlockQuery(path.address, block),
                    source=source,
                    timeout=timeout,
                    wait_for_edits=True,
                )
                if epoch == self.epochs.get(path, 0):
                    self.latest[path, block] = batch
                result = batch.tx_result
            self.count += 1
            if result is None:
                await stream.wait_transmit_idle(path.port)


async def _run_watch(
    stream: BridgeStream,
    watch: _Watch,
    *,
    source: NativeAddress | None,
    duration: float | None,
    timeout: float,
    on_record: Callable[[CaptureRecord], None] | None,
) -> None:
    selected = watch.paths
    if source is not None and (
        not selected or (not watch.io_discovery and any(not p.node for p in selected))
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

    async def receive() -> None:
        while True:
            record = await stream.read()
            if on_record is not None:
                on_record(record)
            watch.observe(record)

    tasks = [asyncio.create_task(receive())]
    if source is not None:
        tasks.extend(
            asyncio.create_task(watch.probe(stream, q, source, timeout))
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
    stream: BridgeStream,
    paths: Iterable[NativePath] | None = None,
    *,
    source: NativeAddress | None = _HOST,
    duration: float | None = None,
    timeout: float = 20,
    schedules: bool = True,
    thermostat_targets: bool = True,
    plant_settings: bool = True,
    setpoint_settings: bool = True,
    io_discovery: bool = True,
    on_change: Callable[[NativePath, NativeState], None] | None = None,
    on_record: Callable[[CaptureRecord], None] | None = None,
) -> NativeWatchResult:
    """Observe selected paths and read configuration for identified devices.

    Synchronous callbacks may inspect immutable snapshots in the current state.
    Passive mode (source=None) never sends. Use a new watch on each connection.
    """
    selected = None if paths is None else frozenset(paths)
    watch = _Watch(
        selected,
        active=source is not None,
        on_change=None,
        include_snow=True,
        schedules=schedules,
        thermostat_targets=thermostat_targets,
        plant_settings=plant_settings,
        setpoint_settings=setpoint_settings,
        io_discovery=io_discovery,
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
        tuple(watch.statuses.values()),
    )


async def watch_climate(
    stream: BridgeStream,
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
    watch = _Watch(selected, active=source is not None, on_change=on_change)
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
