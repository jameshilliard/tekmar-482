"""Continuous native thermostat observations with optional event-driven setup reads."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from math import isfinite
from typing import TYPE_CHECKING

from ._native_payloads.climate import AIR_MASTER, AIR_MEMBER, HEATING_SETUP, MODE_BLOCKS
from .native import NativeAddress, NativeBlockQuery, NativeFrame
from .native_state import TAKING_ADDRESS, NativePath, NativeState
from .raw import CaptureGap

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from ._native_payloads.climate import AirReading, AirStatus
    from .native_state import NativeClimate
    from .raw import BridgeStream, CaptureRecord, NativeBlockBatch

_HOST = NativeAddress(0, 0)
_UPDATE = 0x2DF
_UPDATE_HEADER = 4
type _Job = tuple[NativePath, int]
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


class _PortQueue:
    def __init__(self) -> None:
        self.jobs: dict[_Job, None] = {}
        self.ready = asyncio.Event()

    def add(self, path: NativePath, block: int) -> None:
        self.jobs[path, block] = None
        self.ready.set()

    def discard(self, path: NativePath) -> None:
        self.jobs = {job: None for job in self.jobs if job[0] != path}

    async def take(self) -> _Job:
        while not self.jobs:
            self.ready.clear()
            await self.ready.wait()
        job = next(iter(self.jobs))
        del self.jobs[job]
        return job


class _Watch:
    def __init__(
        self,
        paths: frozenset[NativePath] | None,
        *,
        active: bool,
        on_change: Callable[[ClimateChange], None] | None,
    ) -> None:
        self.state = NativeState(paths)
        self.current: dict[NativePath, NativeClimate] = {}
        self.latest: dict[_Job, NativeBlockBatch] = {}
        self.count = 0
        self.epochs: dict[NativePath, int] = {}
        self.retry_hints: dict[_Job, _ModeHint] = {}
        self.on_change = on_change
        self.queues = {p.port: _PortQueue() for p in paths or ()} if active else {}
        self.paths = paths
        for p in sorted(paths or ()):
            self._queue(p, 0)

    def _queue(self, path: NativePath, block: int) -> None:
        if (q := self.queues.get(path.port)) is not None:
            q.add(path, block)

    def _invalidate(self, path: NativePath) -> None:
        if queue := self.queues.get(path.port):
            queue.discard(path)
            self.epochs[path] = self.epochs.get(path, 0) + 1
            for b in (0, HEATING_SETUP):
                self.retry_hints.pop((path, b), None)
            self.latest = {
                job: batch for job, batch in self.latest.items() if job[0] != path
            }

    def _updated(self, path: NativePath) -> None:
        before = self.current.get(path)
        current = self.state.climate(path)
        if current and before and current.identity.merge(before.identity) is None:
            self._invalidate(path)
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
            self._invalidate(path)
            self.current.pop(path, None)
        if self.on_change is not None and current != before:
            self.on_change(ClimateChange(path, current))

    def observe(self, record: CaptureRecord) -> None:
        changed = self.state.observe(record)
        if isinstance(record, CaptureGap) or (
            isinstance(record, NativeFrame) and record.message_id == TAKING_ADDRESS
        ):
            paths = (
                self.paths if isinstance(record, CaptureGap) and self.paths else changed
            )
            for path in paths:
                self._invalidate(path)
                if (
                    self.current.pop(path, None) is not None
                    and self.on_change is not None
                ):
                    self.on_change(ClimateChange(path, None))
                self._queue(path, 0)
            return
        if isinstance(record, NativeFrame) and record.message_id == _UPDATE:
            # A block update names its destination, not its author. Only a
            # subsequent report can confirm that the destination applied it.
            data = record.message.payload
            if len(data) > _UPDATE_HEADER:
                block = int.from_bytes(data[2:4], "little")
                target = NativePath(record.port, data[0], data[1] & 31)
                if (
                    target in self.current
                    and block in MODE_BLOCKS
                    and "truncated" not in record.decoded
                ):
                    self._queue(target, block)
            return
        for path in changed:
            self._updated(path)
        if isinstance(record, NativeFrame) and record.message_id in (
            AIR_MEMBER,
            AIR_MASTER,
        ):
            self._retry_on_air(record)

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
            if block and path not in self.current:
                continue
            epoch = self.epochs.get(path, 0)
            batch = await stream.read_block_group(
                path.port,
                NativeBlockQuery(path.address, block),
                source=source,
                timeout=timeout,
                wait_for_edits=True,
            )
            self.count += 1
            if epoch == self.epochs.get(path, 0):
                self.latest[path, block] = batch
            if batch.tx_result is None:
                await stream.wait_transmit_idle(path.port)


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
    if source is not None and (not selected or any(not p.node for p in selected)):
        msg = "active climate observation requires explicit addressable device paths"
        raise ValueError(msg)
    if (
        not isfinite(timeout)
        or timeout <= 0
        or (duration is not None and (not isfinite(duration) or duration <= 0))
    ):
        msg = "expected finite positive response timeout and observation duration"
        raise ValueError(msg)
    watch = _Watch(selected, active=source is not None, on_change=on_change)

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
