"""Coalesce configuration reads by device, selector and configuration generation."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ._native_payloads.layouts import MULTI_WEEK
from ._native_payloads.schedules import SETUP_BLOCK
from .native import NativeBlockQuery, NativeFrame
from .native_state import BLOCK_REPORT, TAKING_ADDRESS, NativePath
from .raw import CaptureGap

if TYPE_CHECKING:
    from collections.abc import Callable

    from ._native_payloads.schedules import ScheduleStatus
    from .native import BlockKey, NativeAddress
    from .native_state import NativeClimate, NativeState
    from .raw import BridgeStream, CaptureRecord, NativeBlockBatch


@dataclass(slots=True)
class _Pending:
    path: NativePath
    query: NativeBlockQuery
    revision: int
    seen: asyncio.Event = field(default_factory=asyncio.Event)
    retired: bool = False


class ConfigurationReader:
    """Keep the collector and state observer in wire order across device changes."""

    def __init__(self, state: NativeState) -> None:
        self.state = state
        self.pending: dict[int, _Pending] = {}

    def discard(self, path: NativePath) -> None:
        if (p := self.pending.get(path.port)) is not None and p.path == path:
            p.retired = True
            p.seen.set()

    def discard_pending(self) -> None:
        for pending in self.pending.values():
            self.discard(pending.path)

    def observe(self, record: CaptureRecord) -> tuple[NativePath, ...] | None:
        if isinstance(record, NativeFrame):
            p = self.pending.get(record.port)
            if p is not None and p.query.matches(record.message):
                p.seen.set()
                if not self._current(p):
                    return (
                        None  # Still delivered to raw capture, never to device state.
                    )
        identity_path = (
            NativePath.from_frame(record)
            if isinstance(record, NativeFrame)
            and record.message_id == BLOCK_REPORT
            and record.message.payload.startswith(b"\0\0")
            else None
        )
        old = self.state.identity(identity_path) if identity_path else None
        changed = self.state.observe(record)
        if isinstance(record, CaptureGap):
            self.discard_pending()
        elif isinstance(record, NativeFrame) and record.message_id == TAKING_ADDRESS:
            for path in changed:
                self.discard(path)
        elif old is not None and identity_path is not None:
            current = self.state.identity(identity_path)
            if current is None or current.merge(old) is None:
                self.discard(identity_path)
        return changed

    def _current(self, p: _Pending) -> bool:
        key = p.query.block_id, p.query.selector
        return not p.retired and self.state.block_revision(p.path, key) == p.revision

    async def read(
        self,
        stream: BridgeStream,
        path: NativePath,
        key: BlockKey,
        revision: int,
        *,
        source: NativeAddress,
        timeout: float,
    ) -> tuple[NativeBlockBatch, bool]:
        p = _Pending(path, NativeBlockQuery(path.address, *key), revision)
        self.pending[path.port] = p
        try:
            batch = await stream.read_block_group(
                path.port,
                p.query,
                source=source,
                timeout=timeout,
                wait_for_edits=True,
            )
            if batch.reports:
                await p.seen.wait()
            return batch, self._current(p)
        finally:
            del self.pending[path.port]


class ConfigurationWatch:
    def __init__(
        self,
        state: NativeState,
        queue: Callable[[NativePath, int | BlockKey], None],
        *,
        schedules: bool,
        targets: bool,
        plants: bool,
    ) -> None:
        self.state = state
        self.queue = queue
        self.schedules = schedules
        self.targets = targets
        self.plants = plants
        self.wanted: dict[NativePath, dict[BlockKey, int]] = {}
        self.planned: dict[tuple[NativePath, BlockKey], int] = {}
        self.latest: dict[tuple[NativePath, BlockKey], NativeBlockBatch] = {}
        self.reader = ConfigurationReader(state)
        self.roles: dict[tuple[NativePath, int], int] = {}

    def discard(self, path: NativePath) -> None:
        self.wanted.pop(path, None)
        self.planned = {k: v for k, v in self.planned.items() if k[0] != path}
        self.latest = {k: v for k, v in self.latest.items() if k[0] != path}
        self.roles = {k: v for k, v in self.roles.items() if k[0] != path}
        self.reader.discard(path)

    def reconcile(self, path: NativePath, climate: NativeClimate | None) -> None:
        missing = self.state.missing_schedule_reads(path) if self.schedules else {}
        if self.plants:
            missing.update(self.state.missing_plant_reads(path))
        if self.targets and path.node and climate is not None:
            for b in climate.adjustments.missing_blocks:
                missing[b, None] = self.state.block_revision(path, (b, None))
        self.wanted[path] = missing
        for key, revision in missing.items():
            job = path, key
            if self.planned.get(job) != revision:
                self.planned[job] = revision
                self.latest.pop(job, None)
                self.queue(path, key)
        for job in tuple(self.latest):
            if job[0] != path:
                continue
            if self.state.block_revision(*job) != self.planned.get(job):
                del self.latest[job]

    def schedule_announced(
        self,
        path: NativePath,
        status: ScheduleStatus,
        identification: NativeBlockBatch | None,
    ) -> None:
        slot = path, status.schedule_id
        if self.roles.get(slot) == status.role_flags:
            return
        self.roles[slot] = status.role_flags
        if status.previous_id != status.schedule_id:
            self.roles.pop((path, status.previous_id), None)
        identity = self.state.identity(path)
        if identity is None:
            if identification is not None and not identification.reports:
                self.queue(path, 0)
            return
        key: BlockKey = (SETUP_BLOCK, None)
        if identity.product and identity.product.model == "033":
            key = MULTI_WEEK.start, status.schedule_id
        job = path, key
        if (
            (batch := self.latest.get(job)) is not None
            and not batch.reports
            and key in self.wanted.get(path, {})
        ):
            del self.latest[job]
            self.queue(path, key)

    async def read(
        self,
        stream: BridgeStream,
        path: NativePath,
        key: BlockKey,
        *,
        source: NativeAddress,
        timeout: float,
    ) -> NativeBlockBatch | None:
        revision = self.wanted.get(path, {}).get(key)
        if revision is None:
            return None  # A passive report or demotion retired this queued read.
        batch, current = await self.reader.read(
            stream,
            path,
            key,
            revision,
            source=source,
            timeout=timeout,
        )
        if current:
            self.latest[path, key] = batch
        return batch
