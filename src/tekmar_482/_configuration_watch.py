"""Coalesce configuration reads by device, selector and configuration generation."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ._native_payloads.layouts import MULTI_WEEK
from ._native_payloads.schedules import SETUP_BLOCK
from .native import NativeBlockQuery

if TYPE_CHECKING:
    from collections.abc import Callable

    from ._native_payloads.schedules import ScheduleStatus
    from .native import BlockKey, NativeAddress, NativeFrame
    from .native_state import NativeClimate, NativePath, NativeState
    from .raw import BridgeStream, NativeBlockBatch


@dataclass(slots=True)
class _Pending:
    path: NativePath
    query: NativeBlockQuery
    revision: int
    seen: asyncio.Event = field(default_factory=asyncio.Event)
    matched: bool = False
    retired: bool = False


class ConfigurationWatch:
    def __init__(
        self,
        state: NativeState,
        queue: Callable[[NativePath, int | BlockKey], None],
        *,
        schedules: bool,
        targets: bool,
    ) -> None:
        self.state = state
        self.queue = queue
        self.schedules = schedules
        self.targets = targets
        self.wanted: dict[NativePath, dict[BlockKey, int]] = {}
        self.planned: dict[tuple[NativePath, BlockKey], int] = {}
        self.latest: dict[tuple[NativePath, BlockKey], NativeBlockBatch] = {}
        self.pending: dict[int, _Pending] = {}
        self.roles: dict[tuple[NativePath, int], int] = {}

    def discard(self, path: NativePath) -> None:
        self.wanted.pop(path, None)
        self.planned = {k: v for k, v in self.planned.items() if k[0] != path}
        self.latest = {k: v for k, v in self.latest.items() if k[0] != path}
        self.roles = {k: v for k, v in self.roles.items() if k[0] != path}
        if (p := self.pending.get(path.port)) is not None and p.path == path:
            p.retired = True
            p.seen.set()  # A capture gap may have dropped the awaited frame.

    def reconcile(self, path: NativePath, climate: NativeClimate | None) -> None:
        missing = self.state.missing_schedule_reads(path) if self.schedules else {}
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

    def accept(self, frame: NativeFrame) -> bool:
        p = self.pending.get(frame.port)
        if p is None or p.matched or not p.query.matches(frame.message):
            return True
        p.matched = True
        p.seen.set()
        return self._current(p)

    def _current(self, p: _Pending) -> bool:
        key = p.query.block_id, p.query.selector
        return not p.retired and self.state.block_revision(p.path, key) == p.revision

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
                # The collector and the capture reader see the same frame. Apply
                # earlier role changes in wire order before retiring this request.
                await p.seen.wait()
            if self._current(p):
                self.latest[path, key] = batch
            return batch
        finally:
            del self.pending[path.port]
