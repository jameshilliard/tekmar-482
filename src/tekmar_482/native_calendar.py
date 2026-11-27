"""Live schedule evidence for one native source, independent of capture transport."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ._native_payloads.layouts import MULTI_WEEK
from ._native_payloads.schedules import (
    DAY_NAMES,
    SETUP_BLOCK,
    SINGLE_WEEK,
    ScheduleProfile,
)
from .native_schedule import (
    dependent_rows,
    schedule_configurations,
    schedules_from_reports,
    superseded_rows,
)

if TYPE_CHECKING:
    from ._native_payloads.schedules import ScheduleStatus
    from .native import NativeFrame
    from .native_identity import NativeIdentity
    from .native_schedule import NativeWeeklySchedule, ScheduleKey

_WEEKLY_ROWS = frozenset((*SINGLE_WEEK, *MULTI_WEEK))
SCHEDULE_BLOCKS = _WEEKLY_ROWS | {SETUP_BLOCK}
_SLOTS = range(4)
_DAY_FIELDS = tuple(name + "_raw" for name in DAY_NAMES)


def _has_weekdays(frame: NativeFrame) -> bool:
    fields = frame.decoded
    return all(day in fields for day in _DAY_FIELDS)


@dataclass(frozen=True, slots=True)
class ScheduleSignal:
    status: ScheduleStatus
    frame: NativeFrame


@dataclass(frozen=True, slots=True)
class NativeCalendar:
    schedules: tuple[NativeWeeklySchedule, ...]
    signals: tuple[ScheduleSignal, ...]
    missing_blocks: tuple[ScheduleKey, ...]

    @property
    def incomplete(self) -> bool:
        return bool(self.missing_blocks) or any(
            w.config.binding is None
            or (w.config.binding.owns_table and not w.table_complete)
            for w in self.schedules
        )


class ScheduleBook:
    def __init__(self) -> None:
        self.blocks: dict[ScheduleKey, NativeFrame] = {}
        self.signals: dict[int, ScheduleSignal] = {}
        self.revisions: dict[ScheduleKey, int] = {}

    def invalidate(self, key: ScheduleKey) -> None:
        self.blocks.pop(key, None)
        self.revisions[key] = self.revisions.get(key, 0) + 1

    def invalidate_update(self, key: ScheduleKey) -> None:
        self.invalidate(key)
        for row in dependent_rows(key):
            self.invalidate(row)

    def report(self, key: ScheduleKey, frame: NativeFrame) -> bool:
        old = self.blocks.get(key)
        if old is not None and old.message.payload == frame.message.payload:
            self.blocks[key] = frame
            return False
        for row in superseded_rows(key, old, frame):
            self.invalidate(row)
        if (
            key[0] in _WEEKLY_ROWS
            and old is not None
            and _has_weekdays(old)
            and not _has_weekdays(frame)
        ):
            # A compact confirmation can replace a full row even without a seen write.
            self.revisions[key] = self.revisions.get(key, 0) + 1
        self.blocks[key] = frame
        return True

    def announce(
        self,
        status: ScheduleStatus,
        frame: NativeFrame,
        profile: ScheduleProfile,
    ) -> bool:
        slot = status.schedule_id
        old = self.signals.get(slot)
        self.signals[slot] = ScheduleSignal(status, frame)
        if old is not None and old.status.role_flags == status.role_flags:
            return old.frame.message.payload != frame.message.payload
        changed = status.ownership_changed
        if old is not None:
            changed |= status.master != old.status.master or (
                status.master and status.events_raw != old.status.events_raw
            )
        else:
            # A first announcement can reveal a missed transition, but cannot
            # establish the new local binding. Confirm it with a setup read.
            configs = tuple(
                c
                for c in schedule_configurations(profile, self.blocks)
                if c.target.system in (None, slot + 1)
            )
            claims = [
                c
                for c in configs
                if c.binding
                and c.binding.owns_table
                and c.binding.system_number == slot + 1
            ]
            if status.master:
                changed |= bool(configs) and not any(
                    c.events_raw == status.events_raw for c in claims
                )
            else:
                changed |= bool(claims)
        if changed:
            if status.previous_id != slot:
                self.signals.pop(status.previous_id, None)
            if profile.product is None or profile.product.model == "033":
                for s in {status.previous_id, slot}:
                    self.invalidate_update((MULTI_WEEK.start, s))
            if profile.product is None or profile.product.model != "033":
                self.invalidate_update((SETUP_BLOCK, None))
        return True

    def missing(self, profile: ScheduleProfile) -> tuple[ScheduleKey, ...]:
        product = profile.product
        if product is None:
            return ()
        needed: dict[ScheduleKey, None] = {}
        if product.model == "033":
            needed.update(
                ((MULTI_WEEK.start, s), None)
                for s in _SLOTS
                if (MULTI_WEEK.start, s) not in self.blocks
            )
        elif (SETUP_BLOCK in product.candidate_blocks or self.signals) and (
            SETUP_BLOCK,
            None,
        ) not in self.blocks:
            needed[SETUP_BLOCK, None] = None
        for cfg in schedule_configurations(profile, self.blocks):
            for event in cfg.active_events:
                q = cfg.target.query(event)
                key = q.block_id, q.selector
                row = self.blocks.get(key)
                if row is None or not _has_weekdays(row):
                    needed[key] = None
        return tuple(needed)

    def snapshot(
        self,
        identity: NativeIdentity | None,
        *,
        addressable: bool,
    ) -> NativeCalendar:
        profile = schedule_profile(identity)
        return NativeCalendar(
            tuple(schedules_from_reports(profile, self.blocks)),
            tuple(signal for _, signal in sorted(self.signals.items())),
            self.missing(profile) if addressable else (),
        )


def schedule_profile(identity: NativeIdentity | None) -> ScheduleProfile:
    return (
        ScheduleProfile(identity.product, identity.z_number)
        if identity
        else ScheduleProfile(None, None)
    )
