"""Read and edit the weekly calendar on its native schedule owner."""

from __future__ import annotations

from calendar import Day
from dataclasses import dataclass, replace
from enum import Enum
from typing import TYPE_CHECKING

from ._native_payloads.layouts import MULTI_WEEK
from ._native_payloads.schedules import (
    DAY_NAMES,
    SETUP_BLOCK,
    SINGLE_WEEK,
    DailyEvents,
    ScheduleBinding,
    ScheduleCycle,
    ScheduleEvent,
    ScheduleProfile,
)
from ._native_payloads.writes import BlockPatch, NativeField
from .constants import TN4_PORTS
from .native import NativeBlockQuery, require_enum, schedule_slot
from .native_write import NativeWriteStatus

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Mapping
    from datetime import time
    from enum import IntEnum

    from .native import NativeAddress, NativeFrame
    from .native_write import NativeBlockEditor, NativeWriteResult
    from .raw import BridgeStream, NativeBlockBatch

    type _Read = Callable[[NativeBlockQuery], Awaitable[NativeBlockBatch]]

_SYSTEMS = range(1, 5)
_DISABLED_SLOT = 144


class ScheduleChannel(Enum):
    HEAT = "heat"
    COOL = "cool"


@dataclass(frozen=True, slots=True)
class ScheduleTarget:
    address: NativeAddress
    channel: ScheduleChannel = ScheduleChannel.HEAT
    system: int | None = None

    def __post_init__(self) -> None:
        self.address.request_mask()
        require_enum(self.channel, ScheduleChannel)
        if self.system is not None and (
            self.system not in _SYSTEMS or self.channel is not ScheduleChannel.HEAT
        ):
            msg = "an indexed timer requires system 1..4, not a heating/cooling channel"
            raise ValueError(msg)

    def query(self, event: ScheduleEvent) -> NativeBlockQuery:
        if self.system is None:
            return NativeBlockQuery(self.address, SINGLE_WEEK.start + event)
        return NativeBlockQuery(self.address, MULTI_WEEK.start + event, self.system - 1)


@dataclass(frozen=True, slots=True)
class ScheduleChange:
    """A displayed event and weekday, expanded according to the device's cycle."""

    event: ScheduleEvent
    day: Day
    at: time | None

    def __post_init__(self) -> None:
        require_enum(self.event, ScheduleEvent)
        require_enum(self.day, Day)
        schedule_slot(self.at)


def _integer(fields: Mapping[str, object], key: str) -> int | None:
    value = fields.get(key)
    return value if isinstance(value, int) else None


def _known[E: IntEnum](kind: type[E], raw: int | None) -> E | None:
    return kind(raw) if raw is not None and raw in kind else None


@dataclass(frozen=True, slots=True)
class ScheduleConfiguration:
    target: ScheduleTarget
    profile: ScheduleProfile
    setup: NativeBlockBatch
    binding_raw: int | None
    cycle_raw: int | None
    events_raw: int | None

    @property
    def binding(self) -> ScheduleBinding | None:
        return _known(ScheduleBinding, self.binding_raw)

    @property
    def cycle(self) -> ScheduleCycle | None:
        return _known(ScheduleCycle, self.cycle_raw)

    @property
    def daily_events(self) -> DailyEvents | None:
        return _known(DailyEvents, self.events_raw)

    @property
    def active_events(self) -> tuple[ScheduleEvent, ...]:
        if (
            self.binding is None
            or not self.binding.owns_table
            or self.daily_events is None
        ):
            return ()
        return self.daily_events.events

    def header_matches(self, frame: NativeFrame) -> bool:
        # All four timer rows repeat the selector, mastership, cycle and count.
        return (
            self.target.system is None
            or frame.message.payload[3]
            == self.setup.require_report().message.payload[3]
        )


async def _configuration(read: _Read, target: ScheduleTarget) -> ScheduleConfiguration:
    identity = (await read(NativeBlockQuery(target.address, 0))).require_report()
    profile = ScheduleProfile.from_identification(identity.decoded)
    timer = profile.product is not None and profile.product.model == "033"
    if timer != (target.system is not None):
        msg = "specify system 1..4 for an identified 033 timer only"
        raise ValueError(msg)
    query = (
        target.query(ScheduleEvent.WAKE)
        if timer
        else NativeBlockQuery(target.address, SETUP_BLOCK)
    )
    setup = await read(query)
    return _from_setup(profile, target, setup)


def _from_setup(
    profile: ScheduleProfile,
    target: ScheduleTarget,
    setup: NativeBlockBatch,
) -> ScheduleConfiguration:
    fields = setup.require_report().decoded
    binding: int | None
    if target.system is not None:
        binding = (
            target.system + 1
            if fields.get("master_flag") == 1
            else ScheduleBinding.NONE.value
        )
    elif target.channel is ScheduleChannel.COOL and (
        "cooling_schedule_id" in fields or not profile.shares_heating_schedule
    ):
        binding = _integer(fields, "cooling_schedule_id")
    else:
        binding = _integer(fields, "heating_schedule_id")
    return ScheduleConfiguration(
        target,
        profile,
        setup,
        binding,
        _integer(fields, "schedule_cycling_rate"),
        _integer(fields, "num_daily_events"),
    )


@dataclass(frozen=True, slots=True)
class ScheduleRow:
    event: ScheduleEvent
    display_event: ScheduleEvent
    batch: NativeBlockBatch
    days: tuple[int | None, ...]
    matching_header: bool

    @property
    def complete(self) -> bool:
        return self.matching_header and all(
            v is not None and v <= _DISABLED_SLOT for v in self.days
        )


def _row(
    config: ScheduleConfiguration,
    event: ScheduleEvent,
    batch: NativeBlockBatch,
) -> ScheduleRow:
    frame = (
        batch.reports[0]
        if batch.reports and batch.tx_result and batch.tx_result.may_have_reached_peer
        else None
    )
    matches = frame is not None and config.header_matches(frame)
    days: tuple[int | None, ...] = (None,) * len(DAY_NAMES)
    if frame is not None:
        fields = frame.decoded
        try:
            order = [
                config.profile.wire_day(batch.query.block_id, i)
                for i in range(len(DAY_NAMES))
            ]
        except ValueError:
            pass  # Raw report remains available; do not label unresolved weekdays.
        else:
            days = tuple(_integer(fields, DAY_NAMES[i] + "_raw") for i in order)
    count = config.daily_events
    if count is None:
        msg = "cannot label an event without a daily event configuration"
        raise ValueError(msg)
    return ScheduleRow(event, count.display_event(event), batch, days, matches)


@dataclass(frozen=True, slots=True)
class NativeWeeklySchedule:
    config: ScheduleConfiguration
    rows: tuple[ScheduleRow, ...]

    @property
    def table_complete(self) -> bool:
        return (
            self.config.binding is not None
            and self.config.binding.owns_table
            and self.config.cycle is not None
            and self.config.daily_events is not None
            and len(self.rows) == len(self.config.active_events)
            and all(r.complete for r in self.rows)
        )


@dataclass(frozen=True, slots=True)
class SystemSchedule:
    number: int
    masters: tuple[NativeWeeklySchedule, ...]
    members: tuple[NativeWeeklySchedule, ...]

    @property
    def owner(self) -> NativeWeeklySchedule | None:
        """No owner is implied by silence, or by competing claims."""
        return self.masters[0] if len(self.masters) == 1 else None


type _Key = tuple[int, int | None]


@dataclass(slots=True)
class _DeviceTables:
    profile: ScheduleProfile
    blocks: dict[_Key, NativeBlockBatch]

    def tables(self) -> Iterable[NativeWeeklySchedule]:
        for (block, selector), setup in self.blocks.items():
            address = setup.require_report().message.source
            if block == SETUP_BLOCK:
                targets = [ScheduleTarget(address)]
                if "cooling_schedule_id" in setup.require_report().decoded or (
                    self.profile.product is not None
                    and not self.profile.shares_heating_schedule
                ):
                    targets.append(ScheduleTarget(address, ScheduleChannel.COOL))
            elif (
                block == MULTI_WEEK.start
                and selector is not None
                and selector + 1 in _SYSTEMS
                and self.profile.product is not None
                and self.profile.product.model == "033"
            ):
                targets = [ScheduleTarget(address, system=selector + 1)]
            else:
                continue
            for target in targets:
                cfg = _from_setup(self.profile, target, setup)
                rows = []
                for event in cfg.active_events:
                    query = target.query(event)
                    if (
                        batch := self.blocks.get((query.block_id, query.selector))
                    ) is not None:
                        rows.append(_row(cfg, event, batch))
                yield NativeWeeklySchedule(cfg, tuple(rows))


@dataclass(frozen=True, slots=True)
class ScheduleDirectory:
    """Only schedules evidenced by this snapshot. No implicit cross-line routing."""

    schedules: tuple[NativeWeeklySchedule, ...]

    @classmethod
    def from_batches(cls, batches: Iterable[NativeBlockBatch]) -> ScheduleDirectory:
        devices: dict[tuple[int, int, int], _DeviceTables] = {}
        for batch in batches:
            block = batch.query.block_id
            if (
                not batch.tx_result
                or not batch.tx_result.may_have_reached_peer
                or (
                    block not in (0, SETUP_BLOCK)
                    and block not in SINGLE_WEEK
                    and block not in MULTI_WEEK
                )
            ):
                continue
            for frame in batch.reports:
                if frame.port != batch.port or not batch.query.matches(frame.message):
                    continue
                path = frame.port, frame.subnet, frame.node & 31
                if block == 0:
                    # A new identification starts a new sample at this address.
                    devices[path] = _DeviceTables(
                        ScheduleProfile.from_identification(frame.decoded),
                        {},
                    )
                elif (device := devices.get(path)) is not None:
                    query = replace(batch.query, target=frame.message.source)
                    device.blocks[block, query.selector] = replace(
                        batch,
                        query=query,
                        reports=(frame,),
                    )
        return cls(tuple(t for p in sorted(devices) for t in devices[p].tables()))

    def system(self, number: int, *, ports: Iterable[int]) -> SystemSchedule:
        scope = frozenset(ports)
        if number not in _SYSTEMS or not scope or not scope.issubset(TN4_PORTS):
            msg = "expected system 1..4 and one or more physical ports"
            raise ValueError(msg)
        masters: dict[tuple[int, int, int], NativeWeeklySchedule] = {}
        members = []
        for week in self.schedules:
            cfg = week.config
            binding = cfg.binding
            if (
                cfg.setup.port not in scope
                or binding is None
                or binding.system_number != number
            ):
                continue
            if binding.owns_table:
                addr = cfg.target.address
                masters.setdefault((cfg.setup.port, addr.subnet, addr.block_node), week)
            else:
                members.append(week)
        return SystemSchedule(number, tuple(masters.values()), tuple(members))


async def read_weekly_schedule(
    stream: BridgeStream,
    port: int,
    target: ScheduleTarget,
    *,
    source: NativeAddress,
    timeout: float = 20,
) -> NativeWeeklySchedule:
    """Read the local assignment, then only the active event rows owned here."""

    async def read(query: NativeBlockQuery) -> NativeBlockBatch:
        return await stream.read_block_group(
            port,
            query,
            source=source,
            timeout=timeout,
        )

    config = await _configuration(read, target)
    rows = []
    for event in config.active_events:
        query = target.query(event)
        batch = config.setup if config.setup.query == query else await read(query)
        rows.append(_row(config, event, batch))
        if batch.tx_result is None:
            break  # A pending TX still owns the port.
    return NativeWeeklySchedule(config, tuple(rows))


async def edit_schedule_time(
    editor: NativeBlockEditor,
    target: ScheduleTarget,
    change: ScheduleChange,
    *,
    preview: bool,
) -> NativeWriteResult:
    config = await _configuration(editor.read, target)
    count, cycle = config.daily_events, config.cycle
    if config.binding is None or not config.binding.owns_table:
        msg = "this device is not an identified owner of the requested schedule"
        raise ValueError(msg)
    if count is None or cycle is None:
        msg = "cannot edit a schedule without its cycle and daily event count"
        raise ValueError(msg)
    event = next(
        (e for e in count.events if count.display_event(e) is change.event),
        None,
    )
    if event is None:
        msg = "the displayed event is not active in this schedule"
        raise ValueError(msg)
    query = target.query(event)
    baseline = config.setup if config.setup.query == query else await editor.read(query)
    before = baseline.require_report()
    if not config.header_matches(before):
        msg = "timer event header disagrees with the schedule owner"
        raise ValueError(msg)
    slot = schedule_slot(change.at)
    changes = {
        NativeField[DAY_NAMES[d].upper()]: slot for d in cycle.days_for(change.day)
    }
    result = await editor.write(
        query,
        BlockPatch.from_fields(query.block_id, changes),
        profile=config.profile,
        before=before,
        preview=preview,
    )
    if (
        result.status is NativeWriteStatus.CONFIRMED
        and result.readback is not None
        and not config.header_matches(result.readback.require_report())
    ):
        return replace(result, status=NativeWriteStatus.DIFFERENT)
    return result
