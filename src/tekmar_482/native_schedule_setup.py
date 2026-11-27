"""Prepare and verify a native schedule configuration, including stored rows."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from ._native_payloads.layouts import SnowmeltBlock
from ._native_payloads.schedules import (
    DAY_NAMES,
    DailyEvents,
    ScheduleBinding,
    ScheduleCycle,
    ScheduleEvent,
    ScheduleProfile,
)
from ._native_payloads.snowmelt import SnowmeltApplication
from ._native_payloads.writes import BlockPatch, NativeField
from .exceptions import TekmarError
from .native import NativeBlockQuery, WeeklyEvent, require_enum, schedule_slot
from .native_schedule import read_schedule_configuration, schedule_binding_field
from .native_write import NativeWriteStatus

if TYPE_CHECKING:
    from .native import NativeFrame
    from .native_schedule import ScheduleChange, ScheduleConfiguration, ScheduleTarget
    from .native_write import NativeBlockEditor, NativeWriteResult

_DAY_FIELDS = tuple(NativeField[name.upper()] for name in DAY_NAMES)
_DISABLED = schedule_slot(None)
_DAILY_OR_WEEKLY = frozenset({ScheduleCycle.DAILY, ScheduleCycle.SEVEN_DAY})


@dataclass(frozen=True, slots=True)
class ScheduleSetupChange:
    """Explicit changes to an assignment, table format and displayed events."""

    binding: ScheduleBinding | None = None
    cycle: ScheduleCycle | None = None
    daily_events: DailyEvents | None = None
    times: tuple[ScheduleChange, ...] = ()

    def __post_init__(self) -> None:
        if self.binding is not None:
            require_enum(self.binding, ScheduleBinding)
        if self.cycle is not None:
            require_enum(self.cycle, ScheduleCycle)
        if self.daily_events is not None:
            require_enum(self.daily_events, DailyEvents)
        if self.binding is None and not (self.has_format or self.times):
            msg = "specify a schedule assignment, format or event time"
            raise ValueError(msg)

    @property
    def has_format(self) -> bool:
        return self.cycle is not None or self.daily_events is not None


@dataclass(frozen=True, slots=True)
class ScheduleSetupResult:
    """Prepared blocks in preview, or individual outcomes and unsent blocks."""

    status: NativeWriteStatus
    before: ScheduleConfiguration
    steps: tuple[NativeWriteResult, ...] = ()
    remaining: tuple[NativeWriteResult, ...] = ()
    problem: str | None = None


@dataclass(frozen=True, slots=True)
class _PreparedEdit:
    query: NativeBlockQuery
    patch: BlockPatch
    result: NativeWriteResult


async def _prepare_edit(
    editor: NativeBlockEditor,
    config: ScheduleConfiguration,
    query: NativeBlockQuery,
    before: NativeFrame,
    fields: dict[NativeField, int],
) -> _PreparedEdit | None:
    patch = BlockPatch.from_fields(query.block_id, fields)
    result = await editor.write(
        query,
        patch,
        before=before,
        profile=config.profile,
        preview=True,
    )
    return (
        None
        if result.status is NativeWriteStatus.UNCHANGED
        else _PreparedEdit(
            query,
            patch,
            result,
        )
    )


def _time_edits(
    count: DailyEvents,
    cycle: ScheduleCycle,
    times: tuple[ScheduleChange, ...],
) -> dict[ScheduleEvent, dict[int, int]]:
    displayed = {count.display_event(event): event for event in count.events}
    edits: dict[ScheduleEvent, dict[int, int]] = {}
    for change in times:
        if (event := displayed.get(change.event)) is None:
            msg = "the requested event is not displayed in the resulting schedule"
            raise ValueError(msg)
        days = edits.setdefault(event, {})
        raw = schedule_slot(change.at)
        for day in cycle.days_for(change.day):
            if days.setdefault(day, raw) != raw:
                msg = "conflicting event times for days that belong to one group"
                raise ValueError(msg)
    return edits


def _check_days(
    row: WeeklyEvent,
    changes: dict[int, int],
    cycle: ScheduleCycle,
) -> None:
    days = tuple(changes.get(day, raw) for day, raw in enumerate(row.days))
    if any(raw > _DISABLED for raw in days):
        msg = (
            "replace unrecognized event times before activating or regrouping the table"
        )
        raise ValueError(msg)
    if any(
        days[day] != days[other]
        for day in range(len(days))
        for other in cycle.days_for(day)
    ):
        msg = "supply a common event time where the new format merges different days"
        raise ValueError(msg)


async def _prepare_rows(
    editor: NativeBlockEditor,
    config: ScheduleConfiguration,
    change: ScheduleSetupChange,
    binding: ScheduleBinding,
) -> list[_PreparedEdit]:
    format_changed = (change.cycle is not None and change.cycle != config.cycle) or (
        change.daily_events is not None and change.daily_events != config.daily_events
    )
    configure_table = format_changed or binding != config.binding
    if not binding.owns_table:
        if change.has_format or change.times:
            msg = "only the schedule owner can set a format or event times"
            raise ValueError(msg)
        return []
    if not (configure_table or change.times):
        return []
    cycle = config.cycle if change.cycle is None else change.cycle
    count = config.daily_events if change.daily_events is None else change.daily_events
    if cycle is None or count is None or count is DailyEvents.NONE:
        msg = "the resulting table needs a known cycle and two or four daily events"
        raise ValueError(msg)

    edits = _time_edits(count, cycle, change.times)
    header: dict[NativeField, int] = {}
    indexed = config.target.system is not None
    if indexed and format_changed:
        header = {
            NativeField.SCHEDULE_CYCLING_RATE: cycle,
            NativeField.NUM_DAILY_EVENTS: count,
        }
    events = (
        tuple(ScheduleEvent)
        if header
        else count.events
        if configure_table
        else tuple(edits)
    )
    result: list[_PreparedEdit] = []
    for event in sorted(events, key=lambda e: (indexed and e is ScheduleEvent.WAKE, e)):
        query = config.target.query(event)
        report = (
            config.setup
            if indexed and event is ScheduleEvent.WAKE
            else (await editor.read(query)).require_report()
        )
        if not config.header_matches(report):
            msg = "timer event header disagrees with the schedule owner"
            raise ValueError(msg)
        times = edits.get(event, {})
        if configure_table and event in count.events:
            _check_days(
                WeeklyEvent.from_frame(report, profile=config.profile),
                times,
                cycle,
            )
        fields = header | {_DAY_FIELDS[day]: raw for day, raw in times.items()}
        if fields and (
            item := await _prepare_edit(editor, config, query, report, fields)
        ):
            result.append(item)
    return result


async def _apply_one(
    editor: NativeBlockEditor,
    config: ScheduleConfiguration,
    item: _PreparedEdit,
) -> tuple[NativeWriteResult, str | None]:
    try:
        batch = await editor.read(item.query)
        fresh = batch.require_report()
    except (OSError, TekmarError) as err:
        return replace(item.result, status=NativeWriteStatus.NOT_SENT), str(err)

    before = item.result.before.message.payload
    if fresh.message.payload != before:
        # An indexed control may already have copied the new header to this row.
        # Only a complete match, including untouched bytes, can skip the write.
        if fresh.message.payload == before[:3] + item.result.proposed.payload[4:]:
            return replace(
                item.result,
                status=NativeWriteStatus.UNCHANGED,
                readback=batch,
            ), None
        return replace(
            item.result,
            status=NativeWriteStatus.BASELINE_CHANGED,
            readback=batch,
        ), None

    try:
        result = await editor.write(
            item.query,
            item.patch,
            before=fresh,
            profile=config.profile,
            preview=False,
        )
    except (OSError, TekmarError) as err:
        return replace(item.result, status=NativeWriteStatus.UNCONFIRMED), str(err)
    if (
        config.target.system is not None
        and result.status is NativeWriteStatus.CONFIRMED
        and result.readback is not None
        and result.readback.require_report().message.payload[3]
        != result.proposed.payload[4]
    ):
        result = replace(result, status=NativeWriteStatus.DIFFERENT)
    return result, None


async def _check_model(
    editor: NativeBlockEditor,
    config: ScheduleConfiguration,
    change: ScheduleSetupChange,
    model: str,
) -> tuple[NativeBlockQuery, NativeFrame] | None:
    if model in ("553", "654"):
        if change.cycle is not None and change.cycle not in _DAILY_OR_WEEKLY:
            msg = "this model supports only 24-hour and 7-day schedule formats"
            raise ValueError(msg)
        if (
            change.binding is ScheduleBinding.UNOCCUPIED
            and change.binding != config.binding
        ):
            msg = "this model has no forced unoccupied assignment"
            raise ValueError(msg)
    if model != "654":
        return None
    query = NativeBlockQuery(config.target.address, SnowmeltBlock.APPLICATION_MODE)
    report = (await editor.read(query)).require_report()
    mode = report.decoded.get("application_mode")
    if (
        not isinstance(mode, int)
        or mode not in SnowmeltApplication
        or mode == SnowmeltApplication.SENSOR_ONLY
    ):
        msg = "654 schedule editing requires a confirmed snowmelt control application"
        raise ValueError(msg)
    return query, report


def _setup_fields(
    config: ScheduleConfiguration,
    change: ScheduleSetupChange,
    binding: ScheduleBinding,
) -> dict[NativeField, int]:
    if config.target.system is not None or not (
        binding != config.binding or change.has_format
    ):
        return {}
    field = schedule_binding_field(config.target, config.profile, config.setup.decoded)
    fields: dict[NativeField, int] = {field: binding}
    cycle = config.cycle if change.cycle is None else change.cycle
    count = config.daily_events if change.daily_events is None else change.daily_events
    if cycle is not None:
        fields[NativeField.SCHEDULE_CYCLING_RATE] = cycle
    if count is not None:
        fields[NativeField.NUM_DAILY_EVENTS] = count
    return fields


async def _apply_plan(
    editor: NativeBlockEditor,
    config: ScheduleConfiguration,
    edits: list[_PreparedEdit],
    identity: dict[str, str | int | float],
    baseline: tuple[tuple[NativeBlockQuery, NativeFrame], ...],
) -> ScheduleSetupResult:
    planned = tuple(edit.result for edit in edits)
    try:
        if await editor.identification(config.target.address) != identity:
            return ScheduleSetupResult(
                NativeWriteStatus.BASELINE_CHANGED,
                config,
                remaining=planned,
            )
        for query, before in baseline:
            fresh = (await editor.read(query)).require_report()
            if fresh.message.payload != before.message.payload:
                return ScheduleSetupResult(
                    NativeWriteStatus.BASELINE_CHANGED,
                    config,
                    remaining=planned,
                )
    except (OSError, TekmarError) as err:
        return ScheduleSetupResult(
            NativeWriteStatus.NOT_SENT,
            config,
            remaining=planned,
            problem=str(err),
        )

    results = []
    for i, item in enumerate(edits):
        result, problem = await _apply_one(editor, config, item)
        results.append(result)
        if result.status not in (
            NativeWriteStatus.CONFIRMED,
            NativeWriteStatus.UNCHANGED,
        ):
            return ScheduleSetupResult(
                result.status,
                config,
                tuple(results),
                planned[i + 1 :],
                problem,
            )
    written = {item.query for item in edits}
    for query, before in baseline:
        if query in written:
            continue  # A matching readback already verified this block.
        try:
            fresh = (await editor.read(query)).require_report()
        except (OSError, TekmarError) as err:
            return ScheduleSetupResult(
                NativeWriteStatus.UNCONFIRMED,
                config,
                tuple(results),
                problem=str(err),
            )
        if fresh.message.payload != before.message.payload:
            return ScheduleSetupResult(
                NativeWriteStatus.DIFFERENT,
                config,
                tuple(results),
                problem="schedule setup changed during the edit",
            )
    status = (
        NativeWriteStatus.CONFIRMED
        if any(r.status is NativeWriteStatus.CONFIRMED for r in results)
        else NativeWriteStatus.UNCHANGED
    )
    return ScheduleSetupResult(status, config, tuple(results))


async def edit_schedule_setup(
    editor: NativeBlockEditor,
    target: ScheduleTarget,
    change: ScheduleSetupChange,
    *,
    expected_binding: ScheduleBinding | None,
    preview: bool,
) -> ScheduleSetupResult:
    if expected_binding is not None:
        require_enum(expected_binding, ScheduleBinding)
    elif not preview:
        msg = "applying schedule setup requires the expected current assignment"
        raise ValueError(msg)
    identity = await editor.identification(target.address)
    profile = ScheduleProfile.from_identification(identity)
    if profile.product is None:
        msg = "identify the device before changing schedule setup"
        raise ValueError(msg)
    config, setup = await read_schedule_configuration(editor.read, target, profile)
    current = config.binding
    if current is None:
        msg = "device did not provide a known schedule assignment"
        raise ValueError(msg)
    if expected_binding is not None and expected_binding != current:
        msg = (
            f"current schedule assignment is {current.name.lower()}, "
            "not the expected assignment"
        )
        raise ValueError(msg)
    binding = current if change.binding is None else change.binding
    if target.system is not None and binding != current:
        msg = "033 timer ownership is not editable through schedule setup"
        raise ValueError(msg)
    application = await _check_model(editor, config, change, profile.product.model)
    setup_fields = _setup_fields(config, change, binding)
    setting = (
        await _prepare_edit(
            editor,
            config,
            setup.query,
            config.setup,
            setup_fields,
        )
        if setup_fields
        else None
    )
    edits = await _prepare_rows(editor, config, change, binding)
    # Stage changed rows before enabling or changing single-schedule setup.
    if setting is not None:
        edits.append(setting)
    if not edits:
        return ScheduleSetupResult(NativeWriteStatus.UNCHANGED, config)
    if preview:
        return ScheduleSetupResult(
            NativeWriteStatus.PREVIEW,
            config,
            tuple(edit.result for edit in edits),
        )
    baseline: tuple[tuple[NativeBlockQuery, NativeFrame], ...] = (
        (setup.query, config.setup),
    )
    if application is not None:
        baseline += (application,)
    return await _apply_plan(editor, config, edits, identity, baseline)
