"""CLI for reading, configuring and editing weekly calendar tables."""

from __future__ import annotations

import argparse
from calendar import Day
from datetime import time
from typing import TYPE_CHECKING

from ._native_payloads.schedules import (
    DAY_NAMES,
    DailyEvents,
    ScheduleBinding,
    ScheduleCycle,
    ScheduleEvent,
)
from .native import schedule_slot, schedule_time
from .native_schedule import ScheduleChange, ScheduleChannel, ScheduleTarget
from .native_schedule_setup import ScheduleSetupChange

if TYPE_CHECKING:
    from collections.abc import Callable

    from .native import NativeAddress
    from .native_schedule import NativeWeeklySchedule, ScheduleConfiguration


def _time(value: str) -> time | None:
    if value == "disabled":
        return None
    try:
        at = time.fromisoformat(value)
        schedule_slot(at)
    except ValueError as err:
        msg = "expected local HH:MM on a ten-minute boundary, or disabled"
        raise argparse.ArgumentTypeError(msg) from err
    return at


def add_schedule_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    common: argparse.ArgumentParser,
    add_target: Callable[[argparse.ArgumentParser], None],
) -> None:
    read = subparsers.add_parser(
        "read-schedule",
        parents=[common],
        help="read weekly calendar or system membership",
    )
    write = subparsers.add_parser(
        "write-schedule",
        parents=[common],
        help="preview or edit one displayed event on the owner",
    )
    setup = subparsers.add_parser(
        "configure-schedule",
        parents=[common],
        help="preview or apply a schedule assignment, format and event times",
    )
    for command in (read, write, setup):
        add_target(command)
        scope = command.add_mutually_exclusive_group()
        scope.add_argument(
            "--channel",
            choices=[c.value for c in ScheduleChannel],
            default="heat",
        )
        scope.add_argument(
            "--system",
            type=int,
            choices=range(1, 5),
            help="033 timer slot, display numbers 1..4",
        )
    write.add_argument("event", choices=[e.name.lower() for e in ScheduleEvent])
    write.add_argument("day", choices=DAY_NAMES)
    write.add_argument("at", type=_time, metavar="HH:MM|disabled")
    bindings = [binding.name.lower() for binding in ScheduleBinding]
    setup.add_argument("--binding", choices=bindings, help="new schedule assignment")
    setup.add_argument(
        "--expect-binding",
        choices=bindings,
        help="required with --confirm",
    )
    setup.add_argument(
        "--cycle",
        choices=[cycle.name.lower() for cycle in ScheduleCycle],
    )
    setup.add_argument("--events", type=int, choices=(2, 4))
    setup.add_argument(
        "--time",
        action="append",
        nargs=3,
        metavar=("EVENT", "DAY", "HH:MM|disabled"),
        help="edit a displayed event in the resulting schedule; repeat as needed",
    )
    for command in (write, setup):
        command.add_argument(
            "--confirm",
            action="store_true",
            help="apply and verify; default is preview",
        )


def schedule_target(address: NativeAddress, args: argparse.Namespace) -> ScheduleTarget:
    return ScheduleTarget(address, ScheduleChannel(args.channel), args.system)


def schedule_change(args: argparse.Namespace) -> ScheduleChange:
    return ScheduleChange(
        ScheduleEvent[args.event.upper()],
        Day[args.day.upper()],
        args.at,
    )


def schedule_setup_change(args: argparse.Namespace) -> ScheduleSetupChange:
    try:
        times = tuple(
            ScheduleChange(ScheduleEvent[event.upper()], Day[day.upper()], _time(value))
            for event, day, value in (args.time or ())
        )
    except (KeyError, argparse.ArgumentTypeError) as err:
        msg = "expected --time EVENT WEEKDAY HH:MM|disabled in ten-minute steps"
        raise ValueError(msg) from err
    return ScheduleSetupChange(
        binding=ScheduleBinding[args.binding.upper()]
        if args.binding is not None
        else None,
        cycle=ScheduleCycle[args.cycle.upper()] if args.cycle is not None else None,
        daily_events={2: DailyEvents.TWO, 4: DailyEvents.FOUR}.get(args.events),
        times=times,
    )


def expected_schedule_binding(args: argparse.Namespace) -> ScheduleBinding | None:
    return (
        ScheduleBinding[args.expect_binding.upper()]
        if args.expect_binding is not None
        else None
    )


def _day_value(raw: int | None) -> dict[str, int | str | None] | None:
    if raw is None:
        return None
    try:
        at = schedule_time(raw)
    except ValueError:
        return {"raw": raw, "time": None}
    return {"raw": raw, "time": at.isoformat(timespec="minutes") if at else "disabled"}


def schedule_configuration_fields(cfg: ScheduleConfiguration) -> dict[str, object]:
    return {
        "port": cfg.setup.port,
        "subnet": cfg.target.address.subnet,
        "node": cfg.target.address.node,
        "channel": cfg.target.channel.value if cfg.target.system is None else None,
        "model": cfg.profile.product.model if cfg.profile.product else None,
        "z_number": cfg.profile.z_number,
        "binding": cfg.binding.name.lower() if cfg.binding is not None else None,
        "binding_raw": cfg.binding_raw,
        "system": cfg.target.system
        if cfg.target.system is not None
        else cfg.binding.system_number
        if cfg.binding is not None
        else None,
        "cycle": cfg.cycle.name.lower() if cfg.cycle is not None else None,
        "cycle_raw": cfg.cycle_raw,
        "events_raw": cfg.events_raw,
        "setup_body": cfg.setup.body.hex(),
    }


def schedule_fields(week: NativeWeeklySchedule) -> dict[str, object]:
    return {
        **schedule_configuration_fields(week.config),
        "table_complete": week.table_complete,
        "rows": [
            {
                "event": row.display_event.name.lower(),
                "wire_event": row.event.name.lower(),
                "matching_header": row.matching_header,
                "block": row.query.block_id,
                "tx_status": row.batch.tx_result.status.name
                if row.batch and row.batch.tx_result
                else None,
                "body": row.report.body.hex() if row.report else None,
                "days": dict(zip(DAY_NAMES, map(_day_value, row.days), strict=True)),
            }
            for row in week.rows
        ],
    }
