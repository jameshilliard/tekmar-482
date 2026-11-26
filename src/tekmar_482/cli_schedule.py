"""CLI for reading weekly calendar tables and editing one displayed event."""

from __future__ import annotations

import argparse
from calendar import Day
from datetime import time
from typing import TYPE_CHECKING

from ._native_payloads.schedules import DAY_NAMES, ScheduleEvent
from .native import schedule_slot, schedule_time
from .native_schedule import ScheduleChange, ScheduleChannel, ScheduleTarget

if TYPE_CHECKING:
    from collections.abc import Callable

    from .native import NativeAddress
    from .native_schedule import NativeWeeklySchedule


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
    for command in (read, write):
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
    write.add_argument(
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


def _day_value(raw: int | None) -> dict[str, int | str | None] | None:
    if raw is None:
        return None
    try:
        at = schedule_time(raw)
    except ValueError:
        return {"raw": raw, "time": None}
    return {"raw": raw, "time": at.isoformat(timespec="minutes") if at else "disabled"}


def schedule_fields(week: NativeWeeklySchedule) -> dict[str, object]:
    cfg = week.config
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
        "setup_body": cfg.setup.require_report().body.hex(),
        "table_complete": week.table_complete,
        "rows": [
            {
                "event": row.display_event.name.lower(),
                "wire_event": row.event.name.lower(),
                "matching_header": row.matching_header,
                "block": row.batch.query.block_id,
                "tx_status": row.batch.tx_result.status.name
                if row.batch.tx_result
                else None,
                "body": row.batch.reports[0].body.hex() if row.batch.reports else None,
                "days": dict(zip(DAY_NAMES, map(_day_value, row.days), strict=True)),
            }
            for row in week.rows
        ],
    }
