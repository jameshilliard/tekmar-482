"""Read error counters or watch changes without altering the current tN4 mode."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from ._bridge_diagnostics import ErrorReason, ErrorSource
from .error_reports import ALL_SOURCES
from .management import ErrorReporting

if TYPE_CHECKING:
    import argparse
    from collections.abc import Callable

    from .bridge import BridgeClient
    from .error_reports import GatewayErrorCounter

_SOURCES = {
    "gateway" if source is ErrorSource.GATEWAY else str(int(source)): source
    for source in ErrorSource
}
_REASONS = {reason.name.lower(): reason for reason in ErrorReason}


def add_errors_command(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    common: argparse.ArgumentParser,
) -> None:
    parser = subparsers.add_parser(
        "errors",
        parents=[common],
        help="watch gateway error counters in the current mode",
    )
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--status", action="store_true", help="show enabled sources")
    action.add_argument(
        "--stop",
        action="store_true",
        help="disable unsolicited errors",
    )
    action.add_argument(
        "--read",
        choices=_REASONS,
        help="read one counter without enabling reports",
    )
    parser.add_argument(
        "--source",
        choices=_SOURCES,
        action="append",
        help="port or gateway (repeatable; default all applicable)",
    )
    parser.add_argument("--duration", type=float, help="watch for this many seconds")


def _record(event: GatewayErrorCounter) -> dict[str, object]:
    return {
        "source": event.source.name.lower(),
        "reason": event.reason.name.lower(),
        "last_seen_ms": event.last_seen_ms,
        "count": event.count,
    }


async def run_errors(
    bridge: BridgeClient,
    args: argparse.Namespace,
    emit: Callable[[object], None],
) -> None:
    sources = (
        frozenset(_SOURCES[s] for s in args.source) if args.source else ALL_SOURCES
    )
    if (args.source and (args.stop or args.status)) or (
        args.duration is not None
        and (not args.duration > 0 or args.stop or args.status or args.read)
    ):
        msg = (
            "use --duration only for watching and --source only for watching or reading"
        )
        raise ValueError(msg)
    reason = _REASONS[args.read] if args.read else None
    if (
        reason is not None
        and args.source
        and any(not reason.supports(s) for s in sources)
    ):
        msg = f"{args.read} is not available on the selected sources"
        raise ValueError(msg)
    async with bridge.normal(select_mode=False) as client:
        errors = ErrorReporting(client)
        if args.stop:
            await errors.set_sources(())
            emit({"sources": []})
        elif args.status:
            emit({"sources": [s.name.lower() for s in sorted(await errors.sources())]})
        elif reason is not None:
            for source in sorted(sources):
                if reason.supports(source):
                    emit(_record(await errors.counter(source, reason)))
        else:
            async with errors.watch(sources) as reports:
                window = asyncio.timeout(args.duration)
                try:
                    async with window:
                        async for event in reports:
                            emit(_record(event))
                except TimeoutError:
                    if not window.expired():
                        raise
