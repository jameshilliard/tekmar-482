"""Preview and send explicit native line operations."""

from __future__ import annotations

import argparse
from datetime import datetime
from typing import TYPE_CHECKING

from ._native_payloads.line import GROUP_IDS, ZoneSyncAdjustment, ZoneTargetMode
from ._native_payloads.scenes import NativeScene
from ._native_payloads.water import StageCommand, StageSelection
from .constants import TN4_PORTS
from .native import NativeAddress, NativeMessage
from .native_line import BroadcastIssue, DeviceGroupBinding, LineBroadcast
from .raw import NativePortBusyError, TxStatus

if TYPE_CHECKING:
    from collections.abc import Callable

    from .management import BridgeManagement
    from .raw import BridgeStream

LINE_COMMANDS = {
    "network-init": "send one native network-init probe per selected line",
    "select-scene": "select a system scene",
    "set-clock": "announce local civil time",
    "set-group": "enable or disable a setpoint group",
    "set-zone-sync": "announce zone synchronization, purging and target mode",
}
_STAGE_COMMANDS = {c.name.lower().replace("_", "-"): c for c in StageCommand}


def _wall_time(value: str) -> datetime:
    if "T" not in value and " " not in value:
        msg = "specify a local date and time, e.g. 2030-01-14T12:30"
        raise argparse.ArgumentTypeError(msg)
    stamp = datetime.fromisoformat(value)
    if stamp.second or stamp.microsecond:
        msg = "native clock resolution is one minute"
        raise argparse.ArgumentTypeError(msg)
    return stamp


def add_line_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    parent: argparse.ArgumentParser,
    number: Callable[[str], int],
) -> None:
    for name, help_text in LINE_COMMANDS.items():
        cmd = subparsers.add_parser(name, parents=[parent], help=help_text)
        cmd.add_argument(
            "--raw",
            action="store_true",
            help="switch to Raw mode for this announcement",
        )
        cmd.add_argument(
            "--ports",
            nargs="+",
            type=int,
            choices=TN4_PORTS,
            required=True,
        )
        cmd.add_argument("--source-subnet", type=number, default=0)
        cmd.add_argument("--source-node", type=number, default=0)
        cmd.add_argument(
            "--confirm",
            action="store_true",
            help="transmit; default is offline preview",
        )
        if name == "select-scene":
            cmd.add_argument(
                "scene",
                type=int,
                choices=tuple(s.value for s in NativeScene),
            )
        elif name == "set-clock":
            cmd.add_argument("wall_time", type=_wall_time, metavar="YYYY-MM-DDTHH:MM")
            cmd.add_argument(
                "--dst",
                action=argparse.BooleanOptionalAction,
                default=None,
                help="local time includes a one-hour daylight adjustment",
            )
            cmd.add_argument(
                "--dst-mode",
                type=number,
                help="optional sixth byte; omitted by default",
            )
        elif name == "set-group":
            cmd.add_argument("group", type=int, choices=GROUP_IDS)
            cmd.add_argument("state", choices=("on", "off"))
        elif name == "set-zone-sync":
            cmd.add_argument(
                "--cycle",
                type=int,
                choices=(0, 1),
                required=True,
                help="native cycle selector",
            )
            cmd.add_argument("--purge", choices=("on", "off"), required=True)
            cmd.add_argument(
                "--target",
                choices=[v.name.lower().replace("_", "-") for v in ZoneTargetMode],
                required=True,
            )

    stage = subparsers.add_parser(
        "stage-command",
        parents=[parent],
        help="preview or attempt a native staged demand, purge or override",
    )
    stage.add_argument("operation", choices=_STAGE_COMMANDS)
    stage.add_argument("port", type=int, choices=TN4_PORTS)
    stage.add_argument("subnet", type=number)
    stage.add_argument(
        "--nodes",
        type=int,
        choices=range(1, 25),
        nargs="+",
        required=True,
        metavar="NODE",
        help="one or more native nodes, 1..24",
    )
    stage.add_argument("--stage", type=int, choices=(0, 1), required=True)
    stage.add_argument("--output-level", type=number, required=True, help="raw 0..127")
    stage.add_argument(
        "--confirm",
        action="store_true",
        help="attempt one send; default is offline preview",
    )


def stage_from_args(args: argparse.Namespace) -> tuple[StageCommand, StageSelection]:
    selection = StageSelection.from_nodes(
        args.subnet,
        args.nodes,
        stage=args.stage,
        output_level=args.output_level,
    )
    return _STAGE_COMMANDS[args.operation], selection


def stage_preview(
    port: int,
    command: StageCommand,
    selection: StageSelection,
) -> dict[str, object]:
    message = NativeMessage.stage_command(
        command,
        selection,
        source=NativeAddress(0, 0),
        priority=0,
    )
    return {
        "port": port,
        "message_id": command.value,
        "target_subnet": selection.subnet,
        "target_nodes": selection.nodes,
        "stage": selection.stage,
        "output_level": selection.output_level,
        "body": message.to_bytes().hex(),
    }


async def send_stage(
    manager: BridgeManagement,
    port: int,
    command: StageCommand,
    selection: StageSelection,
    timeout: float,
    emit: Callable[[object], None],
) -> int:
    try:
        tx = await manager.send_stage_command(port, command, selection, timeout=timeout)
    except NativePortBusyError:
        issue = BroadcastIssue.PORT_BUSY
    except TimeoutError:
        issue = BroadcastIssue.DEADLINE
    else:
        if tx is not None:
            emit({"port": port, "tx_status": tx.status.name, "tx_reason": tx.reason})
            return int(tx.status is not TxStatus.ACK)
        issue = BroadcastIssue.EXPIRED_NOT_SENT
    emit({"port": port, "tx_status": issue.value, "tx_reason": None})
    return 1


def prepare_broadcast(
    args: argparse.Namespace,
) -> tuple[LineBroadcast, dict[str, object]]:
    ports = tuple(args.ports)
    source = NativeAddress(args.source_subnet, args.source_node)
    details: dict[str, object]
    if args.command == "network-init":
        send = LineBroadcast.network_init(ports, source=source)
        details = {}
    elif args.command == "select-scene":
        scene = NativeScene(args.scene)
        send = LineBroadcast.scene(ports, scene, source=source)
        details = {"scene": scene.value, "duration_minutes": scene.duration_minutes}
    elif args.command == "set-clock":
        send = LineBroadcast.clock(
            ports,
            args.wall_time,
            dst=args.dst,
            dst_mode=args.dst_mode,
            source=source,
        )
        details = dict(send.message.decoded)
    elif args.command == "set-group":
        send = LineBroadcast.group(
            ports,
            args.group,
            enabled=args.state == "on",
            source=source,
        )
        details = {"group": args.group, "enabled": args.state == "on"}
    else:
        settings = ZoneSyncAdjustment(
            args.cycle,
            args.purge == "on",
            ZoneTargetMode[args.target.upper().replace("-", "_")],
        )
        send = LineBroadcast.zone_sync(ports, settings, source=source)
        details = dict(send.message.decoded)
    return send, details | {"ports": send.ports, "body": send.message.to_bytes().hex()}


async def send_broadcast(
    stream: BridgeManagement | BridgeStream,
    message: LineBroadcast,
    timeout: float,
    emit: Callable[[object], None],
) -> int:
    results = await message.send(stream, timeout=timeout)
    for result in results:
        link = result.outcome
        emit(
            {
                "port": result.port,
                "tx_status": link.value
                if isinstance(link, BroadcastIssue)
                else link.status.name,
                "tx_reason": None if isinstance(link, BroadcastIssue) else link.reason,
            },
        )
    return int(not all(result.link_acknowledged for result in results))


async def device_group(
    stream: BridgeManagement | BridgeStream,
    args: argparse.Namespace,
    address: NativeAddress,
    source: NativeAddress,
    emit: Callable[[object], None],
) -> int:
    binding = await DeviceGroupBinding.read(
        stream,
        args.port,
        address,
        source=source,
        timeout=args.timeout,
    )
    assignment = binding.assignment
    info = {
        "port": binding.port,
        "subnet": address.subnet,
        "node": address.node,
        "application_raw": assignment.application_raw,
        "group_raw": assignment.group_raw,
        "remote_group": assignment.remote_group,
    }
    group = assignment.remote_group if args.expect_group is None else args.expect_group
    if group is None:
        emit(info)
        return 1
    send = binding.announcement(group, enabled=args.state == "on", source=source)
    if not args.confirm:
        body = send.message.to_bytes().hex()
        emit({"preview": info | {"enabled": args.state == "on", "body": body}})
        return 0
    emit({"broadcast": info})
    return await send_broadcast(stream, send, args.timeout, emit)
