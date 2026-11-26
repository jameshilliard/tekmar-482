"""Command line interface for tekmar-482."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Callable, Sequence
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, TextIO

from ._native_payloads.climate import REMOTE_MODES, ThermostatMode
from ._native_payloads.line import GROUP_IDS
from ._native_payloads.scenes import NativeScene, SceneResponse
from ._native_payloads.thermostat import TargetSlot, ThermostatTarget
from ._native_payloads.writes import NativeField, WritableBlock
from ._wire import GatewayEndpoint
from .bridge import BridgeClient
from .cli_line import (
    LINE_COMMANDS,
    add_line_commands,
    device_group,
    prepare_broadcast,
    send_broadcast,
)
from .cli_native_watch import (
    calendar_fields,
    snow_fields,
    snow_group_from_args,
    watch_all_from_args,
)
from .cli_schedule import (
    add_schedule_commands,
    schedule_change,
    schedule_fields,
    schedule_target,
)
from .cli_status import (
    add_status_commands,
    climate_fields,
    plant_register_fields,
    read_status_from_args,
    sensor_fields,
    watch_from_args,
    water_fields,
)
from .client import Tekmar482Client
from .constants import DEFAULT_BAUDRATE, TN4_PORTS
from .decoding import (
    decode_degh,
    decode_network_error,
    decoded_to_dict,
)
from .exceptions import TekmarError
from .firmware import install_firmware
from .gateway import open_gateway
from .native import NativeAddress, NativeBlockQuery
from .native_discovery import NativeDevice, discover_native
from .native_scene import write_scene_response
from .native_schedule import read_weekly_schedule
from .native_sensors import read_thermostat_sensors
from .native_snapshot import read_native_snapshot
from .native_snow_control import SnowmeltOperation
from .native_state import NativeState
from .native_target_edit import ThermostatTargetChange
from .native_water import read_water
from .native_write import NativeWriteStatus
from .raw import CaptureGap, InvalidCapture, PortError, TxStatus
from .trpc import (
    ResponseMatch,
    TrpcCommand,
    TrpcMethodId,
    TrpcPacket,
    TrpcService,
    TrpcServiceId,
    method_from_name,
    service_from_name,
)

if TYPE_CHECKING:
    from .models import (
        AvailableInfo,
        DeviceAttributes,
        DeviceSnapshot,
        DiscoveredDevice,
        DiscoveryResult,
        GatewayInfo,
        GatewaySnapshot,
    )
    from .native import NativeFrame
    from .native_snapshot import NativeSnapshot
    from .native_status import NativeStatusBatch
    from .native_write import NativeWriteResult
    from .raw import BridgeStream, CaptureRecord, NativeBlockBatch


class CliError(Exception):
    """Raised for user-facing CLI errors."""


ClientFactory = Callable[[argparse.Namespace], Tekmar482Client]
_MAX_TCP_PORT = 65535


def build_parser() -> argparse.ArgumentParser:
    """Build the command line parser."""
    parser = argparse.ArgumentParser(
        prog="tekmar-482",
        description="Inspect and communicate with a tekmar 482 gateway.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    parser.add_argument(
        "--vendor",
        action="store_true",
        help="use the original vendor tRPC firmware",
    )
    common = _common_parent()

    subparsers.add_parser(
        "gateway-info",
        parents=[common],
        help="request firmware and protocol version from the gateway",
    )

    discover = subparsers.add_parser(
        "discover",
        parents=[common],
        help="discover gateway metadata and attached device information",
    )
    discover.add_argument(
        "--duration",
        type=float,
        default=30,
        help="native discovery listening window",
    )
    discover.add_argument(
        "--manage-reporting",
        action="store_true",
        help="temporarily disable reporting during discovery and re-enable afterwards",
    )
    setback = discover.add_mutually_exclusive_group()
    setback.add_argument(
        "--enable-setback",
        action="store_const",
        const=True,
        dest="setback_enable",
        help="enable tHA setback support before inventory discovery",
    )
    setback.add_argument(
        "--disable-setback",
        action="store_const",
        const=False,
        dest="setback_enable",
        help="disable tHA setback support before inventory discovery",
    )

    dump = subparsers.add_parser(
        "dump",
        parents=[common],
        help="request all available read-only gateway and device information",
    )
    dump.add_argument(
        "--duration",
        type=float,
        default=30,
        help="native discovery window before device reads",
    )
    dump.add_argument(
        "--block-timeout",
        type=float,
        default=argparse.SUPPRESS,
        help="native block response window in seconds (default 20)",
    )
    dump.add_argument(
        "--line-status",
        action=argparse.BooleanOptionalAction,
        default=argparse.SUPPRESS,
        help="also request shared native status on responding ports (default on)",
    )
    dump.add_argument(
        "--include-setbacks",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="request per-setback setpoint values where supported",
    )
    dump.add_argument(
        "--inventory-timeout",
        type=float,
        default=10.0,
        help="inventory walk timeout in seconds",
    )
    dump.add_argument(
        "--write-delay",
        type=float,
        default=0.0,
        help="optional delay between requests in seconds",
    )
    for scan in (discover, dump):
        _add_scan_options(scan)

    monitor = subparsers.add_parser(
        "monitor",
        parents=[common],
        help="print incoming tRPC messages",
    )
    monitor.add_argument(
        "--duration",
        type=float,
        help="stop after this many seconds",
    )
    monitor.add_argument(
        "--limit",
        type=int,
        help="stop after this many messages",
    )
    monitor.add_argument(
        "--read-timeout",
        type=float,
        default=1.0,
        help="per-read timeout in seconds while monitoring",
    )

    request = subparsers.add_parser(
        "request",
        parents=[common],
        help="send a tRPC message, optionally waiting for a matching response",
    )
    request.add_argument("method", help="tRPC method name or numeric method ID")
    request.add_argument(
        "fields",
        nargs="*",
        metavar="FIELD=VALUE",
        help="body field values, for example address=1001 or setback=0x07",
    )
    request.add_argument(
        "--service",
        default="Request",
        help="tRPC service name or numeric service ID",
    )
    request.add_argument(
        "--response-method",
        help="method to wait for; defaults to the requested method",
    )
    request.add_argument(
        "--match-address",
        type=_int_value,
        help="only accept a response whose address field matches this value",
    )
    request.add_argument(
        "--wait",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="wait for a matching response; default is true for Request service",
    )

    _add_block_commands(subparsers, dump)
    _add_scene_commands(subparsers)
    add_line_commands(
        subparsers,
        _common_parent(default_timeout=20, require_transport=False),
        _int_value,
    )
    add_schedule_commands(
        subparsers,
        _common_parent(default_timeout=20),
        _add_native_target,
    )
    add_status_commands(subparsers, _common_parent(default_timeout=20), _int_value)
    for name in (
        "panel",
        "led-test",
        "probe",
        "diagnostics",
        "send",
        "install",
        "update",
    ):
        command = subparsers.add_parser(name, parents=[common])
        if name in ("diagnostics", "probe"):
            command.add_argument(
                "--duration",
                type=float,
                default=1 if name == "probe" else 30,
            )
        if name == "diagnostics":
            command.add_argument(
                "--ack-port",
                action="append",
                type=int,
                choices=TN4_PORTS,
                default=[],
            )
        if name == "send":
            command.add_argument("port", type=int, choices=range(5))
            command.add_argument(
                "body",
                help="native header and payload in hexadecimal",
            )
        if name == "install":
            command.add_argument("--confirm", action="store_true", required=True)
            command.add_argument(
                "--from-rom",
                action="store_true",
                help="already in unsynchronized ROM ISP",
            )
    return parser


def _add_block_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    dump: argparse.ArgumentParser,
) -> None:
    fields = subparsers.add_parser(
        "write-fields",
        help="list known native write layouts offline",
    )
    fields.add_argument(
        "block",
        nargs="?",
        type=_int_value,
        help="numeric native block ID",
    )
    fields.add_argument("--json", action="store_true")
    parent = _common_parent(default_timeout=20.0)
    block = subparsers.add_parser(
        "read-block",
        parents=[parent],
        help="read one native tN4 block",
    )
    device = subparsers.add_parser(
        "read-device",
        parents=[parent],
        help="identify and read one native device",
    )
    sensors = subparsers.add_parser(
        "read-sensors",
        parents=[parent],
        help="read the room statistics and sensor inputs of one native thermostat",
    )
    _add_device_groups(subparsers, parent)
    write = subparsers.add_parser(
        "write-block",
        parents=[parent],
        help="preview or verify an addressed field edit",
    )
    mode = subparsers.add_parser(
        "set-climate-mode",
        parents=[parent],
        help="preview or verify an installation-supported thermostat mode",
    )
    _add_native_target(mode)
    mode.add_argument("mode", choices=[m.name.lower() for m in REMOTE_MODES])
    adjustment = subparsers.add_parser(
        "set-thermostat-target",
        parents=[parent],
        help="preview or verify one stored thermostat target in Celsius or percent",
    )
    _add_native_target(adjustment)
    adjustment.add_argument(
        "target",
        choices=[t.name.lower() for t in ThermostatTarget],
    )
    adjustment.add_argument("slot", choices=[s.name.lower() for s in TargetSlot])
    adjustment.add_argument(
        "value",
        type=float,
        help="degrees Celsius or physical percent",
    )
    for command in (mode, adjustment):
        command.add_argument(
            "--confirm",
            action="store_true",
            help="update and verify; default is preview",
        )
    for sampler in (device, dump):
        sampler.add_argument(
            "--block-attempts",
            type=int,
            default=argparse.SUPPRESS,
            help="maximum attempts for missing native block replies (default 2)",
        )
    for command in (block, device, sensors, write):
        _add_native_target(command)
    for command in (block, write):
        command.add_argument("block", type=_int_value, help="numeric native block ID")
        command.add_argument(
            "--selector",
            type=_int_value,
            help="secondary ID, including zero",
        )
    write.add_argument(
        "fields",
        metavar="FIELD=RAW",
        nargs="+",
        help="unsigned wire values; use write-fields to list names",
    )
    write.add_argument(
        "--confirm",
        action="store_true",
        help="send once and verify by reading back; default is preview",
    )


def _add_scene_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    response = subparsers.add_parser(
        "scene-response",
        parents=[_common_parent(default_timeout=20)],
        help="preview or edit one device's response without selecting the scene",
    )
    _add_native_target(response)
    response.add_argument(
        "scene",
        type=int,
        choices=tuple(s.value for s in NativeScene if s.response_field),
    )
    response.add_argument(
        "response",
        choices=tuple(r.name.lower() for r in SceneResponse),
    )
    response.add_argument(
        "--confirm",
        action="store_true",
        help="update and verify; default is preview",
    )


def _add_device_groups(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    parent: argparse.ArgumentParser,
) -> None:
    for name, detail in (
        (
            "set-device-group",
            "read a setpoint controller's group assignment before broadcasting",
        ),
        (
            "set-snowmelt-group",
            "read a snowmelt zone's melt/storm assignment before broadcasting",
        ),
    ):
        group = subparsers.add_parser(name, parents=[parent], help=detail)
        _add_native_target(group)
        if name == "set-snowmelt-group":
            group.add_argument(
                "operation",
                choices=[op.name.lower() for op in SnowmeltOperation],
            )
        group.add_argument("state", choices=("on", "off"))
        group.add_argument(
            "--expect-group",
            type=int,
            choices=GROUP_IDS,
            help="group that may receive the command",
        )
        group.add_argument(
            "--confirm",
            action="store_true",
            help="broadcast; default reads the assignment only",
        )


def _add_native_target(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("port", type=int, choices=TN4_PORTS)
    parser.add_argument("subnet", type=_int_value)
    parser.add_argument("node", type=_int_value)
    parser.add_argument("--source-subnet", type=_int_value, default=0)
    parser.add_argument("--source-node", type=_int_value, default=0)
    parser.add_argument(
        "--ack",
        action="store_true",
        help="link-ACK valid incoming frames on this port",
    )


def _common_parent(
    *,
    default_timeout: float = 5.0,
    require_transport: bool = True,
) -> argparse.ArgumentParser:
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument("--vendor", action="store_true", default=argparse.SUPPRESS)
    parent.add_argument(
        "--auto-update",
        action="store_true",
        help="install the bundled build if the custom gateway revision differs",
    )
    transport = parent.add_mutually_exclusive_group(required=require_transport)
    transport.add_argument(
        "--tcp",
        metavar="HOST:PORT",
        help="transparent TCP-to-serial bridge",
    )
    transport.add_argument(
        "--serial",
        metavar="URL",
        help="serialx URL or serial path; requires tekmar-482[serial]",
    )
    parent.add_argument(
        "--baudrate",
        type=int,
        default=DEFAULT_BAUDRATE,
        help="serial baud rate when using --serial",
    )
    parent.add_argument(
        "--timeout",
        type=float,
        default=default_timeout,
        help="request timeout in seconds",
    )
    parent.add_argument(
        "--json",
        action="store_true",
        help="write JSON output",
    )
    return parent


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command line interface."""
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        return 130
    except (
        argparse.ArgumentTypeError,
        CliError,
        TekmarError,
        OSError,
        ValueError,
    ) as err:
        sys.stderr.write(f"error: {err}\n")
        return 2


async def run(
    args: argparse.Namespace,
    *,
    client_factory: ClientFactory | None = None,
    stdout: TextIO = sys.stdout,
) -> int:
    """Run an already-parsed command."""
    if args.command == "write-fields":
        if args.vendor:
            msg = "this operation is not available with --vendor"
            raise CliError(msg)
        blocks = WritableBlock if args.block is None else (WritableBlock(args.block),)
        for block in blocks:
            names = tuple(field.label for field in block.fields)
            if args.json:
                stdout.write(
                    json.dumps(
                        {
                            "block": block.value,
                            "name": block.name.lower(),
                            "fields": names,
                        },
                    )
                    + "\n",
                )
            else:
                stdout.write(f"{block:#04x} {block.name.lower()}: {', '.join(names)}\n")
        return 0
    if not args.vendor:
        return await _native(args, stdout)
    if (
        hasattr(args, "block_timeout")
        or hasattr(args, "line_status")
        or hasattr(args, "block_attempts")
        or any(
            getattr(args, key, None) for key in ("passive", "probe_subnet", "ack_port")
        )
    ):
        msg = "native discovery options are unavailable with --vendor"
        raise CliError(msg)
    if args.auto_update or args.command not in {
        "gateway-info",
        "discover",
        "dump",
        "monitor",
        "request",
    }:
        msg = "this operation is not available with --vendor"
        raise CliError(msg)
    factory = client_factory or client_from_args
    async with factory(args) as client:
        if args.command == "gateway-info":
            await _gateway_info(client, args, stdout)
        elif args.command == "discover":
            await _discover(client, args, stdout)
        elif args.command == "dump":
            await _dump(client, args, stdout)
        elif args.command == "monitor":
            await _monitor(client, args, stdout)
        elif args.command == "request":
            await _request(client, args, stdout)
        else:
            msg = f"unsupported command: {args.command}"
            raise CliError(msg)
    return 0


def _endpoint(args: argparse.Namespace) -> GatewayEndpoint:
    if not (args.tcp or args.serial):
        msg = "specify --serial or --tcp to connect to the gateway"
        raise CliError(msg)
    return (
        GatewayEndpoint.tcp(*parse_host_port(args.tcp), baudrate=args.baudrate)
        if args.tcp
        else GatewayEndpoint.serial(args.serial, baudrate=args.baudrate)
    )


async def _native(args: argparse.Namespace, stdout: TextIO) -> int:
    def emit(value: object) -> None:
        stdout.write((json.dumps(value) if args.json else str(value)) + "\n")
        if args.command in {"watch-climate", "watch-native"}:
            stdout.flush()

    if args.command in LINE_COMMANDS:
        return await _broadcast(args, emit)
    if (
        args.command in {"watch-native", "watch-climate"}
        and not args.device
        and not args.passive
    ):
        msg = f"active {args.command} requires --device PORT SUBNET NODE"
        raise CliError(msg)
    if (
        args.command in {"set-device-group", "set-snowmelt-group"}
        and args.confirm
        and args.expect_group is None
    ):
        msg = "a device-based broadcast requires --expect-group and --confirm"
        raise CliError(msg)
    endpoint = _endpoint(args)
    if args.command in {"install", "update"}:
        changed = await install_firmware(
            endpoint,
            from_rom=getattr(args, "from_rom", False),
        )
        emit({"updated": changed})
        return 0
    async with open_gateway(endpoint, auto_update=args.auto_update) as bridge:
        if not isinstance(bridge, BridgeClient):
            msg = "expected native firmware"
            raise CliError(msg)
        if args.command == "gateway-info":
            emit({"firmware": "custom", "revision": await bridge.revision()})
        elif args.command == "panel":
            emit(asdict(await bridge.panel()))
        elif args.command == "led-test":
            await bridge.test_leds()
        elif args.command == "probe":
            emit({"timers_mask_timestamps": await bridge.probe(duration=args.duration)})
        elif args.command == "request":
            async with bridge.normal() as client:
                await _request(client, args, stdout)
        elif args.command in {
            "read-block",
            "read-device",
            "read-sensors",
            "set-climate-mode",
            "set-thermostat-target",
            "set-device-group",
            "set-snowmelt-group",
            "write-block",
            "scene-response",
            "read-schedule",
            "write-schedule",
        }:
            return await _native_addressed(bridge, args, emit)
        elif args.command in {
            "read-status",
            "watch-native",
            "watch-climate",
            "read-water",
        }:
            return await _native_status(bridge, args, emit)
        elif args.command == "send":
            async with bridge.stream(diagnostics=False) as stream:
                try:
                    async with asyncio.timeout(10):
                        result = await stream.send(args.port, bytes.fromhex(args.body))
                except TimeoutError as err:
                    msg = "no transmit result; transmission outcome unknown"
                    raise CliError(msg) from err
            emit(
                {
                    "tx_result": {
                        "port": result.port,
                        "status": result.status.name,
                        "reason": result.reason,
                    },
                },
            )
            if result.status is not TxStatus.ACK:
                msg = "transmission was not acknowledged"
                raise CliError(msg)
        else:
            await _native_capture(bridge, args, emit)
    return 0


async def _native_status(
    bridge: BridgeClient,
    args: argparse.Namespace,
    emit: Callable[[object], None],
) -> int:
    ack_ports = (
        (args.port,) if getattr(args, "ack", False) else getattr(args, "ack_port", ())
    )
    async with bridge.stream(ack_ports=ack_ports, diagnostics=False) as stream:
        if args.command == "watch-native":
            return await watch_all_from_args(stream, args, emit)
        if args.command == "watch-climate":
            return await watch_from_args(stream, args, emit)
        if args.command == "read-status":
            answer = await read_status_from_args(stream, args)
            emit(_status_fields(answer))
            return int(not answer.answered)
        water = await read_water(
            stream,
            args.ports,
            duration=args.duration,
            timeout=args.timeout,
            plant_settings=not args.no_plant_settings,
            source=None
            if args.passive
            else NativeAddress(args.source_subnet, args.source_node),
        )
    emit(
        {
            "water_sources": [water_fields(s) for s in water.sources],
            "plant_configurations": [
                {**asdict(path), **plant_register_fields(settings)}
                for path, settings in water.plant_configurations
            ],
            "master_queries": [
                _status_fields(q)["status_read"] for q in water.master_queries
            ],
            "block_reads": [_batch_fields(b)["block_read"] for b in water.block_reads],
            "host_records_dropped": water.dropped,
        },
    )
    return int(not any(s.has_measurements for s in water.sources))


async def _broadcast(
    args: argparse.Namespace,
    emit: Callable[[object], None],
) -> int:
    announcement, description = prepare_broadcast(args)
    if not args.timeout > 0:
        msg = "broadcast send timeout must be positive"
        raise CliError(msg)
    if not args.confirm:
        emit({"preview": description})
        return 0
    async with open_gateway(_endpoint(args), auto_update=args.auto_update) as bridge:
        if not isinstance(bridge, BridgeClient):
            msg = "expected native firmware"
            raise CliError(msg)
        async with bridge.stream(diagnostics=False) as stream:
            return await send_broadcast(stream, announcement, args.timeout, emit)


def _native_edits(items: Sequence[str]) -> dict[NativeField, int]:
    edits: dict[NativeField, int] = {}
    for item in items:
        key, sep, value = item.partition("=")
        if not sep or key.upper() not in NativeField.__members__:
            msg = f"expected a known native FIELD=RAW, got {item!r}"
            raise CliError(msg)
        field = NativeField[key.upper()]
        if field in edits:
            msg = f"duplicate native field: {key}"
            raise CliError(msg)
        edits[field] = _int_value(value)
    return edits


def _emit_derived(snapshot: NativeSnapshot, emit: Callable[[object], None]) -> None:
    for week in snapshot.schedule_directory.schedules:
        emit({"weekly_schedule": schedule_fields(week)})
    for device in snapshot.thermostat_sensors:
        emit({"thermostat_sensors": sensor_fields(device)})
    state = snapshot.observed_state()
    for path, plant in state.plant_configurations:
        emit({"plant_registers": {**asdict(path), **plant_register_fields(plant)}})
    for climate in state.thermostats:
        emit({"thermostat_climate": climate_fields(climate)})
    for snow in state.snowmelt_devices:
        emit({"snowmelt": snow_fields(snow)})


async def _native_addressed(
    bridge: BridgeClient,
    args: argparse.Namespace,
    emit: Callable[[object], None],
) -> int:
    address = NativeAddress(args.subnet, args.node)
    source = NativeAddress(args.source_subnet, args.source_node)
    async with bridge.stream(
        ack_ports=(args.port,) if args.ack else (),
        diagnostics=False,
    ) as stream:
        if args.command == "set-device-group":
            return await device_group(stream, args, address, source, emit)
        if args.command == "set-snowmelt-group":
            return await snow_group_from_args(stream, args, address, source, emit)
        if args.command == "read-device":
            snapshot = await read_native_snapshot(
                stream,
                [NativeDevice(args.port, address)],
                source=source,
                timeout=args.timeout,
                attempts=getattr(args, "block_attempts", 2),
                on_block=lambda b: emit(_batch_fields(b)),
                on_status=lambda s: emit(_status_fields(s)),
            )
            _emit_derived(snapshot, emit)
            return 0
        if args.command == "read-sensors":
            sample = await read_thermostat_sensors(
                stream,
                args.port,
                address,
                source=source,
                timeout=args.timeout,
            )
            emit(
                sensor_fields(sample.device)
                | {"unanswered_blocks": sample.unanswered_blocks},
            )
            return int(bool(sample.unanswered_blocks))
        if args.command == "read-schedule":
            week = await read_weekly_schedule(
                stream,
                args.port,
                schedule_target(address, args),
                source=source,
                timeout=args.timeout,
            )
            emit(schedule_fields(week))
            binding = week.config.binding
            return int(
                binding is None or (binding.owns_table and not week.table_complete),
            )
        if args.command == "set-climate-mode":
            result = await stream.write_thermostat_mode(
                args.port,
                address,
                ThermostatMode[args.mode.upper()],
                source=source,
                timeout=args.timeout,
                preview=not args.confirm,
            )
        elif args.command == "set-thermostat-target":
            result = await stream.write_thermostat_target(
                args.port,
                address,
                ThermostatTargetChange(
                    ThermostatTarget[args.target.upper()],
                    TargetSlot[args.slot.upper()],
                    args.value,
                ),
                source=source,
                timeout=args.timeout,
                preview=not args.confirm,
            )
        elif args.command == "write-schedule":
            result = await stream.write_schedule_time(
                args.port,
                schedule_target(address, args),
                schedule_change(args),
                source=source,
                timeout=args.timeout,
                preview=not args.confirm,
            )
        elif args.command == "scene-response":
            result = await write_scene_response(
                stream,
                args.port,
                address,
                NativeScene(args.scene),
                SceneResponse[args.response.upper()],
                source=source,
                timeout=args.timeout,
                preview=not args.confirm,
            )
        elif args.command == "write-block":
            result = await stream.write_block(
                args.port,
                NativeBlockQuery(address, args.block, args.selector),
                _native_edits(args.fields),
                source=source,
                timeout=args.timeout,
                preview=not args.confirm,
            )
        else:
            query = NativeBlockQuery(address, args.block, args.selector)
            try:
                async with asyncio.timeout(args.timeout):
                    report = await stream.read_block(args.port, query, source=source)
            except TimeoutError as err:
                msg = f"no matching block {args.block:#x} report before the deadline"
                raise CliError(msg) from err
            emit(_frame_fields(report))
            return 0
        emit(_write_fields(result))
        return int(
            result.status
            not in {
                NativeWriteStatus.UNCHANGED,
                NativeWriteStatus.PREVIEW,
                NativeWriteStatus.CONFIRMED,
            },
        )


def _write_fields(result: NativeWriteResult) -> dict[str, object]:
    tx = result.tx_result
    return {
        "status": result.status.value,
        "before": _frame_fields(result.before),
        "proposed": result.proposed.to_bytes().hex(),
        "tx_status": tx.status.name if tx else None,
        "tx_reason": tx.reason if tx else None,
        "readback": _batch_fields(result.readback)["block_read"]
        if result.readback
        else None,
    }


async def _native_capture(
    bridge: BridgeClient,
    args: argparse.Namespace,
    emit: Callable[[object], None],
) -> None:
    duration = args.duration
    limit = getattr(args, "limit", None)
    nodes: set[tuple[int, int, int]] = set()
    frames = 0
    observed = NativeState()
    if (duration is not None and duration <= 0) or (limit is not None and limit < 1):
        msg = "duration and frame limit must be positive"
        raise CliError(msg)
    if args.command == "dump":
        emit(
            {
                "panel": asdict(await bridge.panel()),
                "revision": await bridge.revision(),
            },
        )

    def capture(frame: CaptureRecord) -> None:
        nonlocal frames
        if args.command == "dump":
            observed.observe(frame)
        if isinstance(frame, CaptureGap):
            emit({"host_records_dropped": frame.dropped})
        elif isinstance(frame, PortError):
            emit({"port": frame.port, "port_error": frame.reason})
        elif isinstance(frame, InvalidCapture):
            if args.command != "discover":
                emit(
                    {
                        "invalid": frame.reason,
                        "port": frame.port,
                        "timestamp_ms": frame.timestamp_ms,
                        "bit_count": frame.bit_count,
                        "bits": frame.bits.hex(),
                    },
                )
        else:
            frames += 1
            if args.command in {"discover", "dump"}:
                nodes.add((frame.port, frame.subnet, frame.node))
            if args.command != "discover":
                emit(_frame_fields(frame))

    summary = None
    async with bridge.stream(ack_ports=getattr(args, "ack_port", ())) as stream:
        if args.command in {"discover", "dump"}:
            summary = await discover_native(
                stream,
                duration=duration,
                source=None if args.passive else NativeAddress(0, 0),
                subnets=args.probe_subnet,
                on_record=capture,
            )
        else:
            window = asyncio.timeout(duration)
            try:
                async with window:
                    while limit is None or frames < limit:
                        capture(await stream.read())
            except TimeoutError:
                if not window.expired():
                    raise
    if summary is not None:
        emit(
            {
                "observed_nodes": sorted(nodes),
                "devices": [
                    _native_device_fields(device) for device in summary.devices
                ],
                "identity_probes": [
                    {
                        "port": p.port,
                        "subnet": p.subnet,
                        "tx_status": p.result.status.name
                        if p.result is not None
                        else None,
                        "reason": p.result.reason if p.result is not None else None,
                    }
                    for p in summary.probes
                ],
                "host_records_dropped": summary.host_records_dropped,
            },
        )
    if args.command in {"diagnostics", "dump"}:
        _emit_capture_summary(stream, observed, emit)
    if args.command == "dump" and not args.passive and summary and summary.devices:
        # A discovery deadline can cancel a probe before its TxResult. The
        # previous stream has drained that send before we reuse its port.
        async with bridge.stream(ack_ports=args.ack_port) as scan:
            # A new session must not inherit the previous loop mapping.
            observed = NativeState()
            await _dump_blocks(
                scan,
                summary.devices,
                getattr(args, "block_timeout", 20),
                getattr(args, "block_attempts", 2),
                capture,
                emit,
                include_line_status=getattr(args, "line_status", True),
            )
        _emit_capture_summary(scan, observed, emit)


def _emit_capture_summary(
    stream: BridgeStream,
    observed: NativeState,
    emit: Callable[[object], None],
) -> None:
    for diagnostic in stream.diagnostics.values():
        emit(asdict(diagnostic))
    if sources := observed.water_sources:
        emit({"water_sources": [water_fields(s) for s in sources]})
    for path, plant in observed.plant_configurations:
        emit({"plant_registers": {**asdict(path), **plant_register_fields(plant)}})
    for thermostat in observed.thermostats:
        emit({"thermostat_climate": climate_fields(thermostat)})
    for snow in observed.snowmelt_devices:
        emit({"snowmelt": snow_fields(snow)})
    for path in observed.paths:
        calendar = observed.schedule(path)
        if calendar and (calendar.schedules or calendar.signals):
            emit({"native_calendar": {**asdict(path), **calendar_fields(calendar)}})


async def _dump_blocks(
    stream: BridgeStream,
    devices: Sequence[NativeDevice],
    timeout: float,
    attempts: int,
    capture: Callable[[CaptureRecord], None],
    emit: Callable[[object], None],
    *,
    include_line_status: bool,
) -> None:
    async def listen() -> None:
        while True:
            capture(await stream.read())

    async def sample() -> None:
        snapshot = await read_native_snapshot(
            stream,
            devices,
            timeout=timeout,
            attempts=attempts,
            include_line_status=include_line_status,
            on_block=lambda b: emit(_batch_fields(b)),
            on_status=lambda s: emit(_status_fields(s)),
        )
        _emit_derived(snapshot, emit)

    tasks = (asyncio.create_task(listen()), asyncio.create_task(sample()))
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def _status_fields(batch: NativeStatusBatch) -> dict[str, object]:
    result = batch.tx_result
    return {
        "status_read": {
            "port": batch.port,
            "peer": asdict(batch.peer) if batch.peer else None,
            "service": batch.query.kind.name.lower(),
            "request_data": batch.query.payload.hex(),
            "answered": batch.answered,
            "complete": batch.complete,
            "missing_keys": batch.missing_keys,
            "expected_count": batch.expected_count,
            "tx_status": result.status.name if result else None,
            "tx_reason": result.reason if result else None,
            "reports": [_frame_fields(f) for f in batch.reports],
        },
    }


def _batch_fields(batch: NativeBlockBatch) -> dict[str, object]:
    result = batch.tx_result
    return {
        "block_read": {
            "port": batch.port,
            "subnet": batch.query.target.subnet,
            "block": batch.query.block_id,
            "selector": batch.query.selector,
            "target_mask": batch.query.target.request_mask(),
            "tx_status": result.status.name if result is not None else None,
            "tx_reason": result.reason if result is not None else None,
            "missing_nodes": batch.missing_nodes,
            "reports": [_frame_fields(f) for f in batch.reports],
        },
    }


def _frame_fields(frame: NativeFrame) -> dict[str, object]:
    return {
        "port": frame.port,
        "timestamp_ms": frame.timestamp_ms,
        "id": frame.message_id,
        "subnet": frame.subnet,
        "node": frame.node,
        "tha_address": frame.message.source.tha_address(frame.port),
        "body": frame.body.hex(),
        "fields": frame.decoded,
    }


def _native_device_fields(device: NativeDevice) -> dict[str, object]:
    identity = device.identity
    product = identity.product if identity else None
    return {
        "port": device.port,
        "subnet": device.address.subnet,
        "node": device.address.node,
        "identity": asdict(identity) if identity else None,
        "model": product.model if product else None,
        "family": product.family if product else None,
        "candidate_blocks": product.candidate_blocks if product else (),
        "reported_blocks": sorted(device.reported_blocks),
        "reported_selectors": sorted(device.reported_selectors),
    }


def _add_scan_options(parser: argparse.ArgumentParser) -> None:
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--passive",
        action="store_true",
        help="send no native identity requests",
    )
    parser.add_argument(
        "--ack-port",
        action="append",
        type=int,
        choices=TN4_PORTS,
        default=[],
        help="link-ACK valid incoming frames on this port (repeatable)",
    )
    mode.add_argument(
        "--probe-subnet",
        action="append",
        type=_probe_subnet,
        default=[],
        metavar="PORT:SUBNET",
        help="also query a known quiet native subnet (repeatable)",
    )


def _probe_subnet(value: str) -> tuple[int, int]:
    port, _, subnet = value.partition(":")
    try:
        physical = int(port, 0)
        target = NativeAddress(int(subnet, 0), 0)
        if physical in TN4_PORTS:
            return physical, target.subnet
    except ValueError:
        pass
    msg = "expected PORT:SUBNET with port 0..4 and subnet 0..255"
    raise argparse.ArgumentTypeError(msg)


def client_from_args(args: argparse.Namespace) -> Tekmar482Client:
    """Create a client from parsed CLI transport arguments."""
    if args.tcp is not None:
        host, port = parse_host_port(args.tcp)
        return Tekmar482Client.tcp(host, port)
    if args.serial is not None:
        return Tekmar482Client.serial(args.serial, baudrate=args.baudrate)

    msg = "one transport is required"
    raise CliError(msg)


def parse_host_port(value: str) -> tuple[str, int]:
    """Parse `HOST:PORT`, including bracketed IPv6 hosts."""
    if value.startswith("["):
        host, separator, rest = value[1:].partition("]")
        if not separator or not rest.startswith(":"):
            msg = f"invalid host:port value: {value!r}"
            raise argparse.ArgumentTypeError(msg)
        port_text = rest[1:]
    else:
        host, separator, port_text = value.rpartition(":")
        if not separator:
            msg = f"invalid host:port value: {value!r}"
            raise argparse.ArgumentTypeError(msg)

    if not host:
        msg = f"missing host in value: {value!r}"
        raise argparse.ArgumentTypeError(msg)

    try:
        port = int(port_text, 10)
    except ValueError as err:
        msg = f"invalid port in value: {value!r}"
        raise argparse.ArgumentTypeError(msg) from err

    if not 1 <= port <= _MAX_TCP_PORT:
        msg = f"port out of range in value: {value!r}"
        raise argparse.ArgumentTypeError(msg)

    return host, port


def parse_fields(values: Sequence[str]) -> dict[str, int]:
    """Parse command-line FIELD=VALUE pairs."""
    fields: dict[str, int] = {}
    for item in values:
        key, separator, value = item.partition("=")
        if not separator or not key:
            msg = f"field must be KEY=VALUE: {item!r}"
            raise CliError(msg)
        fields[key] = _int_value(value)
    return fields


def parse_service(value: str) -> TrpcServiceId:
    """Parse a CLI service name or numeric service ID."""
    try:
        return service_from_name(value)
    except TekmarError:
        return _int_value(value)


def parse_method(value: str) -> TrpcMethodId:
    """Parse a CLI method name or numeric method ID."""
    try:
        return method_from_name(value)
    except TekmarError:
        return _int_value(value)


async def _gateway_info(
    client: Tekmar482Client,
    args: argparse.Namespace,
    stdout: TextIO,
) -> None:
    info = await client.get_gateway_info(timeout=args.timeout)
    if args.json:
        _write_json(gateway_info_to_dict(info), stdout)
    else:
        stdout.write(f"firmware_revision: {info.firmware_revision}\n")
        stdout.write(f"protocol_version: {info.protocol_version}\n")


async def _discover(
    client: Tekmar482Client,
    args: argparse.Namespace,
    stdout: TextIO,
) -> None:
    result = await client.discover(
        timeout=args.timeout,
        manage_reporting=args.manage_reporting,
        setback_enable=args.setback_enable,
    )
    if args.json:
        _write_json(discovery_to_dict(result), stdout)
    else:
        stdout.write(f"firmware_revision: {result.gateway.firmware_revision}\n")
        stdout.write(f"protocol_version: {result.gateway.protocol_version}\n")
        if not result.devices:
            stdout.write("devices: none\n")
            return

        stdout.write("devices:\n")
        for device in result.devices:
            model = device.model or "unknown"
            kind = device.kind or "unknown"
            raw = device.attributes.raw
            attributes = f"0x{raw:04X}" if raw is not None else "unknown"
            stdout.write(
                f"  - address={device.address} type_code={device.type_code} "
                f"model={model} kind={kind} version={device.version} "
                f"attributes={attributes} "
                f"setback_events={device.setback_events}\n",
            )


async def _dump(
    client: Tekmar482Client,
    args: argparse.Namespace,
    stdout: TextIO,
) -> None:
    snapshot = await client.dump_available_info(
        timeout=args.timeout,
        inventory_timeout=args.inventory_timeout,
        include_setbacks=args.include_setbacks,
        write_delay=args.write_delay,
    )
    if args.json:
        _write_json(available_info_to_dict(snapshot), stdout)
    else:
        gateway = snapshot.gateway
        decoded_gateway = gateway_snapshot_to_dict(gateway)["decoded"]
        stdout.write(f"firmware_revision: {gateway.info.firmware_revision}\n")
        stdout.write(f"protocol_version: {gateway.info.protocol_version}\n")
        stdout.write(
            f"outdoor_temp: {gateway.outdoor_temp} "
            f"decoded={decoded_gateway['outdoor_temp']}\n",
        )
        stdout.write(
            f"network_error: {gateway.network_error} "
            f"decoded={decoded_gateway['network_error']}\n",
        )
        stdout.write(f"reporting_enabled: {gateway.reporting_enabled}\n")
        stdout.write(f"reporting_dip_on: {gateway.reporting_dip_on}\n")
        stdout.write(f"setback_enabled: {gateway.setback_enabled}\n")
        stdout.write("setpoint_groups:\n")
        stdout.writelines(
            f"  {group_id}: {enabled}\n"
            for group_id, enabled in (gateway.setpoint_groups or {}).items()
        )
        if not snapshot.devices:
            stdout.write("devices: none\n")
            return
        stdout.write("devices:\n")
        for device in snapshot.devices:
            info = device.info
            stdout.write(
                f"  - address={info.address} type_code={info.type_code} "
                f"model={info.model or 'unknown'} kind={info.kind or 'unknown'}\n",
            )
            decoded_values = device.decoded_values
            for key, value in device.values.items():
                if key in decoded_values:
                    stdout.write(
                        f"      {key}: {value} decoded={decoded_values[key]}\n",
                    )
                else:
                    stdout.write(f"      {key}: {value}\n")


async def _monitor(
    client: Tekmar482Client,
    args: argparse.Namespace,
    stdout: TextIO,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = None if args.duration is None else loop.time() + args.duration
    count = 0

    while True:
        if args.limit is not None and count >= args.limit:
            return

        remaining = None if deadline is None else deadline - loop.time()
        if remaining is not None and remaining <= 0:
            return

        timeout = (
            args.read_timeout
            if remaining is None
            else min(args.read_timeout, remaining)
        )
        message = await client.read_message(timeout=timeout)
        if message is None:
            continue

        count += 1
        if args.json:
            _write_json(message_to_dict(message), stdout)
        else:
            stdout.write(f"{message}\n")


async def _request(
    client: Tekmar482Client,
    args: argparse.Namespace,
    stdout: TextIO,
) -> None:
    fields = parse_fields(args.fields)
    service = parse_service(args.service)
    method = parse_method(args.method)
    response_method = (
        None if args.response_method is None else parse_method(args.response_method)
    )
    wait = args.wait
    if wait is None:
        wait = service == TrpcService.REQUEST

    command = TrpcCommand(service, method, fields)
    if wait:
        response_match = ResponseMatch.for_command(
            command,
            response_method=response_method,
            address=args.match_address,
        )
        if service == TrpcService.UPDATE:
            response = await client.session.update_response(
                command,
                response_match=response_match,
                timeout=args.timeout,
            )
        else:
            response = await client.session.request_response(
                command,
                response_match=response_match,
                timeout=args.timeout,
            )
        sent = command.to_message()
        payload: dict[str, Any] = {
            "sent": message_to_dict(sent),
            "response": message_to_dict(response) if response is not None else None,
        }
    else:
        sent = await client.send(command)
        payload = {"sent": message_to_dict(sent)}

    if args.json:
        _write_json(payload, stdout)
    else:
        stdout.write(
            f"sent: {payload['sent']['service']} {payload['sent']['method']}\n",
        )
        response_obj = payload.get("response")
        if isinstance(response_obj, dict):
            stdout.write(
                f"response: {response_obj['service']} {response_obj['method']}\n",
            )
            body = response_obj.get("body")
            if isinstance(body, dict) and body:
                stdout.write(f"body: {body}\n")
        elif wait:
            stdout.write("response: timeout\n")


def _int_value(value: str) -> int:
    normalized = value.strip().lower()
    if normalized in {"true", "on", "yes"}:
        return 1
    if normalized in {"false", "off", "no"}:
        return 0
    try:
        return int(value, 0)
    except ValueError as err:
        msg = f"invalid integer value: {value!r}"
        raise argparse.ArgumentTypeError(msg) from err


def _write_json(value: object, stdout: TextIO) -> None:
    stdout.write(json.dumps(value, indent=2, sort_keys=True))
    stdout.write("\n")


def message_to_dict(message: TrpcPacket) -> dict[str, Any]:
    """Convert a tRPC message to JSON-friendly data."""
    return {
        "service": message.service_name,
        "service_id": message.service_id,
        "method": message.method_name,
        "method_id": message.method_id,
        "body": dict(message.body),
        "extra": message.extra.hex().upper(),
    }


def gateway_info_to_dict(info: GatewayInfo) -> dict[str, Any]:
    return asdict(info)


def attributes_to_dict(attributes: DeviceAttributes) -> dict[str, Any]:
    return {
        "raw": attributes.raw,
        "heat_setpoint": attributes.heat_setpoint,
        "cool_setpoint": attributes.cool_setpoint,
        "zone_heating": attributes.zone_heating,
        "zone_cooling": attributes.zone_cooling,
        "slab_setpoint": attributes.slab_setpoint,
        "fan_percent": attributes.fan_percent,
        "relative_humidity": attributes.relative_humidity,
        "humidity_setpoint_min": attributes.humidity_setpoint_min,
        "humidity_setpoint_max": attributes.humidity_setpoint_max,
        "setpoint_device": attributes.setpoint_device,
    }


def device_to_dict(device: DiscoveredDevice) -> dict[str, Any]:
    feature = device.feature
    port, bus, device_number = device.address_parts
    return {
        "address": device.address,
        "address_parts": {
            "port": port,
            "bus": bus,
            "device": device_number,
        },
        "type_code": device.type_code,
        "version": device.version,
        "model": device.model,
        "kind": str(device.kind) if device.kind is not None else None,
        "known_type": device.is_known_type,
        "attributes": attributes_to_dict(device.attributes),
        "setback_events": device.setback_events,
        "features": asdict(feature) if feature is not None else None,
        "supported_methods": {
            "heat_setpoint": device.supports_heat_setpoint,
            "cool_setpoint": device.supports_cool_setpoint,
            "slab_setpoint": device.supports_slab_setpoint,
            "fan_percent": device.supports_fan_percent,
            "humidity": device.supports_humidity,
            "mode_setting": device.supports_mode_setting,
            "setpoint_device": device.supports_setpoint_device,
        },
    }


def discovery_to_dict(result: DiscoveryResult) -> dict[str, Any]:
    return {
        "gateway": gateway_info_to_dict(result.gateway),
        "devices": [device_to_dict(device) for device in result.devices],
        "known_device_count": len(result.known_devices),
        "unknown_device_count": len(result.unknown_devices),
    }


def gateway_snapshot_to_dict(snapshot: GatewaySnapshot) -> dict[str, Any]:
    return {
        "info": gateway_info_to_dict(snapshot.info),
        "outdoor_temp": snapshot.outdoor_temp,
        "network_error": snapshot.network_error,
        "decoded": {
            "outdoor_temp": decoded_to_dict(decode_degh(snapshot.outdoor_temp)),
            "network_error": decoded_to_dict(
                decode_network_error(snapshot.network_error),
            ),
        },
        "reporting_enabled": snapshot.reporting_enabled,
        "reporting_dip_on": snapshot.reporting_dip_on,
        "setback_enabled": snapshot.setback_enabled,
        "setpoint_groups": {
            str(group_id): enabled
            for group_id, enabled in (snapshot.setpoint_groups or {}).items()
        },
    }


def device_snapshot_to_dict(snapshot: DeviceSnapshot) -> dict[str, Any]:
    return {
        "info": device_to_dict(snapshot.info),
        "values": snapshot.values,
        "decoded_values": decoded_to_dict(snapshot.decoded_values),
    }


def available_info_to_dict(snapshot: AvailableInfo) -> dict[str, Any]:
    return {
        "gateway": gateway_snapshot_to_dict(snapshot.gateway),
        "devices": [device_snapshot_to_dict(device) for device in snapshot.devices],
    }
