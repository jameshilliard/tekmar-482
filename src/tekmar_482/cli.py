"""Command line interface for tekmar-482."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Callable, Sequence
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, TextIO

from .client import Tekmar482Client
from .constants import DEFAULT_BAUDRATE
from .decoding import (
    decode_degh,
    decode_device_values,
    decode_network_error,
    decoded_to_dict,
)
from .exceptions import TekmarError
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
        default=0.1,
        help="delay between requests in seconds",
    )

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

    return parser


def _common_parent() -> argparse.ArgumentParser:
    parent = argparse.ArgumentParser(add_help=False)
    transport = parent.add_mutually_exclusive_group(required=True)
    transport.add_argument(
        "--tcp",
        metavar="HOST:PORT",
        help="raw TCP stream carrying binary TPCK frames",
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
        default=5.0,
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
    except (argparse.ArgumentTypeError, CliError, TekmarError, OSError) as err:
        sys.stderr.write(f"error: {err}\n")
        return 2


async def run(
    args: argparse.Namespace,
    *,
    client_factory: ClientFactory | None = None,
    stdout: TextIO = sys.stdout,
) -> int:
    """Run an already-parsed command."""
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
            stdout.write(
                f"  - address={device.address} type_code={device.type_code} "
                f"model={model} kind={kind} version={device.version} "
                f"attributes=0x{device.attributes.raw:04X} "
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
        date_time = None if gateway.date_time is None else gateway.date_time.as_dict()
        stdout.write(f"date_time: {date_time}\n")
        stdout.write(f"reporting_enabled: {gateway.reporting_enabled}\n")
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
        "date_time": None
        if snapshot.date_time is None
        else snapshot.date_time.as_dict(),
        "decoded": {
            "outdoor_temp": decoded_to_dict(decode_degh(snapshot.outdoor_temp)),
            "network_error": decoded_to_dict(
                decode_network_error(snapshot.network_error),
            ),
        },
        "reporting_enabled": snapshot.reporting_enabled,
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
        "decoded_values": decoded_to_dict(decode_device_values(snapshot.values)),
    }


def available_info_to_dict(snapshot: AvailableInfo) -> dict[str, Any]:
    return {
        "gateway": gateway_snapshot_to_dict(snapshot.gateway),
        "devices": [device_snapshot_to_dict(device) for device in snapshot.devices],
    }
