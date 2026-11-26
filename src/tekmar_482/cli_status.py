"""CLI forms for service-specific read-only tN4 requests."""

from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING

from ._native_payloads.queries import NativeStatusQuery, StatusRequest
from .constants import TN4_PORTS
from .native import NativeAddress
from .native_status import read_snowmelt_status

if TYPE_CHECKING:
    import argparse
    from collections.abc import Callable

    from ._native_payloads.water import DhwStatus, LoopStatus, TankStatus
    from .native_sensors import SensorObservation
    from .native_status import NativeStatusBatch
    from .native_water import WaterSample, WaterSource
    from .raw import BridgeStream


def sensor_fields(device: SensorObservation) -> dict[str, object]:
    sensors = device.sensors
    room = sensors.room
    return {
        "port": device.port,
        "subnet": device.address.subnet,
        "node": device.address.node,
        "model": sensors.product.model,
        "z_number": sensors.z_number,
        "room": None
        if room is None
        else {
            "average_raw": room.average_raw,
            "minimum_raw": room.minimum_raw,
            "maximum_raw": room.maximum_raw,
            "average_f": room.average_f,
            "minimum_f": room.minimum_f,
            "maximum_f": room.maximum_f,
        },
        "average_floor_f": sensors.average_floor_f,
        "inputs": [
            {
                "slot": s.slot.name.lower(),
                "setup_raw": s.setup_raw,
                "reported_raw": s.reported_raw,
                "status_role": None
                if s.status_role is None
                else s.status_role.name.lower(),
                "display_role": None
                if s.display_role is None
                else s.display_role.name.lower(),
                "conflict": s.conflict,
                "current_raw": s.current_raw,
                "minimum_raw": s.minimum_raw,
                "maximum_raw": s.maximum_raw,
                "current_f": s.current_f,
                "minimum_f": s.minimum_f,
                "maximum_f": s.maximum_f,
            }
            for s in sensors.inputs
        ],
    }


def _sample_fields[T: (LoopStatus, DhwStatus, TankStatus)](
    sample: WaterSample[T] | None,
) -> dict[str, object] | None:
    if sample is None:
        return None
    result: dict[str, object] = {
        "timestamp_ms": sample.timestamp_ms,
        **sample.value.fields(),
    }
    if sample.extension:
        result["extension_hex"] = sample.extension.hex()
    return result


def water_fields(source: WaterSource) -> dict[str, object]:
    assignment = source.assignment
    identity = source.identity
    return {
        "port": source.port,
        "subnet": source.address.subnet,
        "node": source.address.node,
        "identity": asdict(identity) if identity is not None else None,
        "model": identity.product.model if identity and identity.product else None,
        "topology": asdict(source.topology),
        "assignment": {
            "from": "setup" if assignment.heat_source is not None else "master_present",
            "slot": assignment.slot,
            "subnet": assignment.subnet,
            "kind": assignment.kind.value,
            "mix_number": assignment.mix_number,
        }
        if assignment is not None
        else None,
        "master_status": _sample_fields(source.master),
        "plant_loop_status": _sample_fields(source.plant),
        "dhw_status": _sample_fields(source.dhw),
        "tank_statuses": [_sample_fields(s) for s in source.tanks],
    }


def add_status_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    common: argparse.ArgumentParser,
    number: Callable[[str], int],
) -> None:
    water = subparsers.add_parser(
        "read-water",
        parents=[common],
        help="listen for boiler, mixing and tank measurements on selected ports",
    )
    water.add_argument("ports", nargs="+", type=int, choices=TN4_PORTS)
    water.add_argument(
        "--duration",
        type=float,
        default=30,
        help="capture window in seconds",
    )
    water.add_argument("--passive", action="store_true", help="do not send MasterQuery")
    water.add_argument("--source-subnet", type=number, default=0)
    water.add_argument("--source-node", type=number, default=0)
    cmd = subparsers.add_parser(
        "read-status",
        help="request live status directly from the native bus",
    )
    services = cmd.add_subparsers(dest="status_service", required=True)
    for kind in StatusRequest:
        args = services.add_parser(
            kind.name.lower().replace("_", "-"),
            parents=[common],
        )
        args.set_defaults(status_request=kind)
        args.add_argument("port", type=int, choices=TN4_PORTS)
        args.add_argument("--source-subnet", type=number, default=0)
        args.add_argument("--source-node", type=number, default=0)
        args.add_argument(
            "--ack",
            action="store_true",
            help="link-ACK incoming frames on this port",
        )
        if kind in {StatusRequest.SNOWMELT, StatusRequest.HUMIDITY}:
            args.add_argument("subnet", type=number, help="reporter's native subnet")
            args.add_argument("node", type=number, help="reporter's native node")
        else:
            args.add_argument(
                "--peer",
                type=number,
                nargs=2,
                metavar=("SUBNET", "NODE"),
                help="accept only this reporting source",
            )
        if kind is StatusRequest.SNOWMELT:
            args.add_argument(
                "--keys",
                type=bytes.fromhex,
                help="ordered key bytes in hex; default follows device application",
            )
        elif kind is StatusRequest.HUMIDITY:
            args.add_argument(
                "selector",
                type=number,
                help="raw 32-bit RHQuery selector",
            )
        elif kind in {StatusRequest.IO_CHANNEL, StatusRequest.IO_DISCOVERY}:
            args.add_argument("io_subnet", type=number)
            args.add_argument("io_node", type=number)
            if kind is StatusRequest.IO_CHANNEL:
                args.add_argument("channel", type=number)


async def read_status_from_args(
    stream: BridgeStream,
    args: argparse.Namespace,
) -> NativeStatusBatch:
    kind = args.status_request
    source = NativeAddress(args.source_subnet, args.source_node)
    if kind is StatusRequest.SNOWMELT:
        return await read_snowmelt_status(
            stream,
            args.port,
            NativeAddress(args.subnet, args.node),
            source=source,
            keys=args.keys,
            timeout=args.timeout,
        )
    peer: NativeAddress | None
    if kind is StatusRequest.HUMIDITY:
        peer = NativeAddress(args.subnet, args.node)
        query = NativeStatusQuery.humidity(peer.subnet, args.selector)
    else:
        peer = NativeAddress(*args.peer) if args.peer else None
        if kind in {StatusRequest.IO_CHANNEL, StatusRequest.IO_DISCOVERY}:
            query = NativeStatusQuery.io(
                args.io_subnet,
                args.io_node,
                getattr(args, "channel", None),
            )
        elif kind is StatusRequest.GATEWAY_ACCESS:
            query = NativeStatusQuery.gateway_access()
        else:
            query = NativeStatusQuery(kind)
    return await stream.read_status(
        args.port,
        query,
        source=source,
        peer=peer,
        timeout=args.timeout,
    )
