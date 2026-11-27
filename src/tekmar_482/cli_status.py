"""CLI forms for service-specific read-only tN4 requests."""

from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING

from ._native_payloads.climate import ThermostatMode
from ._native_payloads.queries import NativeStatusQuery, StatusRequest
from ._native_payloads.thermostat import TargetSlot, ThermostatTarget
from .constants import TN4_PORTS
from .management import BridgeManagement
from .native import NativeAddress
from .native_state import NativePath
from .native_status import read_snowmelt_status
from .native_watch import watch_climate

if TYPE_CHECKING:
    import argparse
    from collections.abc import Callable

    from .native_plant import PlantEquipment, WaterSample, WaterValue
    from .native_plant_registers import PlantRegisters
    from .native_sensors import SensorObservation
    from .native_state import NativeClimate
    from .native_status import NativeStatusBatch
    from .native_targets import ThermostatAdjustments
    from .native_watch import ClimateChange
    from .native_water import WaterSource
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


def _sample_fields[T: WaterValue](
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


def _equipment_fields(equipment: PlantEquipment) -> dict[str, object]:
    return {
        "outputs": [_sample_fields(s) for s in equipment.outputs],
        "mixing_present": _sample_fields(equipment.mixing_present),
        "mixing_report": _sample_fields(equipment.mixing_report),
        "stage_demands": [_sample_fields(s) for s in equipment.stage_demands],
        "general_demands": [_sample_fields(s) for s in equipment.general_demands],
        "relay_heartbeats": [_sample_fields(s) for s in equipment.relay_heartbeats],
        "boiler_maximum": _sample_fields(equipment.boiler_maximum),
        "water_cycle": _sample_fields(equipment.cycle),
    }


def plant_register_fields(config: PlantRegisters) -> dict[str, object]:
    return {
        "banks": [
            {**asdict(b), "available_indices": b.available_indices}
            for b in config.banks
        ],
        "runtimes": [
            {
                "register": t.register.name.lower(),
                "selector": t.selector,
                "hours": t.hours,
            }
            for t in config.runtimes
        ],
        "reports": [
            {
                "block": r.block,
                "selector": r.selector,
                "timestamp_ms": r.frame.timestamp_ms,
                "fields": r.frame.decoded,
            }
            for r in config.reports
        ],
        "missing_blocks": [{"block": b, "selector": s} for b, s in config.missing],
    }


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
        "pending_setup": sorted(source.pending_setup),
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
        "equipment": _equipment_fields(source.equipment),
    }


def climate_fields(device: NativeClimate) -> dict[str, object]:
    setup = device.setup
    target = device.active_air_target
    return {
        **asdict(device.path),
        "model": setup.product.model,
        "z_number": device.identity.z_number,
        "mode_raw": setup.mode_raw,
        "mode": setup.mode.name.lower() if setup.mode is not None else None,
        "offered_modes": [
            m.name.lower() for m in ThermostatMode if setup.offers(m) is True
        ],
        "undetermined_modes": [
            m.name.lower() for m in ThermostatMode if setup.offers(m) is None
        ],
        "heating_cph": setup.heating_cph,
        "configured_air_group": setup.air_group_id,
        "cooling_stages": setup.cooling_stages,
        "enabled_heat_pumps": setup.enabled_heat_pumps,
        "air_timestamp_ms": device.air_timestamp_ms,
        "air": device.air.fields() if device.air is not None else None,
        "active_air_target": {
            "kind": target.kind.name.lower(),
            "setback": target.setback,
            "raw": target.raw,
            "celsius": target.celsius,
        }
        if target is not None
        else None,
        "adjustments": adjustment_fields(device.adjustments),
    }


def adjustment_fields(settings: ThermostatAdjustments) -> dict[str, object]:
    fan, rh = settings.fan_setup, settings.humidity_setup
    return {
        "floor_configured": settings.floor_configured,
        "fan_adjustment_enabled": fan.enabled if fan is not None else None,
        "humidify_enabled": rh.enabled(ThermostatTarget.HUMIDIFY)
        if rh is not None
        else None,
        "dehumidify_enabled": rh.enabled(ThermostatTarget.DEHUMIDIFY)
        if rh is not None
        else None,
        "tables": {
            t.target.name.lower(): {
                "block": t.target.value,
                "unit": "celsius" if t.target.temperature else "percent",
                "slots": {
                    s.name.lower(): {"raw": t.raw(s), "value": t.value(s)}
                    for s in TargetSlot
                },
                **(
                    {"maximum_raw": t.maximum_raw}
                    if t.target is ThermostatTarget.FLOOR
                    else {}
                ),
            }
            for t in settings.tables
        },
        "room_limits": {
            t.limit.name.lower(): {s.name.lower(): t.raw(s) for s in TargetSlot}
            for t in settings.limits
        },
        "operating_reports": {
            k: {"received_ms": f.timestamp_ms, **f.decoded}
            for k, f in (
                ("fan", settings.fan.frame if settings.fan else None),
                ("slab", settings.slab.frame if settings.slab else None),
                ("humidity", settings.humidity.frame if settings.humidity else None),
            )
            if f is not None
        },
        "missing_blocks": settings.missing_blocks,
    }


async def watch_from_args(
    stream: BridgeStream,
    args: argparse.Namespace,
    emit: Callable[[object], None],
) -> int:
    def changed(change: ClimateChange) -> None:
        if change.current is None:
            emit({"climate_removed": asdict(change.path)})
        else:
            emit({"thermostat_climate": climate_fields(change.current)})

    result = await watch_climate(
        stream,
        (NativePath(*p) for p in args.device) if args.device else None,
        source=None
        if args.passive
        else NativeAddress(args.source_subnet, args.source_node),
        duration=args.duration,
        timeout=args.timeout,
        on_change=changed,
    )
    emit(
        {
            "completed_reads": result.reads,
            "missing_mode_reports": [asdict(p) for p in result.missing_mode_reports],
            "latest_reads": [
                {
                    "port": b.port,
                    "subnet": b.query.target.subnet,
                    "block": b.query.block_id,
                    "target_mask": b.query.target.request_mask(),
                    "missing_nodes": b.missing_nodes,
                    "tx_status": b.tx_result.status.name if b.tx_result else None,
                }
                for b in result.latest_reads
            ],
            "host_records_dropped": result.dropped,
            "thermostats": len(result.thermostats),
        },
    )
    return int(
        not result.thermostats
        or (not args.passive and bool(result.missing_mode_reports)),
    )


def _add_watch(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    common: argparse.ArgumentParser,
    number: Callable[[str], int],
    name: str,
    description: str,
) -> None:
    watch = subparsers.add_parser(
        name,
        parents=[common],
        help=description,
    )
    if name == "watch-native":
        watch.add_argument(
            "--no-setpoint-settings",
            action="store_true",
            help="skip application and target reads for setpoint controls",
        )
        watch.add_argument(
            "--no-io-discovery",
            action="store_true",
            help="observe I/O telemetry without querying channel directories",
        )
        watch.add_argument(
            "--no-plant-settings",
            action="store_true",
            help="skip addressed plant setup and runtime reads",
        )
        watch.add_argument(
            "--no-thermostat-targets",
            action="store_true",
            help="skip additional thermostat adjustment reads",
        )
        watch.add_argument(
            "--no-schedules",
            action="store_true",
            help="skip calendar reads and output",
        )
    watch.add_argument(
        "--device",
        action="append",
        nargs=3,
        type=number,
        metavar=("PORT", "SUBNET", "NODE"),
    )
    watch.add_argument(
        "--passive",
        action="store_true",
        help="do not request identity or settings",
    )
    watch.add_argument("--duration", type=float, help="stop after this many seconds")
    watch.add_argument(
        "--ack-port",
        action="append",
        type=int,
        choices=TN4_PORTS,
        default=[],
        help="link-ACK incoming frames on this physical port",
    )
    watch.add_argument("--source-subnet", type=number, default=0)
    watch.add_argument("--source-node", type=number, default=0)


def add_status_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    common: argparse.ArgumentParser,
    number: Callable[[str], int],
) -> None:
    _add_watch(
        subparsers,
        common,
        number,
        "watch-climate",
        "stream native thermostat mode and demand changes",
    )
    _add_watch(
        subparsers,
        common,
        number,
        "watch-native",
        "watch native devices, calendars, thermostat and snowmelt state",
    )
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
    water.add_argument(
        "--no-plant-settings",
        action="store_true",
        help="only request master topology and loop assignments",
    )
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
        args.add_argument(
            "--raw",
            action="store_true",
            help="switch to Raw mode for this request",
        )
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
    stream: BridgeStream | BridgeManagement,
    args: argparse.Namespace,
) -> NativeStatusBatch:
    kind = args.status_request
    source = NativeAddress(args.source_subnet, args.source_node)
    if kind is StatusRequest.SNOWMELT:
        if isinstance(stream, BridgeManagement):
            return await stream.read_snowmelt_status(
                args.port,
                NativeAddress(args.subnet, args.node),
                keys=args.keys,
                timeout=args.timeout,
            )
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
    if isinstance(stream, BridgeManagement):
        return await stream.read_status(
            args.port,
            query,
            peer=peer,
            timeout=args.timeout,
        )
    return await stream.read_status(
        args.port,
        query,
        source=source,
        peer=peer,
        timeout=args.timeout,
    )
