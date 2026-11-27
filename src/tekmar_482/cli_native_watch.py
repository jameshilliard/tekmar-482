"""CLI presentation of identified devices and their independently reported state."""

from __future__ import annotations

from dataclasses import asdict
from functools import partial
from typing import TYPE_CHECKING

from ._native_payloads.io import io_terminal
from .cli_schedule import schedule_fields
from .cli_status import climate_fields, plant_register_fields, water_fields
from .management import BridgeManagement
from .native import NativeAddress
from .native_snow_control import SnowmeltGroupStatus, SnowmeltOperation
from .native_state import NativePath
from .native_watch import watch_native

if TYPE_CHECKING:
    import argparse
    from collections.abc import Callable

    from ._native_payloads.line import LineReportKind
    from ._observation import ObservationStream
    from .native_calendar import NativeCalendar
    from .native_io import IoDirectory
    from .native_plant_registers import PlantRegisters
    from .native_setpoint import NativeSetpointControl
    from .native_snowmelt import NativeSnowmelt, SnowmeltChannel
    from .native_state import NativeFaultReport, NativeLineReport, NativeState
    from .native_watch import NativeWatchResult
    from .raw import BridgeStream


def setpoint_fields(control: NativeSetpointControl) -> dict[str, object]:
    identity, app = control.identity, control.assignment.application
    frame, readings = control.status_frame, control.readings
    return {
        **asdict(control.path),
        "model": identity.product.model if identity and identity.product else None,
        "application": app.name.lower() if app is not None else None,
        "application_setup": control.application_setup.decoded
        if control.application_setup
        else None,
        "operation_setup": control.operation_setup.decoded
        if control.operation_setup
        else None,
        "remote_group": control.assignment.remote_group,
        "stored_targets": asdict(control.targets) if control.targets else None,
        "manual_override_time_raw": control.manual_override_time_raw,
        "status": {
            **frame.decoded,
            "received_ms": frame.timestamp_ms,
            "body": frame.body.hex(),
        }
        if frame
        else None,
        "live_readings": asdict(readings) if readings else None,
    }


def fault_fields(report: NativeFaultReport) -> dict[str, object]:
    return {
        **asdict(report.path),
        **report.fault.fields(),
        "raw": report.fault.raw,
        "received_ms": report.frame.timestamp_ms,
        "body": report.frame.body.hex(),
        "extension_hex": report.extension.hex(),
    }


def line_fields(report: NativeLineReport) -> dict[str, object]:
    return {
        **asdict(report.path),
        "kind": report.kind.name.lower(),
        "message_id": report.kind.value,
        **report.frame.decoded,
        "received_ms": report.frame.timestamp_ms,
        "body": report.frame.body.hex(),
    }


class _LineChanges:
    def __init__(self) -> None:
        self._previous: dict[
            NativePath,
            dict[tuple[LineReportKind, int | None], NativeLineReport],
        ] = {}

    def emit(
        self,
        path: NativePath,
        state: NativeState,
        emit: Callable[[object], None],
    ) -> None:
        if not (reports := state.line_reports_for(path)):
            if self._previous.pop(path, None) is not None:
                emit({"line_reports_unknown": asdict(path)})
            return
        previous = self._previous.setdefault(path, {})
        for report in reports:
            key = report.kind, report.group
            if previous.get(key) != report:
                previous[key] = report
                emit({"line_report": line_fields(report)})


def io_fields(directory: IoDirectory, model: str | None = None) -> dict[str, object]:
    endpoint = directory.endpoint
    channels = []
    for channel in directory.channels:
        sample, frame = channel.sample, channel.frame
        fields: dict[str, object] = {
            **sample.fields(),
            "received_ms": frame.timestamp_ms,
            "body": frame.body.hex(),
        }
        if (
            model is not None
            and frame.subnet == endpoint.subnet
            and endpoint.node in (frame.node, frame.node & 31)
            and (name := io_terminal(model, sample.channel)) is not None
        ):
            fields["board_terminal"] = name
        channels.append(fields)
    return {
        "endpoint": asdict(endpoint),
        "advertised_count": directory.advertised_count,
        "complete": directory.complete,
        "channels": channels,
    }


def _channel_fields(channel: SnowmeltChannel) -> dict[str, object]:
    return {
        "role": channel.address.role.name.lower(),
        "number": channel.address.number,
        "received_ms": channel.last_frame.timestamp_ms,
        "samples": [
            {
                "key": s.pair.key,
                "raw": s.pair.raw,
                "received_ms": s.timestamp_ms,
                **s.pair.fields(),
            }
            for s in channel.samples
        ],
    }


def snow_fields(device: NativeSnowmelt) -> dict[str, object]:
    address, app, missing = (
        device.setup.primary,
        device.setup.application,
        device.missing_keys,
    )
    product = device.identity.product
    return {
        **asdict(device.path),
        "model": product.model if product else None,
        "setup": asdict(device.setup),
        "application": app.name.lower() if app is not None else None,
        "primary": {"role": address.role.name.lower(), "number": address.number}
        if address
        else None,
        "missing_keys": list(missing) if missing is not None else None,
        "channels": [_channel_fields(c) for c in device.channels],
    }


async def snow_group_from_args(
    stream: BridgeManagement | BridgeStream,
    args: argparse.Namespace,
    address: NativeAddress,
    source: NativeAddress,
    emit: Callable[[object], None],
) -> int:
    set_group = (
        stream.set_snowmelt_group
        if isinstance(stream, BridgeManagement)
        else partial(stream.set_snowmelt_group, source=source)
    )
    result = await set_group(
        args.port,
        address,
        SnowmeltOperation[args.operation.upper()],
        enabled=args.state == "on",
        expected_group=args.expect_group,
        timeout=args.timeout,
        preview=not args.confirm,
    )
    state = result.observed_state
    emit(
        {
            "status": result.status.value,
            "zone": result.zone,
            "group": result.group,
            "operation": result.operation.name.lower(),
            "enabled": result.enabled,
            "proposed": result.message.to_bytes().hex(),
            "tx_status": result.tx_result.status.name if result.tx_result else None,
            "tx_reason": result.tx_result.reason if result.tx_result else None,
            "operating_state": state.name.lower() if state is not None else None,
            "matches_requested_state": result.matches_requested_state,
        },
    )
    return int(
        result.status is not SnowmeltGroupStatus.PREVIEW
        and result.matches_requested_state is not True,
    )


def _emit_fault(
    path: NativePath,
    fault: NativeFaultReport | None,
    previous: dict[NativePath, NativeFaultReport],
    emit: Callable[[object], None],
) -> None:
    if fault is not None and previous.get(path) != fault:
        previous[path] = fault
        emit({"network_fault": fault_fields(fault)})
    elif fault is None and previous.pop(path, None) is not None:
        emit({"network_fault_unknown": asdict(path)})


async def watch_all_from_args(
    stream: ObservationStream,
    args: argparse.Namespace,
    emit: Callable[[object], None],
) -> int:
    previous: dict[NativePath, dict[str, object]] = {}
    previous_plants: dict[NativePath, PlantRegisters] = {}
    previous_setpoints: dict[NativePath, NativeSetpointControl] = {}
    previous_io: dict[NativePath, tuple[str | None, tuple[IoDirectory, ...]]] = {}
    previous_faults: dict[NativePath, NativeFaultReport] = {}
    lines = _LineChanges()

    def changed(path: NativePath, state: NativeState) -> None:
        lines.emit(path, state, emit)
        _emit_fault(path, state.fault_report(path), previous_faults, emit)
        identity = state.identity(path)
        model = identity.product.model if identity and identity.product else None
        if dirs := state.io_for(path):
            if (model, dirs) != previous_io.get(path):
                previous_io[path] = model, dirs
                emit(
                    {
                        "native_io": {
                            **asdict(path),
                            "directories": [io_fields(d, model) for d in dirs],
                        },
                    },
                )
        elif previous_io.pop(path, None) is not None:
            emit({"native_io_removed": asdict(path)})
        control = state.setpoint_control(path)
        if control is not None and control != previous_setpoints.get(path):
            previous_setpoints[path] = control
            emit({"setpoint_control": setpoint_fields(control)})
        elif control is None and previous_setpoints.pop(path, None) is not None:
            emit({"setpoint_control_removed": asdict(path)})
        if (snow := state.snowmelt(path)) is not None:
            emit({"snowmelt": snow_fields(snow)})
        elif (climate := state.climate(path)) is not None:
            emit({"thermostat_climate": climate_fields(climate)})
        elif channels := state.snowmelt_channels(path):
            emit(
                {
                    "observed_snowmelt": {
                        **asdict(path),
                        "channels": [_channel_fields(c) for c in channels],
                    },
                },
            )
        elif (identity := state.identity(path)) is not None:
            emit({"native_identity": {**asdict(path), **asdict(identity)}})
        if (water := state.water_source(path)) is not None:
            emit({"water_source": water_fields(water)})
        plant = state.plant_registers(path)
        if plant is not None and plant != previous_plants.get(path):
            previous_plants[path] = plant
            emit({"plant_registers": {**asdict(path), **plant_register_fields(plant)}})
        elif plant is None and previous_plants.pop(path, None) is not None:
            emit({"plant_registers_removed": asdict(path)})
        if not args.no_schedules and (calendar := state.schedule(path)) is not None:
            view = calendar_fields(calendar)
            if (path in previous or any(view.values())) and view != previous.get(path):
                previous[path] = view
                emit({"native_calendar": {**asdict(path), **view}})
        if path not in state.paths:
            previous.pop(path, None)
            emit({"native_removed": asdict(path)})

    paths = tuple(NativePath(*p) for p in args.device) if args.device else None
    result = await watch_native(
        stream,
        paths,
        ports=args.ports,
        source=None
        if args.passive
        else NativeAddress(args.source_subnet, args.source_node),
        duration=args.duration,
        timeout=args.timeout,
        stale_after=args.stale_after,
        schedules=not args.no_schedules,
        thermostat_targets=not args.no_thermostat_targets,
        plant_settings=not args.no_plant_settings,
        setpoint_settings=not args.no_setpoint_settings,
        io_discovery=not args.no_io_discovery,
        on_change=changed,
    )
    expected = paths or (() if args.passive else result.state.paths)
    return _watch_summary(result, expected, args, emit)


def _watch_summary(
    result: NativeWatchResult,
    paths: tuple[NativePath, ...],
    args: argparse.Namespace,
    emit: Callable[[object], None],
) -> int:
    state = result.state
    unidentified = [asdict(p) for p in paths if p.node and state.identity(p) is None]
    no_mode = [asdict(t.path) for t in state.thermostats if t.setup.mode_raw is None]
    no_targets = (
        []
        if args.no_thermostat_targets
        else [
            {**asdict(t.path), "missing_blocks": t.adjustments.missing_blocks}
            for t in state.thermostats
            if t.adjustments.missing_blocks
        ]
    )
    no_snow = [
        {
            **asdict(d.path),
            "missing_keys": None if d.missing_keys is None else list(d.missing_keys),
        }
        for d in state.snowmelt_devices
        if d.missing_keys != b""
    ]
    no_plants = (
        []
        if args.no_plant_settings
        else [
            {
                **asdict(p),
                "missing_blocks": [
                    {"block": b, "selector": s} for b, s in settings.missing
                ],
            }
            for p, settings in state.plant_configurations
            if settings.missing
        ]
    )
    no_schedule = (
        []
        if args.no_schedules
        else [
            {**asdict(p), **calendar_fields(c)}
            for p in state.paths
            if (c := state.schedule(p)) is not None and c.incomplete
        ]
    )
    no_io = [
        {**asdict(p), "endpoint": asdict(d.endpoint)}
        for p, d in state.io_directories
        if not d.complete and not args.no_io_discovery
    ]
    no_setpoints = [
        {**asdict(p), "missing_blocks": [b for b, _ in missing]}
        for p in state.paths
        if not args.no_setpoint_settings
        and (missing := state.missing_setpoint_reads(p))
    ]
    emit(
        {
            "completed_reads": result.reads,
            "capture_records_dropped": state.dropped,
            "observed_paths": [asdict(p) for p in state.paths],
            "missing_identities": unidentified,
            "missing_mode_reports": no_mode,
            "incomplete_thermostat_targets": no_targets,
            "incomplete_plant_settings": no_plants,
            "incomplete_setpoint_settings": no_setpoints,
            "incomplete_snowmelt": no_snow,
            "incomplete_schedules": no_schedule,
            "incomplete_io": no_io,
            "status_reads": [
                {
                    "port": s.port,
                    "peer": asdict(s.peer) if s.peer else None,
                    "service": s.query.kind.name.lower(),
                    "payload": s.query.payload.hex(),
                    "requested_keys": list(s.query.keys),
                    "covered": sorted(s.covered),
                    "missing_keys": list(s.missing_keys),
                    "complete": s.complete,
                    "tx_status": s.tx_result.status.name if s.tx_result else None,
                    "tx_reason": s.tx_result.reason if s.tx_result else None,
                }
                for s in result.latest_statuses
            ],
            "unanswered_blocks": [
                {
                    "port": b.port,
                    "subnet": b.query.target.subnet,
                    "block": b.query.block_id,
                    "selector": b.query.selector,
                    "missing_nodes": b.missing_nodes,
                    "tx_status": b.tx_result.status.name if b.tx_result else None,
                    "tx_reason": b.tx_result.reason if b.tx_result else None,
                }
                for b in result.latest_blocks
                if b.missing_nodes or b.tx_result is None
            ],
        },
    )
    return int(
        not state.paths
        or (
            not args.passive
            and bool(
                unidentified
                or no_mode
                or no_targets
                or no_snow
                or no_schedule
                or no_plants
                or no_setpoints
                or no_io,
            )
        ),
    )


def calendar_fields(calendar: NativeCalendar) -> dict[str, object]:
    return {
        "schedules": [schedule_fields(w) for w in calendar.schedules],
        "signals": [
            {
                **s.status.fields(),
                "system": s.status.schedule_id + 1,
                "received_ms": s.frame.timestamp_ms,
                "body": s.frame.body.hex(),
            }
            for s in calendar.signals
        ],
        "missing_blocks": [
            {"block": b, "selector": s} for b, s in calendar.missing_blocks
        ],
    }
