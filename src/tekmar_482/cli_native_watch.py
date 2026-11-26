"""CLI presentation of identified devices and their independently reported state."""

from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING

from .cli_status import climate_fields, water_fields
from .native import NativeAddress
from .native_snow_control import SnowmeltGroupStatus, SnowmeltOperation
from .native_state import NativePath
from .native_watch import watch_native

if TYPE_CHECKING:
    import argparse
    from collections.abc import Callable

    from .native_snowmelt import NativeSnowmelt, SnowmeltChannel
    from .native_state import NativeState
    from .raw import BridgeStream


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
    stream: BridgeStream,
    args: argparse.Namespace,
    address: NativeAddress,
    source: NativeAddress,
    emit: Callable[[object], None],
) -> int:
    result = await stream.set_snowmelt_group(
        args.port,
        address,
        SnowmeltOperation[args.operation.upper()],
        enabled=args.state == "on",
        expected_group=args.expect_group,
        source=source,
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


async def watch_all_from_args(
    stream: BridgeStream,
    args: argparse.Namespace,
    emit: Callable[[object], None],
) -> int:
    def changed(path: NativePath, state: NativeState) -> None:
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
        if path not in state.paths:
            emit({"native_removed": asdict(path)})

    paths = tuple(NativePath(*p) for p in args.device) if args.device else None
    result = await watch_native(
        stream,
        paths,
        source=None
        if args.passive
        else NativeAddress(args.source_subnet, args.source_node),
        duration=args.duration,
        timeout=args.timeout,
        on_change=changed,
    )
    state = result.state
    unidentified = [asdict(p) for p in paths or () if state.identity(p) is None]
    no_mode = [asdict(t.path) for t in state.thermostats if t.setup.mode_raw is None]
    no_snow = [
        {
            **asdict(d.path),
            "missing_keys": None if d.missing_keys is None else list(d.missing_keys),
        }
        for d in state.snowmelt_devices
        if d.missing_keys != b""
    ]
    emit(
        {
            "completed_reads": result.reads,
            "host_records_dropped": state.dropped,
            "observed_paths": [asdict(p) for p in state.paths],
            "missing_identities": unidentified,
            "missing_mode_reports": no_mode,
            "incomplete_snowmelt": no_snow,
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
        or (not args.passive and bool(unidentified or no_mode or no_snow)),
    )
