"""Present Normal tRPC and tN4 trace records on one host timeline."""

from __future__ import annotations

from enum import IntEnum
from typing import TYPE_CHECKING

from .constants import TN4_PORTS
from .normal_trace import GatewayTraceGap, RxFlag, TracePacket, TraceRx, TraceTx
from .raw import CaptureGap, InvalidCapture
from .trpc import TrpcPacket

if TYPE_CHECKING:
    import argparse


def add_trace_command(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    common: argparse.ArgumentParser,
) -> None:
    parser = subparsers.add_parser(
        "trace-normal",
        parents=[common],
        help="trace both sides of the running Normal-mode gateway",
    )
    operation = parser.add_mutually_exclusive_group()
    operation.add_argument(
        "--stop",
        action="store_true",
        help="disable tracing left by a disconnected host",
    )
    operation.add_argument(
        "--status",
        action="store_true",
        help="read the trace mask and lifetime drop counters",
    )
    operation.add_argument(
        "--port",
        type=int,
        choices=TN4_PORTS,
        action="append",
        help="trace this port (repeatable; default all)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        help="capture window in seconds (default until interrupted)",
    )


def _label(value: IntEnum | int) -> str | int:
    return value.name.lower() if isinstance(value, IntEnum) else value


def trace_record(event: TracePacket | CaptureGap) -> dict[str, object]:
    if isinstance(event, CaptureGap):
        return {"trace_host_gap": event.dropped}
    details: dict[str, object] = {"host_monotonic_ns": event.host_monotonic_ns}
    sample = event.decoded
    if isinstance(sample, (TraceRx, TraceTx)):
        frame = sample.frame
        details.update(
            layer="tn4",
            port=frame.port,
            gateway_ms=frame.timestamp_ms,
            message_id=frame.message_id,
            subnet=frame.subnet,
            node=frame.node,
            body_hex=frame.body.hex(),
            fields=frame.decoded,
        )
        if isinstance(sample, TraceRx):
            details.update(
                direction="rx",
                ack=bool(sample.flags & RxFlag.ACK),
                early_start=bool(sample.flags & RxFlag.EARLY_START),
                flags=int(sample.flags),
            )
        else:
            details.update(
                direction="tx",
                origin=_label(sample.origin),
                outcome="ack" if sample.failure is None else _label(sample.failure),
            )
    elif isinstance(sample, GatewayTraceGap):
        details.update(
            layer="tn4",
            trace_gateway_gap=sample.dropped,
            ports=sample.ports,
        )
    elif isinstance(sample, InvalidCapture):
        details.update(
            layer="tn4",
            direction="rx_invalid",
            port=sample.port,
            gateway_ms=sample.timestamp_ms,
            reason=_label(sample.reason),
            bit_count=sample.bit_count,
            bits_hex=sample.bits.hex(),
        )
    elif isinstance(sample, TrpcPacket):
        details.update(
            layer="trpc",
            direction="host_tx" if event.outbound else "host_rx",
            method=sample.method_name or sample.method_id,
            service=sample.service_name or sample.service_id,
            fields=sample.body,
            payload_hex=event.packet.data.hex(),
        )
    else:
        details.update(
            layer="tpck",
            direction="host_tx" if event.outbound else "host_rx",
            type=event.packet.type,
            payload_hex=event.packet.data.hex(),
        )
    return details
