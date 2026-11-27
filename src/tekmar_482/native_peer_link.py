"""Link counters reported by a tN4 peer, not measured by the gateway."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Self

from ._native_payloads.layouts import LinkCounterBlock

if TYPE_CHECKING:
    from collections.abc import Mapping

    from .native import BlockKey, NativeFrame


def _counter(fields: Mapping[str, object], name: str) -> int | None:
    count = fields.get(name)
    return count if isinstance(count, int) else None


@dataclass(frozen=True, slots=True)
class PeerBusErrors:
    """One device-local port's reported error octets."""

    port_id: int
    stuffing_errors: int | None
    crc_failures: int | None
    missing_crc_delimiter: int | None
    missing_ack: int | None
    missing_ack_delimiter: int | None
    missing_eof: int | None
    dominant_bit_errors: int | None

    @classmethod
    def from_frame(cls, port_id: int, frame: NativeFrame) -> Self:
        fields = frame.decoded
        return cls(
            port_id,
            _counter(fields, "stuffing_errors"),
            _counter(fields, "crc_failures"),
            _counter(fields, "missing_crc_delimiter"),
            _counter(fields, "missing_ack"),
            _counter(fields, "missing_ack_delimiter"),
            _counter(fields, "missing_eof"),
            _counter(fields, "dominant_bit_errors"),
        )


@dataclass(frozen=True, slots=True)
class PeerBusEvents:
    """One device-local port's reported busy and arbitration octets."""

    port_id: int
    local_port_busy: int | None
    remote_device_busy: int | None
    arbitration_losses: int | None
    error_event_count: int | None

    @classmethod
    def from_frame(cls, port_id: int, frame: NativeFrame) -> Self:
        fields = frame.decoded
        return cls(
            port_id,
            _counter(fields, "local_port_busy"),
            _counter(fields, "remote_device_busy"),
            _counter(fields, "arbitration_losses"),
            _counter(fields, "error_event_count"),
        )


@dataclass(frozen=True, slots=True)
class PeerLinkCounters:
    """Last reported samples; None means unknown, and counts can decrease."""

    errors: tuple[PeerBusErrors, ...]
    events: tuple[PeerBusEvents, ...]
    received_message_count: int | None

    @classmethod
    def from_blocks(cls, blocks: Mapping[BlockKey, NativeFrame]) -> Self | None:
        errors: dict[int, PeerBusErrors] = {}
        events: dict[int, PeerBusEvents] = {}
        received = None
        for (block, port_id), frame in blocks.items():
            if block == LinkCounterBlock.BUS_ERRORS and port_id is not None:
                errors[port_id] = PeerBusErrors.from_frame(port_id, frame)
            elif block == LinkCounterBlock.BUS_EVENTS and port_id is not None:
                events[port_id] = PeerBusEvents.from_frame(port_id, frame)
            elif block == LinkCounterBlock.RECEIVED_MESSAGES:
                received = _counter(frame.decoded, "received_message_count")
        if not errors and not events and received is None:
            return None
        return cls(
            tuple(errors[p] for p in sorted(errors)),
            tuple(events[p] for p in sorted(events)),
            received,
        )
