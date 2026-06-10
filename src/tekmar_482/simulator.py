"""Trace replay helpers for tests and offline protocol development."""

from __future__ import annotations

import asyncio
import json
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self

from .exceptions import ConnectionClosedError, ProtocolError, TekmarError
from .packet import TYPE_TRPC, Packet
from .transport import PacketTransport
from .trpc import (
    TrpcMethodId,
    TrpcPacket,
    TrpcService,
    method_from_name,
    method_id,
    service_from_name,
    service_id,
)

_TRACE_REPLAY_CLOSED = "trace replay transport closed"


@dataclass(frozen=True, slots=True)
class TraceStep:
    """One expected outbound tRPC message and the packets it should trigger."""

    sent: TrpcPacket
    received: tuple[TrpcPacket, ...] = ()

    @classmethod
    def request(
        cls,
        method: TrpcMethodId,
        *,
        response: TrpcPacket | None = None,
        fields: dict[str, int] | None = None,
    ) -> TraceStep:
        """Create a request/response replay step."""
        return cls(
            sent=TrpcPacket.create(
                service=TrpcService.REQUEST,
                method=method,
                fields=fields,
            ),
            received=() if response is None else (response,),
        )

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> TraceStep:
        """Build a trace step from a JSON-friendly dictionary."""
        sent = _message_from_dict(value["sent"])
        received = tuple(_message_from_dict(item) for item in value.get("received", ()))
        return cls(sent=sent, received=received)

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-friendly dictionary."""
        return {
            "sent": _message_to_dict(self.sent),
            "received": [_message_to_dict(message) for message in self.received],
        }


class TraceReplayTransport(PacketTransport):
    """Packet transport that replays scripted responses to expected writes."""

    def __init__(self, steps: tuple[TraceStep, ...] | list[TraceStep]) -> None:
        self.steps: deque[TraceStep] = deque(steps)
        self.written: list[Packet] = []
        self._packets: deque[Packet] = deque()
        self._packet_event = asyncio.Event()
        self._is_open = False

    @property
    def is_open(self) -> bool:
        """Return whether the transport is open."""
        return self._is_open

    @property
    def pending_steps(self) -> int:
        """Return the number of expected writes not yet observed."""
        return len(self.steps)

    async def open(self) -> None:
        """Open the replay transport."""
        self._is_open = True

    async def close(self) -> None:
        """Close the replay transport."""
        self._is_open = False
        self._packet_event.set()

    async def read_packet(self, timeout: float | None = None) -> Packet | None:
        """Read the next scripted inbound packet, returning None on timeout."""

        async def read_next() -> Packet:
            while True:
                if self._packets:
                    return self._packets.popleft()
                if not self._is_open:
                    raise ConnectionClosedError(_TRACE_REPLAY_CLOSED)
                self._packet_event.clear()
                if self._packets:
                    continue
                await self._packet_event.wait()

        try:
            if timeout is None:
                return await read_next()
            return await asyncio.wait_for(read_next(), timeout)
        except TimeoutError:
            return None

    async def write_packet(self, packet: Packet) -> None:
        """Validate an outbound packet and queue scripted responses."""
        if packet.type != TYPE_TRPC:
            msg = f"trace expected tRPC packet, got packet type {packet.type}"
            raise ProtocolError(msg)
        if not self.steps:
            msg = f"unexpected tRPC write: {TrpcPacket.from_packet(packet)}"
            raise ProtocolError(msg)

        message = TrpcPacket.from_packet(packet)
        step = self.steps[0]
        expected_packet = step.sent.to_packet()
        if packet != expected_packet:
            msg = f"expected tRPC write {step.sent}, got {message}"
            raise ProtocolError(msg)

        self.steps.popleft()
        self.written.append(packet)
        for response in step.received:
            self.queue_message(response)

    def queue_message(self, message: TrpcPacket) -> Self:
        """Queue an unsolicited inbound tRPC message."""
        self._packets.append(message.to_packet())
        self._packet_event.set()
        return self

    def assert_complete(self) -> None:
        """Raise if scripted writes remain unconsumed."""
        if self.steps:
            msg = f"{len(self.steps)} trace replay step(s) were not consumed"
            raise AssertionError(msg)


def load_trace(path: str | Path) -> tuple[TraceStep, ...]:
    """Load trace replay steps from a JSON file."""
    with Path(path).open(encoding="utf-8") as file:
        payload = json.load(file)
    if not isinstance(payload, list):
        msg = "trace file must contain a JSON list"
        raise ProtocolError(msg)
    steps: list[TraceStep] = []
    for item in payload:
        if not isinstance(item, dict):
            msg = "trace file entries must be JSON objects"
            raise ProtocolError(msg)
        steps.append(TraceStep.from_dict(item))
    return tuple(steps)


def dump_trace(
    path: str | Path,
    steps: tuple[TraceStep, ...] | list[TraceStep],
) -> None:
    """Write trace replay steps to a JSON file."""
    with Path(path).open("w", encoding="utf-8") as file:
        json.dump([step.as_dict() for step in steps], file, indent=2, sort_keys=True)
        file.write("\n")


def _message_to_dict(message: TrpcPacket) -> dict[str, Any]:
    return {
        "service": message.service_name or message.service_id,
        "method": message.method_name or message.method_id,
        "body": dict(message.body),
        "extra": message.extra.hex().upper(),
    }


def _message_from_dict(value: dict[str, Any]) -> TrpcPacket:
    service = value["service"]
    method = value["method"]
    body_value = value.get("body", {})
    extra = value.get("extra", "")
    if not isinstance(service, int | str):
        msg = "trace message service must be a string or integer"
        raise ProtocolError(msg)
    if not isinstance(method, int | str):
        msg = "trace message method must be a string or integer"
        raise ProtocolError(msg)
    if not isinstance(body_value, dict):
        msg = "trace message body must be an object"
        raise ProtocolError(msg)
    if not isinstance(extra, str):
        msg = "trace message extra must be a hex string"
        raise ProtocolError(msg)
    return TrpcPacket(
        service_id=service_id(_service_from_trace_value(service)),
        method_id=method_id(_method_from_trace_value(method)),
        body={str(key): int(item) for key, item in body_value.items()},
        extra=bytes.fromhex(extra),
    )


def _service_from_trace_value(value: int | str) -> int:
    if isinstance(value, int):
        return value
    try:
        return service_from_name(value)
    except TekmarError:
        return int(value, 0)


def _method_from_trace_value(value: int | str) -> int:
    if isinstance(value, int):
        return value
    try:
        return method_from_name(value)
    except TekmarError:
        return int(value, 0)
