"""Native tN4 addressing, messages and addressed block operations."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import time
from enum import IntEnum
from typing import TYPE_CHECKING, Self

from ._native_payloads import decode_payload
from .constants import SETPOINT_GROUP_IDS

if TYPE_CHECKING:
    from calendar import Day

_HEADER = 4
_MAX_PAYLOAD = 63
_REQUEST = 0x2D7
_UPDATE = 0x2DF
_REPORT = 0x2E7
_SINGLE = 0x10
_MULTI = 0x38
_DAYS = 7
_DISABLED = 144


def _uint(value: int, width: int) -> int:
    if not 0 <= value < 1 << width:
        msg = f"{value} does not fit a {width}-bit unsigned field"
        raise ValueError(msg)
    return value


@dataclass(frozen=True, slots=True)
class NativeAddress:
    """Subnet and six-bit wire node, not the decimal tRPC device address."""

    subnet: int
    node: int

    def __post_init__(self) -> None:
        _uint(self.subnet, 8)
        _uint(self.node, 6)

    def request_mask(self) -> int:
        index = self.node & 31  # The lock flag is not part of a node selector.
        if not index:
            msg = "node zero cannot be selected by an addressed block request"
            raise ValueError(msg)
        return 1 << (index - 1)


@dataclass(frozen=True, slots=True)
class NativeMessage:
    message_id: int
    source: NativeAddress
    payload: bytes
    priority: int = 1

    def __post_init__(self) -> None:
        _uint(self.message_id, 10)
        _uint(self.priority, 2)
        _uint(len(self.payload), 6)

    def to_bytes(self) -> bytes:
        header = (
            self.priority << 30
            | self.message_id << 20
            | self.source.subnet << 12
            | self.source.node << 6
            | len(self.payload)
        )
        return header.to_bytes(_HEADER, "big") + self.payload

    @classmethod
    def from_bytes(cls, body: bytes) -> Self:
        header = int.from_bytes(body[:_HEADER], "big")
        if len(body) != _HEADER + (header & _MAX_PAYLOAD):
            msg = "native body length does not match its header"
            raise ValueError(msg)
        return cls(
            header >> 20 & 1023,
            NativeAddress(header >> 12 & 255, header >> 6 & 63),
            body[_HEADER:],
            header >> 30,
        )

    @classmethod
    def setpoint_group(
        cls,
        group_id: int,
        *,
        enabled: bool,
        source: NativeAddress,
    ) -> Self:
        """Build a native group update for a single Raw port; no destination node."""
        if group_id not in SETPOINT_GROUP_IDS:
            msg = f"Setpoint group must be in 1..12: {group_id}"
            raise ValueError(msg)
        return cls(0x21F, source, bytes([group_id, int(enabled)]))

    @classmethod
    def request_snowmelt_status(
        cls,
        *,
        source: NativeAddress,
        zone_type: int,
        zone_id: int,
        keys: bytes,
    ) -> Self:
        """Request ordered status keys from a native snowmelt zone on one line."""
        destination = _uint(zone_id, 5) << 3 | _uint(zone_type, 3)
        return cls(0x17F, source, bytes([destination, _uint(len(keys), 6)]) + keys)

    @classmethod
    def request_block(
        cls,
        block_id: int,
        *,
        source: NativeAddress,
        target: NativeAddress,
        argument: int | None = None,
    ) -> Self:
        payload = (
            _uint(block_id, 16).to_bytes(2, "little")
            + bytes([target.subnet])
            + target.request_mask().to_bytes(4, "little")
        )
        if argument is not None:
            payload += _uint(argument, 32).to_bytes(4, "little")
        return cls(_REQUEST, source, payload)

    @classmethod
    def update_block(
        cls,
        block_id: int,
        data: bytes,
        *,
        source: NativeAddress,
        target: NativeAddress,
    ) -> Self:
        payload = bytes([target.subnet, target.node]) + _uint(block_id, 16).to_bytes(
            2,
            "little",
        )
        return cls(_UPDATE, source, payload + data)

    @property
    def decoded(self) -> dict[str, str | int | float]:
        return decode_payload(self.message_id, self.payload)


@dataclass(frozen=True, slots=True)
class NativeFrame:
    port: int
    timestamp_ms: int
    body: bytes
    message: NativeMessage = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "message", NativeMessage.from_bytes(self.body))

    @property
    def message_id(self) -> int:
        return self.message.message_id

    @property
    def subnet(self) -> int:
        return self.message.source.subnet

    @property
    def node(self) -> int:
        return self.message.source.node

    @property
    def decoded(self) -> dict[str, str | int | float]:
        return self.message.decoded


class ScheduleEvent(IntEnum):
    WAKE = 0
    UNOCCUPIED = 1
    OCCUPIED = 2
    SLEEP = 3


@dataclass(frozen=True, slots=True)
class WeeklyEvent:
    """One complete reported weekly event block, not a whole device schedule."""

    port: int
    target: NativeAddress
    event: ScheduleEvent
    days: bytes
    selector: int | None = None

    def __post_init__(self) -> None:
        if len(self.days) != _DAYS:
            msg = "a weekly event requires seven day values"
            raise ValueError(msg)
        if self.selector is not None:
            _uint(self.selector, 8)

    @classmethod
    def from_frame(cls, frame: NativeFrame) -> Self:
        message = frame.message
        block = int.from_bytes(message.payload[:2], "little")
        multi = block in range(_MULTI, _MULTI + len(ScheduleEvent))
        if message.message_id != _REPORT or not (
            multi or block in range(_SINGLE, _SINGLE + len(ScheduleEvent))
        ):
            msg = "expected a weekly event block report"
            raise ValueError(msg)
        width = _DAYS + int(multi)
        if len(message.payload) != 3 + width or message.payload[2] != (1 << width) - 1:
            msg = "cannot edit an incomplete or extended weekly event block"
            raise ValueError(msg)
        return cls(
            frame.port,
            message.source,
            ScheduleEvent(block & 3),
            message.payload[-_DAYS:],
            message.payload[3] if multi else None,
        )

    @property
    def block_id(self) -> int:
        return (_SINGLE if self.selector is None else _MULTI) + self.event

    def time_on(self, day: Day) -> time | None:
        raw = self.days[day]
        if raw == _DISABLED:
            return None
        if raw > _DISABLED:
            msg = f"undefined schedule time byte {raw:#04x}"
            raise ValueError(msg)
        return time(raw // 6, raw % 6 * 10)

    def with_time(self, day: Day, value: time | None) -> Self:
        raw = _DISABLED
        if value is not None:
            if (
                value.tzinfo is not None
                or value.second
                or value.microsecond
                or value.minute % 10
            ):
                msg = "expected a local time on a ten-minute boundary"
                raise ValueError(msg)
            raw = value.hour * 6 + value.minute // 10
        days = bytearray(self.days)
        days[day] = raw
        return replace(self, days=bytes(days))

    def request(self, source: NativeAddress) -> NativeMessage:
        return NativeMessage.request_block(
            self.block_id,
            source=source,
            target=self.target,
            argument=None if self.selector is None else self.selector & 7,
        )

    def update(self, source: NativeAddress) -> NativeMessage:
        prefix = b"" if self.selector is None else bytes([self.selector])
        return NativeMessage.update_block(
            self.block_id,
            prefix + self.days,
            source=source,
            target=self.target,
        )
