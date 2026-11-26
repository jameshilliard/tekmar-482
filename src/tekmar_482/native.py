"""Native tN4 addressing, messages and addressed block operations."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import time
from enum import IntEnum
from typing import TYPE_CHECKING, Self

from ._native_payloads import (
    BLOCK_SELECTORS,
    H_UNAVAILABLE,
    SNOW_SLAB_TARGETS,
    SnowmeltBlock,
    decode_payload,
)
from .constants import SETPOINT_GROUP_IDS, TN4_PORTS
from .units import celsius_to_degh, degh_to_celsius

if TYPE_CHECKING:
    from calendar import Day
    from datetime import datetime

_HEADER = 4
_MAX_PAYLOAD = 63
_REQUEST = 0x2D7
_UPDATE = 0x2DF
_REPORT = 0x2E7
_BLOCK_HEADER = 3
_WORD_MASK = 0b11
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

    def tha_address(self, port: int) -> int | None:
        """Return the display address for a device node, not evidence of inventory."""
        if port not in TN4_PORTS:
            msg = f"No physical tN4 port {port}"
            raise ValueError(msg)
        node = self.node & 31
        if node not in range(1, 25):
            return None
        return port * 1000 + (self.subnet & 7) * 100 + 25 - node


@dataclass(frozen=True, slots=True)
class NativeTargets:
    """A block request can address any subset of nodes 1..31 on one subnet."""

    subnet: int
    mask: int = (1 << 31) - 1

    def __post_init__(self) -> None:
        _uint(self.subnet, 8)
        _uint(self.mask, 31)
        if not self.mask:
            msg = "a block request must select at least one node"
            raise ValueError(msg)

    def request_mask(self) -> int:
        return self.mask


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
    def request_clock(cls, *, source: NativeAddress) -> Self:
        """Ask for a native clock report on one line, not for a gateway clock."""
        return cls(0x317, source, b"")

    @classmethod
    def clock_report(
        cls,
        value: datetime,
        *,
        source: NativeAddress,
        dst: bool = False,
        dst_mode: int | None = None,
    ) -> Self:
        """Supply wall time. Omit dst_mode for the five-byte 482 layout."""
        payload = bytes(
            (
                value.minute,
                value.hour << 3 | value.isoweekday(),
                (value.month - 1) | int(dst) << 4,
                value.day - 1,
                _uint(value.year - 2000, 8),
            ),
        )
        if dst_mode is not None:
            payload += bytes([_uint(dst_mode, 8)])
        return cls(0x31F, source, payload)

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
        target: NativeAddress | NativeTargets,
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


@dataclass(frozen=True, slots=True)
class NativeBlockQuery:
    """An addressed read with a known selector layout, independent of the host port."""

    target: NativeAddress | NativeTargets
    block_id: int
    selector: int | None = None

    def __post_init__(self) -> None:
        self.target.request_mask()
        _uint(self.block_id, 16)
        spec = BLOCK_SELECTORS.get(self.block_id)
        if spec is None:
            if self.selector is not None:
                msg = f"block {self.block_id:#x} has no known selector"
                raise ValueError(msg)
        elif self.selector is None or not 0 <= self.selector <= spec[1]:
            msg = f"block {self.block_id:#x} requires {spec[0]} in 0..{spec[1]}"
            raise ValueError(msg)

    def request(self, source: NativeAddress) -> NativeMessage:
        return NativeMessage.request_block(
            self.block_id,
            source=source,
            target=self.target,
            argument=self.selector or 0,
        )

    def matches(self, message: NativeMessage) -> bool:
        """Match the reporting node and payload; the stream checks the physical port."""
        data = message.payload
        node = message.source.node & 31
        if (
            message.message_id != _REPORT
            or message.source.subnet != self.target.subnet
            or not node
            or not (self.target.request_mask() & (1 << (node - 1)))
            or len(data) < _BLOCK_HEADER
            or int.from_bytes(data[:2], "little") != self.block_id
        ):
            return False
        decoded = message.decoded
        if "truncated" in decoded or decoded.get("block_layout") == "ambiguous":
            return False
        if self.selector is None:
            return True
        return (
            len(data) > _BLOCK_HEADER
            and bool(data[2] & 1)
            and (
                data[_BLOCK_HEADER] & BLOCK_SELECTORS[self.block_id][1] == self.selector
            )
        )


@dataclass(frozen=True, slots=True)
class SnowmeltSetpoint:
    """One complete native slab target report; changing it does not enable a group."""

    port: int
    target: NativeAddress
    block: SnowmeltBlock
    raw: int

    def __post_init__(self) -> None:
        if self.block not in SNOW_SLAB_TARGETS or self.port not in TN4_PORTS:
            msg = "expected a melt, idle or storm target on a physical port"
            raise ValueError(msg)
        self.target.request_mask()
        _uint(self.raw, 16)

    @classmethod
    def from_frame(cls, frame: NativeFrame) -> Self:
        data = frame.message.payload
        if (
            frame.message_id != _REPORT
            or len(data) != _BLOCK_HEADER + 2
            or data[2] != _WORD_MASK
        ):
            msg = "expected a complete, unextended snowmelt setpoint block report"
            raise ValueError(msg)
        return cls(
            frame.port,
            frame.message.source,
            SnowmeltBlock(int.from_bytes(data[:2], "little")),
            int.from_bytes(data[_BLOCK_HEADER:], "little"),
        )

    @property
    def is_off(self) -> bool:
        return self.raw == 0 and self.block != SnowmeltBlock.MELT_SETPOINT

    @property
    def celsius(self) -> float | None:
        if not self.raw or self.raw in H_UNAVAILABLE:
            return None
        return degh_to_celsius(self.raw)

    def update_celsius(
        self,
        value: float | None,
        *,
        source: NativeAddress,
    ) -> NativeMessage:
        if value is None:
            if self.block == SnowmeltBlock.MELT_SETPOINT:
                msg = "the melt slab target has no Off setting"
                raise ValueError(msg)
            raw = 0
        else:
            raw = celsius_to_degh(value)
            if not raw or raw in H_UNAVAILABLE:
                msg = "temperature encodes a reserved wire value"
                raise ValueError(msg)
        return NativeMessage.update_block(
            self.block,
            raw.to_bytes(2, "little"),
            source=source,
            target=self.target,
        )


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
