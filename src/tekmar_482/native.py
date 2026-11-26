"""Native tN4 addressing, messages and addressed block operations."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import time
from typing import TYPE_CHECKING, Self

from ._native_payloads import (
    BLOCK_SELECTORS,
    H_UNAVAILABLE,
    SNOW_SLAB_TARGETS,
    SnowmeltBlock,
    decode_payload,
)
from ._native_payloads.codec import Reader, ShortPayload
from ._native_payloads.line import (
    CLOCK_REPORT,
    GROUP_REPORT,
    GROUP_UPDATE,
    ZONE_ADJUSTMENT,
    ClockReport,
    GroupReport,
    ZoneSyncAdjustment,
    group_command,
)
from ._native_payloads.queries import NativeStatusQuery, StatusRequest
from ._native_payloads.scenes import SCENE_REPORT, NativeScene, SceneReport
from ._native_payloads.schedules import ScheduleEvent, ScheduleProfile
from ._native_payloads.snowmelt import SNOW_REPORT, SnowmeltReport
from .constants import TN4_PORTS
from .units import celsius_to_degh, degh_to_celsius

if TYPE_CHECKING:
    from calendar import Day
    from datetime import datetime
    from enum import Enum

_HEADER = 4
_MAX_PAYLOAD = 63
_REQUEST = 0x2D7
_UPDATE = 0x2DF
_REPORT = 0x2E7
_BLOCK_HEADER = 3
_SCENE_HEADER = 3
_CLOCK_SIZE = 5
_GROUP_SIZE = 2
_WORD_MASK = 0b11
_SINGLE = 0x10
_MULTI = 0x38
_DAYS = 7
_DISABLED = 144
_UNIDENTIFIED_SCHEDULE = ScheduleProfile(None, None)


def require_enum(value: object, kind: type[Enum]) -> None:
    if not isinstance(value, kind):
        msg = f"expected a {kind.__name__}"
        raise TypeError(msg)


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

    @property
    def block_node(self) -> int:
        """Five-bit destination used by addressed tN4 reads and writes."""
        index = self.node & 31
        if not index:
            msg = "node zero cannot be selected for an addressed block"
            raise ValueError(msg)
        return index

    def request_mask(self) -> int:
        return 1 << (self.block_node - 1)

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
    def request_status(cls, query: NativeStatusQuery, *, source: NativeAddress) -> Self:
        """Build a read-only status request at the 483 gateway's native priority."""
        return cls(query.kind, source, query.payload, priority=0)

    @classmethod
    def scene_report(cls, scene: NativeScene, *, source: NativeAddress) -> Self:
        """Announce a scene to one line at the 483's priority and duration."""
        require_enum(scene, NativeScene)
        return cls(SCENE_REPORT, source, scene.payload(), priority=0)

    @classmethod
    def request_clock(cls, *, source: NativeAddress) -> Self:
        """Ask for a native clock report on one line, not for a gateway clock."""
        return cls.request_status(NativeStatusQuery(StatusRequest.CLOCK), source=source)

    @classmethod
    def clock_report(
        cls,
        value: datetime,
        *,
        source: NativeAddress,
        dst: bool | None = None,
        dst_mode: int | None = None,
        priority: int = 1,
    ) -> Self:
        """Encode local civil time. Priority 1 matches the stock 482 broadcast."""
        report = ClockReport.from_wall_time(value, dst=dst, dst_mode=dst_mode)
        return cls(CLOCK_REPORT, source, report.payload, priority)

    @classmethod
    def setpoint_group(
        cls,
        group_id: int,
        *,
        enabled: bool,
        source: NativeAddress,
        priority: int = 1,
    ) -> Self:
        """Build a native group update for a single Raw port; no destination node."""
        return cls(GROUP_UPDATE, source, group_command(group_id, enabled), priority)

    @classmethod
    def zone_sync(
        cls,
        adjustment: ZoneSyncAdjustment,
        *,
        source: NativeAddress,
    ) -> Self:
        """Announce all three zone settings in the 483 test-settings layout."""
        return cls(ZONE_ADJUSTMENT, source, adjustment.payload, priority=0)

    @classmethod
    def request_snowmelt_status(
        cls,
        *,
        source: NativeAddress,
        zone_type: int,
        zone_id: int,
        keys: bytes,
    ) -> Self:
        """Low-level 17F encoder: zone_id is the zero-based *wire* destination."""
        destination = _uint(zone_id, 5) << 3 | _uint(zone_type, 3)
        return cls(
            StatusRequest.SNOWMELT,
            source,
            bytes([destination, _uint(len(keys), 6)]) + keys,
            priority=0,
        )

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
        destination = bytes((target.subnet, target.block_node))
        block = _uint(block_id, 16).to_bytes(2, "little")
        return cls(_UPDATE, source, destination + block + data)

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

    @property
    def clock(self) -> ClockReport | None:
        if self.message_id == CLOCK_REPORT and len(self.message.payload) >= _CLOCK_SIZE:
            return ClockReport(self.message.payload)
        return None

    @property
    def group_report(self) -> GroupReport | None:
        if self.message_id == GROUP_REPORT and len(self.message.payload) >= _GROUP_SIZE:
            return GroupReport(self.message.payload[0], self.message.payload[1])
        return None

    @property
    def snowmelt_report(self) -> SnowmeltReport | None:
        """Return all declared pairs in wire order; leave extensions in the frame."""
        if self.message_id != SNOW_REPORT:
            return None
        try:
            return SnowmeltReport.read(Reader(self.message.payload))
        except ShortPayload:
            return None

    @property
    def scene_report(self) -> SceneReport | None:
        """An observed announcement, possibly our own echo, not device state."""
        if (
            self.message_id == SCENE_REPORT
            and len(self.message.payload) >= _SCENE_HEADER
        ):
            return SceneReport.from_payload(self.message.payload)
        return None


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


def schedule_slot(value: time | None) -> int:
    """Encode a ten-minute local time, or the device's disabled-event value."""
    if value is None:
        return _DISABLED
    if (
        value.tzinfo is not None
        or value.second
        or value.microsecond
        or value.minute % 10
    ):
        msg = "expected a local time on a ten-minute boundary"
        raise ValueError(msg)
    return value.hour * 6 + value.minute // 10


def schedule_time(raw: int) -> time | None:
    if raw == _DISABLED:
        return None
    if not 0 <= raw < _DISABLED:
        msg = f"undefined schedule time byte {raw:#04x}"
        raise ValueError(msg)
    return time(raw // 6, raw % 6 * 10)


@dataclass(frozen=True, slots=True)
class WeeklyEvent:
    """One complete reported weekly event block, not a whole device schedule."""

    port: int
    target: NativeAddress
    event: ScheduleEvent
    days: bytes
    selector: int | None = None
    profile: ScheduleProfile = _UNIDENTIFIED_SCHEDULE

    def __post_init__(self) -> None:
        if len(self.days) != _DAYS:
            msg = "a weekly event requires seven day values"
            raise ValueError(msg)
        if self.selector is not None:
            _uint(self.selector, 8)
        self.profile.wire_day(self.block_id, 0)

    @classmethod
    def from_frame(
        cls,
        frame: NativeFrame,
        *,
        profile: ScheduleProfile = _UNIDENTIFIED_SCHEDULE,
    ) -> Self:
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
        wire_days = message.payload[-_DAYS:]
        return cls(
            frame.port,
            message.source,
            ScheduleEvent(block & 3),
            bytes(wire_days[profile.wire_day(block, i)] for i in range(_DAYS)),
            message.payload[3] if multi else None,
            profile,
        )

    @property
    def block_id(self) -> int:
        return (_SINGLE if self.selector is None else _MULTI) + self.event

    def time_on(self, day: Day) -> time | None:
        return schedule_time(self.days[day])

    def with_time(self, day: Day, value: time | None) -> Self:
        days = bytearray(self.days)
        days[day] = schedule_slot(value)
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
        wire = bytearray(_DAYS)
        for day, raw in enumerate(self.days):
            wire[self.profile.wire_day(self.block_id, day)] = raw
        return NativeMessage.update_block(
            self.block_id,
            prefix + bytes(wire),
            source=source,
            target=self.target,
        )
