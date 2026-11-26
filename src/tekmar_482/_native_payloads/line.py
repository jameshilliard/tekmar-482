"""Native line announcements from the 483 SW 11 gateway, not addressed writes."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import IntEnum

ZONE_ADJUSTMENT = 0x010
ACCESS_STATUS = 0x200
GROUP_REPORT = 0x20f
GROUP_UPDATE = 0x21f
CLOCK_REPORT = 0x31f
NETWORK_INIT_REQUEST = 0x3df
GROUP_IDS = range(1, 13)
_HOUR = timedelta(hours=1)


@dataclass(frozen=True, slots=True)
class ClockReport:
    """A native clock observation; it carries neither a timezone nor a UTC offset."""
    payload: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.payload, bytes) or not 5 <= len(self.payload) <= 63:
            raise ValueError("expected a native clock payload of at least five bytes")

    @classmethod
    def from_wall_time(cls, value: datetime, *, dst: bool | None = None,
                       dst_mode: int | None = None) -> "ClockReport":
        """Encode local civil time at minute resolution, subtracting the DST hour once."""
        if dst is None:
            adjustment = value.dst() or timedelta()
            if adjustment not in (timedelta(), _HOUR):
                raise ValueError("tN4 represents a one-hour daylight adjustment only")
            dst = adjustment == _HOUR
        if not isinstance(dst, bool):
            raise ValueError("expected a boolean daylight flag")
        standard = value.replace(tzinfo=None)
        if not 2000 <= standard.year <= 2256:
            raise ValueError("native clock year must be representable relative to 2000")
        standard -= _HOUR if dst else timedelta()
        if not 2000 <= standard.year <= 2255:
            raise ValueError("native standard-time year must be 2000..2255")
        body = bytes([standard.minute, standard.hour << 3 | standard.isoweekday(),
                      standard.month - 1 | int(dst) << 4, standard.day - 1, standard.year - 2000])
        return cls(body if dst_mode is None else body + bytes([dst_mode]))

    @property
    def date_supported(self) -> bool:
        return self.payload[2] != 0xff

    @property
    def daylight(self) -> bool | None:
        return bool(self.payload[2] & 16) if self.date_supported else None

    @property
    def weekday(self) -> int:
        return self.payload[1] & 7

    @property
    def reported_time(self) -> time | None:
        try:
            return time(self.payload[1] >> 3, self.payload[0])
        except ValueError:
            return None

    @property
    def standard_date(self) -> date | None:
        if self.date_supported:
            try:
                return date(2000 + self.payload[4], (self.payload[2] & 15) + 1, self.payload[3] + 1)
            except ValueError:
                pass
        return None

    @property
    def wall_datetime(self) -> datetime | None:
        day, clock = self.standard_date, self.reported_time
        if day is None or clock is None or day.isoweekday() != self.weekday:
            return None
        return datetime.combine(day, clock) + (_HOUR if self.daylight else timedelta())


class SetpointApplication(IntEnum):
    SETPOINT_ON_OFF = 0
    HOT_TUB = 1
    DHW = 2
    SETPOINT_PWM = 3
    FLOOR = 4
    POOL = 5


@dataclass(frozen=True, slots=True)
class SetpointAssignment:
    """The two independently supported fields of block 26 needed for group control."""
    application_raw: int | None
    group_raw: int | None

    @classmethod
    def from_fields(cls, fields: Mapping[str, object]) -> "SetpointAssignment":
        app, group = fields.get("application_mode"), fields.get("setpoint_enable_address")
        return cls(app if isinstance(app, int) else None, group if isinstance(group, int) else None)

    @property
    def application(self) -> SetpointApplication | None:
        if self.application_raw is None or self.application_raw not in range(len(SetpointApplication)):
            return None
        return SetpointApplication(self.application_raw)

    @property
    def remote_group(self) -> int | None:
        if self.application in (None, SetpointApplication.DHW, SetpointApplication.POOL):
            return None
        return self.group_raw if self.group_raw in GROUP_IDS else None


def group_command(group: int, enabled: bool) -> bytes:
    if group not in GROUP_IDS or not isinstance(enabled, bool):
        raise ValueError("expected setpoint group 1..12 and a boolean enable")
    return bytes([group, int(enabled)])


@dataclass(frozen=True, slots=True)
class GroupReport:
    group: int
    flags: int

    @property
    def requested(self) -> bool:
        return bool(self.flags & 1)

    @property
    def actual(self) -> bool:
        return bool(self.flags & 0x10)


class ZoneTargetMode(IntEnum):
    OUTDOOR_RESET = 0
    RESET_ONLY = 1
    SETPOINT = 2


@dataclass(frozen=True, slots=True)
class ZoneSyncAdjustment:
    cycle: int  # Preserve the wire selector; SW 11 sources disagree about its polarity.
    purging: bool
    target: ZoneTargetMode

    def __post_init__(self) -> None:
        if self.cycle not in (0, 1) or not isinstance(self.purging, bool) or not isinstance(self.target, ZoneTargetMode):
            raise ValueError("expected zone cycle 0/1, a purge flag and a known target mode")

    @property
    def payload(self) -> bytes:
        return bytes([self.cycle, int(self.purging), self.target])
