"""Native line announcements from the 483 SW 11 gateway, not addressed writes."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import IntEnum

from .codec import Fields, Reader, ShortPayload, fahrenheit, temp
from .layouts import SETPOINT_FIELDS
from .scenes import SCENE_REPORT, SceneReport
from .thermostat import TargetSlot

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
    def read(cls, reader: Reader) -> "ClockReport":
        head = reader.take(5)
        return cls(head + reader.take(len(reader.remaining)))

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

    def fields(self) -> Fields:
        minute, hour, month, day, year = self.payload[:5]
        fields: Fields = {"minute": minute, "weekday": self.weekday, "hour": hour >> 3,
                          "date_supported": int(self.date_supported), "month_raw": month & 15,
                          "dst_bit_raw": (month >> 4) & 1, "reserved": month >> 5,
                          "day_raw": day, "year_from_2000": year}
        if self.daylight is not None:
            fields["dst"] = int(self.daylight)
        if (d := self.standard_date) is not None:
            fields.update(year=d.year, month=d.month, day=d.day, weekday_matches=int(d.isoweekday() == self.weekday))
        if (wall := self.wall_datetime) is not None:
            fields["local_datetime"] = wall.isoformat(timespec="minutes")
        if len(self.payload) > 5:
            fields["dst_mode"] = self.payload[5]
        if len(self.payload) > 6:
            fields["extension_hex"] = self.payload[6:].hex()
        return fields


class SetpointApplication(IntEnum):
    SETPOINT_ON_OFF = 0
    HOT_TUB = 1
    DHW = 2
    SETPOINT_PWM = 3
    FLOOR = 4
    POOL = 5

    @property
    def target_block(self) -> int:
        return (0x28, 0x2a, 0x2c, 0x28, 0x2e, 0x30)[self]


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

    @classmethod
    def read(cls, reader: Reader) -> "GroupReport":
        return cls(*reader.take(2))

    @property
    def requested(self) -> bool:
        return bool(self.flags & 1)

    @property
    def actual(self) -> bool:
        return bool(self.flags & 0x10)

    def fields(self) -> Fields:
        return {"group_id": self.group, "flags_raw": self.flags, "requested_enable": int(self.requested),
                "requested_reserved": (self.flags >> 1) & 7,
                "actual_enable": int(self.actual), "actual_reserved": self.flags >> 5}


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


@dataclass(frozen=True, slots=True)
class ZoneSyncReport:
    """Receive selectors unchanged, even when the 483 writer would not send them."""
    cycle: int
    purging: int
    target_raw: int

    @property
    def target(self) -> ZoneTargetMode | None:
        return ZoneTargetMode(self.target_raw) if self.target_raw in range(len(ZoneTargetMode)) else None

    def fields(self) -> Fields:
        return {"synch_cycle": self.cycle, "purging": self.purging, "target_mode": self.target_raw}


@dataclass(frozen=True, slots=True)
class GatewayAccess:
    flags: int

    @property
    def request(self) -> bool:
        return bool(self.flags & 1)

    @property
    def access_open(self) -> bool:
        return bool(self.flags & 2)

    def fields(self) -> Fields:
        return {"request": int(self.request), "access_open": int(self.access_open), "reserved": self.flags >> 2}


@dataclass(frozen=True, slots=True)
class NetworkLock:
    flags: int

    @property
    def access_lock(self) -> bool:
        return bool(self.flags & 1)

    @property
    def address_lock(self) -> bool:
        return bool(self.flags & 2)

    def fields(self) -> Fields:
        return {"access_lock": int(self.access_lock), "address_lock": int(self.address_lock), "reserved": self.flags >> 2}


@dataclass(frozen=True, slots=True)
class WarmWeatherShutdown:
    flags: int

    @property
    def active_slots(self) -> tuple[TargetSlot, ...]:
        return tuple(slot for slot in TargetSlot if self.flags & (1 << slot))

    def fields(self) -> Fields:
        fields: Fields = {name: (self.flags >> i) & 1 for i, name in enumerate(SETPOINT_FIELDS)}
        fields["reserved"] = self.flags >> 5
        return fields


@dataclass(frozen=True, slots=True)
class OutdoorTemperature:
    raw: int
    second_word: int | None

    @classmethod
    def read(cls, reader: Reader) -> "OutdoorTemperature":
        # J1196J 0x3dd8 sends the word twice; 0x3e38 consumes the first.
        raw = reader.word()
        return cls(raw, reader.word() if reader.remaining else None)

    @property
    def fahrenheit(self) -> float | None:
        return fahrenheit(self.raw)

    def fields(self) -> Fields:
        fields = temp("outdoor_temp", self.raw)
        if self.second_word is not None:
            fields.update(second_word_raw=self.second_word, matches_first=int(self.raw == self.second_word))
        return fields


class LineReportKind(IntEnum):
    ZONE_SYNC = ZONE_ADJUSTMENT
    GATEWAY_ACCESS = ACCESS_STATUS
    SETPOINT_GROUP = GROUP_REPORT
    NETWORK_LOCK = 0x2ef
    SCENE = SCENE_REPORT
    WWSD = 0x30f
    CLOCK = CLOCK_REPORT
    OUTDOOR = 0x32f


LineValue = ZoneSyncReport | GatewayAccess | GroupReport | NetworkLock | SceneReport | WarmWeatherShutdown | ClockReport | OutdoorTemperature

LINE_MESSAGES: dict[int, tuple[str, Callable[[Reader], LineValue]]] = {
    LineReportKind.ZONE_SYNC: ("ZoneSynchAdjustment", lambda r: ZoneSyncReport(*r.take(3))),
    LineReportKind.GATEWAY_ACCESS: ("SecureGatewayAccess", lambda r: GatewayAccess(r.byte())),
    LineReportKind.SETPOINT_GROUP: ("SetpointEnableReport", GroupReport.read),
    LineReportKind.NETWORK_LOCK: ("NetworkLock", lambda r: NetworkLock(r.byte())),
    LineReportKind.SCENE: ("SceneReport", SceneReport.read),
    LineReportKind.WWSD: ("WWSDStatus", lambda r: WarmWeatherShutdown(r.byte())),
    LineReportKind.CLOCK: ("RealTimeClockReport", ClockReport.read),
    LineReportKind.OUTDOOR: ("OutdoorTemperatureReport", OutdoorTemperature.read),
}


def line_report(message: int, payload: bytes) -> LineValue | None:
    """Only complete announcements. The gateway-access request shares its reply ID."""
    entry = LINE_MESSAGES.get(message)
    if entry is None:
        return None
    try:
        value = entry[1](Reader(payload))
    except ShortPayload:
        return None
    return None if isinstance(value, GatewayAccess) and value.request else value
