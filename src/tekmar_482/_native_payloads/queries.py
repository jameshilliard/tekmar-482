"""Read-only native status exchanges from the 483 SW 11 registries and models."""

from dataclasses import dataclass
from enum import IntEnum

from . import decode_payload


class StatusRequest(IntEnum):
    ZONE_SYNC = 0x011
    SNOWMELT = 0x17f
    IO_CHANNEL = 0x1f4
    IO_DISCOVERY = 0x1f6
    WWSD = 0x307
    CLOCK = 0x317
    HUMIDITY = 0x322
    OUTDOOR = 0x327
    MASTER = 0x3f7

    @property
    def reply_id(self) -> int:
        return _REPLIES[self]


_REPLIES = {
    StatusRequest.ZONE_SYNC: 0x010, StatusRequest.SNOWMELT: 0x17e,
    StatusRequest.IO_CHANNEL: 0x1f3, StatusRequest.IO_DISCOVERY: 0x1f5,
    StatusRequest.WWSD: 0x30f, StatusRequest.CLOCK: 0x31f,
    StatusRequest.HUMIDITY: 0x323, StatusRequest.OUTDOOR: 0x32f,
    StatusRequest.MASTER: 0x3ff,
}
_WIDTHS = {StatusRequest.IO_CHANNEL: 3, StatusRequest.IO_DISCOVERY: 2, StatusRequest.HUMIDITY: 5}


class SnowmeltRole(IntEnum):
    ZONE = 1
    SENSOR = 2

    @property
    def maximum(self) -> int:
        return 12 if self is SnowmeltRole.ZONE else 24

    @classmethod
    def for_application(cls, application: int) -> "SnowmeltRole":
        if application not in _APPLICATION_KEYS:
            raise ValueError(f"unknown snowmelt application {application}")
        return cls.SENSOR if application == 4 else cls.ZONE


class SnowmeltKey(IntEnum):
    WATER_DETECTION = 0x19
    HEAT_SEQUENCE = 0x1a
    OPERATING_STATE = 0x1b
    WARM_WEATHER_SHUTDOWN = 0x1c
    COLD_WEATHER_CUTOFF = 0x1d
    ZONE_TYPE = 0x1e
    ELECTRIC_OFF_TIME = 0x1f
    ZONE_ID = 0x20
    SENSOR_FLAGS = 0x21
    SLAB_TEMPERATURE = 0x22
    SLAB_SETPOINT = 0x23
    EDGE_TEMPERATURE = 0x24
    MELT_GROUP = 0x25
    STORM_GROUP = 0x26
    BOILER_OUTPUT = 0x27
    MIX_OUTPUT = 0x28
    PWM_OUTPUT = 0x29
    SYSTEM_PUMP = 0x2a
    ALERT_RELAY = 0x2b
    RETURN_TEMPERATURE = 0x2c
    SUPPLY_TEMPERATURE = 0x2d
    SUPPLY_TARGET = 0x2e
    KEEP_ALIVE = 0x2f
    MELTING_TIME = 0x30


# SnowmeltControl.stale_keys_to_request: every application includes water
# detection and slab temperature. A sensor-only installation needs just those.
_APPLICATION_KEYS = {
    0: bytes.fromhex("19 22 1b 23 2d 2a 29"),
    1: bytes.fromhex("19 22 1b 23 2d 2a 2e 27"),
    2: bytes.fromhex("19 22 1b 23 2d 2a 2e 28"),
    3: bytes.fromhex("19 22 1b 23 29"),
    4: bytes.fromhex("19 22"),
}


@dataclass(frozen=True, slots=True)
class ReplyPart:
    items: frozenset[int]
    expected_count: int


@dataclass(frozen=True, slots=True)
class NativeStatusQuery:
    """One 483-style query. The request payload never includes the native header."""

    kind: StatusRequest
    payload: bytes = b""

    def __post_init__(self) -> None:
        if not isinstance(self.kind, StatusRequest) or not isinstance(self.payload, bytes):
            raise ValueError("expected a typed status request and immutable payload")
        if self.kind is StatusRequest.SNOWMELT:
            if not 3 <= len(self.payload) <= 63 or self.payload[1] != len(self.payload) - 2:
                raise ValueError("a snowmelt request needs 1..61 ordered keys")
            role = SnowmeltRole(self.payload[0] & 7)
            if self.payload[0] >> 3 >= role.maximum:
                raise ValueError("snowmelt destination outside the configured range")
        elif len(self.payload) != _WIDTHS.get(self.kind, 0):
            raise ValueError(f"wrong payload width for {self.kind.name.lower()} request")

    @classmethod
    def snowmelt(cls, role: SnowmeltRole, number: int, keys: bytes) -> "NativeStatusQuery":
        if not isinstance(role, SnowmeltRole) or not 1 <= number <= role.maximum:
            raise ValueError("expected configured zone 1..12 or sensor 1..24")
        if not isinstance(keys, bytes) or not 1 <= len(keys) <= 61:
            raise ValueError("a snowmelt request needs 1..61 ordered keys")
        return cls(StatusRequest.SNOWMELT, bytes([((number - 1) << 3) | role, len(keys)]) + keys)

    @classmethod
    def for_snowmelt_device(cls, application: int, number: int, keys: bytes | None = None) -> "NativeStatusQuery":
        role = SnowmeltRole.for_application(application)
        return cls.snowmelt(role, number, _APPLICATION_KEYS[application] if keys is None else keys)

    @classmethod
    def io(cls, subnet: int, node: int, channel: int | None = None) -> "NativeStatusQuery":
        if channel is None:
            return cls(StatusRequest.IO_DISCOVERY, bytes([subnet, node]))
        return cls(StatusRequest.IO_CHANNEL, bytes([subnet, node, channel]))

    @classmethod
    def humidity(cls, subnet: int, selector: int) -> "NativeStatusQuery":
        if not 0 <= selector < 1 << 32:
            raise ValueError("RHQuery needs an unsigned 32-bit selector")
        return cls(StatusRequest.HUMIDITY, bytes([subnet]) + selector.to_bytes(4, "little"))

    @property
    def keys(self) -> bytes:
        return self.payload[2:] if self.kind is StatusRequest.SNOWMELT else b""

    def match(self, message_id: int, source_subnet: int, data: bytes) -> ReplyPart | None:
        if message_id != self.kind.reply_id or "truncated" in decode_payload(message_id, data):
            return None
        if self.kind is StatusRequest.SNOWMELT:
            # Requests use (configured number - 1), reports use the configured number.
            if data[0] != self.payload[0] + 8:
                return None
            requested = frozenset(self.keys)
            items = requested.intersection(data[2:2 + 3 * data[1]:3])
            return ReplyPart(items, len(requested)) if items else None
        if self.kind in (StatusRequest.IO_CHANNEL, StatusRequest.IO_DISCOVERY):
            if not data.startswith(self.payload):
                return None
            count = data[3] if self.kind is StatusRequest.IO_DISCOVERY else 1
            return ReplyPart(frozenset([data[2]]), count)
        if self.kind is StatusRequest.HUMIDITY and source_subnet != self.payload[0]:
            return None
        return ReplyPart(frozenset([0]), 1)
