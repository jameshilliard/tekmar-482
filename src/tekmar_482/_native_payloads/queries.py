"""Read-only native status exchanges from the 483 SW 11 registries and models."""

from dataclasses import dataclass
from enum import IntEnum

from . import decode_payload
from .line import ACCESS_STATUS
from .snowmelt import SNOW_REPORT, SNOW_REQUEST, SnowmeltAddress, SnowmeltApplication, SnowmeltKey as SnowmeltKey, SnowmeltRole as SnowmeltRole


class StatusRequest(IntEnum):
    ZONE_SYNC = 0x011
    SNOWMELT = SNOW_REQUEST
    IO_CHANNEL = 0x1f4
    IO_DISCOVERY = 0x1f6
    GATEWAY_ACCESS = ACCESS_STATUS
    WWSD = 0x307
    CLOCK = 0x317
    HUMIDITY = 0x322
    OUTDOOR = 0x327
    MASTER = 0x3f7

    @property
    def reply_id(self) -> int:
        return _REPLIES[self]


_REPLIES = {
    StatusRequest.ZONE_SYNC: 0x010, StatusRequest.SNOWMELT: SNOW_REPORT,
    StatusRequest.IO_CHANNEL: 0x1f3, StatusRequest.IO_DISCOVERY: 0x1f5,
    StatusRequest.WWSD: 0x30f, StatusRequest.CLOCK: 0x31f,
    StatusRequest.HUMIDITY: 0x323, StatusRequest.OUTDOOR: 0x32f,
    StatusRequest.MASTER: 0x3ff,
    StatusRequest.GATEWAY_ACCESS: ACCESS_STATUS,
}
_WIDTHS = {StatusRequest.IO_CHANNEL: 3, StatusRequest.IO_DISCOVERY: 2, StatusRequest.HUMIDITY: 5}


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
        elif self.kind is StatusRequest.GATEWAY_ACCESS:
            if self.payload != b"\x01":
                raise ValueError("gateway access query must request status only")
        elif len(self.payload) != _WIDTHS.get(self.kind, 0):
            raise ValueError(f"wrong payload width for {self.kind.name.lower()} request")

    @classmethod
    def snowmelt(cls, role: SnowmeltRole, number: int, keys: bytes) -> "NativeStatusQuery":
        address = SnowmeltAddress(role, number)
        if not isinstance(keys, bytes) or not 1 <= len(keys) <= 61:
            raise ValueError("a snowmelt request needs 1..61 ordered keys")
        return cls(StatusRequest.SNOWMELT, bytes([address.request_byte, len(keys)]) + keys)

    @classmethod
    def for_snowmelt_device(cls, application: int, number: int, keys: bytes | None = None) -> "NativeStatusQuery":
        role = SnowmeltRole.for_application(application)
        return cls.snowmelt(role, number, SnowmeltApplication(application).status_keys if keys is None else keys)

    @classmethod
    def gateway_access(cls) -> "NativeStatusQuery":
        return cls(StatusRequest.GATEWAY_ACCESS, b"\x01")

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
        if self.kind is StatusRequest.GATEWAY_ACCESS and data[0] & 1:
            return None  # The query uses the same ID as the status; an echo is not a reply.
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
