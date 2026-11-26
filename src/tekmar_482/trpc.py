"""tRPC message formatting for the tekmar 482 gateway."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import IntEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, Protocol, Self, runtime_checkable

from .constants import SETBACK_FAN_MAP, SETBACK_SETPOINT_MAP, ThaSetback, ThaValue
from .exceptions import (
    ProtocolError,
    UnknownFieldError,
    UnknownMethodError,
    UnknownServiceError,
)
from .packet import TYPE_TRPC, Packet

if TYPE_CHECKING:
    from datetime import datetime

_HEADER_SIZE = 5


@dataclass(frozen=True, slots=True)
class FieldSpec:
    """A fixed-width unsigned integer field in a tRPC message body."""

    name: str
    size: int
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class MethodSpec:
    """A tRPC method name and body field layout."""

    name: str
    fields: tuple[FieldSpec, ...] = ()
    aliases: tuple[str, ...] = ()

    @property
    def size(self) -> int:
        """Return the packed body size in bytes."""
        return sum(item.size for item in self.fields)

    @property
    def field_names(self) -> frozenset[str]:
        """Return all field names used by this method."""
        return frozenset(item.name for item in self.fields)

    @property
    def field_aliases(self) -> dict[str, str]:
        """Return accepted body field aliases mapped to canonical names."""
        aliases: dict[str, str] = {}
        for item in self.fields:
            aliases[item.name] = item.name
            for alias in item.aliases:
                aliases[alias] = item.name
        return aliases

    def normalize_fields(self, fields: Mapping[str, int]) -> dict[str, int]:
        """Normalize accepted aliases to canonical field names."""
        aliases = self.field_aliases
        normalized: dict[str, int] = {}
        unknown: list[str] = []
        for key, value in fields.items():
            canonical = aliases.get(key)
            if canonical is None:
                unknown.append(key)
            else:
                normalized[canonical] = value

        if unknown:
            msg = f"unknown field(s) for {self.name}: {', '.join(sorted(unknown))}"
            raise UnknownFieldError(msg)

        return normalized


class TrpcService(IntEnum):
    """Known tRPC service IDs."""

    UPDATE = 0x00
    REQUEST = 0x01
    REPORT = 0x02
    RESPONSE_UPDATE = 0x03
    RESPONSE_REQUEST = 0x04


class TrpcMethod(IntEnum):
    """Known tRPC method IDs exposed by the tekmar 482."""

    NULL_METHOD = 0x00000000
    NETWORK_ERROR = 0x00000107
    REPORTING_STATE = 0x0000010F
    OUTDOOR_TEMP = 0x00000117
    DEVICE_ATTRIBUTES = 0x0000011F
    MODE_SETTING = 0x00000127
    ACTIVE_DEMAND = 0x0000012F
    CURRENT_TEMPERATURE = 0x00000137
    CURRENT_FLOOR_TEMPERATURE = 0x00000138
    SETPOINT_GROUP_ENABLE = 0x0000013D
    SETPOINT_DEVICE = 0x0000013E
    HEAT_SETPOINT = 0x0000013F
    COOL_SETPOINT = 0x00000147
    SLAB_SETPOINT = 0x0000014F
    RELATIVE_HUMIDITY = 0x00000150
    HUMIDITY_SET_MAX = 0x00000151
    HUMIDITY_SET_MIN = 0x00000152
    FAN_PERCENT = 0x00000157
    TAKING_ADDRESS = 0x0000015F
    DEVICE_INVENTORY = 0x00000167
    SETBACK_ENABLE = 0x0000016F
    SETBACK_STATE = 0x00000177
    SETBACK_EVENTS = 0x0000017F
    FIRMWARE_REVISION = 0x00000187
    PROTOCOL_VERSION = 0x0000018F
    DEVICE_TYPE = 0x00000197
    DEVICE_VERSION = 0x0000019F
    DATE_TIME = 0x000001A7


type TrpcServiceId = TrpcService | int
type TrpcMethodId = TrpcMethod | int
type TrpcFieldMap = Mapping[str, int]


@runtime_checkable
class TrpcPayload(Protocol):
    """Object that can provide tRPC body fields."""

    def as_fields(self) -> TrpcFieldMap:
        """Return canonical tRPC body fields."""


type TrpcFieldsInput = TrpcFieldMap | TrpcPayload


class AddressPayload:
    """Mixin for payloads addressed to a single device."""

    address: int

    def coalesce_key(self, method: TrpcMethodId) -> tuple[int, ...]:
        """Return a key for superseding unsent writes to the same device."""
        return (method_id(method), self.address)


class AddressSetbackPayload(AddressPayload):
    """Mixin for payloads addressed to a device setback slot."""

    setback: int

    def match_fields(self) -> TrpcFieldMap:
        """Return fields that identify matching responses for this setback."""
        return {"setback": int(self.setback)}

    def coalesce_key(self, method: TrpcMethodId) -> tuple[int, int, int]:
        """Return a key for superseding unsent writes to the same slot."""
        return (method_id(method), self.address, int(self.setback))


@dataclass(frozen=True, slots=True)
class AddressFields(AddressPayload):
    """Payload containing a tHA device address."""

    address: int

    def as_fields(self) -> TrpcFieldMap:
        return {"address": self.address}


@dataclass(frozen=True, slots=True)
class AddressSetbackFields(AddressSetbackPayload):
    """Payload containing a tHA device address and setback selector."""

    address: int
    setback: int = ThaSetback.CURRENT

    def as_fields(self) -> TrpcFieldMap:
        return {"address": self.address, "setback": int(self.setback)}


@dataclass(frozen=True, slots=True)
class ReportingStateFields:
    """Payload for the gateway ReportingState method."""

    enabled: bool

    def as_fields(self) -> TrpcFieldMap:
        return {"state": int(self.enabled)}


@dataclass(frozen=True, slots=True)
class EnableFields:
    """Payload for methods that carry a single enable flag."""

    enabled: bool

    def as_fields(self) -> TrpcFieldMap:
        return {"enable": int(self.enabled)}


@dataclass(frozen=True, slots=True)
class SetpointGroupFields:
    """Payload for a setpoint group request or update."""

    group_id: int
    enabled: bool | None = None

    def as_fields(self) -> TrpcFieldMap:
        fields = {"group_id": self.group_id}
        if self.enabled is not None:
            fields["enable"] = int(self.enabled)
        return fields

    def match_fields(self) -> TrpcFieldMap:
        """Return fields that identify this setpoint group."""
        return {"group_id": self.group_id}


@dataclass(frozen=True, slots=True)
class DateTimeFields:
    """Payload for the gateway DateTime method."""

    year: int
    month: int
    day: int
    weekday: int
    hour: int
    minute: int

    @classmethod
    def from_datetime(cls, value: datetime) -> DateTimeFields:
        """Build DateTime fields from a datetime-like object."""
        return cls(
            year=value.year,
            month=value.month,
            day=value.day,
            weekday=value.isoweekday(),
            hour=value.hour,
            minute=value.minute,
        )

    def as_fields(self) -> TrpcFieldMap:
        return {
            "year": self.year,
            "month": self.month,
            "day": self.day,
            "weekday": self.weekday,
            "hour": self.hour,
            "minute": self.minute,
        }


@dataclass(frozen=True, slots=True)
class SetbackSetpointFields(AddressSetbackPayload):
    """Payload for HeatSetpoint/CoolSetpoint/SlabSetpoint writes."""

    address: int
    setpoint: int
    setback: int = ThaSetback.CURRENT

    def as_fields(self) -> TrpcFieldMap:
        return {
            "address": self.address,
            "setback": int(self.setback),
            "setpoint": self.setpoint,
        }


@dataclass(frozen=True, slots=True)
class SetbackPercentFields(AddressSetbackPayload):
    """Payload for FanPercent writes."""

    address: int
    percent: int
    setback: int = ThaSetback.CURRENT

    def as_fields(self) -> TrpcFieldMap:
        return {
            "address": self.address,
            "setback": int(self.setback),
            "percent": self.percent,
        }


@dataclass(frozen=True, slots=True)
class SetbackTemperatureFields(AddressSetbackPayload):
    """Payload for SetpointDevice target temperature writes."""

    address: int
    temp: int
    setback: int = ThaSetback.CURRENT

    def as_fields(self) -> TrpcFieldMap:
        return {
            "address": self.address,
            "setback": int(self.setback),
            "temp": self.temp,
        }


@dataclass(frozen=True, slots=True)
class ModeSettingFields(AddressPayload):
    """Payload for ModeSetting writes."""

    address: int
    mode: int

    def as_fields(self) -> TrpcFieldMap:
        return {"address": self.address, "mode": self.mode}


SERVICE_SPECS: dict[TrpcService, str] = {
    TrpcService.UPDATE: "Update",
    TrpcService.REQUEST: "Request",
    TrpcService.REPORT: "Report",
    TrpcService.RESPONSE_UPDATE: "Response:Update",
    TrpcService.RESPONSE_REQUEST: "Response:Request",
}
SERVICE_IDS: dict[str, TrpcService] = {
    name: TrpcService(value) for value, name in SERVICE_SPECS.items()
}


def _field(name: str, size: int, *aliases: str) -> FieldSpec:
    return FieldSpec(name, size, aliases)


def _fields(*items: FieldSpec) -> tuple[FieldSpec, ...]:
    return items


METHOD_SPECS: dict[TrpcMethod, MethodSpec] = {
    TrpcMethod.NULL_METHOD: MethodSpec("NullMethod"),
    TrpcMethod.NETWORK_ERROR: MethodSpec("NetworkError", _fields(_field("error", 2))),
    TrpcMethod.REPORTING_STATE: MethodSpec(
        "ReportingState",
        _fields(_field("state", 1)),
    ),
    TrpcMethod.OUTDOOR_TEMP: MethodSpec(
        "OutdoorTemp",
        _fields(_field("temp", 2)),
        aliases=("OutdoorTemperature",),
    ),
    TrpcMethod.DEVICE_ATTRIBUTES: MethodSpec(
        "DeviceAttributes",
        _fields(_field("address", 2), _field("attributes", 2)),
    ),
    TrpcMethod.MODE_SETTING: MethodSpec(
        "ModeSetting",
        _fields(_field("address", 2), _field("mode", 1)),
    ),
    TrpcMethod.ACTIVE_DEMAND: MethodSpec(
        "ActiveDemand",
        _fields(_field("address", 2), _field("demand", 1)),
    ),
    TrpcMethod.CURRENT_TEMPERATURE: MethodSpec(
        "CurrentTemperature",
        _fields(_field("address", 2), _field("temp", 2)),
    ),
    TrpcMethod.CURRENT_FLOOR_TEMPERATURE: MethodSpec(
        "CurrentFloorTemperature",
        _fields(_field("address", 2), _field("temp", 2)),
    ),
    TrpcMethod.SETPOINT_GROUP_ENABLE: MethodSpec(
        "SetpointGroupEnable",
        _fields(
            _field("group_id", 1, "IDnumber", "groupid"),
            _field("enable", 1, "Enable"),
        ),
    ),
    TrpcMethod.SETPOINT_DEVICE: MethodSpec(
        "SetpointDevice",
        _fields(_field("address", 2), _field("setback", 1), _field("temp", 2)),
    ),
    TrpcMethod.HEAT_SETPOINT: MethodSpec(
        "HeatSetpoint",
        _fields(_field("address", 2), _field("setback", 1), _field("setpoint", 1)),
    ),
    TrpcMethod.COOL_SETPOINT: MethodSpec(
        "CoolSetpoint",
        _fields(_field("address", 2), _field("setback", 1), _field("setpoint", 1)),
    ),
    TrpcMethod.SLAB_SETPOINT: MethodSpec(
        "SlabSetpoint",
        _fields(_field("address", 2), _field("setback", 1), _field("setpoint", 1)),
    ),
    TrpcMethod.RELATIVE_HUMIDITY: MethodSpec(
        "RelativeHumidity",
        _fields(_field("address", 2), _field("percent", 1, "RHpercent")),
    ),
    TrpcMethod.HUMIDITY_SET_MAX: MethodSpec(
        "HumiditySetMax",
        _fields(_field("address", 2), _field("percent", 1, "percentMax")),
    ),
    TrpcMethod.HUMIDITY_SET_MIN: MethodSpec(
        "HumiditySetMin",
        _fields(_field("address", 2), _field("percent", 1, "percentMin")),
    ),
    TrpcMethod.FAN_PERCENT: MethodSpec(
        "FanPercent",
        _fields(_field("address", 2), _field("setback", 1), _field("percent", 1)),
    ),
    TrpcMethod.TAKING_ADDRESS: MethodSpec(
        "TakingAddress",
        _fields(_field("old_address", 2), _field("new_address", 2)),
    ),
    TrpcMethod.DEVICE_INVENTORY: MethodSpec(
        "DeviceInventory",
        _fields(_field("address", 2)),
    ),
    TrpcMethod.SETBACK_ENABLE: MethodSpec(
        "SetbackEnable",
        _fields(_field("enable", 1)),
    ),
    TrpcMethod.SETBACK_STATE: MethodSpec(
        "SetbackState",
        _fields(_field("address", 2), _field("setback", 1)),
    ),
    TrpcMethod.SETBACK_EVENTS: MethodSpec(
        "SetbackEvents",
        _fields(_field("address", 2), _field("events", 1)),
    ),
    TrpcMethod.FIRMWARE_REVISION: MethodSpec(
        "FirmwareRevision",
        _fields(_field("revision", 2)),
    ),
    TrpcMethod.PROTOCOL_VERSION: MethodSpec(
        "ProtocolVersion",
        _fields(_field("version", 2)),
    ),
    TrpcMethod.DEVICE_TYPE: MethodSpec(
        "DeviceType",
        _fields(_field("address", 2), _field("type", 4)),
    ),
    TrpcMethod.DEVICE_VERSION: MethodSpec(
        "DeviceVersion",
        _fields(_field("address", 2), _field("j_number", 4)),
    ),
    TrpcMethod.DATE_TIME: MethodSpec(
        "DateTime",
        _fields(
            _field("year", 2),
            _field("month", 1),
            _field("day", 1),
            _field("weekday", 1),
            _field("hour", 1),
            _field("minute", 1),
        ),
    ),
}
METHOD_IDS: dict[str, TrpcMethod] = {
    spec.name: method for method, spec in METHOD_SPECS.items()
}
for _method, _method_spec in METHOD_SPECS.items():
    for _alias in _method_spec.aliases:
        METHOD_IDS[_alias] = _method


def service_id(service: TrpcServiceId) -> int:
    """Resolve a service enum or numeric service ID."""
    return int(service)


def method_id(method: TrpcMethodId) -> int:
    """Resolve a method enum or numeric method ID."""
    return int(method)


def service_from_name(name: str) -> TrpcService:
    """Resolve a known service display name."""
    try:
        return SERVICE_IDS[name]
    except KeyError as err:
        msg = f"unknown tRPC service: {name!r}"
        raise UnknownServiceError(msg) from err


def method_from_name(name: str) -> TrpcMethod:
    """Resolve a known method display name or alias."""
    try:
        return METHOD_IDS[name]
    except KeyError as err:
        msg = f"unknown tRPC method: {name!r}"
        raise UnknownMethodError(msg) from err


def service_name(service: TrpcServiceId) -> str | None:
    """Return a known service display name."""
    try:
        return SERVICE_SPECS[TrpcService(service)]
    except ValueError:
        return None


def method_spec(method: TrpcMethodId) -> MethodSpec | None:
    """Return a known method spec."""
    try:
        return METHOD_SPECS[TrpcMethod(method)]
    except ValueError:
        return None


def method_name(method: TrpcMethodId) -> str | None:
    """Return a known method display name."""
    spec = method_spec(method)
    return spec.name if spec is not None else None


_SETPOINT_FIELDS = {
    TrpcMethod.HEAT_SETPOINT: ("setpoint", ThaValue.NA_8),
    TrpcMethod.COOL_SETPOINT: ("setpoint", ThaValue.NA_8),
    TrpcMethod.SLAB_SETPOINT: ("setpoint", ThaValue.NA_8),
    TrpcMethod.SETPOINT_DEVICE: ("temp", ThaValue.NA_16),
}
_SETBACK_INDEXED_METHODS = frozenset((*_SETPOINT_FIELDS, TrpcMethod.FAN_PERCENT))


def _echo_field(method: TrpcMethodId) -> str:
    return (
        "percent"
        if method == TrpcMethod.FAN_PERCENT
        else _SETPOINT_FIELDS[TrpcMethod(method)][0]
    )


def _validate_update(
    service: TrpcServiceId,
    method: TrpcMethodId,
    fields: TrpcFieldMap,
) -> None:
    if service != TrpcService.UPDATE:
        return
    if (
        method in _SETBACK_INDEXED_METHODS
        and "setback" in fields
        and fields["setback"] not in ThaSetback
    ):
        msg = f"{method_name(method)}: invalid setback selector {fields['setback']}"
        raise ValueError(msg)
    if method in _SETPOINT_FIELDS:
        name, na = _SETPOINT_FIELDS[TrpcMethod(method)]
        value = fields.get(name)
        if value is None:
            msg = f"{method_name(method)} update requires {name}"
            raise ValueError(msg)
        if not 0 <= value < na:
            msg = f"{method_name(method)} {name} must be in 0..{na - 1}, got {value}"
            raise ValueError(msg)


def _pack_int(value: int, size: int) -> bytes:
    try:
        return value.to_bytes(size, byteorder="little")
    except OverflowError as err:
        msg = f"{value} does not fit an unsigned {size}-byte field"
        raise ValueError(msg) from err


def _unpack_int(data: bytes) -> int:
    return int.from_bytes(data, byteorder="little", signed=False)


def response_service_for(service: TrpcServiceId) -> TrpcService | None:
    """Return the normal response service for a sent service."""
    service_value = service_id(service)
    if service_value == TrpcService.REQUEST:
        return TrpcService.RESPONSE_REQUEST
    if service_value == TrpcService.UPDATE:
        return TrpcService.RESPONSE_UPDATE
    return None


def freeze_fields(fields: TrpcFieldsInput | None = None) -> TrpcFieldMap:
    """Return an immutable copy of raw mapping or payload object fields."""
    if fields is None:
        return MappingProxyType({})
    raw_fields = fields.as_fields() if isinstance(fields, TrpcPayload) else fields
    return MappingProxyType(dict(raw_fields))


@dataclass(frozen=True, slots=True)
class TrpcCommand:
    """Typed tRPC command before it is serialized to a packet."""

    service: TrpcServiceId = TrpcService.REQUEST
    method: TrpcMethodId = TrpcMethod.NULL_METHOD
    fields: TrpcFieldMap = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Freeze fields so callers cannot mutate commands after creation."""
        object.__setattr__(self, "fields", freeze_fields(self.fields))
        _validate_update(self.service, self.method, self.fields)

    @classmethod
    def request(
        cls,
        method: TrpcMethodId,
        fields: TrpcFieldsInput | None = None,
    ) -> Self:
        """Create a Request command."""
        return cls(TrpcService.REQUEST, method, freeze_fields(fields))

    @classmethod
    def update(
        cls,
        method: TrpcMethodId,
        fields: TrpcFieldsInput | None = None,
    ) -> Self:
        """Create an Update command."""
        return cls(TrpcService.UPDATE, method, freeze_fields(fields))

    @property
    def service_id(self) -> int:
        """Return the numeric service ID."""
        return service_id(self.service)

    @property
    def method_id(self) -> int:
        """Return the numeric method ID."""
        return method_id(self.method)

    def to_message(self) -> TrpcPacket:
        """Create a tRPC message for this command."""
        return TrpcPacket.create(
            service=self.service,
            method=self.method,
            fields=self.fields,
        )


@dataclass(frozen=True, slots=True)
class DeviceReplySelector:
    """Identify a device reply that must not be credited to a later exchange."""

    service: TrpcService
    method: int
    address: int
    slot: int | None
    echo: tuple[str, int] | None

    @classmethod
    def for_command(cls, command: TrpcCommand) -> Self | None:
        if (
            command.method not in _SETBACK_INDEXED_METHODS
            and command.method != TrpcMethod.MODE_SETTING
        ):
            return None
        return cls.for_message(command.to_message())

    @classmethod
    def for_message(cls, message: TrpcPacket) -> Self | None:
        service = response_service_for(message.service_id)
        if (
            message.method is TrpcMethod.MODE_SETTING
            and service is TrpcService.RESPONSE_UPDATE
        ):
            return cls(
                service,
                message.method_id,
                message.body.get("address", 0),
                None,
                ("mode", message.body.get("mode", 0)),
            )
        if service is None or message.method_id not in _SETBACK_INDEXED_METHODS:
            return None
        echo = None
        if service is TrpcService.RESPONSE_UPDATE:
            key = _echo_field(message.method_id)
            echo = (key, message.body.get(key, 0))
        return cls(
            service,
            message.method_id,
            message.body.get("address", 0),
            message.body.get("setback", 0),
            echo,
        )

    def can_satisfy(self, match: ResponseMatch) -> bool:
        """Return whether the outstanding reply could satisfy this matcher."""
        slot = match.fields.get("setback")
        return (
            (match.service_ids is None or self.service in match.service_ids)
            and self.method == match.method_id
            and (match.address is None or self.address == match.address)
            and self.address == match.fields.get("address", self.address)
            and (
                slot is None
                or self.slot == slot
                or ThaSetback.CURRENT in (self.slot, slot)
            )
            and (
                self.echo is None
                or match.fields.get(self.echo[0], self.echo[1]) == self.echo[1]
            )
        )

    def overlaps(self, other: DeviceReplySelector) -> bool:
        """Return whether a single firmware echo could satisfy either selector."""
        return (
            self.service == other.service
            and self.method == other.method
            and self.address == other.address
            and self.echo == other.echo
            and (
                self.slot == other.slot or ThaSetback.CURRENT in (self.slot, other.slot)
            )
        )

    def shares_write_target(
        self,
        other: DeviceReplySelector,
        *,
        setbacks_enabled: bool,
    ) -> bool:
        """Compare storage slots, not echoed values, when ordering writes."""
        if self.method != other.method or self.address != other.address:
            return False
        if (
            not setbacks_enabled
            or self.slot is None
            or other.slot is None
            or ThaSetback.CURRENT in (self.slot, other.slot)
        ):
            return True
        slots = (
            SETBACK_FAN_MAP
            if self.method == TrpcMethod.FAN_PERCENT
            else SETBACK_SETPOINT_MAP
        )
        return slots.get(self.slot, self.slot) == slots.get(other.slot, other.slot)

    def accepts(self, message: TrpcPacket) -> bool:
        slot = message.get("setback")
        return (
            message.service is self.service
            and message.method_id == self.method
            and message.get("address") == self.address
            and (self.echo is None or message.get(self.echo[0]) == self.echo[1])
            and (
                slot == self.slot
                or (self.slot == ThaSetback.CURRENT and slot in ThaSetback)
            )
        )


@dataclass(frozen=True, slots=True)
class ResponseMatch:
    """Typed predicate for matching tRPC responses."""

    method_id: int
    service_ids: frozenset[int] | None = None
    address: int | None = None
    fields: TrpcFieldMap = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Freeze field predicates so matches cannot change while pending."""
        object.__setattr__(self, "fields", freeze_fields(self.fields))

    @classmethod
    def create(
        cls,
        method: TrpcMethodId,
        *,
        services: Iterable[TrpcServiceId] | None = None,
        address: int | None = None,
        fields: TrpcFieldsInput | None = None,
    ) -> Self:
        """Create a normalized response match predicate."""
        return cls(
            method_id(method),
            (
                None
                if services is None
                else frozenset(service_id(service) for service in services)
            ),
            address,
            freeze_fields(fields),
        )

    @classmethod
    def for_command(
        cls,
        command: TrpcCommand,
        *,
        response_method: TrpcMethodId | None = None,
        response_services: Iterable[TrpcServiceId] | None = None,
        address: int | None = None,
        fields: TrpcFieldsInput | None = None,
    ) -> Self:
        """Create the default response match for a command."""
        services: Iterable[TrpcServiceId] | None
        if response_services is None:
            response_service = response_service_for(command.service)
            services = None if response_service is None else (response_service,)
        else:
            services = response_services

        method = command.method if response_method is None else response_method
        spec = method_spec(method)
        if (
            method == command.method
            and command.service in {TrpcService.REQUEST, TrpcService.UPDATE}
            and spec is not None
        ):
            sent = spec.normalize_fields(command.fields)
            predicates = spec.normalize_fields(freeze_fields(fields))
            # The inventory address starts a walk; it is not an echoed selector.
            if (
                address is None
                and "address" not in predicates
                and "address" in spec.field_names
                and method != TrpcMethod.DEVICE_INVENTORY
            ):
                address = sent.get("address", 0)
            if method == TrpcMethod.SETPOINT_GROUP_ENABLE:
                predicates.setdefault("group_id", sent.get("group_id", 0))
            if (
                method == TrpcMethod.MODE_SETTING
                and command.service == TrpcService.UPDATE
            ):
                predicates.setdefault("mode", sent.get("mode", 0))
            if method in _SETBACK_INDEXED_METHODS:
                predicates.setdefault("setback", sent.get("setback", 0))
                if command.service == TrpcService.UPDATE:
                    key = _echo_field(method)
                    predicates.setdefault(key, sent.get(key, 0))
            fields = predicates

        return cls.create(
            method,
            services=services,
            address=address,
            fields=fields,
        )

    def matches(self, message: TrpcPacket) -> bool:
        """Return whether a tRPC message satisfies this match."""
        if self.service_ids is not None and message.service_id not in self.service_ids:
            return False
        if message.method_id != self.method_id:
            return False
        if self.address is not None and message.get("address") != self.address:
            return False
        for key, expected in self.fields.items():
            actual = message.get(key)
            if actual == expected:
                continue
            # The 482 can answer a CURRENT read or write with the resolved slot.
            if (
                key == "setback"
                and expected == ThaSetback.CURRENT
                and message.service
                in {
                    TrpcService.RESPONSE_REQUEST,
                    TrpcService.RESPONSE_UPDATE,
                }
                and message.method in _SETBACK_INDEXED_METHODS
                and actual in ThaSetback
            ):
                continue
            return False
        return True


@dataclass(slots=True)
class TrpcPacket:
    """A tRPC packet carried inside a tekmar packet of type 6."""

    service_id: int
    method_id: int
    body: dict[str, int] = field(default_factory=dict)
    extra: bytes = b""

    @classmethod
    def create(
        cls,
        *,
        service: TrpcServiceId = TrpcService.REQUEST,
        method: TrpcMethodId = TrpcMethod.NULL_METHOD,
        fields: TrpcFieldsInput | None = None,
    ) -> Self:
        """Create a tRPC packet by service/method enum or numeric ID."""
        resolved_service_id = service_id(service)
        resolved_method_id = method_id(method)
        return cls(
            resolved_service_id,
            resolved_method_id,
            cls._normalize_fields(resolved_method_id, freeze_fields(fields)),
        )

    @property
    def service(self) -> TrpcService | None:
        """Return the service enum, if known."""
        try:
            return TrpcService(self.service_id)
        except ValueError:
            return None

    @property
    def service_name(self) -> str | None:
        """Return the service display name, if known."""
        return service_name(self.service_id)

    @property
    def method(self) -> TrpcMethod | None:
        """Return the method enum, if known."""
        try:
            return TrpcMethod(self.method_id)
        except ValueError:
            return None

    @property
    def method_name(self) -> str | None:
        """Return the method display name, if known."""
        return method_name(self.method_id)

    def to_bytes(self) -> bytes:
        """Serialize the tRPC header and body."""
        _validate_update(self.service_id, self.method_id, self.body)
        spec = method_spec(self.method_id)
        body = bytearray()
        if spec is not None:
            normalized = spec.normalize_fields(self.body)
            for item in spec.fields:
                body.extend(_pack_int(normalized.get(item.name, 0), item.size))
        else:
            body.extend(self.extra)

        if spec is not None:
            body.extend(self.extra)

        return (
            _pack_int(self.service_id, 1) + _pack_int(self.method_id, 4) + bytes(body)
        )

    def to_packet(self) -> Packet:
        """Wrap this tRPC message in a tekmar packet."""
        return Packet(TYPE_TRPC, self.to_bytes())

    @classmethod
    def from_packet(cls, packet: Packet) -> Self:
        """Parse a tRPC packet from a tekmar packet."""
        if packet.type != TYPE_TRPC:
            msg = f"packet type {packet.type} is not TYPE_TRPC"
            raise ProtocolError(msg)
        return cls.from_bytes(packet.data)

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        """Parse tRPC header and body bytes."""
        if len(data) < _HEADER_SIZE:
            msg = "tRPC payload must contain a 5-byte header"
            raise ProtocolError(msg)

        parsed_service_id = data[0]
        parsed_method_id = _unpack_int(data[1:_HEADER_SIZE])
        body_data = data[_HEADER_SIZE:]
        spec = method_spec(parsed_method_id)
        if spec is None:
            return cls(parsed_service_id, parsed_method_id, {}, body_data)

        if (
            parsed_service_id
            in (
                TrpcService.REPORT,
                TrpcService.RESPONSE_REQUEST,
                TrpcService.RESPONSE_UPDATE,
            )
            and len(body_data) < spec.size
        ):
            msg = (
                f"Short {spec.name} body: need {spec.size} bytes, got {len(body_data)}"
            )
            raise ProtocolError(msg)
        offset = 0
        body: dict[str, int] = {}
        for item in spec.fields:
            next_offset = offset + item.size
            if next_offset <= len(body_data):
                body[item.name] = _unpack_int(body_data[offset:next_offset])
            offset = next_offset

        return cls(
            service_id=parsed_service_id,
            method_id=parsed_method_id,
            body=body,
            extra=body_data[spec.size :],
        )

    @staticmethod
    def _normalize_fields(method: int, fields: Mapping[str, int]) -> dict[str, int]:
        spec = method_spec(method)
        if spec is None:
            return dict(fields)
        return spec.normalize_fields(fields)

    def get(self, field: str, default: int | None = None) -> int | None:
        """Return a body field by canonical name or accepted alias."""
        spec = method_spec(self.method_id)
        canonical = spec.field_aliases.get(field, field) if spec is not None else field
        return self.body.get(canonical, default)

    def require(self, field: str) -> int:
        """Return a required body field by canonical name or accepted alias."""
        value = self.get(field)
        if value is None:
            msg = (
                f"message {self.method_name or self.method_id!r} has no field {field!r}"
            )
            raise UnknownFieldError(msg)
        return value

    def __str__(self) -> str:
        service = self.service_name or f"0x{self.service_id:02X}"
        method = self.method_name or f"0x{self.method_id:08X}"
        if not self.body and not self.extra:
            return f"{service} {method}"

        values = ", ".join(f"{key}={value}" for key, value in self.body.items())
        if self.extra:
            values = (
                f"{values}, extra={self.extra.hex().upper()}"
                if values
                else (f"extra={self.extra.hex().upper()}")
            )
        return f"{service} {method} <{values}>"
