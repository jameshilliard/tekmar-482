"""Decode raw tHA values into integration-friendly values."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .constants import (
    SETBACK_DESCRIPTION,
    TN_ERRORS,
    ActiveDemand,
    DeviceMode,
    ThaSetback,
    ThaValue,
)
from .units import celsius_to_fahrenheit, dege_to_celsius, degh_to_celsius

if TYPE_CHECKING:
    from collections.abc import Mapping


@dataclass(frozen=True, slots=True)
class DecodedTemperature:
    """A decoded tHA temperature value."""

    raw: int
    celsius: float
    fahrenheit: float


@dataclass(frozen=True, slots=True)
class DecodedNetworkError:
    """A decoded tekmar network error value."""

    raw: int
    low_byte: int
    high_byte: int
    description: str


@dataclass(frozen=True, slots=True)
class DecodedNamedValue:
    """A decoded integer enum-like value."""

    raw: int
    name: str
    description: str | None = None


def decode_network_error(value: int | None) -> DecodedNetworkError | None:
    """Decode a gateway NetworkError value."""
    if value is None:
        return None
    low_byte = value & 0xFF
    high_byte = (value >> 8) & 0xFF
    return DecodedNetworkError(
        raw=value,
        low_byte=low_byte,
        high_byte=high_byte,
        description=TN_ERRORS.get(low_byte, "Unknown Error"),
    )


def decode_degh(value: object) -> DecodedTemperature | None:
    """Decode a two-byte tHA degH temperature value."""
    if not isinstance(value, int) or value == ThaValue.NA_16:
        return None
    celsius = degh_to_celsius(value)
    if celsius is None:
        return None
    fahrenheit = celsius_to_fahrenheit(celsius)
    return DecodedTemperature(
        raw=value,
        celsius=round(celsius, 1),
        fahrenheit=round(fahrenheit, 1),
    )


def decode_dege(value: object) -> DecodedTemperature | None:
    """Decode a one-byte tHA degE setpoint value."""
    if not isinstance(value, int) or value == ThaValue.NA_8:
        return None
    celsius = dege_to_celsius(value)
    if celsius is None:
        return None
    return DecodedTemperature(
        raw=value,
        celsius=round(celsius, 1),
        fahrenheit=round(celsius_to_fahrenheit(celsius), 1),
    )


def decode_percent(value: object) -> int | None:
    """Decode a one-byte percent value."""
    if not isinstance(value, int) or value == ThaValue.NA_8:
        return None
    return value


def decode_active_demand(value: object) -> DecodedNamedValue | None:
    """Decode an ActiveDemand value."""
    if not isinstance(value, int) or value == ThaValue.NA_8:
        return None
    try:
        name = ActiveDemand(value).name.lower()
    except ValueError:
        name = "unknown"
    return DecodedNamedValue(raw=value, name=name)


def decode_device_mode(value: object) -> DecodedNamedValue | None:
    """Decode a DeviceMode value."""
    if not isinstance(value, int) or value == ThaValue.NA_8:
        return None
    try:
        name = DeviceMode(value).name.lower()
    except ValueError:
        name = "unknown"
    return DecodedNamedValue(raw=value, name=name)


def decode_setback(value: object) -> DecodedNamedValue | None:
    """Decode a tHA setback value."""
    if not isinstance(value, int) or value == ThaValue.NA_8:
        return None
    try:
        setback = ThaSetback(value)
    except ValueError:
        return DecodedNamedValue(raw=value, name="unknown")
    return DecodedNamedValue(
        raw=value,
        name=setback.name.lower(),
        description=SETBACK_DESCRIPTION.get(setback),
    )


def decode_setpoint_map(value: object) -> dict[str, DecodedTemperature | None] | None:
    """Decode a setback-keyed setpoint mapping."""
    if not isinstance(value, dict):
        return None
    return {str(key): decode_dege(raw) for key, raw in value.items()}


def decode_temperature_map(
    value: object,
) -> dict[str, DecodedTemperature | None] | None:
    """Decode a setback-keyed degH temperature mapping."""
    if not isinstance(value, dict):
        return None
    return {str(key): decode_degh(raw) for key, raw in value.items()}


def decode_percent_map(value: object) -> dict[str, int | None] | None:
    """Decode a setback-keyed percent mapping."""
    if not isinstance(value, dict):
        return None
    return {str(key): decode_percent(raw) for key, raw in value.items()}


def decode_device_values(values: Mapping[str, object]) -> dict[str, object]:
    """Decode a raw DeviceSnapshot values mapping."""
    decoded: dict[str, object] = {}
    for key, value in values.items():
        if key in {"current_temperature", "current_floor_temperature"}:
            decoded[key] = decode_degh(value)
        elif key in {"heat_setpoints", "cool_setpoints", "slab_setpoints"}:
            decoded[key] = decode_setpoint_map(value)
        elif key == "setpoint_targets":
            decoded[key] = decode_temperature_map(value)
        elif key == "fan_percent":
            decoded[key] = decode_percent_map(value)
        elif key == "active_demand":
            decoded[key] = decode_active_demand(value)
        elif key == "setback_state":
            decoded[key] = decode_setback(value)
        elif key == "mode_setting":
            decoded[key] = decode_device_mode(value)
        elif key in {
            "relative_humidity",
            "humidity_setpoint_min",
            "humidity_setpoint_max",
        }:
            decoded[key] = decode_percent(value)
        elif key == "setpoint_target":
            decoded[key] = decode_degh(value)
    return decoded


def decoded_to_dict(value: object) -> object:
    """Convert decoded dataclasses into JSON-friendly dictionaries."""
    if isinstance(value, DecodedTemperature):
        return {
            "raw": value.raw,
            "celsius": value.celsius,
            "fahrenheit": value.fahrenheit,
        }
    if isinstance(value, DecodedNetworkError):
        return {
            "raw": value.raw,
            "low_byte": value.low_byte,
            "high_byte": value.high_byte,
            "description": value.description,
        }
    if isinstance(value, DecodedNamedValue):
        return {
            "raw": value.raw,
            "name": value.name,
            "description": value.description,
        }
    if isinstance(value, dict):
        return {str(key): decoded_to_dict(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [decoded_to_dict(item) for item in value]
    return value
