"""Unit conversion helpers for tekmar encoded values."""

from __future__ import annotations

from .constants import ThaValue


def celsius_to_fahrenheit(value: float) -> float:
    return (value * 9 / 5) + 32


def fahrenheit_to_celsius(value: float) -> float:
    return (value - 32) / 1.8


def celsius_to_dege(value: float) -> int:
    """Convert Celsius to tHA degE, used by one-byte setpoint values."""
    return _encode_temperature(value * 2, ThaValue.NA_8)


def dege_to_celsius(value: int) -> float | None:
    """Convert tHA degE to Celsius."""
    if value == ThaValue.NA_8:
        return None
    return value / 2


def fahrenheit_to_degh(value: float) -> int:
    """Convert Fahrenheit to tHA degH."""
    return _encode_temperature((value * 10) + 850, ThaValue.NA_16)


def _encode_temperature(raw: float, na: ThaValue) -> int:
    if not 0 <= raw <= na - 1:
        msg = f"raw temperature {raw} is outside the wire range 0..{na - 1}"
        raise ValueError(msg)
    return round(raw)


def degh_to_fahrenheit(value: int) -> float | None:
    """Convert tHA degH to Fahrenheit."""
    if value == ThaValue.NA_16:
        return None
    return (value - 850) / 10


def degh_to_celsius(value: int) -> float | None:
    """Convert tHA degH to Celsius."""
    fahrenheit = degh_to_fahrenheit(value)
    if fahrenheit is None:
        return None
    return fahrenheit_to_celsius(fahrenheit)


def celsius_to_degh(value: float) -> int:
    """Convert Celsius to tHA degH."""
    return fahrenheit_to_degh(celsius_to_fahrenheit(value))
