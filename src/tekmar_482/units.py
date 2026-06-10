"""Unit conversion helpers for tekmar encoded values."""

from __future__ import annotations

from .constants import ThaValue


def celsius_to_fahrenheit(value: float) -> float:
    return (value * 9 / 5) + 32


def fahrenheit_to_celsius(value: float) -> float:
    return (value - 32) / 1.8


def celsius_to_dege(value: float) -> int:
    """Convert Celsius to tHA degE, used by one-byte setpoint values."""
    return round(value * 2)


def dege_to_celsius(value: int) -> float | None:
    """Convert tHA degE to Celsius."""
    if value == ThaValue.NA_8:
        return None
    return value / 2


def fahrenheit_to_degh(value: float) -> int:
    """Convert Fahrenheit to tHA degH."""
    return round((value * 10) + 850)


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
