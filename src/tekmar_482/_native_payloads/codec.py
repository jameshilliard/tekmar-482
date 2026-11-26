"""Common primitives for native payload decoders."""

from .layouts import H_UNAVAILABLE

Fields = dict[str, str | int | float]
SETBACK = ("wake", "unoccupied_4", "occupied_4", "sleep", "occupied_2", "unoccupied_2", "away", "unknown")


class ShortPayload(ValueError):
    pass


class Reader:
    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def take(self, n: int) -> bytes:
        end = self.pos + n
        if end > len(self.data):
            raise ShortPayload(f"need_{end}_bytes_have_{len(self.data)}")
        field = self.data[self.pos:end]
        self.pos = end
        return field

    def byte(self) -> int:
        return self.take(1)[0]

    def word(self) -> int:
        return int.from_bytes(self.take(2), "little")

    @property
    def remaining(self) -> bytes:
        return self.data[self.pos:]


def fahrenheit(raw: int | None, *, deg_e: bool = False) -> float | None:
    if raw is None or raw == 0 or raw in ((0xff,) if deg_e else H_UNAVAILABLE):
        return None
    return round(raw * 0.9 + 32, 1) if deg_e else (raw - 850) / 10


def temp(name: str, raw: int, *, deg_e: bool = False) -> Fields:
    fields: Fields = {name + "_raw": raw}
    if (value := fahrenheit(raw, deg_e=deg_e)) is not None:
        fields[name + "_f"] = value
    elif raw != 0:
        fields[name] = "unavailable"
    return fields


def status_flags(value: int) -> Fields:
    setback = (value >> 1) & 7
    return {"enabled": value & 1, "setback_state": setback, "setback_name": SETBACK[setback],
            "optimum_start": (value >> 4) & 3, "priority": value >> 6}
