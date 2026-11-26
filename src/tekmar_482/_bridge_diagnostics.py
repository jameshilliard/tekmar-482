"""Custom bridge diagnostic reply layouts indexed by the advertised wire version."""

from dataclasses import dataclass
from struct import Struct


@dataclass(frozen=True, slots=True)
class CounterLayout:
    panel: Struct
    capture: Struct
    drops: Struct
    trace: Struct
    maximum: int


LAYOUTS = {
    1: CounterLayout(
        Struct("<7I"),
        Struct("<12I"),
        Struct("<BII"),
        Struct("<B5I"),
        (1 << 32) - 1,
    ),
    2: CounterLayout(
        Struct("<2I2Q3I"),
        Struct("<2I5Q5I"),
        Struct("<BQQ"),
        Struct("<B5Q"),
        (1 << 64) - 1,
    ),
}
