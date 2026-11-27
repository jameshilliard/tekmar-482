"""Native I/O services from the 483 SW 11 registry and IoChannel model."""

from dataclasses import dataclass
from enum import IntEnum

from .codec import Fields, Reader

IO_CHANGE, IO_REPLY, IO_REQUEST, IO_DISCOVERY, IO_DISCOVER = range(0x1f2, 0x1f7)


class IoType(IntEnum):
    UNUSED = 0
    RELAY = 1
    VOLTAGE_OUTPUT = 2
    VOLTAGE_INPUT = 3
    CURRENT_OUTPUT = 4
    SENSOR_H = 5
    HUMIDITY = 6
    DEMAND = 7
    INTEGER = 8
    VARIABLE_SPEED_PUMP = 9
    SENSOR_F = 10
    PWM_CYCLE = 11
    DISABLED = 255


@dataclass(frozen=True, order=True, slots=True)
class IoEndpoint:
    """Two payload octets, not a native frame's six-bit header node."""
    subnet: int
    node: int

    def __post_init__(self) -> None:
        if not (0 <= self.subnet <= 255 and 0 <= self.node <= 255):
            raise ValueError("I/O endpoint must be two unsigned octets")


_TERMINALS = {
    "553": ("W1", "Acc", "Y", "G/O", "S1", "S2"),
    "557": ("Acc", "W2", "W1", "S1", "S2", "S3", "Y2", "Y1", "G1", "OB"),
    "654": ("Ht", "Mod", "Mod", "Mod", "Alrt/Sys", "Red", "Sup", "Out/Bret", "Yel", "Brn Slab", "Blu"),
}


def io_terminal(model: str, channel: int) -> str | None:
    """483 board-terminal reference, not a discovered label or proof of a channel."""
    terminals = _TERMINALS.get(model, ())
    return terminals[channel] if 0 <= channel < len(terminals) else None


@dataclass(frozen=True, slots=True)
class IoReport:
    endpoint: IoEndpoint
    channel: int
    type_id: int
    value: int
    count: int | None = None
    label: bytes | None = None

    @classmethod
    def read_value(cls, r: Reader) -> "IoReport":
        endpoint = IoEndpoint(*r.take(2))
        return cls(endpoint, r.byte(), r.byte(), r.word())

    @classmethod
    def read_discovery(cls, r: Reader) -> "IoReport":
        endpoint = IoEndpoint(*r.take(2))
        channel, count = r.take(2)
        label = r.take(6)
        return cls(endpoint, channel, r.byte(), r.word(), count, label)

    @property
    def kind(self) -> IoType | None:
        try:
            return IoType(self.type_id)
        except ValueError:
            return None

    @property
    def label_text(self) -> str | None:
        """Printable wire label, distinct from the model's terminal name."""
        if self.label is None:
            return None
        try:
            text = self.label.replace(b"\0", b"").decode("ascii")
        except UnicodeDecodeError:
            return None
        return text if text and text.isprintable() else None

    @property
    def active(self) -> bool | None:
        if self.kind in (IoType.RELAY, IoType.DEMAND) and self.value in (0, 1):
            return bool(self.value)
        return None

    @property
    def percent(self) -> int | None:
        if self.kind in (IoType.HUMIDITY, IoType.VARIABLE_SPEED_PUMP, IoType.PWM_CYCLE) and self.value <= 100:
            return self.value
        return None

    def fields(self) -> Fields:
        fields: Fields = {"subnet_id": self.endpoint.subnet, "node_id": self.endpoint.node,
                          "channel_id": self.channel, "type_id": self.type_id, "io_value": self.value}
        if self.count is not None and self.label is not None:
            fields.update(channel_count=self.count, label_hex=self.label.hex())
            if (label := self.label_text) is not None:
                fields["label_text"] = label
        if self.kind is not None:
            fields["io_type"] = self.kind.name.lower()
        if self.active is not None:
            fields["active"] = int(self.active)
        if self.percent is not None:
            fields["percent"] = self.percent
        return fields
