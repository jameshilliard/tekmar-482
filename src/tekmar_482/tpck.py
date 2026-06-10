"""TPCK binary framing for tekmar packets."""

from __future__ import annotations

from enum import Enum, auto
from typing import TYPE_CHECKING

from .exceptions import ProtocolError
from .packet import Packet

if TYPE_CHECKING:
    from collections.abc import Iterable

SOF = 0xCA
EOF = 0x35
ESC = 0x2F
_STUFFED_BYTES = frozenset({SOF, EOF, ESC})
_MAX_DATA_LENGTH = 0xFF


class _State(Enum):
    WAIT_SOF = auto()
    LENGTH = auto()
    TYPE = auto()
    DATA = auto()
    CHECKSUM = auto()
    EOF = auto()


def checksum(packet_type: int, data: bytes) -> int:
    """Return the 8-bit TPCK checksum for a packet type and payload."""
    return (packet_type + len(data) + sum(data)) & 0xFF


def serialize(packet: Packet) -> bytes:
    """Serialize a packet into a TPCK frame."""
    if len(packet.data) > _MAX_DATA_LENGTH:
        msg = "TPCK payloads are limited to 255 bytes"
        raise ProtocolError(msg)

    frame = bytearray((SOF,))

    def append_stuffed(value: int) -> None:
        if value in _STUFFED_BYTES:
            frame.append(ESC)
        frame.append(value)

    append_stuffed(len(packet.data))
    append_stuffed(packet.type)
    for value in packet.data:
        append_stuffed(value)
    append_stuffed(checksum(packet.type, packet.data))
    frame.append(EOF)
    return bytes(frame)


class TpckParser:
    """Incremental TPCK stream parser."""

    def __init__(self, *, strict_checksum: bool = False) -> None:
        self.strict_checksum = strict_checksum
        self._state = _State.WAIT_SOF
        self._escaped = False
        self._length = 0
        self._type = 0
        self._data = bytearray()
        self._checksum = 0

    def reset(self) -> None:
        """Reset parser state and wait for the next start-of-frame."""
        self._state = _State.WAIT_SOF
        self._escaped = False
        self._length = 0
        self._type = 0
        self._data.clear()
        self._checksum = 0

    def feed_many(self, data: bytes | bytearray | Iterable[int]) -> list[Packet]:
        """Feed bytes into the parser and return all completed packets."""
        packets: list[Packet] = []
        for value in data:
            packet = self.feed(value)
            if packet is not None:
                packets.append(packet)
        return packets

    def feed(self, value: int) -> Packet | None:
        """Feed one byte into the parser."""
        value &= 0xFF

        if self._state is _State.WAIT_SOF:
            if value == SOF:
                self._start_frame()
            return None

        if self._escaped:
            self._escaped = False
        elif value == ESC:
            self._escaped = True
            return None
        elif value == SOF:
            self._start_frame()
            return None

        if self._state is _State.LENGTH:
            self._length = value
            self._state = _State.TYPE
            return None

        if self._state is _State.TYPE:
            self._type = value
            self._data.clear()
            self._state = _State.DATA if self._length else _State.CHECKSUM
            return None

        if self._state is _State.DATA:
            self._data.append(value)
            if len(self._data) >= self._length:
                self._state = _State.CHECKSUM
            return None

        if self._state is _State.CHECKSUM:
            self._checksum = value
            self._state = _State.EOF
            return None

        if self._state is _State.EOF and value == EOF:
            packet = self._finish_frame()
            self.reset()
            return packet

        return None

    def _start_frame(self) -> None:
        self._state = _State.LENGTH
        self._escaped = False
        self._length = 0
        self._type = 0
        self._data.clear()
        self._checksum = 0

    def _finish_frame(self) -> Packet | None:
        data = bytes(self._data)
        expected = checksum(self._type, data)
        if expected != self._checksum:
            if self.strict_checksum:
                msg = (
                    f"invalid TPCK checksum: expected 0x{expected:02X}, "
                    f"got 0x{self._checksum:02X}"
                )
                raise ProtocolError(msg)
            return None
        return Packet(self._type, data)
