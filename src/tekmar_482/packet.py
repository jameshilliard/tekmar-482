"""tekmar packet representation."""

from __future__ import annotations

from dataclasses import dataclass

from .exceptions import ProtocolError

TYPE_GENERAL = 0
TYPE_TN4 = 1
TYPE_HW_OVR_TEXT = 2
TYPE_HW_OVR_BINARY = 3
TYPE_DISPLAY = 4
TYPE_NVM = 5
TYPE_TRPC = 6
_MAX_PACKET_TYPE = 0xFF


@dataclass(frozen=True, slots=True)
class Packet:
    """A tekmar packet with an 8-bit type and opaque payload bytes."""

    type: int = TYPE_GENERAL
    data: bytes = b""

    def __post_init__(self) -> None:
        if not 0 <= self.type <= _MAX_PACKET_TYPE:
            msg = f"packet type must fit in one byte: {self.type!r}"
            raise ProtocolError(msg)
