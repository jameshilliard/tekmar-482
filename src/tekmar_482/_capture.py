"""Timestamped tN4 observations common to Raw and Normal trace."""

from dataclasses import dataclass
from enum import IntEnum
from struct import Struct
from typing import Self

from .constants import TN4_PORTS
from .native import NativeFrame, RxFlag, native_body_size

_PREFIX = Struct("<BBI")
_INVALID_DETAIL = Struct("<BH")


class CaptureEvent(IntEnum):
    NATIVE = 0
    TX_ATTEMPT = 1
    INVALID = 4
    GAP = 5


class InvalidReason(IntEnum):
    STUFFING = 1
    CRC = 2
    CRC_DELIMITER = 3
    BAD_END = 4


@dataclass(frozen=True, slots=True)
class InvalidCapture:
    port: int
    timestamp_ms: int
    reason: InvalidReason | int
    bit_count: int
    bits: bytes

    @classmethod
    def from_detail(cls, port: int, stamp: int, detail: bytes) -> Self | None:
        """Decode the shared invalid-frame evidence."""
        if len(detail) < _INVALID_DETAIL.size:
            return None
        code, count = _INVALID_DETAIL.unpack_from(detail)
        bits = detail[_INVALID_DETAIL.size :]
        if len(bits) != (count + 7) // 8 or (
            count % 8 and bits[-1] & ((1 << (8 - count % 8)) - 1)
        ):
            return None
        return cls(
            port,
            stamp,
            InvalidReason(code) if code in InvalidReason else code,
            count,
            bits,
        )


@dataclass(frozen=True, slots=True)
class StampedRecord:
    event: int
    port: int
    timestamp_ms: int
    body: bytes

    @classmethod
    def read(cls, payload: bytes) -> Self | None:
        if len(payload) < _PREFIX.size:
            return None
        event, port, timestamp_ms = _PREFIX.unpack_from(payload)
        if port not in TN4_PORTS:
            return None
        return cls(event, port, timestamp_ms, payload[_PREFIX.size :])

    def received(self, capture_index: int) -> NativeFrame | InvalidCapture | None:
        if self.event == CaptureEvent.INVALID:
            return InvalidCapture.from_detail(self.port, self.timestamp_ms, self.body)
        if self.event != CaptureEvent.NATIVE:
            return None
        end = native_body_size(self.body)
        flags: RxFlag | None
        match len(self.body) - end:
            case 1:
                flags = RxFlag(self.body[end])
            case 0:
                flags = None
            case _:
                return None
        return NativeFrame(
            self.port,
            self.timestamp_ms,
            self.body[:end],
            capture_index=capture_index,
            rx_flags=flags,
        )
