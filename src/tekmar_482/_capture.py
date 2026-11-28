"""Timestamped tN4 observations common to Raw and Normal trace."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from enum import IntEnum
from struct import Struct
from typing import TYPE_CHECKING, Self

from .constants import TN4_PORTS
from .exceptions import ConnectionClosedError
from .native import NativeFrame, RxFlag, native_body_size

if TYPE_CHECKING:
    from collections.abc import Iterator

_GAP = Struct("<BBQ")
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


@dataclass(frozen=True, slots=True)
class CaptureGap:
    """Missing capture records before the next observation (host backlog by default)."""

    dropped: int

    def affects(self, _port: int, /) -> bool:
        """Whether this physical port's capture continuity was lost."""
        return True  # The host backlog does not preserve the lost ports.


@dataclass(frozen=True, slots=True)
class GatewayTraceGap(CaptureGap):
    """Lost gateway trace records across these ports, not a per-port count."""

    port_mask: int

    def affects(self, port: int, /) -> bool:
        return bool(self.port_mask & (1 << port))

    @property
    def ports(self) -> tuple[int, ...]:
        return tuple(p for p in TN4_PORTS if self.affects(p))

    @classmethod
    def decode(cls, data: bytes) -> Self | None:
        if len(data) != _GAP.size:
            return None
        event, mask, count = _GAP.unpack(data)
        if event != CaptureEvent.GAP or not 0 < mask < 1 << len(TN4_PORTS):
            return None
        return cls(count, mask) if count >= mask.bit_count() else None


class CaptureBuffer[T]:
    def __init__(self, backlog: int) -> None:
        if backlog <= 0:
            msg = "trace backlog must be positive"
            raise ValueError(msg)
        self._queue: deque[T] = deque(maxlen=backlog)
        self._ready = asyncio.Event()
        self._lost = 0
        self._arrived = 0
        self._consumed = 0
        self._advanced = asyncio.Event()
        self.host_drops = 0
        self.gateway_drops: tuple[int, ...] | None = None
        self.closed = False
        self._error: BaseException | None = None

    def _put(self, item: T) -> None:
        if self.closed:
            return
        self._arrived += 1
        if len(self._queue) == self._queue.maxlen:
            self._lost += 1
            self.host_drops += 1
        self._queue.append(item)
        self._ready.set()

    def finish(self, error: BaseException | None = None) -> None:
        if not self.closed:
            self.closed = True
            self._error = error
            self._ready.set()
            self._advanced.set()

    def __aiter__(self) -> Self:
        return self

    def _pop(self) -> T | CaptureGap | None:
        if self._lost:
            lost, self._lost = self._lost, 0
            self._consumed += lost
            self._advanced.set()
            return CaptureGap(lost)
        if not self._queue:
            return None
        self._consumed += 1
        self._advanced.set()
        return self._queue.popleft()

    async def wait_consumed(self) -> None:
        """Wait until the consumer receives all earlier observations or their loss."""
        target = self._arrived
        while True:
            if self._error is not None:
                raise self._error
            if self._consumed >= target:
                return
            if self.closed:
                msg = "capture ended before its observations were consumed"
                raise ConnectionClosedError(msg)
            self._advanced.clear()
            await self._advanced.wait()

    def drain(self) -> Iterator[T | CaptureGap]:
        """Return buffered observations, including those before the stop reply."""
        while (item := self._pop()) is not None:
            yield item

    async def __anext__(self) -> T | CaptureGap:
        while True:
            if (item := self._pop()) is not None:
                return item
            if self.closed:
                if self._error:
                    raise self._error
                raise StopAsyncIteration
            self._ready.clear()
            await self._ready.wait()
