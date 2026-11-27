"""Read gateway-owned traffic counters without enabling tracing."""

from __future__ import annotations

from dataclasses import dataclass
from struct import Struct
from typing import TYPE_CHECKING

from .constants import TN4_PORTS
from .exceptions import ProtocolError
from .native_state import NativePath

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

_DIRECTORY = Struct("<HH")
_INDEX = Struct("<H")
_PORT = Struct("<B7Q")
_PATH = Struct("<4B4Q")
_ENTRY = Struct("<H3B4Q")
_MAX_NATIVE_NODE = 31


@dataclass(frozen=True, slots=True)
class FrameCounts:
    rx_valid: int
    tx_ack: int
    tx_no_ack: int
    tx_failed: int

    @property
    def tx_attempts(self) -> int:
        """All recorded attempts, not just acknowledged completions."""
        return self.tx_ack + self.tx_no_ack + self.tx_failed


@dataclass(frozen=True, slots=True)
class PortTraffic:
    port: int
    counts: FrameCounts
    rx_invalid: int
    rx_untracked: int
    tx_unattributed: int


@dataclass(frozen=True, slots=True)
class DeviceTraffic:
    path: NativePath
    counts: FrameCounts


def _unpack(layout: Struct, body: bytes) -> tuple[int, ...]:
    if len(body) != layout.size:
        msg = (
            f"invalid traffic response length: expected {layout.size}, got {len(body)}"
        )
        raise ProtocolError(msg)
    return layout.unpack(body)


class TrafficCounters:
    """Read boot-lifetime counters on the active session without changing modes."""

    def __init__(self, request: Callable[[bytes], Awaitable[bytes]]) -> None:
        self._request = request

    async def port(self, port: int) -> PortTraffic:
        if port not in TN4_PORTS:
            msg = f"invalid tN4 port: {port}"
            raise ValueError(msg)
        p, rx, ack, no_ack, failed, invalid, untracked, unattributed = _unpack(
            _PORT,
            await self._request(bytes([port])),
        )
        if p != port:
            msg = "traffic response port mismatch"
            raise ProtocolError(msg)
        return PortTraffic(
            p,
            FrameCounts(rx, ack, no_ack, failed),
            invalid,
            untracked,
            unattributed,
        )

    async def device(self, path: NativePath) -> DeviceTraffic | None:
        key = bytes((path.port, path.subnet, path.node))
        p, s, n, present, *values = _unpack(_PATH, await self._request(key))
        if (
            (p, s, n) != tuple(key)
            or present not in (0, 1)
            or (not present and any(values))
        ):
            msg = "invalid device traffic response"
            raise ProtocolError(msg)
        return DeviceTraffic(path, FrameCounts(*values)) if present else None

    async def devices(self) -> AsyncIterator[DeviceTraffic]:
        """Enumerate paths allocated at the start of the read, even quiet ones."""
        capacity, used = _unpack(_DIRECTORY, await self._request(b""))
        if not capacity or used > capacity:
            msg = "invalid traffic directory size"
            raise ProtocolError(msg)
        for index in range(used):
            i, p, s, n, *values = _unpack(
                _ENTRY,
                await self._request(_INDEX.pack(index)),
            )
            if i != index or p not in TN4_PORTS or n > _MAX_NATIVE_NODE:
                msg = "invalid traffic directory entry"
                raise ProtocolError(msg)
            yield DeviceTraffic(NativePath(p, s, n), FrameCounts(*values))
