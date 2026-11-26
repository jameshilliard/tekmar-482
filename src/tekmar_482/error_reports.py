"""Gateway diagnostics, independent of operating mode and frame trace."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Self

from ._bridge_diagnostics import ERROR_COUNTER, ErrorReason, ErrorSource
from .exceptions import ProtocolError

if TYPE_CHECKING:
    from collections.abc import Iterable

ALL_SOURCES = frozenset(ErrorSource)


def source_mask(sources: Iterable[ErrorSource]) -> int:
    mask = 0
    for source in sources:
        if source not in ALL_SOURCES:
            msg = f"invalid gateway error source: {source!r}"
            raise ValueError(msg)
        mask |= 1 << source
    return mask


@dataclass(frozen=True, slots=True)
class GatewayErrorCounter:
    source: ErrorSource
    reason: ErrorReason
    last_seen_ms: int
    count: int

    @classmethod
    def decode(cls, body: bytes) -> Self:
        if len(body) == ERROR_COUNTER.size:
            source, reason, ms, count = ERROR_COUNTER.unpack(body)
            if source in ErrorSource and reason in ErrorReason:
                key, code = ErrorSource(source), ErrorReason(reason)
                if code.supports(key):
                    return cls(key, code, ms, count)
        msg = f"invalid gateway error counter: {body.hex()}"
        raise ProtocolError(msg)


class ErrorReports:
    """At most one unread total per source/reason. A slow consumer never blocks RX."""

    def __init__(self, mask: int) -> None:
        self._mask = mask
        self._enabled = False
        self._done = False
        self._failure: BaseException | None = None
        self._pending: dict[tuple[ErrorSource, ErrorReason], GatewayErrorCounter] = {}
        self._ready = asyncio.Event()

    def acknowledge(self, body: bytes) -> None:
        # The mask reply fences off queued reports from a previous selection.
        self._enabled = body == bytes([self._mask])

    def record(self, body: bytes) -> None:
        if not self._enabled or self._done:
            return
        try:
            counter = GatewayErrorCounter.decode(body)
        except ProtocolError as err:
            self.finish(err)
        else:
            if self._mask & (1 << counter.source):
                self._pending[counter.source, counter.reason] = counter
                self._ready.set()

    def finish(self, error: BaseException | None = None) -> None:
        if not self._done:
            self._done = True
            self._failure = error
            self._ready.set()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> GatewayErrorCounter:
        while True:
            if self._pending:
                return self._pending.pop(next(iter(self._pending)))
            if self._failure is not None:
                raise self._failure
            if self._done:
                raise StopAsyncIteration
            self._ready.clear()
            await self._ready.wait()
