"""Per-address scheduling of report-triggered thermostat mode reads."""

from __future__ import annotations

import asyncio
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .exceptions import TekmarError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterator

LOGGER = logging.getLogger(__name__)
_UNCERTAIN_WRITE_ERRORS = (TimeoutError, asyncio.CancelledError)


@dataclass(slots=True)
class _Address:
    writes: int = 0
    deadline: float | None = None
    task: asyncio.Task[None] | None = None

    def cancel_read(self) -> None:
        if self.task is not None:
            self.task.cancel()
            self.task = None


class ModeReads:
    """Remember mode triggers until a read or a confirmed write covers them."""

    def __init__(self, request: Callable[[int], Awaitable[None]]) -> None:
        self._request = request
        self._addresses: dict[int, _Address] = {}

    def trigger(self, address: int, *, quiet_window: float) -> None:
        state = self._addresses.setdefault(address, _Address())
        state.deadline = asyncio.get_running_loop().time() + quiet_window
        self._schedule(address, state)

    @contextmanager
    def writing(self, address: int) -> Iterator[None]:
        state = self._addresses.setdefault(address, _Address())
        state.writes += 1
        if state.task is not None:
            # Even an interrupted in-flight read still needs an answer.
            if state.deadline is None:
                state.deadline = asyncio.get_running_loop().time()
            state.cancel_read()
        try:
            yield
        except _UNCERTAIN_WRITE_ERRORS:
            # The device may have accepted the write even without an echo.
            if state.deadline is None:
                state.deadline = asyncio.get_running_loop().time()
            raise
        finally:
            state.writes -= 1
            if self._addresses.get(address) is state:
                self._schedule(address, state)

    def confirmed(self, address: int) -> None:
        if (state := self._addresses.get(address)) is not None:
            state.deadline = None
            state.cancel_read()
            self._schedule(address, state)

    def _schedule(self, address: int, state: _Address) -> None:
        if state.writes or state.task is not None:
            return
        if state.deadline is None:
            del self._addresses[address]
            return
        state.task = asyncio.create_task(
            self._read(address, state),
            name=f"tekmar 482 mode refresh {address}",
        )

    async def _read(self, address: int, state: _Address) -> None:
        loop = asyncio.get_running_loop()
        try:
            while state.deadline is not None:
                if (remaining := state.deadline - loop.time()) > 0:
                    await asyncio.sleep(remaining)
                    continue
                state.deadline = None
                try:
                    await self._request(address)
                except TimeoutError:
                    # A new trigger already schedules a fresh read. Otherwise
                    # give this trigger one more chance after its wire timeout.
                    if state.deadline is None:
                        await self._request(address)
        except (OSError, TekmarError) as err:
            LOGGER.debug("Failed refreshing tekmar 482 mode for %s: %s", address, err)
        finally:
            if state.task is asyncio.current_task():
                state.task = None
                self._schedule(address, state)

    async def clear(self) -> None:
        states = tuple(self._addresses.values())
        self._addresses.clear()
        tasks = [state.task for state in states if state.task is not None]
        for state in states:
            state.cancel_read()
        await asyncio.gather(*tasks, return_exceptions=True)
