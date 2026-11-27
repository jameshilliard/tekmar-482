"""Long-lived Normal-mode native monitoring on a single gateway connection."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from math import isfinite
from typing import TYPE_CHECKING

from .bridge import BridgeClient
from .constants import TN4_PORTS
from .exceptions import ConnectionClosedError
from .management import BridgeManagement
from .native import NativeAddress
from .native_state import NativeState
from .native_watch import watch_native

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ._wire import GatewayEndpoint
    from .native_state import NativePath


class NativeMonitor:
    """Maintain native state and a shared Normal command channel across reconnects."""

    def __init__(
        self,
        endpoint: GatewayEndpoint,
        *,
        ports: Iterable[int] = TN4_PORTS,
        passive: bool = False,
        reconnect_interval: float = 10,
        timeout: float = 20,
        stale_after: float | None = None,
    ) -> None:
        self._ports = frozenset(ports)
        if not self._ports or not self._ports.issubset(TN4_PORTS):
            msg = "select at least one valid physical tN4 port"
            raise ValueError(msg)
        intervals = (reconnect_interval, timeout, stale_after)
        if any(v is not None and (not isfinite(v) or v <= 0) for v in intervals):
            msg = "monitor intervals must be finite and positive"
            raise ValueError(msg)
        self._endpoint = endpoint
        self._passive = passive
        self._reconnect_interval = reconnect_interval
        self._timeout = timeout
        self._stale_after = stale_after
        self._state = NativeState()
        self._management: BridgeManagement | None = None
        self._last_error: Exception | None = None
        self._revision = 0
        self._changed = asyncio.Event()
        self._running = False

    @property
    def state(self) -> NativeState:
        """Latest observations; do not mutate from a consumer."""
        return self._state

    @property
    def management(self) -> BridgeManagement | None:
        """Operations share this connection. Obtain it again after a reconnect."""
        return self._management

    @property
    def last_error(self) -> Exception | None:
        return self._last_error

    @property
    def revision(self) -> int:
        return self._revision

    async def wait_for_change(self, after: int) -> int:
        """Wake for new state or connection changes, coalescing intermediate updates."""
        while self._revision <= after:
            await self._changed.wait()
        return self._revision

    async def run(self) -> None:
        """Run until cancelled. Reopen a lost link, but never flash or change modes."""
        if self._running:
            msg = "native monitor is already running"
            raise RuntimeError(msg)
        self._running = True
        try:
            while True:
                try:
                    await self._watch_connection()
                except (ConnectionClosedError, OSError) as err:
                    self._last_error = err
                except Exception as err:
                    self._last_error = err
                    raise
                finally:
                    self._management = None
                    for path in self._state.paths:
                        self._state.forget(path)
                    self._state = NativeState()
                    self._publish()
                await asyncio.sleep(self._reconnect_interval)
        finally:
            self._running = False

    def _publish(self) -> None:
        self._revision += 1
        self._changed.set()
        self._changed = asyncio.Event()

    def _observed(self, _path: NativePath, state: NativeState) -> None:
        self._state = state
        self._publish()

    async def _watch_connection(self) -> None:
        # Attach only to the running application, not the ROM provisioning path.
        async with AsyncExitStack() as stack:
            async with asyncio.timeout(self._timeout):
                wire = await stack.enter_async_context(self._endpoint.connect())
                bridge = BridgeClient(wire)
                await bridge.identify()
                client = await stack.enter_async_context(
                    bridge.normal(select_mode=False),
                )
                manager = BridgeManagement(client)
                link = await stack.enter_async_context(
                    manager.native_session(self._ports),
                )
            self._management = manager
            self._last_error = None
            self._publish()
            await watch_native(
                link,
                source=None if self._passive else NativeAddress(0, 0),
                timeout=self._timeout,
                stale_after=self._stale_after,
                check_port_subnets=True,
                on_change=self._observed,
            )
