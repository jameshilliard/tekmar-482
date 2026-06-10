"""Push-oriented controller for a tekmar 482 gateway."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Self

from .constants import DeviceMode, ThaSetback
from .exceptions import ProtocolError, TekmarError
from .models import AvailableInfo, DeviceRuntime, DiscoveredDevice, DiscoveryResult
from .state import Tekmar482State, is_topology_message, require_device_support
from .trpc import TrpcMethod, TrpcPacket

if TYPE_CHECKING:
    from types import TracebackType

    from .client import Tekmar482Client

StateCallback = Callable[[AvailableInfo], None]
TopologyCallback = Callable[[TrpcPacket], None]
ErrorCallback = Callable[[BaseException], None]


class Tekmar482Controller:
    """Manage discovery, reporting, reconnects, and live state updates."""

    def __init__(
        self,
        client: Tekmar482Client,
        *,
        discovery_timeout: float | None = 30,
        poll_timeout: float | None = 2,
        reconnect_interval: float | timedelta = 10,
        include_datetime: bool = True,
        include_setbacks: bool = False,
        include_setpoint_groups: bool = False,
    ) -> None:
        self.client = client
        self.discovery_timeout = discovery_timeout
        self.poll_timeout = poll_timeout
        self.reconnect_interval = (
            reconnect_interval.total_seconds()
            if isinstance(reconnect_interval, timedelta)
            else reconnect_interval
        )
        self.include_datetime = include_datetime
        self.include_setbacks = include_setbacks
        self.include_setpoint_groups = include_setpoint_groups
        self.discovery: DiscoveryResult | None = None
        self.state: Tekmar482State | None = None
        self.last_error: BaseException | None = None
        self._state_callbacks: set[StateCallback] = set()
        self._topology_callbacks: set[TopologyCallback] = set()
        self._error_callbacks: set[ErrorCallback] = set()
        self._report_task: asyncio.Task[None] | None = None
        self._running = False

    @property
    def data(self) -> AvailableInfo | None:
        """Return the latest runtime snapshot."""
        return None if self.state is None else self.state.data

    @property
    def is_running(self) -> bool:
        """Return whether the controller report loop is active."""
        return self._running

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc, traceback
        await self.stop()

    def add_listener(self, callback: StateCallback) -> Callable[[], None]:
        """Register a callback for state updates."""
        self._state_callbacks.add(callback)

        def remove() -> None:
            self._state_callbacks.discard(callback)

        return remove

    def add_topology_listener(self, callback: TopologyCallback) -> Callable[[], None]:
        """Register a callback for TakingAddress/topology-change reports."""
        self._topology_callbacks.add(callback)

        def remove() -> None:
            self._topology_callbacks.discard(callback)

        return remove

    def add_error_listener(self, callback: ErrorCallback) -> Callable[[], None]:
        """Register a callback for report-loop connection errors."""
        self._error_callbacks.add(callback)

        def remove() -> None:
            self._error_callbacks.discard(callback)

        return remove

    def device(self, address: int) -> Tekmar482DeviceHandle:
        """Return a capability-aware handle for one discovered device."""
        return Tekmar482DeviceHandle(self, address)

    async def start(self) -> AvailableInfo:
        """Open the gateway, seed runtime state, and start report handling."""
        if self._running and self.state is not None:
            return self.state.data

        await self.client.open()
        reporting_disabled = False
        try:
            await self.client.set_reporting_state(enabled=False)
            reporting_disabled = True
            self.discovery = await self.client.discover(
                timeout=self.discovery_timeout,
                manage_reporting=False,
            )
            data = await self.refresh_runtime()
            response = await self.client.set_reporting_state(enabled=True)
            reporting_disabled = False
            if response is not None and self.state is not None:
                self.state.apply_message(response)
                data = self.state.data
            self._running = True
            self._notify_state()
            self._report_task = asyncio.create_task(
                self._report_loop(),
                name="tekmar 482 controller report loop",
            )
        except BaseException:
            if reporting_disabled and self.client.is_open:
                with suppress(OSError, TekmarError):
                    await self.client.set_reporting_state(enabled=True)
            with suppress(OSError, TekmarError):
                await self.client.close()
            raise
        else:
            return data

    async def stop(self) -> None:
        """Stop reporting and close the gateway connection."""
        self._running = False
        task = self._report_task
        self._report_task = None
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        if self.client.is_open:
            with suppress(OSError, TekmarError):
                await self.client.set_reporting_state(enabled=False)
        await self.client.close()

    async def refresh_runtime(self) -> AvailableInfo:
        """Poll a full runtime seed snapshot and replace controller state."""
        if self.discovery is None:
            self.discovery = await self.client.discover(
                timeout=self.discovery_timeout,
                manage_reporting=False,
            )
        data = await self.client.poll_runtime(
            self.discovery,
            timeout=self.poll_timeout,
            include_datetime=self.include_datetime,
            include_setbacks=self.include_setbacks,
            include_setpoint_groups=self.include_setpoint_groups,
        )
        self.state = Tekmar482State(self.discovery, data)
        return data

    async def rediscover(self, *, pause_reporting: bool = False) -> DiscoveryResult:
        """Rediscover gateway topology and refresh runtime state.

        Reporting stays enabled by default so live report consumers do not miss
        updates while topology is refreshed. Set `pause_reporting=True` for the
        older conservative behavior of disabling reports around discovery.
        """
        reporting_paused = pause_reporting and self._running
        if reporting_paused:
            await self.client.set_reporting_state(enabled=False)

        try:
            self.discovery = await self.client.discover(
                timeout=self.discovery_timeout,
                manage_reporting=False,
            )
            await self.refresh_runtime()
        except BaseException:
            if reporting_paused and self.client.is_open:
                with suppress(OSError, TekmarError):
                    await self.client.set_reporting_state(enabled=True)
            raise

        if reporting_paused:
            response = await self.client.set_reporting_state(enabled=True)
            if response is not None and self.state is not None:
                self.state.apply_message(response)
        self._notify_state()
        return self.discovery

    def handle_message(self, message: TrpcPacket) -> None:
        """Apply one report/response message to controller state."""
        self._handle_message(message)

    async def _report_loop(self) -> None:
        while self._running:
            try:
                if not self.client.is_open:
                    await self.client.open()
                    await self.client.set_reporting_state(enabled=True)

                async with self.client.reports(replay_backlog=True) as reports:
                    async for message in reports:
                        self._handle_message(message)
            except asyncio.CancelledError:
                raise
            except (OSError, TekmarError) as err:
                self.last_error = err
                self._notify_error(err)
                await self.client.close()
                await asyncio.sleep(self.reconnect_interval)

    def _handle_message(self, message: TrpcPacket) -> None:
        if is_topology_message(message):
            for callback in tuple(self._topology_callbacks):
                callback(message)
            return

        if self.state is None:
            return

        if self.state.apply_message(message):
            self._notify_state()

    def _notify_state(self) -> None:
        if self.state is None:
            return
        for callback in tuple(self._state_callbacks):
            callback(self.state.data)

    def _notify_error(self, err: BaseException) -> None:
        for callback in tuple(self._error_callbacks):
            callback(err)


@dataclass(frozen=True, slots=True)
class Tekmar482DeviceHandle:
    """High-level operations for one discovered tekmar device."""

    controller: Tekmar482Controller
    address: int

    @property
    def info(self) -> DiscoveredDevice:
        """Return static discovery metadata for this device."""
        discovery = self.controller.discovery
        if discovery is None:
            msg = "tekmar 482 controller has not discovered devices"
            raise ProtocolError(msg)
        for device in discovery.devices:
            if device.address == self.address:
                return device
        msg = f"unknown tekmar device address: {self.address}"
        raise ProtocolError(msg)

    @property
    def runtime(self) -> DeviceRuntime | None:
        """Return latest typed runtime values for this device."""
        if self.controller.state is None:
            return None
        return self.controller.state.runtime(self.address)

    async def set_heat_setpoint_celsius(
        self,
        celsius: float,
        *,
        setback: int = ThaSetback.CURRENT,
        confirm: bool = True,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Set the latest heat setpoint after validating device support."""
        self._require_support(TrpcMethod.HEAT_SETPOINT)
        return await self.controller.client.set_latest_heat_setpoint_celsius(
            self.address,
            celsius,
            setback=setback,
            confirm=confirm,
            timeout=timeout,
        )

    async def set_cool_setpoint_celsius(
        self,
        celsius: float,
        *,
        setback: int = ThaSetback.CURRENT,
        confirm: bool = True,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Set the latest cool setpoint after validating device support."""
        self._require_support(TrpcMethod.COOL_SETPOINT)
        return await self.controller.client.set_latest_cool_setpoint_celsius(
            self.address,
            celsius,
            setback=setback,
            confirm=confirm,
            timeout=timeout,
        )

    async def set_slab_setpoint_celsius(
        self,
        celsius: float,
        *,
        setback: int = ThaSetback.CURRENT,
        confirm: bool = True,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Set the latest slab setpoint after validating device support."""
        self._require_support(TrpcMethod.SLAB_SETPOINT)
        return await self.controller.client.set_latest_slab_setpoint_celsius(
            self.address,
            celsius,
            setback=setback,
            confirm=confirm,
            timeout=timeout,
        )

    async def set_setpoint_device_celsius(
        self,
        celsius: float,
        *,
        setback: int = ThaSetback.CURRENT,
        confirm: bool = True,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Set the latest setpoint-device target after validating support."""
        self._require_support(TrpcMethod.SETPOINT_DEVICE)
        return await self.controller.client.set_latest_setpoint_device_celsius(
            self.address,
            celsius,
            setback=setback,
            confirm=confirm,
            timeout=timeout,
        )

    async def set_fan_percent(
        self,
        percent: int,
        *,
        setback: int = ThaSetback.CURRENT,
        confirm: bool = True,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Set the latest fan percent after validating device support."""
        self._require_support(TrpcMethod.FAN_PERCENT)
        return await self.controller.client.set_latest_fan_percent(
            self.address,
            percent,
            setback=setback,
            confirm=confirm,
            timeout=timeout,
        )

    async def set_mode(
        self,
        mode: int | DeviceMode,
        *,
        confirm: bool = True,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Set thermostat mode after validating device support."""
        self._require_support(TrpcMethod.MODE_SETTING)
        return await self.controller.client.set_mode(
            self.address,
            mode,
            confirm=confirm,
            timeout=timeout,
        )

    def _require_support(self, method: TrpcMethod) -> None:
        if self.controller.state is not None:
            self.controller.state.require_device_support(self.address, method)
            return
        require_device_support(self.info, method)
