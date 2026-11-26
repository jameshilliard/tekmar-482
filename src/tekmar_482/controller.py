"""Push-oriented controller for a tekmar 482 gateway."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from contextlib import ExitStack, contextmanager, suppress
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Self

from ._mode import ModeReads
from ._poll import RuntimePoll, filter_discovery_events, observe_runtime
from .constants import DeviceMode, ThaSetback
from .exceptions import (
    ConnectionClosedError,
    InventoryDrainTimeoutError,
    ProtocolError,
    TekmarError,
)
from .models import AvailableInfo, DeviceRuntime, DiscoveredDevice, DiscoveryResult
from .state import (
    MessageApplyResult,
    Tekmar482State,
    is_runtime_message,
    is_topology_message,
)
from .trpc import (
    AddressFields,
    SetpointGroupFields,
    TrpcMethod,
    TrpcPacket,
    TrpcService,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import TracebackType

    from .client import Tekmar482Client
    from .session import ReportSubscription
    from .trpc import TrpcCommand

StateCallback = Callable[[AvailableInfo], None]
TopologyCallback = Callable[[TrpcPacket], None]
ErrorCallback = Callable[[BaseException], None]

LOGGER = logging.getLogger(__name__)

# 482 firmware updates cached thermostat mode from tN4 status frames but does not
# emit ModeSetting reports. A mode change can surface only as these adjacent
# reports, so the controller performs one targeted ModeSetting read after them.
MODE_REFRESH_TRIGGER_METHODS = {
    TrpcMethod.ACTIVE_DEMAND,
    TrpcMethod.COOL_SETPOINT,
    TrpcMethod.CURRENT_FLOOR_TEMPERATURE,
    TrpcMethod.CURRENT_TEMPERATURE,
    TrpcMethod.FAN_PERCENT,
    TrpcMethod.HEAT_SETPOINT,
    TrpcMethod.SLAB_SETPOINT,
    TrpcMethod.SETBACK_STATE,
    TrpcMethod.SETPOINT_DEVICE,
}

# Report bursts from the 482 are serialized at the gateway baud rate. A short
# per-address quiet window collapses one firmware report burst into one
# ModeSetting request without recurring background reads.
MODE_REFRESH_BURST_WINDOW = 0.05
_REPORTING_CLEANUP_TIMEOUT = 5
_GATEWAY_ERRORS = (OSError, TekmarError)
# Firmware 06 clears an unrefreshed setpoint-group flag after 60 one-second ticks
# without emitting an off report. The cached flag is not a slab-demand signal.
_GROUP_CACHE_SECONDS = 60


def _notify_listeners[T](listeners: set[Callable[[T], None]], value: T) -> None:
    for listener in tuple(listeners):
        try:
            listener(value)
        except Exception:
            LOGGER.exception("tekmar 482 listener %r failed", listener)


class Tekmar482Controller:
    """Manage discovery, reporting, reconnects, and live state updates."""

    def __init__(
        self,
        client: Tekmar482Client,
        *,
        discovery_timeout: float | None = 30,
        poll_timeout: float | None = 2,
        reconnect_interval: float | timedelta = 10,
        include_setbacks: bool = False,
        include_setpoint_groups: bool = False,
        setback_enable: bool | None = None,
        mode_refresh_burst_window: float = MODE_REFRESH_BURST_WINDOW,
    ) -> None:
        self.client = client
        self.discovery_timeout = discovery_timeout
        self.poll_timeout = poll_timeout
        self.reconnect_interval = (
            reconnect_interval.total_seconds()
            if isinstance(reconnect_interval, timedelta)
            else reconnect_interval
        )
        self.include_setbacks = include_setbacks
        self.include_setpoint_groups = include_setpoint_groups
        self.setback_enable = setback_enable
        self.mode_refresh_burst_window = max(0.0, mode_refresh_burst_window)
        self.state: Tekmar482State | None = None
        self.last_error: BaseException | None = None
        self._state_callbacks: set[StateCallback] = set()
        self._topology_callbacks: set[TopologyCallback] = set()
        self._error_callbacks: set[ErrorCallback] = set()
        self._mode_reads = ModeReads(self._request_mode)
        self._wire_observer = ExitStack()
        self._topology_revision = 0
        self._address_revisions: dict[int, int] = {}
        self._inventory_revision = 0
        self._startup_topology: TrpcPacket | None = None
        self._group_deadlines: dict[int, float] = {}
        self._group_tasks: dict[int, asyncio.Task[None]] = {}
        self._report_task: asyncio.Task[None] | None = None
        self._reports: ReportSubscription | None = None
        self._lifecycle_lock = asyncio.Lock()
        self._refresh_lock = asyncio.Lock()
        self._running = False
        self._ready_generation: int | None = None
        self._reporting_paused = False
        self._inventory_confirmed = False
        self._active_device_writes = 0
        self._writes_idle = asyncio.Event()
        self._writes_idle.set()

    @property
    def discovery(self) -> DiscoveryResult | None:
        """Return discovery metadata with the latest observed schedule configuration."""
        return self.state.discovery if self.state is not None else None

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

    def require_ready_state(self, *, address: int | None = None) -> Tekmar482State:
        """Return state confirmed on the open connection, never the offline cache."""
        session = self.client.session
        if (
            self.state is None
            or self._ready_generation != session.connection_generation
            or not self.client.is_open
            or session.reader_error is not None
        ):
            msg = (
                "tekmar 482 controller has no confirmed state on the current connection"
            )
            raise ConnectionClosedError(msg)
        if not self._inventory_confirmed:
            msg = "tekmar 482 inventory is being revalidated"
            raise ConnectionClosedError(msg)
        if (
            address is not None
            and self._address_revisions.get(address, 0) > self._inventory_revision
        ):
            msg = f"Device address {address} has changed; waiting for rediscovery"
            raise ConnectionClosedError(msg)
        return self.state

    def device_revision(self, address: int) -> int:
        """Identify the confirmed owner of an address on the current connection."""
        self.require_ready_state(address=address)
        return self._address_revisions.get(address, 0)

    def _addresses_changed_since(self, revision: int) -> set[int]:
        return {
            addr for addr, moved in self._address_revisions.items() if moved > revision
        }

    @contextmanager
    def device_write(self, address: int, method: TrpcMethod) -> Iterator[None]:
        """Admit a validated write and finish it before topology can be replaced."""
        self.require_ready_state(address=address).require_device_support(
            address,
            method,
        )
        self._active_device_writes += 1
        self._writes_idle.clear()
        try:
            yield
        finally:
            self._active_device_writes -= 1
            if not self._active_device_writes:
                self._writes_idle.set()

    async def start(self) -> AvailableInfo:
        """Open the gateway, seed runtime state, and start report handling."""
        async with self._lifecycle_lock:
            if self._running:
                return self.require_ready_state().data
            try:
                state = await self._connect()
            except BaseException:
                with suppress(OSError, TekmarError):
                    await self.client.close()
                raise
            self._running = True
            self._report_task = asyncio.create_task(
                self._report_loop(),
                name="tekmar 482 controller report loop",
            )
            self._notify_state()
            return state.data

    async def _connect(self) -> Tekmar482State:
        async with self._refresh_lock:
            await self.client.open()
            generation = self.client.session.connection_generation
            self._address_revisions.clear()
            self._inventory_revision = self._topology_revision
            try:
                self._wire_observer.enter_context(
                    self.client.session.observe_messages(self._observe_wire),
                )
                with self.client.session.connection_scope():
                    await self._set_reporting_confirmed(enabled=False)
                    state = await self._read_state(None)
                    # Subscribe before enabling push so the first reports aren't lost.
                    self._reports = self.client.reports(
                        include_matched=frozenset({TrpcMethod.SETPOINT_GROUP_ENABLE})
                        if self.include_setpoint_groups
                        else frozenset(),
                    )
                    state.apply_message(
                        await self._set_reporting_confirmed(enabled=True),
                    )
            except BaseException:
                await self._stop_observing()
                if self._reports is not None:
                    self._reports.close()
                    self._reports = None
                if self.client.session.connection_generation == generation:
                    await self._set_reporting_best_effort(enabled=True)
                raise
            state.invalidate_devices(
                self._addresses_changed_since(self._inventory_revision),
            )
            self.state = state
            self._ready_generation = generation
            self._inventory_confirmed = True
            self.last_error = None
            if self._startup_topology is not None:
                _notify_listeners(self._topology_callbacks, self._startup_topology)
                self._startup_topology = None
            return state

    async def stop(self) -> None:
        """Stop reporting and close the gateway connection."""
        async with self._lifecycle_lock:
            self._running = False
            self._ready_generation = None
            task = self._report_task
            self._report_task = None
            try:
                if task is not None:
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
                await self._mode_reads.clear()
                await self._stop_observing()
                if self._reports is not None:
                    self._reports.close()
                    self._reports = None
                await self._set_reporting_best_effort(enabled=False)
            finally:
                await self.client.close()

    async def refresh_runtime(self) -> AvailableInfo:
        """Poll runtime state and notify listeners if the snapshot changed."""
        async with self._refresh_lock:
            state = await self._read_state(self.discovery)
            changed = state.data != self.data
            self.state = state
            if changed:
                self._notify_state()
            return state.data

    async def _read_state(
        self,
        discovery: DiscoveryResult | None,
        *,
        resume_reporting: bool = False,
    ) -> Tekmar482State:
        revision = (
            self._topology_revision if discovery is None else self._inventory_revision
        )
        expected_setback = self.setback_enable if discovery is None else None
        reports = self._reports
        if reports is not None:
            for message, command in reports.drain_events():
                self._handle_message(message, command)
        previous = self.data if reports is not None else None
        with observe_runtime(self.client.session) as observations:
            if discovery is None:
                discovery = await self.client.discover(
                    timeout=self.discovery_timeout,
                    manage_reporting=False,
                    setback_enable=self.setback_enable,
                )
                filter_discovery_events(
                    observations,
                    discovery,
                    previous,
                    changed_addresses=frozenset(
                        self._addresses_changed_since(self._inventory_revision),
                    ),
                )
            poll = RuntimePoll.plan(
                discovery,
                include_setbacks=self.include_setbacks,
                include_setpoint_groups=self.include_setpoint_groups,
            )
            await poll.execute(
                self.client.session,
                timeout=self.poll_timeout,
                expected_setback=expected_setback,
                on_reporting_disabled=self._reporting_disabled,
            )
            if resume_reporting:
                await self._set_reporting_confirmed(enabled=True)
        if reports is not None:
            # Deliver queued notifications before replacing their state. Nothing
            # may yield between this drain and installation of the ordered seed.
            for message, command in reports.drain_events():
                self._handle_message(message, command)
        data = poll.reduce(observations, previous)
        state = Tekmar482State(discovery, data)
        # A refresh is not identification; a move during an inventory needs
        # another inventory. Neither may resurrect the old occupant's samples.
        state.invalidate_devices(self._addresses_changed_since(revision))
        return state

    async def rediscover(self, *, pause_reporting: bool = False) -> DiscoveryResult:
        """Rediscover gateway topology and refresh runtime state.

        Reporting stays enabled by default so live report consumers do not miss
        updates while topology is refreshed. Set `pause_reporting=True` for the
        older conservative behavior of disabling reports around discovery.
        """
        async with self._refresh_lock:
            confirmed = self._inventory_confirmed
            self._inventory_confirmed = False
            try:
                await self._writes_idle.wait()
            except asyncio.CancelledError:
                # The scan hadn't started. The previously known inventory still applies.
                self._inventory_confirmed = confirmed
                raise
            reporting_paused = pause_reporting and self._running
            generation = self.client.session.connection_generation
            self._reporting_paused = reporting_paused
            try:
                if reporting_paused:
                    await self._set_reporting_confirmed(enabled=False)
                revision = self._topology_revision
                state = await self._read_state(None, resume_reporting=reporting_paused)
            except InventoryDrainTimeoutError:
                if self._reports is not None:
                    # End this stream. The report loop owns closing, reconnecting
                    # and publishing a fresh topology, including reporting setup.
                    self._ready_generation = None
                    self._reports.close()
                raise
            except BaseException:
                if reporting_paused:
                    try:
                        if self.client.session.connection_generation == generation:
                            await self._set_reporting_best_effort(enabled=True)
                    finally:
                        # Even a successful restore cannot recover missed reports.
                        self._invalidate_stream(
                            ProtocolError("Paused rediscovery did not complete"),
                        )
                raise
            finally:
                self._reporting_paused = False
            if state.data.gateway.reporting_enabled is False:
                self._reporting_disabled()
            self.state = state
            self._inventory_revision = revision
            self._inventory_confirmed = True
            self._notify_state()
            return state.discovery

    def handle_message(self, message: TrpcPacket) -> None:
        """Apply one report/response message to controller state."""
        if is_topology_message(message):
            self._mark_changed_addresses(message)
        self._handle_message(message)

    async def set_device_mode(
        self,
        address: int,
        mode: int | DeviceMode,
        *,
        confirm: bool = True,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Set thermostat mode and suppress redundant mode refreshes."""
        with (
            self.device_write(address, TrpcMethod.MODE_SETTING),
            self._mode_reads.writing(address),
        ):
            return await self.client.set_mode(
                address,
                mode,
                confirm=confirm,
                timeout=timeout,
            )

    async def set_setpoint_group(
        self,
        group_id: int,
        *,
        enabled: bool,
        timeout: float | None = 5,
    ) -> TrpcPacket:
        """Set a gateway setpoint group and wait for confirmation."""
        self.require_ready_state()
        return await self.client.set_setpoint_group(
            group_id,
            enabled=enabled,
            timeout=timeout,
        )

    async def _report_loop(self) -> None:
        while self._running:
            try:
                if self._reports is None:
                    await self._connect()
                    self._notify_state()
                    continue
                async with self._reports as reports:
                    while (event := await reports.next_event()) is not None:
                        self._handle_message(*event)
                if self.last_error is not None:
                    raise self.last_error
                msg = "tekmar 482 report stream ended"
                raise ConnectionClosedError(msg)
            except asyncio.CancelledError:
                raise
            except _GATEWAY_ERRORS as err:
                self._ready_generation = None
                self._reports = None
                self.last_error = err
                self._notify_error(err)
                await self._stop_observing()
                await self._mode_reads.clear()
                await self.client.close()
                await asyncio.sleep(self.reconnect_interval)

    async def _set_reporting_confirmed(self, *, enabled: bool) -> TrpcPacket:
        response = await self.client.set_reporting_state(enabled=enabled)
        if response is None:
            msg = f"No acknowledgment for ReportingState={int(enabled)}"
            raise TimeoutError(msg)
        return response

    async def _set_reporting_best_effort(self, *, enabled: bool) -> None:
        if self.client.is_open:
            with suppress(OSError, TekmarError):
                async with asyncio.timeout(_REPORTING_CLEANUP_TIMEOUT):
                    await self.client.set_reporting_state(enabled=enabled)

    def _reporting_disabled(self) -> None:
        if (
            not self._reporting_paused
            and self._ready_generation is not None
            and self._reports is not None
        ):
            # Re-seed on reconnect: changes may have been missed while push was off.
            err = ProtocolError("Gateway unexpectedly disabled reporting")
            self._invalidate_stream(err)
            raise err

    def _invalidate_stream(self, err: TekmarError) -> None:
        if self._reports is not None:
            self._ready_generation = None
            self.last_error = err
            self._reports.close()

    def _handle_message(
        self,
        message: TrpcPacket,
        command: TrpcCommand | None = None,
    ) -> None:
        if is_topology_message(message):
            LOGGER.debug("Received tekmar 482 topology report %s", message)
            _notify_listeners(self._topology_callbacks, message)
            return

        if self.state is None:
            LOGGER.debug(
                "Ignoring tekmar 482 report before state is initialized: %s",
                message,
            )
            return

        address = message.get("address")
        if address is not None:
            if self._address_revisions.get(address, 0) > self._inventory_revision:
                return
            if (
                message.method is TrpcMethod.MODE_SETTING
                and message.service is TrpcService.RESPONSE_UPDATE
                and command is not None
                and command.service == TrpcService.UPDATE
            ):
                # Clear only triggers preceding this acknowledgement in wire order.
                self._mode_reads.confirmed(address)
            elif (
                self._running
                and message.service is TrpcService.REPORT
                and message.method in MODE_REFRESH_TRIGGER_METHODS
                and (device := self.state.device(address)) is not None
                and device.supports_mode_setting
            ):
                self._mode_reads.trigger(
                    address,
                    quiet_window=self.mode_refresh_burst_window,
                )
        if (
            message.method is TrpcMethod.REPORTING_STATE
            and is_runtime_message(message)
            and message.get("state") == 0
        ):
            self._reporting_disabled()
        result = self.state.apply_message_result(message, command)
        if result is MessageApplyResult.CHANGED:
            LOGGER.debug("Applied tekmar 482 report %s", message)
            self._notify_state()
        elif result is MessageApplyResult.UNSUPPORTED:
            LOGGER.debug(
                "Ignored unsupported tekmar 482 report %s",
                message,
            )

    def _notify_state(self) -> None:
        if self.state is None:
            return
        _notify_listeners(self._state_callbacks, self.state.data)

    def _notify_error(self, err: BaseException) -> None:
        _notify_listeners(self._error_callbacks, err)

    def _mark_changed_addresses(self, message: TrpcPacket) -> None:
        addresses = {
            address
            for field in ("old_address", "new_address")
            if (address := message.get(field)) is not None
        }
        self._topology_revision += 1
        self._address_revisions.update(
            dict.fromkeys(addresses, self._topology_revision),
        )
        self.client.session.discard_device_updates(addresses)
        if (
            self._ready_generation is not None
            and self.state is not None
            and self.state.invalidate_devices(addresses)
        ):
            self._notify_state()

    def _observe_wire(self, message: TrpcPacket, _command: TrpcCommand | None) -> None:
        if is_topology_message(message):
            self._mark_changed_addresses(message)
            if self._reports is None:
                self._startup_topology = message
            return
        if (
            not self.include_setpoint_groups
            or message.method is not TrpcMethod.SETPOINT_GROUP_ENABLE
            or not is_runtime_message(message)
        ):
            return
        group = message.get("group_id")
        if group is None or group not in range(1, 13):
            return
        if message.get("enable") != 1:
            self._group_deadlines.pop(group, None)
            if task := self._group_tasks.pop(group, None):
                task.cancel()
            return
        self._group_deadlines[group] = (
            asyncio.get_running_loop().time() + _GROUP_CACHE_SECONDS
        )
        if group not in self._group_tasks:
            self._group_tasks[group] = asyncio.create_task(
                self._watch_group(group),
                name=f"tekmar 482 group {group} expiry",
            )

    async def _stop_observing(self) -> None:
        self._wire_observer.close()
        self._startup_topology = None
        tasks = tuple(self._group_tasks.values())
        self._group_deadlines.clear()
        self._group_tasks.clear()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _watch_group(self, group: int) -> None:
        loop = asyncio.get_running_loop()
        try:
            while (deadline := self._group_deadlines.get(group)) is not None:
                await asyncio.sleep(max(0, deadline - loop.time()))
                async with self._refresh_lock:
                    if self._group_deadlines.get(group) != deadline:
                        continue
                    if (
                        self._ready_generation
                        != self.client.session.connection_generation
                    ):
                        return
                    try:
                        async with asyncio.timeout(self.poll_timeout):
                            response = await self.client.request_response(
                                TrpcMethod.SETPOINT_GROUP_ENABLE,
                                fields=SetpointGroupFields(group),
                                publish_match=True,
                                timeout=None,
                            )
                    except TimeoutError:
                        response = None
                    if (
                        response is None
                        and self._group_deadlines.get(group) == deadline
                    ):
                        if self.state is not None and self.state.expire_setpoint_group(
                            group,
                        ):
                            self._notify_state()
                        return
        except _GATEWAY_ERRORS:
            # The report loop owns connection errors and reconnects.
            return
        finally:
            if self._group_tasks.get(group) is asyncio.current_task():
                del self._group_tasks[group]
                self._group_deadlines.pop(group, None)

    async def _request_mode(self, address: int) -> None:
        response = await self.client.request_response(
            TrpcMethod.MODE_SETTING,
            publish_match=True,
            timeout=self.poll_timeout,
            fields=AddressFields(address),
        )
        if response is None:
            msg = f"Timed out refreshing tekmar 482 mode for {address}"
            raise TimeoutError(msg)


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
        with self.controller.device_write(self.address, TrpcMethod.HEAT_SETPOINT):
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
        with self.controller.device_write(self.address, TrpcMethod.COOL_SETPOINT):
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
        with self.controller.device_write(self.address, TrpcMethod.SLAB_SETPOINT):
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
        with self.controller.device_write(self.address, TrpcMethod.SETPOINT_DEVICE):
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
        with self.controller.device_write(self.address, TrpcMethod.FAN_PERCENT):
            return await self.controller.client.set_latest_fan_percent(
                self.address,
                self.info.encode_fan_percent(percent),
                setback=setback,
                confirm=confirm,
                timeout=timeout,
            )

    async def set_humidity_min(
        self,
        percent: int,
        *,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Set the humidification target on devices configured for it."""
        with self.controller.device_write(self.address, TrpcMethod.HUMIDITY_SET_MIN):
            return await self.controller.client.set_humidity_min(
                self.address,
                percent,
                timeout=timeout,
            )

    async def set_humidity_max(
        self,
        percent: int,
        *,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Set the dehumidification target on devices configured for it."""
        with self.controller.device_write(self.address, TrpcMethod.HUMIDITY_SET_MAX):
            return await self.controller.client.set_humidity_max(
                self.address,
                percent,
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
        return await self.controller.set_device_mode(
            self.address,
            mode,
            confirm=confirm,
            timeout=timeout,
        )
