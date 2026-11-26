"""Runtime poll planning. All observed values are interpreted by the state reducer."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Self

from .constants import ThaSetback, schedule_setbacks_for_events
from .exceptions import ProtocolError
from .models import (
    AvailableInfo,
    DeviceRuntime,
    DeviceSnapshot,
    DeviceValue,
    GatewaySnapshot,
    SetpointValues,
)
from .state import (
    DEVICE_FIELDS,
    DEVICE_METADATA_FIELDS,
    GATEWAY_METHODS,
    device_supports_method,
    reconcile_runtime,
)
from .trpc import (
    AddressFields,
    AddressSetbackFields,
    SetpointGroupFields,
    TrpcCommand,
    TrpcMethod,
    TrpcService,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator

    from .models import DiscoveredDevice, DiscoveryResult
    from .session import ReportEvent, Tekmar482Session
    from .trpc import TrpcPacket


_FLOOR_TEMPERATURE_VERSION = 3
_NAMED_SLOTS = tuple(slot for slot in ThaSetback if slot is not ThaSetback.CURRENT)
_GATEWAY_READS = (
    TrpcMethod.REPORTING_STATE,
    TrpcMethod.SETBACK_ENABLE,
    TrpcMethod.OUTDOOR_TEMP,
    TrpcMethod.NETWORK_ERROR,
)


@contextmanager
def observe_runtime(session: Tekmar482Session) -> Iterator[list[ReportEvent]]:
    """Record replies and reports in wire order with their matched commands."""
    events: list[ReportEvent] = []

    def record(message: TrpcPacket, command: TrpcCommand | None) -> None:
        if (
            message.method in DEVICE_FIELDS
            or message.method in GATEWAY_METHODS
            or message.method in DEVICE_METADATA_FIELDS
            or message.method is TrpcMethod.DEVICE_TYPE
        ):
            events.append((message, command))

    with session.connection_scope(), session.observe_messages(record):
        yield events


def filter_discovery_events(
    events: list[ReportEvent],
    discovery: DiscoveryResult,
    previous: AvailableInfo | None,
    *,
    changed_addresses: frozenset[int] = frozenset(),
) -> None:
    """Discard samples predating identification of a new or replaced device."""
    old = {} if previous is None else {s.info.address: s.info for s in previous.devices}
    identified = {
        d.address
        for d in discovery.devices
        if d.type_code is not None
        and old.get(d.address) == d
        and d.address not in changed_addresses
    }
    types = {
        d.address: d.type_code for d in discovery.devices if d.type_code is not None
    }
    verified = []
    for message, command in events:
        address = message.get("address")
        if message.method is TrpcMethod.DEVICE_TYPE:
            if (
                address is not None
                and command is not None
                and command.method == TrpcMethod.DEVICE_TYPE
                and message.service is TrpcService.RESPONSE_REQUEST
                and address in types
                and message.get("type") == types[address]
            ):
                identified.add(address)
        elif address is None or address in identified:
            verified.append((message, command))
    events[:] = verified


def _pollable(
    device: DiscoveredDevice,
    method: TrpcMethod,
    version: int | None,
) -> bool:
    if (
        method is TrpcMethod.CURRENT_FLOOR_TEMPERATURE
        and version != _FLOOR_TEMPERATURE_VERSION
    ):
        return False
    if method in {
        TrpcMethod.RELATIVE_HUMIDITY,
        TrpcMethod.HUMIDITY_SET_MIN,
        TrpcMethod.HUMIDITY_SET_MAX,
    } and version not in {2, 3}:
        return False
    return device_supports_method(device, method)


@dataclass(frozen=True, slots=True)
class RuntimePoll:
    """The ordered reads and empty snapshot shape for a single scan."""

    discovery: DiscoveryResult
    empty: AvailableInfo
    commands: tuple[TrpcCommand, ...]

    @classmethod
    def plan(
        cls,
        discovery: DiscoveryResult,
        *,
        include_setbacks: bool = False,
        exhaustive_setbacks: bool = False,
        include_gateway: bool = True,
        include_setpoint_groups: bool = True,
    ) -> Self:
        commands: list[TrpcCommand] = []
        groups = range(1, 13) if include_gateway and include_setpoint_groups else ()
        if include_gateway:
            commands.extend(TrpcCommand.request(method) for method in _GATEWAY_READS)
            commands.extend(
                TrpcCommand.request(
                    TrpcMethod.SETPOINT_GROUP_ENABLE,
                    SetpointGroupFields(group),
                )
                for group in groups
            )
        gateway = GatewaySnapshot(
            info=discovery.gateway,
            setpoint_groups=dict.fromkeys(groups) if groups else None,
        )
        devices = []
        optional: list[TrpcCommand] = []
        for device in discovery.devices:
            advertised = schedule_setbacks_for_events(device.setback_events)
            named = (
                _NAMED_SLOTS
                if exhaustive_setbacks or advertised is None
                else advertised
            )
            slots = (
                (ThaSetback.CURRENT, *named)
                if include_setbacks
                else (ThaSetback.CURRENT,)
            )
            empty_slots = SetpointValues(dict.fromkeys(slots))
            fields = {
                method: descriptor
                for method, descriptor in DEVICE_FIELDS.items()
                if _pollable(device, method, discovery.gateway.protocol_version)
            }
            # Declare every requested slot, even if the gateway never answers.
            slots_by_value = {f.key: empty_slots for f in fields.values() if f.indexed}
            runtime = DeviceRuntime.create(
                info=device,
                available_values=frozenset(f.key for f in fields.values()),
                heat_setpoints=slots_by_value.get(DeviceValue.HEAT_SETPOINTS),
                cool_setpoints=slots_by_value.get(DeviceValue.COOL_SETPOINTS),
                slab_setpoints=slots_by_value.get(DeviceValue.SLAB_SETPOINTS),
                fan_percent=slots_by_value.get(DeviceValue.FAN_PERCENT),
                setpoint_targets=slots_by_value.get(DeviceValue.SETPOINT_TARGETS),
            )
            devices.append(DeviceSnapshot(runtime))
            for method, descriptor in fields.items():
                if descriptor.indexed:
                    for slot in slots:
                        target = commands if slot is ThaSetback.CURRENT else optional
                        target.append(
                            TrpcCommand.request(
                                method,
                                AddressSetbackFields(device.address, slot),
                            ),
                        )
                else:
                    commands.append(
                        TrpcCommand.request(method, AddressFields(device.address)),
                    )
        return cls(
            discovery,
            AvailableInfo(gateway, tuple(devices)),
            (*commands, *optional),
        )

    async def execute(
        self,
        session: Tekmar482Session,
        *,
        timeout: float | None,
        write_delay: float = 0,
        expected_setback: bool | None = None,
        on_reporting_disabled: Callable[[], None] | None = None,
    ) -> None:
        """Issue reads and verify any requested setback change by its readback."""
        readback = None
        for command in self.commands:
            reply = await session.request_response(
                command,
                timeout=timeout,
            )
            if (
                command.method is TrpcMethod.REPORTING_STATE
                and reply is not None
                and reply.get("state") == 0
                and on_reporting_disabled is not None
            ):
                on_reporting_disabled()
            if (
                expected_setback is not None
                and command.method is TrpcMethod.SETBACK_ENABLE
                and reply is not None
            ):
                enable = reply.require("enable")
                readback = bool(enable) if enable in (0, 1) else None
            if write_delay > 0:
                await asyncio.sleep(write_delay)
        if expected_setback is not None and readback is not expected_setback:
            msg = f"Requested SetbackEnable={expected_setback}, read back {readback}"
            raise ProtocolError(msg)

    def reduce(
        self,
        events: Iterable[ReportEvent],
        previous: AvailableInfo | None = None,
    ) -> AvailableInfo:
        return reconcile_runtime(self.discovery, self.empty, previous, events)

    async def snapshot(
        self,
        session: Tekmar482Session,
        *,
        timeout: float | None,
        write_delay: float,
    ) -> AvailableInfo:
        with observe_runtime(session) as events:
            await self.execute(session, timeout=timeout, write_delay=write_delay)
        return self.reduce(events)
