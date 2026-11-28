"""Send native announcements and retain independent per-port link results."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
from math import isfinite
from typing import TYPE_CHECKING, Self

from ._native_payloads.line import NETWORK_INIT_REQUEST, SetpointAssignment
from ._native_payloads.products import product_revision
from .constants import TN4_PORTS
from .management import BridgeManagement
from .native import NativeAddress, NativeBlockQuery, NativeMessage
from .native_identity import check_identity
from .raw import NativePortBusyError, TxResult, TxStatus

if TYPE_CHECKING:
    from datetime import datetime

    from ._native_payloads.line import ZoneSyncAdjustment
    from ._native_payloads.scenes import NativeScene
    from .native_identity import NativeIdentity
    from .raw import BridgeStream, RawLink

_HOST = NativeAddress(0, 0)
_SETPOINT_SETUP = 0x26


class BroadcastIssue(Enum):
    PORT_BUSY = "port_busy"
    DEADLINE = "outcome_unknown"
    EXPIRED_NOT_SENT = "expired_not_sent"


@dataclass(frozen=True, slots=True)
class BroadcastResult:
    port: int
    outcome: TxResult | BroadcastIssue

    @property
    def link_acknowledged(self) -> bool:
        return (
            isinstance(self.outcome, TxResult) and self.outcome.status is TxStatus.ACK
        )


@dataclass(frozen=True, slots=True)
class LineBroadcast:
    """One native body and a specific set of physical lines. Never a node write."""

    ports: tuple[int, ...]
    message: NativeMessage

    def __post_init__(self) -> None:
        if (
            not self.ports
            or len(set(self.ports)) != len(self.ports)
            or not all(port in TN4_PORTS for port in self.ports)
        ):
            msg = "select one or more distinct physical ports (0..4)"
            raise ValueError(msg)

    @classmethod
    def network_init(
        cls,
        ports: tuple[int, ...],
        *,
        source: NativeAddress = _HOST,
    ) -> Self:
        """Send the stock 482's class-1 empty probe; no application reply is defined."""
        return cls(ports, NativeMessage(NETWORK_INIT_REQUEST, source, b"", priority=1))

    @classmethod
    def scene(
        cls,
        ports: tuple[int, ...],
        scene: NativeScene,
        *,
        source: NativeAddress = _HOST,
    ) -> Self:
        return cls(ports, NativeMessage.scene_report(scene, source=source))

    @classmethod
    def clock(
        cls,
        ports: tuple[int, ...],
        wall_time: datetime,
        *,
        source: NativeAddress = _HOST,
        dst: bool | None = None,
        dst_mode: int | None = None,
    ) -> Self:
        return cls(
            ports,
            NativeMessage.clock_report(
                wall_time,
                source=source,
                dst=dst,
                dst_mode=dst_mode,
                priority=0,
            ),
        )

    @classmethod
    def group(
        cls,
        ports: tuple[int, ...],
        group: int,
        *,
        enabled: bool,
        source: NativeAddress = _HOST,
    ) -> Self:
        return cls(
            ports,
            NativeMessage.setpoint_group(
                group,
                enabled=enabled,
                source=source,
                priority=0,
            ),
        )

    @classmethod
    def zone_sync(
        cls,
        ports: tuple[int, ...],
        settings: ZoneSyncAdjustment,
        *,
        source: NativeAddress = _HOST,
    ) -> Self:
        return cls(ports, NativeMessage.zone_sync(settings, source=source))

    async def send(
        self,
        stream: BridgeManagement | RawLink,
        *,
        timeout: float = 20,
    ) -> tuple[BroadcastResult, ...]:
        """Send once per port. A timeout does not cancel an admitted transmission."""
        if not isfinite(timeout) or timeout <= 0:
            msg = "broadcast send timeout must be finite and positive"
            raise ValueError(msg)

        async def one(port: int) -> BroadcastResult:
            try:
                async with asyncio.timeout(timeout):
                    if isinstance(stream, BridgeManagement):
                        tx = await stream.send_announcement(
                            port,
                            self.message,
                            timeout=timeout,
                        )
                        return BroadcastResult(
                            port,
                            tx if tx is not None else BroadcastIssue.EXPIRED_NOT_SENT,
                        )
                    return BroadcastResult(port, await stream.send(port, self.message))
            except NativePortBusyError:
                return BroadcastResult(port, BroadcastIssue.PORT_BUSY)
            except TimeoutError:
                return BroadcastResult(port, BroadcastIssue.DEADLINE)

        results = await asyncio.gather(
            *(one(p) for p in self.ports),
            return_exceptions=True,
        )
        collected = []
        for result in results:
            if isinstance(result, BaseException):
                raise result
            collected.append(result)
        return tuple(collected)


@dataclass(frozen=True, slots=True)
class DeviceGroupBinding:
    port: int
    address: NativeAddress
    assignment: SetpointAssignment

    @classmethod
    async def read(
        cls,
        stream: BridgeManagement | BridgeStream,
        port: int,
        address: NativeAddress,
        *,
        source: NativeAddress = _HOST,
        expected_identity: NativeIdentity | None = None,
        timeout: float = 20,
    ) -> Self:
        """Resolve a 161/162's configured group from live identity and setup."""
        if isinstance(stream, BridgeManagement) and source != _HOST:
            msg = "Normal group lookup uses the gateway source address"
            raise ValueError(msg)

        async def read(block: int) -> dict[str, str | int | float]:
            query = NativeBlockQuery(address, block)
            if isinstance(stream, BridgeManagement):
                return (await stream.read_block(port, query, timeout=timeout)).decoded
            batch = await stream.read_block_group(
                port,
                query,
                source=source,
                timeout=timeout,
            )
            return batch.require_report().decoded

        fields = await read(0)
        check_identity(expected_identity, fields)
        product, _ = product_revision(fields)
        if product is None or product.family != "setpoint_control":
            msg = "group lookup requires an identified setpoint control"
            raise ValueError(msg)
        return cls(
            port,
            address,
            SetpointAssignment.from_fields(await read(_SETPOINT_SETUP)),
        )

    def announcement(
        self,
        expected_group: int,
        *,
        enabled: bool,
        source: NativeAddress = _HOST,
    ) -> LineBroadcast:
        """Prepare a send only for the group the caller expects."""
        if self.assignment.remote_group != expected_group:
            msg = f"device lacks the expected remote-enable group {expected_group}"
            raise ValueError(msg)
        return LineBroadcast.group(
            (self.port,),
            expected_group,
            enabled=enabled,
            source=source,
        )
