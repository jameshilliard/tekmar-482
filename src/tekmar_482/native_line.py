"""Send native announcements and retain independent per-port link results."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Self

from ._native_payloads.line import SetpointAssignment
from ._native_payloads.products import product_revision
from .constants import TN4_PORTS
from .native import NativeAddress, NativeBlockQuery, NativeMessage
from .raw import NativePortBusyError, TxResult, TxStatus

if TYPE_CHECKING:
    from datetime import datetime

    from ._native_payloads.line import ZoneSyncAdjustment
    from ._native_payloads.scenes import NativeScene
    from .raw import BridgeStream

_HOST = NativeAddress(0, 0)
_SETPOINT_SETUP = 0x26


class BroadcastIssue(Enum):
    PORT_BUSY = "port_busy"
    DEADLINE = "outcome_unknown"


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
        stream: BridgeStream,
        *,
        timeout: float = 20,
    ) -> tuple[BroadcastResult, ...]:
        """Send once per port. A timeout does not cancel an admitted transmission."""
        if not timeout > 0:
            msg = "broadcast send timeout must be positive"
            raise ValueError(msg)

        async def one(port: int) -> BroadcastResult:
            deadline = asyncio.timeout(timeout)
            try:
                async with deadline:
                    return BroadcastResult(port, await stream.send(port, self.message))
            except NativePortBusyError:
                return BroadcastResult(port, BroadcastIssue.PORT_BUSY)
            except TimeoutError:
                if not deadline.expired():
                    raise
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
        stream: BridgeStream,
        port: int,
        address: NativeAddress,
        *,
        source: NativeAddress = _HOST,
        timeout: float = 20,
    ) -> Self:
        """Resolve a 161/162's configured group from live identity and setup."""

        async def read(block: int) -> dict[str, str | int | float]:
            batch = await stream.read_block_group(
                port,
                NativeBlockQuery(address, block),
                source=source,
                timeout=timeout,
            )
            return batch.require_report().decoded

        product, _ = product_revision(await read(0))
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
