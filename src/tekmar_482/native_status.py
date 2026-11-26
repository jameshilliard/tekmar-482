"""Live, read-only native queries, distinct from addressed block operations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ._native_payloads import SnowmeltBlock
from ._native_payloads.queries import NativeStatusQuery, SnowmeltRole
from .exceptions import TekmarError
from .native import NativeBlockQuery

if TYPE_CHECKING:
    from .native import NativeAddress, NativeFrame
    from .raw import BridgeStream, TxResult


@dataclass(frozen=True, slots=True)
class NativeStatusBatch:
    port: int
    query: NativeStatusQuery
    peer: NativeAddress | None
    tx_result: TxResult | None
    reports: tuple[NativeFrame, ...]
    covered: frozenset[int]
    expected_count: int | None

    @property
    def complete(self) -> bool:
        return bool(self.expected_count) and len(self.covered) == self.expected_count

    @property
    def answered(self) -> bool:
        return (
            self.tx_result is not None
            and self.tx_result.may_have_reached_peer
            and self.complete
        )

    @property
    def missing_keys(self) -> tuple[int, ...]:
        return tuple(
            key for key in dict.fromkeys(self.query.keys) if key not in self.covered
        )


@dataclass(slots=True)
class StatusCollector:
    query: NativeStatusQuery
    peer: NativeAddress | None
    reports: list[NativeFrame] = field(default_factory=list, init=False)
    covered: set[int] = field(default_factory=set, init=False)
    expected_count: int | None = field(default=None, init=False)

    def observe(self, frame: NativeFrame) -> bool:
        if self.peer is not None and (
            frame.subnet != self.peer.subnet or (frame.node ^ self.peer.node) & 31
        ):
            return False
        part = self.query.match(frame.message_id, frame.subnet, frame.message.payload)
        if part is None:
            return False
        # A shared line can have multiple clocks, controllers or I/O devices.
        # Never assemble one complete answer from different physical senders.
        self.peer = frame.message.source
        if self.expected_count is None:
            self.expected_count = part.expected_count
        elif self.expected_count != part.expected_count:
            self.expected_count = 0  # Conflicting discovery counts are not completion.
        if not part.items.issubset(self.covered):
            self.reports.append(frame)
            self.covered.update(part.items)
        return self.expected_count > 0 and len(self.covered) == self.expected_count

    def batch(self, port: int, result: TxResult | None) -> NativeStatusBatch:
        return NativeStatusBatch(
            port,
            self.query,
            self.peer,
            result,
            tuple(self.reports),
            frozenset(self.covered),
            self.expected_count,
        )


async def read_snowmelt_status(
    stream: BridgeStream,
    port: int,
    peer: NativeAddress,
    *,
    source: NativeAddress,
    keys: bytes | None = None,
    timeout: float = 20,
) -> NativeStatusBatch:
    """Read the application and configured number, then request that peer's status."""

    async def setting(block: SnowmeltBlock, name: str) -> int:
        batch = await stream.read_block_group(
            port,
            NativeBlockQuery(peer, block),
            source=source,
            timeout=timeout,
        )
        value = batch.require_report().decoded.get(name)
        if not isinstance(value, int):
            msg = f"device did not report {name} in block {block:#04x}"
            raise TekmarError(msg)
        return value

    application = await setting(SnowmeltBlock.APPLICATION_MODE, "application_mode")
    role = SnowmeltRole.for_application(application)
    number = (
        await setting(SnowmeltBlock.SENSOR_ID, "sensor_id")
        if role is SnowmeltRole.SENSOR
        else await setting(SnowmeltBlock.ZONE_ID, "zone_id")
    )
    query = NativeStatusQuery.for_snowmelt_device(application, number, keys)
    return await stream.read_status(
        port,
        query,
        source=source,
        peer=peer,
        timeout=timeout,
    )
