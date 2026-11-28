"""Live, read-only native queries, distinct from addressed block operations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ._native_payloads import SnowmeltBlock
from ._native_payloads.queries import NativeStatusQuery, SnowmeltRole, StatusRequest
from .exceptions import TekmarError
from .native import NativeBlockQuery

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from ._native_payloads.snowmelt import SnowmeltPair
    from .native import NativeAddress, NativeFrame
    from .raw import NativeReader, TxResult


@dataclass(frozen=True, slots=True)
class NativeStatusBatch:
    port: int
    query: NativeStatusQuery
    peer: NativeAddress | None
    tx_result: TxResult | None
    reports: tuple[NativeFrame, ...]
    covered: frozenset[int]
    expected_count: int | None
    expired_unsent: bool = False
    capture_lost: bool = False

    @property
    def transmit_pending(self) -> bool:
        return self.tx_result is None and not self.expired_unsent

    @property
    def complete(self) -> bool:
        return bool(self.expected_count) and len(self.covered) == self.expected_count

    @property
    def answered(self) -> bool:
        return (
            self.tx_result is not None
            and self.tx_result.completed_frame
            and self.complete
        )

    @property
    def missing_keys(self) -> tuple[int, ...]:
        return tuple(
            key for key in dict.fromkeys(self.query.keys) if key not in self.covered
        )

    def snowmelt_pair(self, key: int) -> SnowmeltPair | None:
        """Return the newest reported word, including an unknown or repeated key."""
        if self.query.kind is StatusRequest.SNOWMELT:
            for frame in reversed(self.reports):
                if (report := frame.snowmelt_report) is not None:
                    for pair in reversed(report.pairs):
                        if pair.key == key:
                            return pair
        return None


@dataclass(slots=True)
class StatusCollector:
    query: NativeStatusQuery
    peer: NativeAddress | None
    reports: list[NativeFrame] = field(default_factory=list, init=False)
    covered: set[int] = field(default_factory=set, init=False)
    expected_count: int | None = field(default=None, init=False)
    _latest: dict[int, NativeFrame] = field(default_factory=dict, init=False)

    def observe(self, frame: NativeFrame) -> bool:
        return (
            self.add(frame)
            and self.expected_count is not None
            and self.expected_count > 0
            and len(self.covered) == self.expected_count
        )

    def add(self, frame: NativeFrame) -> bool:
        """Accept a matching part, whether or not it completes the exchange."""
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
        if self.query.kind in (StatusRequest.SNOWMELT, StatusRequest.IO_DISCOVERY):
            # A partial answer can revise a key or channel before the last
            # arrives. Retain the latest evidence per item, in arrival order.
            self._latest.update(dict.fromkeys(part.items, frame))
            active = {id(f) for f in self._latest.values()}
            self.reports = [f for f in self.reports if id(f) in active]
            self.reports.append(frame)
            self.covered.update(part.items)
        elif not part.items.issubset(self.covered):
            self.reports.append(frame)
            self.covered.update(part.items)
        return True

    def batch(
        self,
        port: int,
        result: TxResult | None,
        *,
        expired_unsent: bool = False,
        capture_lost: bool = False,
    ) -> NativeStatusBatch:
        return NativeStatusBatch(
            port,
            self.query,
            self.peer,
            result,
            tuple(self.reports),
            frozenset(self.covered),
            self.expected_count,
            expired_unsent,
            capture_lost,
        )


async def snowmelt_status_query(
    peer: NativeAddress,
    read: Callable[[NativeBlockQuery], Awaitable[NativeFrame]],
    keys: bytes | None,
) -> NativeStatusQuery:
    """Resolve a native peer's separately configured snowmelt zone or sensor number."""

    async def setting(block: SnowmeltBlock, name: str) -> int:
        frame = await read(NativeBlockQuery(peer, block))
        value = frame.decoded.get(name)
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
    return NativeStatusQuery.for_snowmelt_device(application, number, keys)


async def read_snowmelt_status(
    stream: NativeReader,
    port: int,
    peer: NativeAddress,
    *,
    source: NativeAddress,
    keys: bytes | None = None,
    timeout: float = 20,
) -> NativeStatusBatch:
    """Read the application and configured number, then request that peer's status."""

    async def read(query: NativeBlockQuery) -> NativeFrame:
        batch = await stream.read_block_group(
            port,
            query,
            source=source,
            timeout=timeout,
        )
        return batch.require_report()

    query = await snowmelt_status_query(peer, read, keys)
    return await stream.read_status(
        port,
        query,
        source=source,
        peer=peer,
        timeout=timeout,
    )
