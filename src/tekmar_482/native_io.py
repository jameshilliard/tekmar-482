"""I/O channels scoped to one reporting peer and one full payload endpoint."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from ._native_payloads.io import (
    IO_DISCOVERY,
    IO_REPLY,
    IoEndpoint,
    IoReport,
    io_terminal,
)
from ._native_payloads.queries import NativeStatusQuery, StatusRequest
from .constants import DEFAULT_BLOCK_TIMEOUT
from .native import NativeAddress

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from .native import NativeFrame
    from .native_identity import NativeIdentity
    from .native_status import NativeStatusBatch
    from .raw import NativeReader

_HOST = NativeAddress(0, 0)


@dataclass(frozen=True, slots=True)
class IoChannel:
    """Latest sample with discovery metadata retained only for the same type."""

    sample: IoReport
    frame: NativeFrame = field(compare=False)


@dataclass(frozen=True, slots=True)
class IoDirectory:
    endpoint: IoEndpoint
    channels: tuple[IoChannel, ...]
    advertised_count: int | None
    complete: bool

    def describes(self, sample: IoReport) -> bool:
        """Test channel identity and count, independently of a changing value."""
        return (
            sample.endpoint == self.endpoint
            and (sample.count is None or sample.count == self.advertised_count)
            and any(
                c.sample.channel == sample.channel
                and c.sample.type_id == sample.type_id
                for c in self.channels
            )
        )

    @staticmethod
    def from_batch(batch: NativeStatusBatch) -> IoDirectory:
        """Interpret one discovery exchange, including incomplete or silent ones."""
        return IoDirectoryRead(batch).directory


@dataclass(frozen=True, slots=True)
class IoDirectoryRead:
    """One directory discovery and the exact channel reads used to supplement it."""

    discovery: NativeStatusBatch
    supplemental: tuple[NativeStatusBatch, ...] = ()

    def __post_init__(self) -> None:
        if self.discovery.query.kind is not StatusRequest.IO_DISCOVERY:
            msg = "expected an I/O discovery batch"
            raise ValueError(msg)

    @property
    def endpoint(self) -> IoEndpoint:
        return IoEndpoint(*self.discovery.query.payload)

    @property
    def directory(self) -> IoDirectory:
        book = IoBook(self.endpoint)
        book.reconcile(self, None)
        return book.snapshot()


def _same_reporter(frame: NativeFrame, discovery: NativeStatusBatch) -> bool:
    peer = discovery.peer
    return (
        peer is not None
        and frame.port == discovery.port
        and frame.subnet == peer.subnet
        and (frame.node ^ peer.node) & 31 == 0
    )


def _samples(
    exchange: NativeStatusBatch,
    read: IoDirectoryRead,
    message_id: int,
) -> dict[int, IoChannel]:
    endpoint = read.endpoint
    return {
        sample.channel: IoChannel(sample, frame)
        for frame in exchange.reports
        if frame.message_id == message_id
        and _same_reporter(frame, read.discovery)
        and (sample := frame.io_report) is not None
        and sample.endpoint == endpoint
    }


def _consistent(
    discovery: NativeStatusBatch,
    samples: dict[int, IoChannel],
) -> bool:
    return (
        discovery.tx_result is not None
        and discovery.tx_result.completed_frame
        and bool(discovery.expected_count)
        and bool(samples)
        and samples.keys() == discovery.covered
        and all(c.sample.count == discovery.expected_count for c in samples.values())
    )


def _direct_sample(
    answer: NativeStatusBatch,
    read: IoDirectoryRead,
    channel: int,
) -> IoChannel | None:
    if (
        not answer.answered
        or answer.port != read.discovery.port
        or answer.query
        != NativeStatusQuery.io(
            read.endpoint.subnet,
            read.endpoint.node,
            channel,
        )
        or answer.covered != {channel}
    ):
        return None
    samples = _samples(answer, read, IO_REPLY)
    return samples[channel] if samples.keys() == {channel} else None


def _candidate_channels(
    read: IoDirectoryRead,
    before: IoDirectory | None,
    identity: NativeIdentity | None,
) -> frozenset[int]:
    discovery = read.discovery
    count, peer, endpoint = discovery.expected_count, discovery.peer, read.endpoint
    if not count or peer is None:
        return frozenset()
    if before is not None and before.complete:
        if (
            before.endpoint == endpoint
            and before.advertised_count == count == len(before.channels)
            and all(_same_reporter(c.frame, discovery) for c in before.channels)
        ):
            return frozenset(c.sample.channel for c in before.channels)
        return frozenset()
    if (
        identity is not None
        and (product := identity.product) is not None
        and endpoint.subnet == peer.subnet
        and endpoint.node in (peer.node, peer.node & 31)
        and io_terminal(product.model, count - 1) is not None
        and io_terminal(product.model, count) is None
    ):
        return frozenset(range(count))
    return frozenset()


async def recover_io_directory(
    discovery: NativeStatusBatch,
    read_channel: Callable[[NativeStatusQuery], Awaitable[NativeStatusBatch | None]],
    *,
    before: IoDirectory | None = None,
    identity: NativeIdentity | None = None,
) -> IoDirectoryRead:
    """Read missing known channels once; never treat discovery counts as IDs."""
    read = IoDirectoryRead(discovery)
    initial = _samples(discovery, read, IO_DISCOVERY)
    if discovery.answered or not _consistent(discovery, initial):
        return read
    candidates = _candidate_channels(read, before, identity)
    if not discovery.covered <= candidates:
        return read
    endpoint = read.endpoint
    attempts = []
    for channel in sorted(candidates - discovery.covered):
        query = NativeStatusQuery.io(endpoint.subnet, endpoint.node, channel)
        answer = await read_channel(query)
        if answer is None:
            break
        attempts.append(answer)
        if _direct_sample(answer, read, channel) is None:
            break
    return IoDirectoryRead(discovery, tuple(attempts))


async def read_io_directory(
    stream: NativeReader,
    port: int,
    endpoint: IoEndpoint,
    *,
    peer: NativeAddress,
    source: NativeAddress = _HOST,
    timeout: float = DEFAULT_BLOCK_TIMEOUT,
    before: IoDirectory | None = None,
    identity: NativeIdentity | None = None,
) -> IoDirectoryRead:
    """Discover one endpoint, supplementing only from confirmed channel candidates."""

    async def read(query: NativeStatusQuery) -> NativeStatusBatch:
        return await stream.read_status(
            port,
            query,
            source=source,
            peer=peer,
            timeout=timeout,
        )

    discovery = await read(NativeStatusQuery.io(endpoint.subnet, endpoint.node))
    return await recover_io_directory(discovery, read, before=before, identity=identity)


class IoBook:
    def __init__(self, endpoint: IoEndpoint) -> None:
        self.endpoint = endpoint
        self._channels: dict[int, IoChannel] = {}
        self.count: int | None = None
        self.complete = False

    @property
    def channels(self) -> tuple[IoChannel, ...]:
        return tuple(self._channels[k] for k in sorted(self._channels))

    def snapshot(self) -> IoDirectory:
        return IoDirectory(self.endpoint, self.channels, self.count, self.complete)

    def observe(self, sample: IoReport, frame: NativeFrame) -> bool:
        old = self._channels.get(sample.channel)
        previous = old.sample if old else None
        topology = self.count, self.complete
        same_type = previous is not None and previous.type_id == sample.type_id
        if not same_type or (sample.count is not None and sample.count != self.count):
            self.complete = False
        if sample.count is not None:
            self.count = sample.count
        elif same_type and previous is not None:
            sample = replace(sample, count=previous.count, label=previous.label)
        self._channels[sample.channel] = IoChannel(sample, frame)
        return previous != sample or topology != (self.count, self.complete)

    def reconcile(
        self,
        read: IoDirectoryRead,
        before: IoDirectory | None,
    ) -> None:
        if read.endpoint != self.endpoint:
            msg = "I/O discovery belongs to a different endpoint"
            raise ValueError(msg)
        batch = read.discovery
        initial = _samples(batch, read, IO_DISCOVERY)
        seen = initial.copy()
        if not batch.answered and _consistent(batch, initial):
            for answer in read.supplemental:
                if answer.query.kind is not StatusRequest.IO_CHANNEL:
                    break
                channel = answer.query.payload[2]
                sample = _direct_sample(answer, read, channel)
                if channel in seen or sample is None:
                    break
                seen[channel] = sample
        # The capture consumer and the request waiter see the same reports.
        # Never replace or prune a channel observed since this exchange began.
        prior = {c.sample.channel: c for c in before.channels} if before else {}
        for channel, evidence in seen.items():
            if self._channels.get(channel) is prior.get(channel):
                self.observe(evidence.sample, evidence.frame)
                prior[channel] = self._channels[channel]
        self.complete = False
        if len(seen) != batch.expected_count or not _consistent(batch, initial):
            return
        if batch.answered:
            for k in prior.keys() - seen.keys():
                if self._channels.get(k) is prior[k]:
                    del self._channels[k]
        self.complete = (
            self._channels.keys() == seen.keys()
            and self.count == batch.expected_count
            and all(
                c.sample.type_id == seen[k].sample.type_id
                and (
                    c.sample.count == self.count
                    or (k not in initial and c.sample.count is None)
                )
                for k, c in self._channels.items()
            )
        )
