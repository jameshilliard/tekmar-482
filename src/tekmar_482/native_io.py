"""I/O channels scoped to one reporting peer and one full payload endpoint."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Self

from ._native_payloads.io import IO_DISCOVERY, IoEndpoint, IoReport
from ._native_payloads.queries import StatusRequest

if TYPE_CHECKING:
    from .native import NativeFrame
    from .native_status import NativeStatusBatch


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

    @classmethod
    def from_batch(cls, batch: NativeStatusBatch) -> Self:
        """Interpret one discovery exchange, including incomplete or silent ones."""
        if batch.query.kind is not StatusRequest.IO_DISCOVERY:
            msg = "expected an I/O discovery batch"
            raise ValueError(msg)
        book = IoBook(IoEndpoint(*batch.query.payload))
        book.reconcile(batch, None)
        return cls(book.endpoint, book.channels, book.count, book.complete)


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
        batch: NativeStatusBatch,
        before: IoDirectory | None,
    ) -> None:
        # The capture consumer and the request waiter see the same reports.
        # Never replace or prune a channel observed since this exchange began.
        prior = {c.sample.channel: c for c in before.channels} if before else {}
        seen: set[int] = set()
        for frame in batch.reports:
            sample = frame.io_report
            if (
                sample is None
                or sample.endpoint != self.endpoint
                or frame.message_id != IO_DISCOVERY
                or frame.port != batch.port
                or batch.peer is None
                or frame.subnet != batch.peer.subnet
                or (frame.node ^ batch.peer.node) & 31
            ):
                continue
            channel = sample.channel
            seen.add(channel)
            if self._channels.get(channel) is prior.get(channel):
                self.observe(sample, frame)
                prior[channel] = self._channels[channel]
        self.complete = False
        if not batch.answered or seen != batch.covered:
            return
        for k in prior.keys() - seen:
            if self._channels.get(k) is prior[k]:
                del self._channels[k]
        self.complete = (
            self._channels.keys() == seen
            and self.count == batch.expected_count
            and all(c.sample.count == self.count for c in self._channels.values())
        )
