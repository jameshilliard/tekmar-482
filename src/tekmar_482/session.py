"""Low-level async tRPC session dispatcher."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Self

from .exceptions import ConnectionClosedError
from .packet import TYPE_TRPC
from .trpc import ResponseMatch, TrpcCommand, TrpcPacket

if TYPE_CHECKING:
    from collections.abc import Callable, Hashable
    from types import TracebackType

    from .packet import Packet
    from .transport import PacketTransport

type MessagePredicate = Callable[[TrpcPacket], bool]
type _ReportUnsubscribe = Callable[[], None]
type _ReportItem = TrpcPacket | BaseException | None

_SESSION_NOT_OPEN = "tekmar 482 session is not open"
_SESSION_CLOSED = "tekmar 482 session closed"


@dataclass(slots=True)
class _PendingRequest:
    match: ResponseMatch
    future: asyncio.Future[TrpcPacket]
    publish_match: bool = False


@dataclass(slots=True)
class _QueuedUpdate:
    command: TrpcCommand
    future: asyncio.Future[TrpcPacket | None]
    response_match: ResponseMatch | None = None
    response_timeout: float | None = None


class ReportSubscription:
    """Async iterator for unsolicited tRPC messages from a session."""

    def __init__(
        self,
        queue: asyncio.Queue[_ReportItem],
        unsubscribe: _ReportUnsubscribe,
    ) -> None:
        self._queue = queue
        self._unsubscribe = unsubscribe
        self._closed = False

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc, traceback
        self.close()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> TrpcPacket:
        message = await self._queue.get()
        if message is None:
            raise StopAsyncIteration
        if isinstance(message, BaseException):
            raise message
        return message

    def close(self) -> None:
        """Unsubscribe and wake any pending iterator."""
        if self._closed:
            return
        self._closed = True
        self._unsubscribe()


class Tekmar482Session:
    """Own the packet stream and dispatch tRPC messages.

    The session is the single reader for a transport. It matches request/response
    calls, keeps unmatched messages available for monitors, and publishes
    unsolicited messages to report subscriptions.
    """

    def __init__(
        self,
        transport: PacketTransport,
        *,
        max_message_backlog: int = 512,
        max_packet_backlog: int = 512,
    ) -> None:
        self.transport = transport
        self.max_message_backlog = max_message_backlog
        self.max_packet_backlog = max_packet_backlog
        self._condition = asyncio.Condition()
        self._transaction_lock = asyncio.Lock()
        self._pending: list[_PendingRequest] = []
        self._message_backlog: list[TrpcPacket] = []
        self._packet_backlog: list[Packet] = []
        self._report_queues: set[asyncio.Queue[_ReportItem]] = set()
        self._coalesced_updates: OrderedDict[Hashable, _QueuedUpdate] = OrderedDict()
        self._coalesced_condition = asyncio.Condition()
        self._coalesced_task: asyncio.Task[None] | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._reader_error: BaseException | None = None
        self._closed = True

    @property
    def is_open(self) -> bool:
        """Return whether the underlying transport is open."""
        return self.transport.is_open

    @property
    def pending_request_count(self) -> int:
        """Return the number of request/response calls awaiting a match."""
        return len(self._pending)

    @property
    def queued_update_count(self) -> int:
        """Return the number of coalesced updates waiting to be written."""
        return len(self._coalesced_updates)

    @property
    def message_backlog_count(self) -> int:
        """Return the number of unmatched messages in the backlog."""
        return len(self._message_backlog)

    @property
    def packet_backlog_count(self) -> int:
        """Return the number of observed packets in the backlog."""
        return len(self._packet_backlog)

    @property
    def report_subscription_count(self) -> int:
        """Return the number of active report subscribers."""
        return len(self._report_queues)

    @property
    def reader_error(self) -> BaseException | None:
        """Return the last fatal reader error, if any."""
        return self._reader_error

    async def __aenter__(self) -> Self:
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc, traceback
        await self.close()

    async def open(self) -> None:
        """Open the transport and start the single reader task."""
        if self._reader_error is not None and self.transport.is_open:
            await self.transport.close()

        if not self.transport.is_open:
            await self.transport.open()

        self._closed = False
        self._reader_error = None
        if self._reader_task is None or self._reader_task.done():
            self._reader_task = asyncio.create_task(
                self._read_loop(),
                name="tekmar 482 session reader",
            )
        if self._coalesced_task is None or self._coalesced_task.done():
            self._coalesced_task = asyncio.create_task(
                self._coalesced_update_loop(),
                name="tekmar 482 coalesced update writer",
            )

    async def close(self) -> None:
        """Cancel the reader, fail pending calls, and close the transport."""
        self._closed = True
        task = self._reader_task
        self._reader_task = None
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        task = self._coalesced_task
        self._coalesced_task = None
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        await self._fail_pending(ConnectionClosedError(_SESSION_CLOSED))
        await self._fail_coalesced_updates(
            ConnectionClosedError(_SESSION_CLOSED),
        )
        async with self._condition:
            self._condition.notify_all()

        for queue in tuple(self._report_queues):
            self._unsubscribe_reports(queue)

        await self.transport.close()

    async def read_packet(self, timeout: float | None = None) -> Packet | None:
        """Read the next observed packet, returning None on timeout."""

        async def wait_next() -> Packet | None:
            async with self._condition:
                while True:
                    if self._packet_backlog:
                        return self._packet_backlog.pop(0)
                    if self._reader_error is not None:
                        raise self._reader_error
                    if self._closed:
                        return None
                    await self._condition.wait()

        try:
            if timeout is None:
                return await wait_next()
            return await asyncio.wait_for(wait_next(), timeout)
        except TimeoutError:
            return None

    async def write_packet(self, packet: Packet) -> None:
        """Write one packet to the transport."""
        self._raise_if_unavailable()
        await self.transport.write_packet(packet)

    async def read_message(self, timeout: float | None = None) -> TrpcPacket | None:
        """Read the next unmatched tRPC message, returning None on timeout."""

        async def wait_next() -> TrpcPacket | None:
            async with self._condition:
                while True:
                    if self._message_backlog:
                        return self._message_backlog.pop(0)
                    if self._reader_error is not None:
                        raise self._reader_error
                    if self._closed:
                        return None
                    await self._condition.wait()

        try:
            if timeout is None:
                return await wait_next()
            return await asyncio.wait_for(wait_next(), timeout)
        except TimeoutError:
            return None

    async def read_until(
        self,
        predicate: MessagePredicate,
        *,
        timeout: float | None = None,
    ) -> TrpcPacket | None:
        """Read unmatched tRPC messages until one matches or timeout expires."""

        async def wait_match() -> TrpcPacket | None:
            async with self._condition:
                while True:
                    if (message := self._pop_message_backlog(predicate)) is not None:
                        return message
                    if self._reader_error is not None:
                        raise self._reader_error
                    if self._closed:
                        return None
                    await self._condition.wait()

        try:
            if timeout is None:
                return await wait_match()
            return await asyncio.wait_for(wait_match(), timeout)
        except TimeoutError:
            return None

    async def write_message(self, message: TrpcPacket) -> None:
        """Write one tRPC message."""
        await self.write_packet(message.to_packet())

    async def send(self, command: TrpcCommand) -> TrpcPacket:
        """Write one typed tRPC command."""
        async with self._transaction_lock:
            return await self._send_unlocked(command)

    async def request_response(
        self,
        command: TrpcCommand,
        *,
        response_match: ResponseMatch | None = None,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Send a command and wait for a matching request response."""
        return await self._send_and_wait_response(
            command,
            response_match=response_match,
            timeout=timeout,
        )

    async def update_response(
        self,
        command: TrpcCommand,
        *,
        response_match: ResponseMatch | None = None,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Send a command and wait for a matching update response."""
        return await self._send_and_wait_response(
            command,
            response_match=response_match,
            publish_match=True,
            timeout=timeout,
        )

    async def coalesced_update(
        self,
        command: TrpcCommand,
        *,
        coalesce_key: Hashable,
    ) -> TrpcPacket | None:
        """Send a coalescible Update message.

        If a newer unsent update with the same key replaces this command before it
        is written, this returns None. Commands already being written are never
        canceled.
        """
        self._raise_if_unavailable()

        future: asyncio.Future[TrpcPacket | None] = (
            asyncio.get_running_loop().create_future()
        )

        async with self._coalesced_condition:
            if (previous := self._coalesced_updates.get(coalesce_key)) and (
                not previous.future.done()
            ):
                previous.future.set_result(None)
            self._coalesced_updates[coalesce_key] = _QueuedUpdate(command, future)
            self._coalesced_condition.notify()

        try:
            return await future
        except asyncio.CancelledError:
            await self._remove_coalesced_update(coalesce_key, future)
            raise

    async def coalesced_update_response(
        self,
        command: TrpcCommand,
        *,
        coalesce_key: Hashable,
        response_match: ResponseMatch | None = None,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Send a coalescible Update and wait for a matching response/report.

        If a newer unsent update with the same key replaces this command before it
        is written, this returns None. Matched Report messages are still published
        to report subscribers so live state consumers see the same update.
        """
        self._raise_if_unavailable()

        match = response_match or ResponseMatch.for_command(command)
        future: asyncio.Future[TrpcPacket | None] = (
            asyncio.get_running_loop().create_future()
        )

        async with self._coalesced_condition:
            if (previous := self._coalesced_updates.get(coalesce_key)) and (
                not previous.future.done()
            ):
                previous.future.set_result(None)
            self._coalesced_updates[coalesce_key] = _QueuedUpdate(
                command,
                future,
                response_match=match,
                response_timeout=timeout,
            )
            self._coalesced_condition.notify()

        try:
            return await future
        except asyncio.CancelledError:
            await self._remove_coalesced_update(coalesce_key, future)
            raise

    def reports(
        self,
        *,
        max_queue_size: int = 0,
        replay_backlog: bool = False,
    ) -> ReportSubscription:
        """Subscribe to unmatched tRPC messages."""
        queue: asyncio.Queue[_ReportItem] = asyncio.Queue(
            maxsize=max_queue_size,
        )
        if self._reader_error is not None:
            self._put_report_item(queue, self._reader_error)
            return ReportSubscription(queue, lambda: None)

        if replay_backlog:
            for message in self._message_backlog:
                self._put_report(queue, message)
        self._report_queues.add(queue)
        return ReportSubscription(queue, lambda: self._unsubscribe_reports(queue))

    async def _read_loop(self) -> None:
        while not self._closed:
            try:
                packet = await self.transport.read_packet()
                if packet is None:
                    await asyncio.sleep(0.01)
                    continue
                await self._dispatch_packet(packet)
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001
                await self._set_reader_error(err)
                return

    async def _coalesced_update_loop(self) -> None:
        while True:
            async with self._coalesced_condition:
                while not self._coalesced_updates:
                    if self._closed:
                        return
                    await self._coalesced_condition.wait()
                _key, update = self._coalesced_updates.popitem(last=False)

            if update.future.cancelled():
                continue

            try:
                if update.response_match is None:
                    async with self._transaction_lock:
                        message = update.command.to_message()
                        await self.write_message(message)
                    if not update.future.done():
                        update.future.set_result(message)
                else:
                    result = await self._write_update_and_wait_response(update)
            except asyncio.CancelledError:
                if not update.future.done():
                    update.future.cancel()
                raise
            except Exception as err:  # noqa: BLE001
                if not update.future.done():
                    update.future.set_exception(err)
            else:
                if update.response_match is not None and not update.future.done():
                    update.future.set_result(result)

    async def _dispatch_packet(self, packet: Packet) -> None:
        async with self._condition:
            self._packet_backlog.append(packet)
            self._trim_packet_backlog()
            self._condition.notify_all()

        if packet.type == TYPE_TRPC:
            await self._dispatch_message(TrpcPacket.from_packet(packet))

    async def _dispatch_message(self, message: TrpcPacket) -> None:
        publish = False
        publish_matched = False
        async with self._condition:
            for pending in tuple(self._pending):
                if pending.future.done():
                    self._pending.remove(pending)
                    continue
                if pending.match.matches(message):
                    pending.future.set_result(message)
                    self._pending.remove(pending)
                    self._condition.notify_all()
                    if pending.publish_match:
                        publish_matched = True
                        break
                    return

            if publish_matched:
                self._condition.notify_all()
            else:
                self._message_backlog.append(message)
                self._trim_message_backlog()
                self._condition.notify_all()
                publish = True

        if publish or publish_matched:
            self._publish_report(message)

    async def _send_and_wait_response(
        self,
        command: TrpcCommand,
        *,
        response_match: ResponseMatch | None = None,
        publish_match: bool = False,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        match = response_match or ResponseMatch.for_command(command)
        return await self._write_and_wait_response(
            command=command,
            match=match,
            publish_match=publish_match,
            timeout=timeout,
        )

    async def _write_update_and_wait_response(
        self,
        update: _QueuedUpdate,
    ) -> TrpcPacket | None:
        if update.response_match is None:
            msg = "queued update does not have a response match"
            raise RuntimeError(msg)

        return await self._write_and_wait_response(
            command=update.command,
            match=update.response_match,
            publish_match=True,
            timeout=update.response_timeout,
        )

    async def _write_and_wait_response(
        self,
        *,
        command: TrpcCommand,
        match: ResponseMatch,
        publish_match: bool,
        timeout: float | None,
    ) -> TrpcPacket | None:

        async with self._transaction_lock:
            future: asyncio.Future[TrpcPacket] = (
                asyncio.get_running_loop().create_future()
            )
            pending = _PendingRequest(match, future, publish_match=publish_match)
            async with self._condition:
                self._pending.append(pending)

            try:
                await self._send_unlocked(command)
                if timeout is None:
                    return await future
                return await asyncio.wait_for(future, timeout)
            except TimeoutError:
                return None
            finally:
                await self._remove_pending(pending)

    async def _send_unlocked(
        self,
        command: TrpcCommand,
    ) -> TrpcPacket:
        self._raise_if_unavailable()
        message = command.to_message()
        await self.write_message(message)
        return message

    async def _remove_pending(self, pending: _PendingRequest) -> None:
        async with self._condition:
            with suppress(ValueError):
                self._pending.remove(pending)

    async def _set_reader_error(self, err: BaseException) -> None:
        self._closed = True
        self._reader_error = err
        await self._fail_pending(err)
        await self._fail_coalesced_updates(err)
        for queue in tuple(self._report_queues):
            self._report_queues.discard(queue)
            self._put_report_item(queue, err)
        async with self._condition:
            self._condition.notify_all()

    def _raise_if_unavailable(self) -> None:
        if self._reader_error is not None:
            raise self._reader_error
        if self._closed:
            raise ConnectionClosedError(_SESSION_NOT_OPEN)

    async def _fail_pending(self, err: BaseException) -> None:
        async with self._condition:
            for pending in self._pending:
                if not pending.future.done():
                    pending.future.set_exception(err)
            self._pending.clear()
            self._condition.notify_all()

    async def _remove_coalesced_update(
        self,
        coalesce_key: Hashable,
        future: asyncio.Future[TrpcPacket | None],
    ) -> None:
        async with self._coalesced_condition:
            update = self._coalesced_updates.get(coalesce_key)
            if update is not None and update.future is future:
                del self._coalesced_updates[coalesce_key]
                self._coalesced_condition.notify_all()

    async def _fail_coalesced_updates(self, err: BaseException) -> None:
        async with self._coalesced_condition:
            for update in self._coalesced_updates.values():
                if not update.future.done():
                    update.future.set_exception(err)
            self._coalesced_updates.clear()
            self._coalesced_condition.notify_all()

    def _pop_message_backlog(
        self,
        predicate: MessagePredicate,
    ) -> TrpcPacket | None:
        for index, message in enumerate(self._message_backlog):
            if predicate(message):
                return self._message_backlog.pop(index)
        return None

    def _trim_message_backlog(self) -> None:
        if self.max_message_backlog <= 0:
            self._message_backlog.clear()
            return
        del self._message_backlog[: -self.max_message_backlog]

    def _trim_packet_backlog(self) -> None:
        if self.max_packet_backlog <= 0:
            self._packet_backlog.clear()
            return
        del self._packet_backlog[: -self.max_packet_backlog]

    def _publish_report(self, message: TrpcPacket) -> None:
        for queue in tuple(self._report_queues):
            self._put_report(queue, message)

    def _put_report(
        self,
        queue: asyncio.Queue[_ReportItem],
        message: TrpcPacket,
    ) -> None:
        self._put_report_item(queue, message)

    def _put_report_item(
        self,
        queue: asyncio.Queue[_ReportItem],
        item: _ReportItem,
    ) -> None:
        try:
            queue.put_nowait(item)
        except asyncio.QueueFull:
            with suppress(asyncio.QueueEmpty):
                queue.get_nowait()
            with suppress(asyncio.QueueFull):
                queue.put_nowait(item)

    def _unsubscribe_reports(
        self,
        queue: asyncio.Queue[_ReportItem],
    ) -> None:
        self._report_queues.discard(queue)
        self._put_report_item(queue, None)
