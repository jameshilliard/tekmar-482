"""Low-level async tRPC session dispatcher."""

from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING, Self

from .constants import ThaSetback, ThaValue
from .exceptions import ConnectionClosedError, InventoryDrainTimeoutError, ProtocolError
from .packet import TYPE_TRPC
from .trpc import (
    AddressFields,
    ReplySelector,
    ResponseMatch,
    TrpcCommand,
    TrpcMethod,
    TrpcPacket,
    TrpcService,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Hashable, Iterator
    from types import TracebackType

    from .normal_trace import NormalTrace
    from .packet import Packet
    from .transport import PacketTransport

type MessagePredicate = Callable[[TrpcPacket], bool]
type _ReportUnsubscribe = Callable[[], None]
type ReportEvent = tuple[TrpcPacket, TrpcCommand | None]
type _ReportItem = ReportEvent | BaseException | None

_SESSION_NOT_OPEN = "tekmar 482 session is not open"
_SESSION_CLOSED = "tekmar 482 session closed"
_INVENTORY_DRAIN_TIMEOUT = 5

LOGGER = logging.getLogger(__name__)


class _DetachedEcho(Enum):
    ABANDONED = auto()
    UNATTRIBUTED = auto()


@dataclass(slots=True)
class _UnconfirmedEcho:
    count: int = 0
    command: TrpcCommand | None = None
    disowned: bool = False


@dataclass(slots=True)
class _PendingRequest:
    command: TrpcCommand
    match: ResponseMatch
    future: asyncio.Future[TrpcPacket]
    publish_match: bool = False
    complete: MessagePredicate | None = None


@dataclass(slots=True)
class _QueuedUpdate:
    generation: int
    command: TrpcCommand
    future: asyncio.Future[TrpcPacket | None]
    response_match: ResponseMatch | None = None
    timeout: float | None = None
    deadline: float | None = None
    turn: object | None = None


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
        event = await self.next_event()
        if event is None:
            raise StopAsyncIteration
        return event[0]

    async def next_event(self) -> ReportEvent | None:
        """Read a packet and its matched command, or None at end of stream."""
        item = await self._queue.get()
        if isinstance(item, tuple):
            return item
        # Leave EOF for later calls and wake any other waiting consumer.
        self._queue.put_nowait(None)
        if item is not None:
            raise item
        return None

    def close(self) -> None:
        """Unsubscribe and wake any pending iterator."""
        if self._closed:
            return
        self._closed = True
        self._unsubscribe()

    def drain(self) -> Iterator[TrpcPacket]:
        """Consume already queued messages, leaving a terminal signal in place."""
        return (message for message, _ in self.drain_events())

    def drain_events(self) -> Iterator[ReportEvent]:
        """Drain queued messages with their original command context."""
        while not self._queue.empty():
            item = self._queue.get_nowait()
            if not isinstance(item, tuple):
                self._queue.put_nowait(item)
                break
            yield item


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
        readback_methods: frozenset[int] = frozenset(),
    ) -> None:
        self.transport = transport
        self.readback_methods = readback_methods
        self.max_message_backlog = max_message_backlog
        self.max_packet_backlog = max_packet_backlog
        self._condition = asyncio.Condition()
        self._lifecycle_lock = asyncio.Lock()
        self._generation = 0
        self._operation_generation = ContextVar[int | None](
            "tekmar_connection",
            default=None,
        )
        self._transaction_lock = asyncio.Lock()
        self._inventory_drained = asyncio.Event()
        self._inventory_address = 0
        self._inventory_drained.set()
        self._pending: list[_PendingRequest] = []
        self._write_order: dict[object, ReplySelector] = {}
        self._writes_changed = asyncio.Event()
        self._setbacks_enabled = False
        self._unconfirmed_echoes: dict[ReplySelector, _UnconfirmedEcho] = {}
        self._tracked_replies: dict[
            asyncio.Future[TrpcPacket],
            ReplySelector,
        ] = {}
        self._message_backlog: list[TrpcPacket] = []
        self._packet_backlog: list[Packet] = []
        self._report_queues: dict[
            asyncio.Queue[_ReportItem],
            frozenset[TrpcMethod],
        ] = {}
        self._observers: set[Callable[[TrpcPacket, TrpcCommand | None], None]] = set()
        self._trace: NormalTrace | None = None
        self._coalesced_updates: OrderedDict[Hashable, _QueuedUpdate] = OrderedDict()
        self._writer_wake = asyncio.Event()
        self._coalesced_task: asyncio.Task[None] | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._reader_error: BaseException | None = None
        self._closed = True

    @property
    def is_open(self) -> bool:
        """Return whether the underlying transport is open."""
        return self.transport.is_open

    @property
    def connection_generation(self) -> int:
        """Identify the connection on which a multi-step operation started."""
        return self._generation

    @contextmanager
    def connection_scope(self) -> Iterator[None]:
        """Keep nested exchanges on the connection that started the operation."""
        generation = self._generation
        self._check_generation(generation)
        token = self._operation_generation.set(generation)
        try:
            yield
            self._check_generation(generation)
            self._raise_if_unavailable()
        finally:
            self._operation_generation.reset(token)

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
        async with self._lifecycle_lock:
            if not self._closed and self.transport.is_open:
                return
            if self._reader_task is not None:
                await self._close()
            await self._open()

    async def _open(self) -> None:
        if not self.transport.is_open:
            await self.transport.open()

        self._generation += 1
        self._setbacks_enabled = False
        self._packet_backlog.clear()
        self._message_backlog.clear()
        self._closed = False
        self._reader_error = None
        self._reader_task = asyncio.create_task(
            self._read_loop(),
            name="tekmar 482 session reader",
        )
        self._coalesced_task = asyncio.create_task(
            self._coalesced_update_loop(),
            name="tekmar 482 coalesced update writer",
        )

    async def close(self) -> None:
        """Cancel the reader, fail pending calls, and close the transport."""
        async with self._lifecycle_lock:
            await self._close()

    async def _close(self) -> None:
        self._closed = True
        if self._trace is not None:
            self._trace.finish()
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
        self._fail_coalesced_updates(
            ConnectionClosedError(_SESSION_CLOSED),
        )
        async with self._condition:
            self._condition.notify_all()

        for queue in tuple(self._report_queues):
            self._unsubscribe_reports(queue)

        await self.transport.close()

    async def read_packet(self, timeout: float | None = None) -> Packet | None:
        """Read the next observed packet, returning None on timeout."""
        generation = self._generation

        async def wait_next() -> Packet | None:
            async with self._condition:
                while True:
                    self._check_generation(generation)
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
        """Write original bytes, tracking decodable tRPC commands like send()."""
        self._raise_if_unavailable()
        if packet.type == TYPE_TRPC:
            try:
                message = TrpcPacket.from_packet(packet)
            except ProtocolError:
                pass
            else:
                await self._write_unconfirmed(message, packet)
                return
        await self._write_transport(packet)

    async def _write_transport(self, packet: Packet) -> None:
        if self._trace is not None:
            self._trace.record(packet, outbound=True)
        await self.transport.write_packet(packet)

    async def read_message(self, timeout: float | None = None) -> TrpcPacket | None:
        """Read the next unmatched tRPC message, returning None on timeout."""
        generation = self._generation

        async def wait_next() -> TrpcPacket | None:
            async with self._condition:
                while True:
                    self._check_generation(generation)
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
        generation = self._generation

        async def wait_match() -> TrpcPacket | None:
            async with self._condition:
                while True:
                    self._check_generation(generation)
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

    async def _write_unconfirmed(self, message: TrpcPacket, packet: Packet) -> None:
        generation = self._generation
        selector = self._tracked_selector(message)
        with self._write_turn(selector) as turn:
            while True:
                async with self._transaction_lock:
                    self._check_generation(generation)
                    self._raise_if_unavailable()
                    if not self._write_blocked(turn):
                        await self._write_message_unlocked(message, selector, packet)
                        return
                    changed = self._writes_changed
                await changed.wait()

    async def _write_message_unlocked(
        self,
        message: TrpcPacket,
        unconfirmed: ReplySelector | None,
        packet: Packet,
    ) -> None:
        self._raise_if_unavailable()
        if (
            message.method is TrpcMethod.SETBACK_ENABLE
            and message.service is TrpcService.UPDATE
        ):
            # Do not assume slot independence while a setting change is unconfirmed.
            self._observe_setbacks(enabled=False)
        if (
            message.service == TrpcService.REQUEST
            and message.method == TrpcMethod.DEVICE_INVENTORY
        ):
            if not self._inventory_drained.is_set():
                msg = "Previous inventory has not ended; wait or reconnect"
                raise ProtocolError(msg)
            self._inventory_address = message.body.get("address", 0)
            self._inventory_drained.clear()
        if unconfirmed is not None:
            # Register before transport I/O: the reply can arrive during drain.
            self._track_unconfirmed(unconfirmed, message)
        LOGGER.debug("Sending tekmar 482 tRPC %s", message)
        await self._write_transport(packet)

    async def wait_for_inventory_drain(self, *, timeout: float | None) -> None:
        """Wait for an earlier walk, distinguishing a lost terminator from busy I/O."""
        if self._inventory_drained.is_set():
            return
        generation = self._generation
        try:
            async with asyncio.timeout(
                timeout if timeout is not None else _INVENTORY_DRAIN_TIMEOUT,
            ):
                await self._inventory_drained.wait()
        except TimeoutError as err:
            self._check_generation(generation)
            if self._inventory_drained.is_set() or any(
                p.command.method == TrpcMethod.DEVICE_INVENTORY and not p.future.done()
                for p in self._pending
            ):
                raise
            raise InventoryDrainTimeoutError from err

    async def request_inventory(
        self,
        start_address: int,
        *,
        timeout: float | None,
    ) -> tuple[int, ...]:
        """Walk the inventory at zero, or look up one nonzero address."""
        command = TrpcCommand.request(
            TrpcMethod.DEVICE_INVENTORY,
            AddressFields(start_address),
        )
        addresses: dict[int, None] = {}

        def complete(message: TrpcPacket) -> bool:
            address = message.require("address")
            if address == ThaValue.NA_16 or (address == 0 and not start_address):
                return True
            if start_address and start_address != address:
                return False
            addresses[address] = None
            return bool(start_address)

        # No transaction ID is available. After timeout/cancellation, leave the
        # old walk draining before registering another response matcher.
        self._raise_if_unavailable()
        generation = self._generation
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        try:
            while True:
                await self.wait_for_inventory_drain(
                    timeout=None if deadline is None else deadline - loop.time(),
                )
                async with asyncio.timeout_at(deadline):
                    async with self._transaction_lock:
                        self._check_generation(generation)
                        if not self._inventory_drained.is_set():
                            continue
                        await self._exchange_unlocked(
                            command,
                            ResponseMatch.for_command(command),
                            generation=generation,
                            publish_match=False,
                            timeout=None,
                            complete=complete,
                        )
                        return tuple(addresses)
        except InventoryDrainTimeoutError:
            raise
        except TimeoutError as err:
            msg = "Timed out waiting for a complete device inventory"
            raise TimeoutError(msg) from err

    async def send(self, command: TrpcCommand) -> TrpcPacket:
        """Write one typed tRPC command."""
        message = command.to_message()
        await self.write_message(message)
        return message

    @contextmanager
    def _write_turn(
        self,
        target: ReplySelector | None,
    ) -> Iterator[object | None]:
        if target is None or target.service is not TrpcService.RESPONSE_UPDATE:
            yield None
            return
        turn = object()
        self._write_order[turn] = target
        try:
            yield turn
        finally:
            del self._write_order[turn]
            self._wake_writers()

    def _write_blocked(self, turn: object | None) -> bool:
        if turn is not None:
            target = self._write_order[turn]
            for key, older in self._write_order.items():
                if key is turn:
                    break
                if older.shares_write_target(
                    target,
                    setbacks_enabled=self._setbacks_enabled,
                ):
                    return True
        return False

    def _wake_writers(self) -> None:
        self._writes_changed.set()
        self._writes_changed = asyncio.Event()
        self._writer_wake.set()

    def _observe_setbacks(self, *, enabled: bool) -> None:
        if self._setbacks_enabled != enabled:
            self._setbacks_enabled = enabled
            self._wake_writers()

    def discard_device_updates(self, addresses: set[int]) -> None:
        """Fail unsent device updates when their wire addresses change ownership."""
        for selector, entry in self._unconfirmed_echoes.items():
            if selector.address in addresses:
                entry.disowned = True
        for key, update in tuple(self._coalesced_updates.items()):
            if update.command.fields.get("address") in addresses:
                del self._coalesced_updates[key]
                if not update.future.done():
                    update.future.set_exception(
                        ConnectionClosedError("Device address changed before write"),
                    )
        self._writer_wake.set()

    async def request_response(
        self,
        command: TrpcCommand,
        *,
        response_match: ResponseMatch | None = None,
        publish_match: bool = False,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        """Send a command and wait for a matching request response."""
        return await self._send_and_wait_response(
            command,
            response_match=response_match,
            publish_match=publish_match,
            timeout=timeout,
        )

    async def update_response(
        self,
        command: TrpcCommand,
        *,
        response_match: ResponseMatch | None = None,
        timeout: float | None = 5,
        on_sending: Callable[[], None] | None = None,
    ) -> TrpcPacket | None:
        """Wait for an update response; notify on_sending just before transport I/O."""
        return await self._send_and_wait_response(
            command,
            response_match=response_match,
            publish_match=True,
            timeout=timeout,
            on_sending=on_sending,
        )

    async def coalesced_update(
        self,
        command: TrpcCommand,
        *,
        coalesce_key: Hashable,
        timeout: float | None = None,
    ) -> TrpcPacket | None:
        """Send a coalescible Update message.

        If a newer unsent update with the same key replaces this command before it
        is written, this returns None. Commands already being written are never
        superseded. The timeout starts when the command takes the wire.
        """
        return await self._queue_update(command, coalesce_key, None, timeout)

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
        is written, this returns None. An unacknowledged write raises TimeoutError.
        Matched Report messages are still published
        to report subscribers so live state consumers see the same update.
        """
        match = response_match or ResponseMatch.for_command(
            command,
            readback=command.method in self.readback_methods,
        )
        return await self._queue_update(command, coalesce_key, match, timeout)

    async def _queue_update(
        self,
        command: TrpcCommand,
        coalesce_key: Hashable,
        match: ResponseMatch | None,
        timeout: float | None,
    ) -> TrpcPacket | None:
        generation = self._generation
        future: asyncio.Future[TrpcPacket | None] = (
            asyncio.get_running_loop().create_future()
        )

        self._check_generation(generation)
        self._raise_if_unavailable()
        with self._write_turn(
            self._tracked_selector(command),
        ) as turn:
            if (
                previous := self._coalesced_updates.pop(coalesce_key, None)
            ) and not previous.future.done():
                previous.future.set_result(None)
            update = self._coalesced_updates[coalesce_key] = _QueuedUpdate(
                generation,
                command,
                future,
                response_match=match,
                timeout=timeout,
                turn=turn,
            )
            self._writer_wake.set()
            try:
                return await future
            except asyncio.CancelledError:
                if self._coalesced_updates.get(coalesce_key) is update:
                    del self._coalesced_updates[coalesce_key]
                    self._writer_wake.set()
                raise

    def reports(
        self,
        *,
        max_queue_size: int = 0,
        replay_backlog: bool = False,
        include_matched: frozenset[TrpcMethod] = frozenset(),
    ) -> ReportSubscription:
        """Subscribe to public messages and selected methods' matched replies."""
        queue: asyncio.Queue[_ReportItem] = asyncio.Queue(
            maxsize=max_queue_size,
        )
        if self._reader_error is not None:
            self._put_report_item(queue, self._reader_error)
            return ReportSubscription(queue, lambda: None)

        if replay_backlog:
            for message in self._message_backlog:
                self._put_report_item(queue, (message, None))
        self._report_queues[queue] = include_matched
        return ReportSubscription(queue, lambda: self._unsubscribe_reports(queue))

    @contextmanager
    def observe_messages(
        self,
        callback: Callable[[TrpcPacket, TrpcCommand | None], None],
    ) -> Iterator[None]:
        """Observe frames in receive order, with the matched command if any."""
        self._observers.add(callback)
        try:
            yield
        finally:
            self._observers.discard(callback)

    @contextmanager
    def trace_packets(self, trace: NormalTrace) -> Iterator[None]:
        """Attach one diagnostic observer without consuming application replies."""
        self._raise_if_unavailable()
        if self._trace is not None:
            msg = "a Normal trace already owns this session"
            raise ProtocolError(msg)
        self._trace = trace
        try:
            yield
        finally:
            self._trace = None
            trace.finish()

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
                LOGGER.debug("tekmar 482 session reader stopped: %s", err)
                await self._set_reader_error(err)
                return

    async def _coalesced_update_loop(self) -> None:
        while not self._closed:
            await self._writer_wake.wait()
            async with self._transaction_lock:
                self._writer_wake.clear()
                if update := self._next_writable_update():
                    await self._write_queued_update(update)
                    self._writer_wake.set()
                    continue
            deadlines = [
                u.deadline
                for u in self._coalesced_updates.values()
                if u.deadline is not None
            ]
            if deadlines:
                with suppress(TimeoutError):
                    async with asyncio.timeout_at(min(deadlines)):
                        await self._writer_wake.wait()
                self._writer_wake.set()

    def _next_writable_update(self) -> _QueuedUpdate | None:
        now = asyncio.get_running_loop().time()
        for key, update in list(self._coalesced_updates.items()):
            if update.future.done():
                del self._coalesced_updates[key]
                continue
            if update.deadline is not None and now >= update.deadline:
                del self._coalesced_updates[key]
                update.future.set_exception(
                    TimeoutError("Timed out waiting to send device update"),
                )
                continue
            if update.timeout is not None and update.deadline is None:
                update.deadline = now + update.timeout
            if self._write_blocked(update.turn) or (
                update.response_match is not None
                and self._waiting_for_echo(
                    update.response_match,
                )
            ):
                continue
            return self._coalesced_updates.pop(key)
        return None

    async def _write_queued_update(self, update: _QueuedUpdate) -> None:
        try:
            if update.response_match is None:
                async with asyncio.timeout(update.timeout):
                    result = await self._send_unlocked(
                        update.command,
                        update.generation,
                        unconfirmed=True,
                    )
            else:
                result = await self._exchange_unlocked(
                    update.command,
                    update.response_match,
                    generation=update.generation,
                    publish_match=True,
                    timeout=None
                    if update.deadline is None
                    else update.deadline - asyncio.get_running_loop().time(),
                )
        except asyncio.CancelledError:
            if not update.future.done():
                update.future.set_exception(ConnectionClosedError(_SESSION_CLOSED))
            raise
        except Exception as err:  # noqa: BLE001
            if not update.future.done():
                update.future.set_exception(err)
        else:
            if not update.future.done():
                update.future.set_result(result)

    async def _dispatch_packet(self, packet: Packet) -> None:
        if self._trace is not None:
            self._trace.record(packet, outbound=False)
        async with self._condition:
            self._packet_backlog.append(packet)
            self._trim_packet_backlog()
            self._condition.notify_all()

        if packet.type == TYPE_TRPC:
            try:
                message = TrpcPacket.from_packet(packet)
            except ProtocolError as err:
                LOGGER.debug("Ignoring malformed tRPC packet: %s", err)
                return
            await self._dispatch_message(message)

    async def _dispatch_message(self, message: TrpcPacket) -> None:
        LOGGER.debug("Received tekmar 482 tRPC %s", message)
        async with self._condition:
            echo = self._take_detached_echo(message)
            if echo is not None:
                self._wake_writers()
                self._condition.notify_all()
                if echo is _DetachedEcho.ABANDONED:
                    return
            if message.method is TrpcMethod.SETBACK_ENABLE and message.service in {
                TrpcService.REPORT,
                TrpcService.RESPONSE_REQUEST,
                TrpcService.RESPONSE_UPDATE,
            }:
                self._observe_setbacks(enabled=message.get("enable") == 1)
            if (
                message.service == TrpcService.RESPONSE_REQUEST
                and message.method == TrpcMethod.DEVICE_INVENTORY
                and message.get("address") in {self._inventory_address, ThaValue.NA_16}
            ):
                self._inventory_drained.set()
            for pending in tuple(self._pending):
                if pending.future.done():
                    self._pending.remove(pending)
                    continue
                if echo is None and pending.match.matches(message):
                    if pending.complete is None or pending.complete(message):
                        pending.future.set_result(message)
                        self._pending.remove(pending)
                    command: TrpcCommand | None = pending.command
                    publish = pending.publish_match
                    break
            else:
                command = echo if isinstance(echo, TrpcCommand) else None
                self._message_backlog.append(message)
                self._trim_message_backlog()
                publish = True
            self._condition.notify_all()

        for callback in tuple(self._observers):
            try:
                callback(message, command)
            except Exception:
                LOGGER.exception("tekmar 482 message observer failed")
        self._publish_report((message, command), public=publish)

    def _track_unconfirmed(
        self,
        selector: ReplySelector,
        message: TrpcPacket,
    ) -> None:
        command = (
            TrpcCommand.update(message.method_id, message.body)
            if message.service is TrpcService.UPDATE
            else None
        )
        entry = self._unconfirmed_echoes.setdefault(
            selector,
            _UnconfirmedEcho(command=command),
        )
        if command != entry.command:
            entry.command = None
        entry.count += 1
        # A named and a CURRENT command can produce identical echoes. Once
        # ambiguous, neither becomes attributable just because one echo arrived.
        for other, outstanding in self._unconfirmed_echoes.items():
            if other != selector and other.overlaps(selector):
                entry.command = outstanding.command = None
                if outstanding.disowned:
                    entry.disowned = True
        if any(
            f.cancelled() and old != selector and old.overlaps(selector)
            for f, old in self._tracked_replies.items()
        ):
            entry.command = None

    def _take_detached_echo(
        self,
        message: TrpcPacket,
    ) -> TrpcCommand | _DetachedEcho | None:
        """Claim at most one reply from a command that no longer has a waiter."""
        # Prefer a named selector: a remaining CURRENT can still cover any
        # other delayed echo even when two unconfirmed writes look identical.
        for wildcard in (False, True):
            for future, selector in self._tracked_replies.items():
                if (
                    future.cancelled()
                    and (selector.slot == ThaSetback.CURRENT) == wildcard
                    and selector.accepts(message)
                ):
                    del self._tracked_replies[future]
                    return _DetachedEcho.ABANDONED
            for selector, entry in self._unconfirmed_echoes.items():
                if (
                    selector.slot == ThaSetback.CURRENT
                ) == wildcard and selector.accepts(message):
                    entry.count -= 1
                    if not entry.count:
                        del self._unconfirmed_echoes[selector]
                    if entry.disowned:
                        return _DetachedEcho.ABANDONED
                    return entry.command or _DetachedEcho.UNATTRIBUTED
        return None

    async def _send_and_wait_response(
        self,
        command: TrpcCommand,
        *,
        response_match: ResponseMatch | None = None,
        publish_match: bool = False,
        timeout: float | None = 5,
        on_sending: Callable[[], None] | None = None,
    ) -> TrpcPacket | None:
        match = response_match or ResponseMatch.for_command(
            command,
            readback=command.method in self.readback_methods,
        )
        generation = self._generation
        try:
            with self._write_turn(
                self._tracked_selector(command),
            ) as turn:
                return await self._ordered_exchange(
                    command,
                    match,
                    generation,
                    turn=turn,
                    publish_match=publish_match,
                    timeout=timeout,
                    on_sending=on_sending,
                )
        except TimeoutError:
            return None

    async def _ordered_exchange(
        self,
        command: TrpcCommand,
        match: ResponseMatch,
        generation: int,
        *,
        turn: object | None,
        publish_match: bool,
        timeout: float | None,
        on_sending: Callable[[], None] | None,
    ) -> TrpcPacket:
        async with asyncio.timeout(None) as budget:
            while True:
                async with self._transaction_lock:
                    self._check_generation(generation)
                    self._raise_if_unavailable()
                    if timeout is not None and budget.when() is None:
                        budget.reschedule(
                            asyncio.get_running_loop().time() + timeout,
                        )
                    if not self._write_blocked(turn) and not self._waiting_for_echo(
                        match,
                    ):
                        return await self._exchange_unlocked(
                            command,
                            match,
                            generation=generation,
                            publish_match=publish_match,
                            timeout=None,
                            on_sending=on_sending,
                        )
                    changed = self._writes_changed
                await changed.wait()

    def _waiting_for_echo(self, match: ResponseMatch) -> bool:
        return any(old.can_satisfy(match) for old in self._unconfirmed_echoes) or any(
            future.cancelled() and old.can_satisfy(match)
            for future, old in self._tracked_replies.items()
        )

    def needs_cache_sync(self, command: TrpcCommand) -> bool:
        """Whether an earlier inline cache read could answer this command."""
        return self._inline_cache_read(self._tracked_selector(command)) and (
            self._waiting_for_echo(ResponseMatch.for_command(command))
        )

    def _inline_cache_read(self, selector: ReplySelector | None) -> bool:
        return (
            selector is not None
            and selector.service is TrpcService.RESPONSE_REQUEST
            and selector.slot is None
            and selector.method not in self.readback_methods
        )

    def _retire_inline_reads(self) -> None:
        # A matched ProtocolVersion request passed the same dispatcher and
        # UART FIFO. Deferred device jobs, writes and walks are not covered.
        for future, selector in tuple(self._tracked_replies.items()):
            if future.cancelled() and self._inline_cache_read(selector):
                del self._tracked_replies[future]
        for selector in tuple(self._unconfirmed_echoes):
            if self._inline_cache_read(selector):
                del self._unconfirmed_echoes[selector]
        self._wake_writers()

    async def _exchange_unlocked(
        self,
        command: TrpcCommand,
        match: ResponseMatch,
        *,
        generation: int,
        publish_match: bool,
        timeout: float | None,
        complete: MessagePredicate | None = None,
        on_sending: Callable[[], None] | None = None,
    ) -> TrpcPacket:
        future: asyncio.Future[TrpcPacket] = asyncio.get_running_loop().create_future()
        pending = _PendingRequest(
            command,
            match,
            future,
            publish_match=publish_match,
            complete=complete,
        )
        async with self._condition:
            self._pending.append(pending)
            if (selector := self._tracked_selector(command)) is not None:
                self._tracked_replies[future] = selector

        try:
            async with asyncio.timeout(timeout):
                await self._send_unlocked(command, generation, on_sending)
                reply = await future
                if (
                    command.service == TrpcService.REQUEST
                    and command.method == TrpcMethod.PROTOCOL_VERSION
                    and reply.service is TrpcService.RESPONSE_REQUEST
                    and reply.method is TrpcMethod.PROTOCOL_VERSION
                ):
                    self._retire_inline_reads()
                return reply
        except asyncio.CancelledError:
            future.cancel()
            raise
        except TimeoutError as err:
            future.cancel()
            msg = f"Timed out exchanging {command.to_message()}"
            raise TimeoutError(msg) from err
        finally:
            if not future.cancelled():
                self._tracked_replies.pop(future, None)
            await self._remove_pending(pending)

    async def _send_unlocked(
        self,
        command: TrpcCommand,
        generation: int,
        on_sending: Callable[[], None] | None = None,
        *,
        unconfirmed: bool = False,
    ) -> TrpcPacket:
        self._check_generation(generation)
        self._raise_if_unavailable()
        message = command.to_message()
        if on_sending is not None:
            on_sending()
        await self._write_message_unlocked(
            message,
            self._tracked_selector(command) if unconfirmed else None,
            message.to_packet(),
        )
        return message

    def _tracked_selector(
        self,
        message: TrpcCommand | TrpcPacket,
    ) -> ReplySelector | None:
        selector = ReplySelector.from_fields(
            message.service_id,
            message.method_id,
            message.fields if isinstance(message, TrpcCommand) else message.body,
            readback=message.method_id in self.readback_methods,
        )
        return selector if selector is not None and selector.tracks_reply else None

    async def _remove_pending(self, pending: _PendingRequest) -> None:
        async with self._condition:
            with suppress(ValueError):
                self._pending.remove(pending)

    async def _set_reader_error(self, err: BaseException) -> None:
        self._closed = True
        if self._trace is not None:
            self._trace.finish(err)
        self._reader_error = err
        await self._fail_pending(err)
        self._fail_coalesced_updates(err)
        for queue in tuple(self._report_queues):
            self._report_queues.pop(queue, None)
            self._put_report_item(queue, err)
        async with self._condition:
            self._condition.notify_all()

    def _check_generation(self, generation: int) -> None:
        if generation != self._generation or self._operation_generation.get() not in (
            None,
            generation,
        ):
            raise ConnectionClosedError(_SESSION_CLOSED)

    def _raise_if_unavailable(self) -> None:
        self._check_generation(self._generation)
        if self._reader_error is not None:
            raise self._reader_error
        if self._closed:
            raise ConnectionClosedError(_SESSION_NOT_OPEN)

    async def _fail_pending(self, err: BaseException) -> None:
        async with self._condition:
            self._tracked_replies.clear()
            self._unconfirmed_echoes.clear()
            self._wake_writers()
            self._inventory_drained.set()
            for pending in self._pending:
                if not pending.future.done():
                    pending.future.set_exception(err)
            self._pending.clear()
            self._condition.notify_all()

    def _fail_coalesced_updates(self, err: BaseException) -> None:
        for update in self._coalesced_updates.values():
            if not update.future.done():
                update.future.set_exception(err)
        self._coalesced_updates.clear()
        self._writer_wake.set()

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

    def _publish_report(self, event: ReportEvent, *, public: bool) -> None:
        for queue, included in self._report_queues.items():
            if public or event[0].method in included:
                self._put_report_item(queue, event)

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
        self._report_queues.pop(queue, None)
        self._put_report_item(queue, None)
