"""Async transports for tekmar packets."""

from __future__ import annotations

import asyncio
from collections import deque
from importlib import import_module
from typing import TYPE_CHECKING, Protocol, Self, runtime_checkable

from .exceptions import ConnectionClosedError, ProtocolError, TransportError
from .tpck import TpckParser, serialize

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

    from .packet import Packet

_SERIAL_TRANSPORT_CLOSED = "serial transport closed"
_TCP_TRANSPORT_CLOSED = "TCP transport closed"
_NULL_TRANSPORT_CLOSED = "null transport closed"


class PacketTransport(Protocol):
    """Transport interface used by Tekmar482Client."""

    @property
    def is_open(self) -> bool:
        """Return whether the transport is open."""

    async def open(self) -> None:
        """Open the transport."""

    async def close(self) -> None:
        """Close the transport."""

    async def read_packet(self, timeout: float | None = None) -> Packet | None:
        """Read one packet, returning None on timeout."""

    async def write_packet(self, packet: Packet) -> None:
        """Write one packet."""


@runtime_checkable
class _SerialxModule(Protocol):
    def create_serial_connection(
        self,
        loop: asyncio.AbstractEventLoop,
        protocol_factory: Callable[[], asyncio.Protocol],
        url: str,
        baudrate: int,
        **serial_options: object,
    ) -> Awaitable[tuple[asyncio.BaseTransport, asyncio.Protocol]]:
        """Create an asyncio serial connection for a serialx URL."""


@runtime_checkable
class _WritableTransport(Protocol):
    def write(self, data: bytes | bytearray | memoryview) -> None:
        """Write bytes to the transport."""

    def close(self) -> None:
        """Close the transport."""

    def is_closing(self) -> bool:
        """Return whether the transport is closing."""


class _TpckProtocol(asyncio.Protocol):
    """Asyncio protocol that frames raw bytes into tekmar packets."""

    def __init__(self, *, closed_message: str) -> None:
        self._closed_message = closed_message
        self._parser = TpckParser()
        self._packets: deque[Packet] = deque()
        self._packet_event = asyncio.Event()
        self._transport: _WritableTransport | None = None
        self._closed_waiter: asyncio.Future[None] | None = None
        self._drain_waiter: asyncio.Future[None] | None = None
        self._paused = False
        self._error: BaseException | None = None

    @property
    def is_open(self) -> bool:
        return (
            self._transport is not None
            and not self._transport.is_closing()
            and self._error is None
        )

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        if not isinstance(transport, _WritableTransport):
            self._error = TransportError("asyncio transport is not writable")
            transport.close()
            return
        self._transport = transport
        self._closed_waiter = asyncio.get_running_loop().create_future()
        self._error = None

    def data_received(self, data: bytes) -> None:
        try:
            packets = self._parser.feed_many(data)
        except ProtocolError as err:
            self._error = err
            if self._transport is not None:
                self._transport.close()
            self._packet_event.set()
            return

        if packets:
            self._packets.extend(packets)
            self._packet_event.set()

    def eof_received(self) -> bool | None:
        return None

    def connection_lost(self, exc: Exception | None) -> None:
        self._transport = None
        self._error = exc or ConnectionClosedError(self._closed_message)
        self._packet_event.set()
        self._wake_drain_waiter(self._error)
        if self._closed_waiter is not None and not self._closed_waiter.done():
            self._closed_waiter.set_result(None)

    def pause_writing(self) -> None:
        self._paused = True

    def resume_writing(self) -> None:
        self._paused = False
        self._wake_drain_waiter(None)

    async def close(self) -> None:
        transport = self._transport
        if transport is not None and not transport.is_closing():
            transport.close()
        waiter = self._closed_waiter
        if waiter is not None and not waiter.done():
            await asyncio.shield(waiter)
        self._packets.clear()
        self._parser.reset()

    async def read_packet(self, timeout: float | None = None) -> Packet | None:
        async def read_next() -> Packet:
            while True:
                if self._packets:
                    return self._packets.popleft()
                if self._error is not None:
                    raise self._error
                if not self.is_open:
                    raise ConnectionClosedError(self._closed_message)

                self._packet_event.clear()
                if self._packets:
                    continue
                if self._error is not None:
                    raise self._error
                await self._packet_event.wait()

        try:
            if timeout is None:
                return await read_next()
            return await asyncio.wait_for(read_next(), timeout)
        except TimeoutError:
            return None

    async def write_packet(self, packet: Packet) -> None:
        transport = self._require_transport()
        transport.write(serialize(packet))
        await self._drain()
        if self._error is not None:
            raise self._error

    async def _drain(self) -> None:
        while self._paused:
            waiter = self._drain_waiter
            if waiter is None or waiter.done():
                waiter = asyncio.get_running_loop().create_future()
                self._drain_waiter = waiter
            await waiter

    def _require_transport(self) -> _WritableTransport:
        if self._error is not None:
            raise self._error
        if self._transport is None or self._transport.is_closing():
            raise ConnectionClosedError(self._closed_message)
        return self._transport

    def _wake_drain_waiter(self, err: BaseException | None) -> None:
        waiter = self._drain_waiter
        self._drain_waiter = None
        if waiter is None or waiter.done():
            return
        if err is None:
            waiter.set_result(None)
        else:
            waiter.set_exception(err)


class SerialxTpckTransport:
    """TPCK transport over a serialx URL.

    The URL may be a local serial path, `socket://host:port`, `rfc2217://host:port`,
    or any other URL supported by serialx.
    """

    def __init__(
        self,
        url: str,
        *,
        baudrate: int = 9600,
        serial_options: Mapping[str, object] | None = None,
    ) -> None:
        self.url = url
        self.baudrate = baudrate
        self.serial_options = dict(serial_options or {})
        self._protocol: _TpckProtocol | None = None

    @property
    def is_open(self) -> bool:
        return self._protocol is not None and self._protocol.is_open

    async def open(self) -> None:
        if self.is_open:
            return

        try:
            serialx = import_module("serialx")
        except ImportError as err:
            msg = (
                "serial support requires the optional serial extra; install "
                "with `tekmar-482[serial]`"
            )
            raise TransportError(msg) from err

        if not isinstance(serialx, _SerialxModule):
            msg = "serialx module does not expose create_serial_connection"
            raise TransportError(msg)

        protocol = _TpckProtocol(closed_message=_SERIAL_TRANSPORT_CLOSED)
        await serialx.create_serial_connection(
            asyncio.get_running_loop(),
            lambda: protocol,
            self.url,
            self.baudrate,
            **self.serial_options,
        )
        self._protocol = protocol

    async def close(self) -> None:
        protocol = self._protocol
        self._protocol = None
        if protocol is not None:
            await protocol.close()

    async def read_packet(self, timeout: float | None = None) -> Packet | None:
        return await self._require_protocol().read_packet(timeout)

    async def write_packet(self, packet: Packet) -> None:
        await self._require_protocol().write_packet(packet)

    def _require_protocol(self) -> _TpckProtocol:
        if self._protocol is None:
            msg = "serial transport is not open"
            raise TransportError(msg)
        return self._protocol


class RawTcpTpckTransport:
    """TPCK transport over a raw TCP stream."""

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self._protocol: _TpckProtocol | None = None

    @property
    def is_open(self) -> bool:
        return self._protocol is not None and self._protocol.is_open

    async def open(self) -> None:
        if self.is_open:
            return
        protocol = _TpckProtocol(closed_message=_TCP_TRANSPORT_CLOSED)
        await asyncio.get_running_loop().create_connection(
            lambda: protocol,
            self.host,
            self.port,
        )
        self._protocol = protocol

    async def close(self) -> None:
        protocol = self._protocol
        self._protocol = None
        if protocol is not None:
            await protocol.close()

    async def read_packet(self, timeout: float | None = None) -> Packet | None:
        return await self._require_protocol().read_packet(timeout)

    async def write_packet(self, packet: Packet) -> None:
        await self._require_protocol().write_packet(packet)

    def _require_protocol(self) -> _TpckProtocol:
        if self._protocol is None:
            msg = "TCP transport is not open"
            raise TransportError(msg)
        return self._protocol


class NullTransport:
    """In-memory transport useful for tests and examples."""

    def __init__(self) -> None:
        self.packets: deque[Packet] = deque()
        self.written: list[Packet] = []
        self._packet_event = asyncio.Event()
        self._is_open = False

    @property
    def is_open(self) -> bool:
        return self._is_open

    async def open(self) -> None:
        self._is_open = True

    async def close(self) -> None:
        self._is_open = False
        self._packet_event.set()

    async def read_packet(self, timeout: float | None = None) -> Packet | None:
        async def read_next() -> Packet:
            while True:
                if self.packets:
                    return self.packets.popleft()
                if not self._is_open:
                    raise ConnectionClosedError(_NULL_TRANSPORT_CLOSED)
                self._packet_event.clear()
                if self.packets:
                    continue
                await self._packet_event.wait()

        try:
            if timeout is None:
                return await read_next()
            return await asyncio.wait_for(read_next(), timeout)
        except TimeoutError:
            return None

    async def write_packet(self, packet: Packet) -> None:
        self.written.append(packet)

    def queue_packet(self, packet: Packet) -> Self:
        self.packets.append(packet)
        self._packet_event.set()
        return self
