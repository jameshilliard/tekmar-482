"""Async binary tRPC management and opaque type-1 tN4 records."""

from __future__ import annotations

import asyncio
import re
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ._bridge_diagnostics import CAPTURE_COUNTERS, PANEL, PORT_DROPS, WIRE_VERSION
from .client import Tekmar482Client
from .constants import TN4_PORTS
from .exceptions import (
    ConnectionClosedError,
    ProtocolError,
    StockFirmwareError,
    TekmarError,
)
from .management import (
    BridgeCapabilities,
    BridgeFeature,
    BridgeManagement,
    BridgeMethod,
    BridgeMode,
    unpack_diagnostic,
)
from .packet import TYPE_TN4, TYPE_TRPC, Packet
from .raw import PortDiagnostic, RawDispatcher
from .tpck import TpckParser, serialize
from .trpc import TrpcMethod, TrpcService
from .wire_trace import current_observer

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator

    from ._wire import Wire
    from .raw import BridgeStream

_DEFAULT_BAUD = 9600
_LITERAL_RAW_SINCE = 3
_RAW_EXTENSIONS = 1
_TICK_MASK = 0xFFFFFFFF


@dataclass(frozen=True, slots=True)
class PanelState:
    boot_inputs: int
    inputs: int
    test_presses: int
    dip_changes: int
    led_mask: int
    pin_mask: int
    testing: bool


class _NullCapabilitiesError(ProtocolError):
    """The extension request received the stock unknown-method reply."""


class BridgeClient:
    """Negotiated binary application connection. Use open_gateway for its lifetime."""

    def __init__(self, wire: Wire) -> None:
        self._wire = wire
        self._parser = TpckParser(strict_checksum=True)
        self._pending: Iterator[int] = iter(())
        self._busy = asyncio.Lock()
        self._features = BridgeFeature(0)
        self._observer = current_observer()

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[None]:
        if self._busy.locked() or self._wire.writer.is_closing():
            msg = "bridge is busy or closed"
            raise ProtocolError(msg)
        async with self._busy:
            try:
                yield
            except BaseException:
                self._wire.writer.close()
                raise

    async def _next_packet(self) -> Packet:
        while True:
            # A mode acknowledgement and the first new-dialect frame can share
            # a single read. Leave the unread bytes for the next packet.
            for byte in self._pending:
                if (packet := self._parser.feed(byte)) is not None:
                    self._observe(packet, outbound=False)
                    return packet
            chunk = await self._wire.reader.read(256)
            if not chunk:
                msg = "gateway disconnected"
                raise ConnectionClosedError(msg)
            self._pending = iter(chunk)

    def _observe(self, packet: Packet, *, outbound: bool) -> None:
        if self._observer is not None:
            self._observer(packet, outbound, self._parser.literal_header)

    async def _write_packet(self, packet: Packet) -> None:
        self._observe(packet, outbound=True)
        await self._wire.write(
            serialize(packet, literal_header=self._parser.literal_header),
        )

    async def _write_raw(self, body: bytes) -> None:
        await self._write_packet(Packet(TYPE_TN4, body))

    async def _call(
        self,
        method: BridgeMethod | TrpcMethod,
        body: bytes = b"",
        *,
        update: bool = False,
        consume: Callable[[Packet], object] | None = None,
    ) -> bytes:
        service = TrpcService.UPDATE if update else TrpcService.REQUEST
        response = (
            TrpcService.RESPONSE_UPDATE if update else TrpcService.RESPONSE_REQUEST
        )
        key = method.to_bytes(4, "little")
        request = Packet(TYPE_TRPC, bytes([service]) + key + body)
        async with asyncio.timeout(5):
            await self._write_packet(request)
            while True:
                packet = await self._next_packet()
                if packet.type != TYPE_TRPC:
                    if consume and packet.type == TYPE_TN4:
                        consume(packet)
                    continue
                header, payload = packet.data[:5], packet.data[5:]
                if header == bytes([response]) + key:
                    return payload
                if header == bytes([response]) + bytes(4):
                    msg = "gateway does not implement this extension"
                    if method is BridgeMethod.CAPABILITIES and not payload:
                        raise _NullCapabilitiesError(msg)
                    raise ProtocolError(msg)
                if (
                    header
                    == bytes([response]) + BridgeMethod.ERROR.to_bytes(4, "little")
                    and payload[:4] == key
                ):
                    if payload[4:] == b"\x01" and method in {
                        BridgeMethod.MODE,
                        BridgeMethod.ENTER_UPDATE,
                    }:
                        # Busy explicitly rejects the transition before it takes effect.
                        await asyncio.sleep(0.025)
                        await self._write_packet(request)
                        continue
                    msg = f"bridge management rejection: {payload[4:].hex()}"
                    raise ProtocolError(msg)

    async def identify(self, *, for_update: bool = False) -> None:
        """Identify this protocol; the installer can also use older ROM handoffs."""
        async with self._transaction():
            if self._wire.baudrate != _DEFAULT_BAUD:
                burst = b"\xca" * max(1, self._wire.baudrate // 1250)
                for preamble in (burst, burst, b"\xca"):
                    await self._wire.write(preamble)
                    await asyncio.sleep(0.08)
            # A reconnect may start in Raw mode. Discard pre-negotiation
            # checksum failures; the capabilities reply has the same framing.
            self._parser.strict_checksum = False
            try:
                data = await self._call(BridgeMethod.CAPABILITIES)
            except _NullCapabilitiesError as err:
                version = await self._call(TrpcMethod.PROTOCOL_VERSION, bytes(2))
                if version in (b"\x01\0", b"\x02\0", b"\x03\0"):
                    msg = "stock tHA firmware has no command to enter the bootloader"
                    raise StockFirmwareError(msg) from err
                msg = "unrecognized tHA protocol version after NullMethod"
                raise ProtocolError(msg) from err
            finally:
                self._parser.strict_checksum = True
            caps = BridgeCapabilities.from_payload(data, for_update=for_update)
            current = caps.wire_version == WIRE_VERSION
            self._features = (
                caps.features if current else caps.features & BridgeFeature.ROM_UPDATE
            )
            self._parser.literal_header = (
                caps.mode is BridgeMode.RAW and caps.wire_version >= _LITERAL_RAW_SINCE
            )

    async def enter_update_mode(self) -> None:
        async with self._transaction():
            if not self._features & BridgeFeature.ROM_UPDATE or await self._call(
                BridgeMethod.ENTER_UPDATE,
                update=True,
            ):
                msg = "gateway did not confirm firmware update handoff"
                raise ProtocolError(msg)

    async def revision(self) -> str | None:
        async with self._transaction():
            data = await self._call(BridgeMethod.BUILD)
            if data == b"unversioned":
                return None
            if not re.fullmatch(rb"[0-9a-f]{40}", data):
                msg = "invalid firmware revision"
                raise ProtocolError(msg)
            return data.decode("ascii")

    async def panel(self) -> PanelState:
        async with self._transaction():
            boot, inputs, presses, changes, leds, pins, testing = unpack_diagnostic(
                PANEL,
                await self._call(BridgeMethod.PANEL),
            )
            return PanelState(boot, inputs, presses, changes, leds, pins, bool(testing))

    async def test_leds(self) -> None:
        async with self._transaction():
            await self._call(BridgeMethod.LED_TEST, update=True)

    async def probe(self, *, duration: float = 1) -> tuple[int, ...]:
        if not 0 < duration < 2**32 / 1_000_000:
            msg = "invalid capture window"
            raise ValueError(msg)
        async with self._transaction():
            before = unpack_diagnostic(
                CAPTURE_COUNTERS,
                await self._call(BridgeMethod.COUNTERS),
            )
            await asyncio.sleep(duration)
            after = unpack_diagnostic(
                CAPTURE_COUNTERS,
                await self._call(BridgeMethod.COUNTERS),
            )
            seen = sum(1 << i for i in TN4_PORTS if before[2 + i] != after[2 + i])
            return (
                (after[0] - before[0]) & _TICK_MASK,
                (after[1] - before[1]) & _TICK_MASK,
                seen,
                *after[7:],
            )

    async def _set_byte(
        self,
        method: BridgeMethod,
        setting: int,
        consume: Callable[[Packet], object] | None = None,
    ) -> None:
        value = bytes([setting])
        if await self._call(method, value, update=True, consume=consume) != value:
            msg = f"{method.name} change not acknowledged"
            raise ProtocolError(msg)

    async def _set_mode(
        self,
        mode: BridgeMode,
        consume: Callable[[Packet], object] | None = None,
    ) -> None:
        raw = mode is BridgeMode.RAW
        value = bytes((mode, _RAW_EXTENSIONS if raw else 0))
        if (
            await self._call(BridgeMethod.MODE, value, update=True, consume=consume)
            != value
        ):
            msg = "bridge mode change not acknowledged"
            raise ProtocolError(msg)
        self._parser.literal_header = raw

    @asynccontextmanager
    async def normal(
        self,
        *,
        select_mode: bool = True,
    ) -> AsyncIterator[Tekmar482Client]:
        """Borrow the link for tRPC. A debugger can attach without a mode change."""
        async with self._transaction():
            if select_mode:
                await self._set_mode(BridgeMode.NORMAL)
            transport = _SessionTransport(
                self._next_packet,
                self._write_packet,
                self._wire,
            )
            async with Tekmar482Client(
                transport,
                reports_configured_mode=True,
                readback_methods=frozenset(
                    {
                        TrpcMethod.MODE_SETTING,
                        TrpcMethod.SETPOINT_DEVICE,
                        TrpcMethod.HEAT_SETPOINT,
                        TrpcMethod.COOL_SETPOINT,
                        TrpcMethod.SLAB_SETPOINT,
                        TrpcMethod.FAN_PERCENT,
                        TrpcMethod.HUMIDITY_SET_MIN,
                        TrpcMethod.HUMIDITY_SET_MAX,
                    },
                ),
            ) as client:
                yield client

    @asynccontextmanager
    async def stream(
        self,
        *,
        diagnostics: bool = True,
        ack_ports: Iterable[int] = (),
        capture_backlog: int = 512,
    ) -> AsyncIterator[BridgeStream]:
        ack_mask = 0
        for port in ack_ports:
            if port not in TN4_PORTS:
                msg = f"invalid ACK port {port}"
                raise ValueError(msg)
            ack_mask |= 1 << port
        if not self._features & BridgeFeature.RAW:
            msg = "gateway did not advertise binary Raw support"
            raise ProtocolError(msg)

        async def receive_control() -> Packet:
            # The tRPC session owns the only physical reader in both modes.
            try:
                while (packet := await self._next_packet()).type == TYPE_TN4:
                    stream.feed(packet)
            except Exception as err:
                stream.fail(err)
                raise
            return packet

        client = Tekmar482Client(
            _SessionTransport(receive_control, self._write_packet, self._wire),
        )
        management = BridgeManagement(client)
        stream = RawDispatcher(
            self._write_raw,
            capture_backlog,
            error_reporting=management.error_reporting,
            traffic=management.traffic,
        )
        async with self._transaction():
            await self._set_mode(BridgeMode.RAW)
            try:
                await self._set_byte(
                    BridgeMethod.ACK_MASK,
                    ack_mask,
                    stream.feed,
                )
                with stream.active():
                    async with client:
                        yield stream
            except BaseException:
                # Restore the hardware mode when possible, without masking the
                # original caller cancellation, disconnect or protocol error.
                if (
                    not self._wire.reader.at_eof()
                    and not self._wire.writer.is_closing()
                ):
                    with suppress(OSError, TekmarError):
                        await self._set_mode(BridgeMode.NORMAL, stream.feed)
                raise
            else:
                await self._set_mode(BridgeMode.NORMAL, stream.feed)
                stream.check_drained()
                if diagnostics:
                    for p in TN4_PORTS:
                        port, valid, invalid = unpack_diagnostic(
                            PORT_DROPS,
                            await self._call(BridgeMethod.STATS, bytes([p])),
                        )
                        if p != port:
                            msg = "drop counter port mismatch"
                            raise ProtocolError(msg)
                        stream.diagnostics[p] = PortDiagnostic(p, valid, invalid)
            finally:
                stream.fail(ConnectionClosedError("raw session closed"))


class _SessionTransport:
    """The session borrows the existing decoder and never reopens the physical link."""

    def __init__(
        self,
        receive: Callable[[], Awaitable[Packet]],
        send: Callable[[Packet], Awaitable[None]],
        wire: Wire,
    ) -> None:
        self._receive = receive
        self._send = send
        self._wire = wire
        self._closed = False

    @property
    def is_open(self) -> bool:
        return not self._closed and not self._wire.writer.is_closing()

    async def open(self) -> None:
        if not self.is_open:
            msg = "gateway session has ended"
            raise ConnectionClosedError(msg)

    async def close(self) -> None:
        self._closed = True

    async def read_packet(self, timeout: float | None = None) -> Packet | None:
        await self.open()
        try:
            async with asyncio.timeout(timeout):
                return await self._receive()
        except TimeoutError:
            return None

    async def write_packet(self, packet: Packet) -> None:
        await self.open()
        await self._send(packet)
