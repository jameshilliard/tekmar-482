"""Async binary tRPC management and opaque type-1 tN4 records."""

from __future__ import annotations

import asyncio
import re
import struct
from collections import deque
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .client import Tekmar482Client
from .constants import TN4_PORTS
from .exceptions import ConnectionClosedError, ProtocolError, TekmarError
from .management import BridgeFeature, BridgeMethod, BridgeMode
from .packet import TYPE_TN4, TYPE_TRPC, Packet
from .raw import PortDiagnostic, RawDispatcher
from .tpck import TpckParser, serialize
from .trpc import TrpcMethod, TrpcService

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Iterable

    from ._wire import Wire
    from .raw import BridgeStream

_DEFAULT_BAUD = 9600
_TICK_MASK = 0xFFFFFFFF
_CAPABILITIES = struct.Struct("4B")
_PANEL = struct.Struct("<7I")
_COUNTERS = struct.Struct("<12I")
_DROPS = struct.Struct("<BII")


@dataclass(frozen=True, slots=True)
class PanelState:
    boot_inputs: int
    inputs: int
    test_presses: int
    dip_changes: int
    led_mask: int
    pin_mask: int
    testing: bool


class BridgeClient:
    """Negotiated binary application connection. Use open_gateway for its lifetime."""

    def __init__(self, wire: Wire) -> None:
        self._wire = wire
        self._parser = TpckParser(strict_checksum=True)
        self._packets: deque[Packet] = deque()
        self._busy = asyncio.Lock()
        self._features = BridgeFeature(0)

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
        while not self._packets:
            chunk = await self._wire.reader.read(256)
            if not chunk:
                msg = "gateway disconnected"
                raise ConnectionClosedError(msg)
            self._packets.extend(self._parser.feed_many(chunk))
        return self._packets.popleft()

    async def _write_raw(self, body: bytes) -> None:
        await self._wire.write(serialize(Packet(TYPE_TN4, body)))

    async def _call(
        self,
        method: BridgeMethod,
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
        request = serialize(Packet(TYPE_TRPC, bytes([service]) + key + body))
        async with asyncio.timeout(5):
            await self._wire.write(request)
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
                        await self._wire.write(request)
                        continue
                    msg = f"bridge management rejection: {payload[4:].hex()}"
                    raise ProtocolError(msg)

    async def identify(self) -> None:
        async with self._transaction():
            if self._wire.baudrate != _DEFAULT_BAUD:
                burst = b"\xca" * max(1, self._wire.baudrate // 1250)
                for preamble in (burst, burst, b"\xca"):
                    await self._wire.write(preamble)
                    await asyncio.sleep(0.08)
            data = await self._call(BridgeMethod.CAPABILITIES)
            if (
                len(data) != _CAPABILITIES.size
                or data[:1] != b"\x01"
                or data[1] > 1
                or data[2] != len(TN4_PORTS)
            ):
                msg = "invalid binary bridge capabilities"
                raise ProtocolError(msg)
            self._features = BridgeFeature(data[3])

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
            boot, inputs, presses, changes, leds, pins, testing = _PANEL.unpack(
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
            before = _COUNTERS.unpack(await self._call(BridgeMethod.COUNTERS))
            await asyncio.sleep(duration)
            after = _COUNTERS.unpack(await self._call(BridgeMethod.COUNTERS))
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

    @asynccontextmanager
    async def normal(self) -> AsyncIterator[Tekmar482Client]:
        """Borrow the negotiated link for one exclusive Normal-mode tRPC session."""
        async with self._transaction():
            await self._set_byte(BridgeMethod.MODE, BridgeMode.NORMAL)
            transport = _NormalTransport(self._next_packet, self._wire)
            async with Tekmar482Client(
                transport,
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
        stream = RawDispatcher(self._write_raw, capture_backlog)
        async with self._transaction():
            await self._set_byte(BridgeMethod.MODE, BridgeMode.RAW)
            try:
                await self._set_byte(
                    BridgeMethod.ACK_MASK,
                    ack_mask,
                    stream.feed,
                )
                async with stream.receiving(self._next_packet):
                    yield stream
            except BaseException:
                # Restore the hardware mode when possible, without masking the
                # original caller cancellation, disconnect or protocol error.
                if (
                    not self._wire.reader.at_eof()
                    and not self._wire.writer.is_closing()
                ):
                    with suppress(OSError, TekmarError):
                        await self._set_byte(
                            BridgeMethod.MODE,
                            BridgeMode.NORMAL,
                            stream.feed,
                        )
                raise
            else:
                await self._set_byte(BridgeMethod.MODE, BridgeMode.NORMAL, stream.feed)
                stream.check_drained()
                if diagnostics:
                    for p in TN4_PORTS:
                        port, valid, invalid = _DROPS.unpack(
                            await self._call(BridgeMethod.STATS, bytes([p])),
                        )
                        if p != port:
                            msg = "drop counter port mismatch"
                            raise ProtocolError(msg)
                        stream.diagnostics[p] = PortDiagnostic(p, valid, invalid)
            finally:
                stream.fail(ConnectionClosedError("raw session closed"))


class _NormalTransport:
    """The session borrows the existing decoder and never reopens the physical link."""

    def __init__(self, receive: Callable[[], Awaitable[Packet]], wire: Wire) -> None:
        self._receive = receive
        self._wire = wire
        self._closed = False

    @property
    def is_open(self) -> bool:
        return not self._closed and not self._wire.writer.is_closing()

    async def open(self) -> None:
        if not self.is_open:
            msg = "Normal session has ended"
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
        await self._wire.write(serialize(packet))
