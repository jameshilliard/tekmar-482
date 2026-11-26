"""Async binary tRPC management and opaque type-1 tN4 records."""

from __future__ import annotations

import asyncio
import re
import struct
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .client import Tekmar482Client
from .constants import TN4_PORTS
from .exceptions import ConnectionClosedError, ProtocolError
from .management import BridgeFeature, BridgeMethod, BridgeMode
from .native import NativeFrame, NativeMessage
from .packet import TYPE_TN4, TYPE_TRPC, Packet
from .tpck import TpckParser, serialize
from .trpc import TrpcMethod, TrpcService

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from ._wire import Wire

_DEFAULT_BAUD = 9600
_RAW_HEADER = 2
_INVALID_SERVICE = 4
_RESULT_SERVICE = 2
_RESULT_WITH_REASON = 4
_HEADER = 4
_RX_HEADER = 6
_INVALID_HEADER = 9
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


@dataclass(frozen=True, slots=True)
class PortDiagnostic:
    port: int
    valid_dropped: int
    invalid_dropped: int


@dataclass(frozen=True, slots=True)
class InvalidCapture:
    port: int
    timestamp_ms: int
    reason: int
    bit_count: int
    bits: bytes


def valid_body(body: bytes) -> bool:
    return len(body) >= _HEADER and body[3] & 63 == len(body) - _HEADER


@dataclass(slots=True)
class BridgeStream:
    """One raw session, with one reader and independent per-port submissions."""

    _client: BridgeClient
    diagnostics: dict[int, PortDiagnostic] = field(default_factory=dict, init=False)
    _pending: set[int] = field(default_factory=set, init=False)
    _early: deque[NativeFrame | InvalidCapture] = field(
        default_factory=deque,
        init=False,
    )
    tx_result: tuple[int, str] | None = None
    _closed: bool = False

    async def submit(self, port: int, body: bytes | NativeMessage) -> None:
        if isinstance(body, NativeMessage):
            body = body.to_bytes()
        if self._closed or port in self._pending:
            msg = f"raw session closed or send already in progress on port {port}"
            raise ProtocolError(msg)
        if port not in TN4_PORTS or not valid_body(body):
            msg = "expected a physical port and complete native frame"
            raise ValueError(msg)
        self._pending.add(port)
        await self._client.write_raw(bytes((1, port)) + body)

    def consume(self, packet: Packet) -> NativeFrame | InvalidCapture | None:
        data = packet.data
        if (
            packet.type != TYPE_TN4
            or len(data) < _RAW_HEADER
            or data[1] not in TN4_PORTS
        ):
            msg = "unexpected raw packet type, length or port"
            raise ProtocolError(msg)
        kind, port = data[:2]
        if kind == 0 and valid_body(data[_RX_HEADER:]):
            return NativeFrame(port, int.from_bytes(data[2:6], "little"), data[6:])
        if kind == _INVALID_SERVICE and len(data) >= _INVALID_HEADER:
            count = int.from_bytes(data[7:9], "little")
            bits = data[9:]
            if (count + 7) // 8 == len(bits) and (
                not count % 8 or not bits[-1] & ((1 << (8 - count % 8)) - 1)
            ):
                return InvalidCapture(
                    port,
                    int.from_bytes(data[2:6], "little"),
                    data[6],
                    count,
                    bits,
                )
        if kind == _RESULT_SERVICE and port in self._pending:
            if data[2:] == b"\x00":
                result = "ACK"
            elif len(data) == _RESULT_WITH_REASON and data[2] in (1, 2):
                result = f"{'REJECTED' if data[2] == 1 else 'FAILED'}:{data[3]}"
            else:
                msg = "malformed transmit result"
                raise ProtocolError(msg)
            self._pending.remove(port)
            self.tx_result = (port, result)
            return None
        msg = f"unexpected raw service {kind} on port {port}"
        raise ProtocolError(msg)

    async def read(self) -> NativeFrame | InvalidCapture | None:
        if self._closed:
            msg = "raw session closed"
            raise ProtocolError(msg)
        return (
            self._early.popleft()
            if self._early
            else self.consume(await self._client.next_packet())
        )

    def buffer(self, packet: Packet) -> None:
        if (record := self.consume(packet)) is not None:
            self._early.append(record)

    def close(self) -> None:
        self._closed = True


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

    async def next_packet(self) -> Packet:
        while not self._packets:
            chunk = await self._wire.reader.read(256)
            if not chunk:
                msg = "gateway disconnected"
                raise ConnectionClosedError(msg)
            self._packets.extend(self._parser.feed_many(chunk))
        return self._packets.popleft()

    async def write_raw(self, body: bytes) -> None:
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
                packet = await self.next_packet()
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

    async def _mode(
        self,
        mode: BridgeMode,
        consume: Callable[[Packet], object] | None = None,
    ) -> None:
        value = bytes([mode])
        if (
            await self._call(BridgeMethod.MODE, value, update=True, consume=consume)
            != value
        ):
            msg = "mode change not acknowledged"
            raise ProtocolError(msg)

    @asynccontextmanager
    async def normal(self) -> AsyncIterator[Tekmar482Client]:
        """Borrow the negotiated link for one exclusive Normal-mode tRPC session."""
        async with self._transaction():
            await self._mode(BridgeMode.NORMAL)
            transport = _NormalTransport(self, self._wire)
            async with Tekmar482Client(
                transport,
                readback_methods=frozenset(
                    {
                        TrpcMethod.MODE_SETTING,
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
        ack_port: int | None = None,
        tx: tuple[int, bytes] | None = None,
    ) -> AsyncIterator[BridgeStream]:
        if ack_port is not None and ack_port not in TN4_PORTS:
            msg = "invalid ACK port"
            raise ValueError(msg)
        if not self._features & BridgeFeature.RAW:
            msg = "gateway did not advertise binary Raw support"
            raise ProtocolError(msg)
        async with self._transaction():
            await self._mode(BridgeMode.RAW)
            stream = BridgeStream(self)
            try:
                mask = bytes([0 if ack_port is None else 1 << ack_port])
                if (
                    await self._call(
                        BridgeMethod.ACK_MASK,
                        mask,
                        update=True,
                        consume=stream.buffer,
                    )
                    != mask
                ):
                    msg = "ACK mask not acknowledged"
                    raise ProtocolError(msg)
                if tx is not None:
                    await stream.submit(*tx)
                yield stream
            finally:
                try:
                    await self._mode(BridgeMode.NORMAL, stream.consume)
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
                    stream.close()


class _NormalTransport:
    """The session borrows the existing decoder and never reopens the physical link."""

    def __init__(self, bridge: BridgeClient, wire: Wire) -> None:
        self._bridge = bridge
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
                return await self._bridge.next_packet()
        except TimeoutError:
            return None

    async def write_packet(self, packet: Packet) -> None:
        await self.open()
        await self._wire.write(serialize(packet))
