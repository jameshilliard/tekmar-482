"""Async client for the native tN4 bridge firmware."""

from __future__ import annotations

import asyncio
import binascii
import re
from contextlib import asynccontextmanager
from dataclasses import KW_ONLY, dataclass, field
from typing import TYPE_CHECKING

from ._native_payloads import decode_payload
from ._native_records import FrameAudit, dropped_records
from .exceptions import ProtocolError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from ._wire import Wire

IDENTITY = b"TEKMAR-482-DEV"
_PORTS = range(5)
_HEADER = 4
_INPUT_MASK = 0b11
_LED_MASK = 0b111111
_TIMER_WRAP_SECONDS = 2**32 / 1_000_000


@dataclass(frozen=True, slots=True)
class NativeFrame:
    port: int
    start_us: int
    crc: int
    trailer: str
    body: bytes

    @property
    def message_id(self) -> int:
        return (int.from_bytes(self.body[:_HEADER]) >> 20) & 1023

    @property
    def subnet(self) -> int:
        return (int.from_bytes(self.body[:_HEADER]) >> 12) & 255

    @property
    def node(self) -> int:
        return (int.from_bytes(self.body[:_HEADER]) >> 6) & 63

    @property
    def decoded(self) -> dict[str, str | int | float]:
        return decode_payload(self.message_id, self.body[_HEADER:])


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
    edges: int
    starts: int
    stuffing_errors: int
    crc_errors: int
    ack: int
    no_ack: int
    bad_trailer: int
    partial: int
    dropped: int
    max_delay_us: int
    received_crc: int
    expected_crc: int
    _: KW_ONLY
    levels: int | None
    rejected_start_us: int | None
    rejected_body: bytes | None


@dataclass(slots=True)
class BridgeStream:
    """A stream owns the UART until its enclosing context exits."""

    _wire: Wire
    _audit: FrameAudit
    _ack_port: int | None
    _tx: tuple[int, bytes] | None
    diagnostics: dict[int, PortDiagnostic] = field(default_factory=dict, init=False)
    _echoes: set[str] = field(default_factory=set, init=False)
    _ended: bool = False

    @property
    def tx_result(self) -> tuple[int, str] | None:
        return self._audit.tx_result

    @property
    def ack_summary(self) -> tuple[int, int, int] | None:
        return self._audit.ack_summary

    def _record(self, row: bytes) -> NativeFrame | None:
        line = row + b"\r\n"
        if (drops := dropped_records(line)) is not None:
            self._audit.finish(drops)
            self._ended = True
            return None
        self._audit.record(line)
        parts = row.split()
        port = int(parts[1])
        if parts[0] == b"F":
            frame = NativeFrame(
                port,
                int(parts[2], 16),
                int(parts[3], 16),
                parts[4].decode("ascii"),
                bytes.fromhex(parts[5].decode("ascii")),
            )
            if self._tx == (port, frame.body):
                self._echoes.add(frame.trailer)
            return frame
        if parts[0] == b"D":
            words = [int(v, 16) for v in parts[2:14]]
            tail = iter(parts[14:])
            levels, start, raw = (next(tail, b"") for _ in range(3))
            self.diagnostics[port] = PortDiagnostic(
                port,
                *words,
                levels=int(levels, 16) if levels else None,
                rejected_start_us=int(start, 16) if start else None,
                rejected_body=bytes.fromhex(raw.decode("ascii")) if raw else None,
            )
        return None

    async def read(self) -> NativeFrame | None:
        """Read one UART record; TX results and summaries update this object."""
        if self._ended:
            msg = "bridge stream already ended"
            raise ProtocolError(msg)
        try:
            return self._record(await self._wire.line())
        except ValueError as err:
            raise ProtocolError(str(err)) from err

    async def _stop(self) -> None:
        async with asyncio.timeout(5):
            await self._wire.write(b"\n")
            while not self._ended:
                await self.read()
        if (self.ack_summary[0] if self.ack_summary else None) != self._ack_port:
            msg = "receive ACK summary does not match requested port"
            raise ProtocolError(msg)
        if self._tx is not None:
            result = self.tx_result
            if result is None or result[0] != self._tx[0]:
                msg = "missing transmit result for requested port"
                raise ProtocolError(msg)
            expected = {"ACK": "A", "NOACK": "N"}.get(result[1])
            if expected is not None and expected not in self._echoes:
                msg = "transmit result lacks matching CRC-verified bus echo"
                raise ProtocolError(msg)
        elif self.tx_result is not None:
            msg = "unexpected transmit result on receiver"
            raise ProtocolError(msg)


class BridgeClient:
    """Native bridge connection. Use open_gateway() to manage its lifetime."""

    def __init__(self, wire: Wire) -> None:
        self._wire = wire
        self._busy = asyncio.Lock()

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

    async def identify(self) -> None:
        async with self._transaction():
            await self._wire.command(b"ID?")
            await self._wire.expect(IDENTITY)

    async def revision(self) -> str | None:
        """Return None for unstamped builds and legacy images without BUILD?."""
        async with self._transaction():
            await self._wire.command(b"BUILD?")
            try:
                async with asyncio.timeout(1):
                    reply = await self._wire.line()
            except TimeoutError:
                # Prove the gateway is still in application command mode.
                await self._wire.command(b"ID?")
                await self._wire.expect(IDENTITY)
                return None
            if reply == b"BUILD unversioned":
                return None
            if not re.fullmatch(rb"BUILD [0-9a-f]{40}", reply):
                msg = "invalid bridge build revision"
                raise ProtocolError(msg)
            return reply[6:].decode("ascii")

    async def panel(self) -> PanelState:
        async with self._transaction(), asyncio.timeout(5):
            await self._wire.command(b"PANEL?")
            reply = await self._wire.line()
            if not re.fullmatch(rb"PANEL(?: [0-9a-f]{8}){7}", reply):
                msg = "invalid panel state"
                raise ProtocolError(msg)
            boot, inputs, presses, changes, leds, pins, test = (
                int(v, 16) for v in reply.split()[1:]
            )
            if (
                max(boot, inputs) > _INPUT_MASK
                or leds != pins
                or pins > _LED_MASK
                or test > 1
            ):
                msg = "invalid panel flags or LED readback mismatch"
                raise ProtocolError(msg)
            return PanelState(boot, inputs, presses, changes, leds, pins, bool(test))

    async def test_leds(self) -> None:
        async with self._transaction():
            await self._wire.command(b"LED-TEST")
            await self._wire.expect(b"OK LED")

    async def probe(self, *, duration: float = 1) -> tuple[int, ...]:
        """Return timer counts, port mask and five edge timestamps."""
        if not 0 < duration < _TIMER_WRAP_SECONDS:
            msg = "probe window must fit in a 32-bit microsecond timer"
            raise ValueError(msg)
        async with self._transaction():
            await self._wire.command(b"RX-BEGIN")
            await self._wire.expect(b"OK RX")
            try:
                await asyncio.sleep(duration)
            finally:
                await self._wire.command(b"RX-END")
                async with asyncio.timeout(5):
                    reply = await self._wire.line()
            if not re.fullmatch(rb"RX(?: [0-9a-f]{8}){8}", reply):
                msg = "invalid edge probe reply"
                raise ProtocolError(msg)
            return tuple(int(v, 16) for v in reply.split()[1:])

    @asynccontextmanager
    async def stream(
        self,
        *,
        diagnostics: bool = True,
        ack_port: int | None = None,
        tx: tuple[int, bytes] | None = None,
    ) -> AsyncIterator[BridgeStream]:
        if ack_port is not None and (ack_port not in _PORTS or tx is not None):
            msg = "receive ACK requires one port (0..4) and no transmission"
            raise ValueError(msg)
        if tx is not None:
            port, body = tx
            if (
                port not in _PORTS
                or len(body) < _HEADER
                or body[3] & 63 != len(body) - _HEADER
            ):
                msg = "expected a valid port and complete native frame body"
                raise ValueError(msg)
            check = binascii.crc_hqx(bytes([port]) + body, 0xFFFF)
            command, greeting = f"TX {port} {body.hex()} {check:04x}".encode(), b"OK TX"
        elif ack_port is not None:
            command, greeting = (
                f"RX-ACK {ack_port}".encode(),
                f"OK ACK {ack_port}".encode(),
            )
        else:
            command, greeting = (
                (b"RX-DIAG", b"OK DIAG")
                if diagnostics
                else (b"RX-STREAM", b"OK FRAMES")
            )
        async with self._transaction():
            await self._wire.command(command)
            await self._wire.expect(greeting)
            stream = BridgeStream(
                self._wire,
                FrameAudit(diagnostics or tx is not None or ack_port is not None),
                ack_port,
                tx,
            )
            try:
                yield stream
            finally:
                await stream._stop()  # noqa: SLF001
