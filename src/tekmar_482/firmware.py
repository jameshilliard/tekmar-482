"""Install only the bundled LPC2146 image using the documented ROM ISP."""

from __future__ import annotations

import asyncio
import binascii
import re
import struct
from dataclasses import dataclass
from importlib.resources import files
from typing import TYPE_CHECKING

from .bridge import BridgeClient
from .exceptions import (
    ConnectionClosedError,
    FirmwareImageUnavailableError,
    ProtocolError,
)

if TYPE_CHECKING:
    from ._wire import GatewayEndpoint, Wire

_PAGE = 4096
_SRAM = 0x40001000
_PART = 0x0402FF23
_CLOCK = b"12000"
_ROM_VECTORS = 64
_SMALL_SECTORS_END = 0x8000
_MAX_IMAGE = 0x10000
_TEXT_START = 0x200
_VECTOR_INSTRUCTION = 0xE59FF018
_CHECKSUM_SLOT = 5


@dataclass(frozen=True, slots=True)
class FirmwareBundle:
    revision: str
    image: bytes

    @classmethod
    def load(cls) -> FirmwareBundle:
        try:
            image = files("tekmar_482").joinpath("_images", "bridge.bin").read_bytes()
        except FileNotFoundError as err:
            msg = "No flash image in this source checkout; install from the sdist"
            raise FirmwareImageUnavailableError(msg) from err
        revision = image[0x108:0x130]
        if (
            image[0x100:0x108] != b"TK48REV\x00"
            or not re.fullmatch(rb"[0-9a-f]{40}", revision)
            or not _TEXT_START < len(image) <= _MAX_IMAGE
            or sum(struct.unpack_from("<8I", image)) & 0xFFFFFFFF
            or image[0x1FC:0x200] != b"\xff" * 4
            or any(
                v != _VECTOR_INSTRUCTION
                for i, v in enumerate(struct.unpack_from("<8I", image))
                if i != _CHECKSUM_SLOT
            )
            or any(
                v % 4 or not _TEXT_START <= v < len(image)
                for v in struct.unpack_from("<8I", image, 32)
            )
        ):
            msg = "invalid bundled flash image or revision"
            raise ProtocolError(msg)
        return cls(revision.decode("ascii"), image)


async def _command(wire: Wire, command: str) -> None:
    await wire.write(command.encode("ascii") + b"\r\n")
    await wire.expect(b"0")


async def _synchronize(wire: Wire) -> None:
    await wire.write(b"?")
    await wire.expect(b"Synchronized")
    await _finish_sync(wire)


async def _finish_sync(wire: Wire) -> None:
    for command, ack in ((b"Synchronized", b"OK"), (_CLOCK, b"OK"), (b"A 0", b"0")):
        await wire.write(command + b"\r\n")
        await wire.expect(command)
        await wire.expect(ack)
    await _command(wire, "J")
    await wire.expect(str(_PART).encode())


async def _probe_line(wire: Wire) -> bytes | None:
    """Read a short ROM identification line, declining unsolicited binary traffic."""
    buf = bytearray()
    while len(buf) <= len(b"Synchronized\r"):
        byte = await wire.reader.read(1)
        if not byte:
            msg = "gateway disconnected during identification"
            raise ConnectionClosedError(msg)
        if byte == b"\n":
            return bytes(buf[:-1]) if buf.endswith(b"\r") else None
        if byte != b"\r" and not byte.isalnum() and byte != b"?":
            return None
        buf.extend(byte)
    return None


async def _rom_ready(wire: Wire) -> bool:
    """Identify a fresh or synchronized ROM without unlocking it."""
    await wire.write(b"?")
    try:
        async with asyncio.timeout(0.4):
            banner = await _probe_line(wire)
    except TimeoutError:
        pass
    else:
        if banner == b"Synchronized":
            await _finish_sync(wire)
            return True
        if banner not in {b"?", b""}:
            return False

    # Terminate the probe byte in an already synchronized command parser.
    # '?' is not an ISP command; J is the read-only part identification.
    await wire.write(b"\r\nJ\r\n")
    try:
        async with asyncio.timeout(0.4):
            row = await _probe_line(wire)
    except TimeoutError:
        return False
    if row is None:
        return False
    async with asyncio.timeout(2):
        for _ in range(5):
            if row == b"0":
                break
            if row not in {b"", b"?", b"1", b"J"}:
                return False
            row = await _probe_line(wire)
            if row is None:
                return False
        else:
            msg = "no ISP part identification status"
            raise ProtocolError(msg)
        await wire.expect(str(_PART).encode())
        await wire.write(b"A 0\r\n")
        reply = await wire.line()
        if reply == b"A 0":
            reply = await wire.line()
        if reply != b"0":
            msg = "cannot establish echo-off ISP session"
            raise ProtocolError(msg)
        await _command(wire, "J")
        await wire.expect(str(_PART).encode())
    return True


async def _upload(wire: Wire, page: bytes) -> None:
    await _command(wire, f"W {_SRAM} {len(page)}")
    for start in range(0, len(page), 900):
        block = page[start : start + 900]
        for offset in range(0, len(block), 45):
            uu = (
                binascii.b2a_uu(block[offset : offset + 45], backtick=True).rstrip(
                    b"\n",
                )
                + b"\r\n"
            )
            await wire.write(uu)
            # drain() does not mean the ROM drained its input FIFO.
            await asyncio.sleep(len(uu) * 10 / wire.baudrate + 0.002)
        await wire.write(str(sum(block)).encode() + b"\r\n")
        await wire.expect(b"OK")


def _sector(address: int) -> int:
    return address // _PAGE if address < _SMALL_SECTORS_END else 8


async def _program(wire: Wire, image: bytes) -> None:
    """UM10139 chapter 21. Never retry an ambiguous erase or copy."""
    padded = image + bytes([0xFF]) * (-len(image) % _PAGE)
    # Stage and checksum a page before unlocking flash. Program boot vectors last.
    offsets = [*range(_PAGE, len(padded), _PAGE), 0]
    await _upload(wire, padded[offsets[0] : offsets[0] + _PAGE])
    await _command(wire, "U 23130")
    last = _sector(len(padded) - 1)
    await _command(wire, f"P 0 {last}")
    await _command(wire, f"E 0 {last}")
    for index, offset in enumerate(offsets):
        if index:
            await _upload(wire, padded[offset : offset + _PAGE])
        sector = _sector(offset)
        await _command(wire, f"P {sector} {sector}")
        await _command(wire, f"C {offset} {_SRAM} {_PAGE}")
        # ROM remaps its own vector table over the first 64 bytes.
        skip = 0 if offset else _ROM_VECTORS
        await _command(wire, f"M {offset + skip} {_SRAM + skip} {_PAGE - skip}")
    await wire.write(b"G 0 A\r\n")


async def install_firmware(
    endpoint: GatewayEndpoint,
    *,
    from_rom: bool = False,
) -> bool:
    """Install the bundled custom image. Return False if already current.

    Manual migration from stock requires physical ROM entry.
    A confirmed ROM session is provisioned regardless of previous contents.
    An identified custom application is replaced unless its revision matches.
    """
    bundle = await asyncio.to_thread(FirmwareBundle.load)
    async with endpoint.connect() as wire:
        if not await _rom_ready(wire):
            if from_rom:
                msg = "no confirmed LPC2146 ROM on the selected endpoint"
                raise ProtocolError(msg)
            bridge = BridgeClient(wire)
            await bridge.identify()
            revision = await bridge.revision()
            if revision == bundle.revision:
                return False
            await bridge.enter_update_mode()
            await asyncio.sleep(0.1)
            await _synchronize(wire)
        await _program(wire, bundle.image)
        await asyncio.sleep(0.15)
    await _verify_boot(endpoint, bundle)
    return True


async def provision_if_rom(endpoint: GatewayEndpoint) -> bool:
    """Flash a positively identified, idle ROM. Leave applications untouched."""
    async with endpoint.connect() as wire:
        if not await _rom_ready(wire):
            return False
        bundle = await asyncio.to_thread(FirmwareBundle.load)
        await _program(wire, bundle.image)
        await asyncio.sleep(0.15)
    await _verify_boot(endpoint, bundle)
    return True


async def _verify_boot(endpoint: GatewayEndpoint, bundle: FirmwareBundle) -> None:
    # Open a fresh stream: some ROMs leave a partial Go reply behind.
    async with endpoint.connect() as wire:
        bridge = BridgeClient(wire)
        await bridge.identify()
        if await bridge.revision() != bundle.revision:
            msg = "gateway did not boot the programmed revision"
            raise ProtocolError(msg)
