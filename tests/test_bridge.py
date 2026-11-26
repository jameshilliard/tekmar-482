from __future__ import annotations

import asyncio
import binascii
import json
import struct
import unittest
from contextlib import asynccontextmanager
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING
from unittest.mock import patch

from tekmar_482 import (
    BridgeClient,
    ConnectionClosedError,
    FirmwareBundle,
    GatewayEndpoint,
    NativeFrame,
    Packet,
    ProtocolError,
    Tekmar482Client,
    TxResult,
    TxStatus,
    install_firmware,
    open_gateway,
)
from tekmar_482.cli import build_parser, run
from tekmar_482.tpck import serialize

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    type Handler = Callable[
        [asyncio.StreamReader, asyncio.StreamWriter],
        Awaitable[None],
    ]

# Sector numbers for consecutive 4 KiB pages in the LPC2146 flash window.
_PAGE_SECTORS = tuple(range(8)) + (8,) * 8


@asynccontextmanager
async def listen(handler: Handler) -> AsyncIterator[GatewayEndpoint]:
    tasks: list[asyncio.Task[None]] = []

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await handler(reader, writer)
        finally:
            writer.close()
            await writer.wait_closed()

    def accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        tasks.append(asyncio.create_task(serve(reader, writer)))

    async with await asyncio.start_server(accept, "127.0.0.1", 0) as server:
        try:
            yield GatewayEndpoint.tcp(
                "127.0.0.1",
                server.sockets[0].getsockname()[1],
                baudrate=187500,
            )
        finally:
            await asyncio.gather(*tasks)


def management(method: int, service: int, body: bytes = b"") -> bytes:
    return serialize(
        Packet(6, bytes([service]) + struct.pack("<I", 0x544BF000 + method) + body),
    )


class BridgeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        # Exercise two flash pages and real image validation without a binary fixture.
        resources = Path(self.enterContext(TemporaryDirectory()))
        image = bytearray(4608)
        vectors = [0xE59FF018] * 8
        vectors[5] = (-7 * vectors[0]) & 0xFFFFFFFF
        struct.pack_into("<16I", image, 0, *vectors, *([0x200] * 8))
        image[0x1FC:0x200] = b"\xff" * 4
        image[0x100:0x130] = b"TK48REV\x00" + b"1" * 40
        (resources / "_images").mkdir()
        self.image_path = resources / "_images/bridge.bin"
        self.image_path.write_bytes(image)
        self.enterContext(patch("tekmar_482.firmware.files", return_value=resources))

    async def page_data(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> bytearray:
        writer.write(b"0\r\n")
        staged = bytearray()
        while len(staged) < 4096:
            block = bytearray()
            length = min(900, 4096 - len(staged))
            while len(block) < length:
                block.extend(binascii.a2b_uu(await reader.readline()))
            assert await reader.readline() == f"{sum(block)}\r\n".encode()
            writer.write(b"OK\r\n")
            staged.extend(block)
        return staged

    async def booted(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        revision: str,
    ) -> None:
        caps = b"\xca" * 301 + management(1, 1)
        assert await reader.readexactly(len(caps)) == caps
        writer.write(management(1, 4, bytes([1, 0, 5, 3])))
        request = management(4, 1)
        if first := await reader.read(1):
            assert first + await reader.readexactly(len(request) - 1) == request
            writer.write(management(4, 4, revision.encode()))

    async def test_custom_panel_stream_and_echo(self) -> None:
        async def app(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            if (first := await reader.readexactly(1)) == b"?":
                assert await reader.readexactly(5) == b"\r\nJ\r\n"
                assert not await reader.read()
                return
            caps = b"\xca" * 301 + management(1, 1)
            assert first + await reader.readexactly(len(caps) - 1) == caps
            writer.write(management(1, 4, bytes([1, 0, 5, 3])))
            for method, request, reply in [
                (
                    7,
                    management(7, 1),
                    management(7, 4, struct.pack("<7I", 1, 1, 0, 0, 0, 0, 0)),
                ),
                (2, management(2, 0, bytes([1])), management(2, 3, bytes([1]))),
                (9, management(9, 0, bytes([0])), management(9, 3, bytes([0]))),
            ]:
                assert await reader.readexactly(len(request)) == request, method
                writer.write(reply)
            body = bytes.fromhex("12300001a5")
            send = serialize(Packet(1, bytes([1, 0]) + body))
            assert await reader.readexactly(len(send)) == send
            writer.write(serialize(Packet(1, bytes(6) + body)))
            writer.write(serialize(Packet(1, bytes([2, 0, 0]))))
            mode = management(2, 0, bytes(1))
            assert await reader.readexactly(len(mode)) == mode
            writer.write(management(2, 3, bytes(1)))
            for port in range(5):
                req = management(3, 1, bytes([port]))
                assert await reader.readexactly(len(req)) == req
                writer.write(management(3, 4, struct.pack("<BII", port, 0, 1)))

        async with listen(app) as endpoint, open_gateway(endpoint) as client:
            assert isinstance(client, BridgeClient)
            assert (await client.panel()).inputs == 1
            async with client.stream() as stream:
                result = await stream.send(0, bytes.fromhex("12300001a5"))
                assert result == TxResult(0, TxStatus.ACK)
                frame = await stream.read()
                assert (
                    isinstance(frame, NativeFrame)
                    and frame.message_id == 0x123
                    and frame.decoded == {}
                )
            assert stream.diagnostics[0].invalid_dropped == 1
            assert set(stream.diagnostics) == set(range(5))

    async def test_normal_inventory_borrows_the_negotiated_link(self) -> None:
        async def app(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            mode = management(2, 0, bytes(1))
            assert await reader.readexactly(len(mode)) == mode
            writer.write(management(6, 3, struct.pack("<IB", 0x544BF002, 1)))
            assert await reader.readexactly(len(mode)) == mode
            writer.write(management(2, 3, bytes(1)))
            walk = serialize(Packet(6, bytes.fromhex("01670100000000")))
            assert await reader.readexactly(len(walk)) == walk
            for address in (23, 1102, 0):
                frame = serialize(Packet(6, struct.pack("<BIH", 4, 0x167, address)))
                writer.write(frame[:3])
                await writer.drain()
                writer.write(frame[3:])
            build = management(4, 1)
            assert await reader.readexactly(len(build)) == build
            writer.write(management(4, 4, b"unversioned"))

        async with listen(app) as endpoint, endpoint.connect() as wire:
            bridge = BridgeClient(wire)
            async with bridge.normal() as client:
                with self.assertRaises(ProtocolError):
                    await bridge.revision()
                assert await client.get_inventory_addresses() == (23, 1102)
            with self.assertRaises(ConnectionClosedError):
                await client.open()
            assert await bridge.revision() is None

    async def test_raw_exit_drains_a_result_before_reusing_link(self) -> None:
        sent = asyncio.Event()
        body = bytes.fromhex("12300000")
        raw_frame = serialize(Packet(1, bytes(6) + body))

        async def app(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            for request, reply in (
                (
                    b"\xca" * 301 + management(1, 1),
                    management(1, 4, bytes([1, 0, 5, 3])),
                ),
                (management(2, 0, b"\x01"), management(2, 3, b"\x01")),
                (management(9, 0, b"\0"), raw_frame + management(9, 3, b"\0")),
            ):
                assert await reader.readexactly(len(request)) == request
                writer.write(reply)
            send = serialize(Packet(1, b"\x01\x01" + body))
            assert await reader.readexactly(len(send)) == send
            sent.set()
            normal = management(2, 0, b"\0")
            assert await reader.readexactly(len(normal)) == normal
            writer.write(serialize(Packet(1, b"\x02\x01\0")) + management(2, 3, b"\0"))
            build = management(4, 1)
            assert await reader.readexactly(len(build)) == build
            writer.write(management(4, 4, b"unversioned"))

        async with (
            listen(app) as endpoint,
            endpoint.connect() as wire,
            asyncio.timeout(3),
        ):
            bridge = BridgeClient(wire)
            await bridge.identify()
            async with bridge.stream(diagnostics=False) as stream:
                assert await stream.read() == NativeFrame(0, 0, body)
                pending = asyncio.create_task(stream.send(1, body))
                await sent.wait()
                waiting = asyncio.create_task(stream.read())
            assert await pending == TxResult(1, TxStatus.ACK)
            with self.assertRaises(ConnectionClosedError):
                await waiting
            with self.assertRaises(ConnectionClosedError):
                await stream.send(1, body)
            assert await bridge.revision() is None

    async def test_cli_send_completes_without_rx_capture(self) -> None:
        body = bytes.fromhex("12300000")

        async def app(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            if (first := await reader.readexactly(1)) == b"?":
                assert await reader.readexactly(5) == b"\r\nJ\r\n"
                assert not await reader.read()
                return
            caps = management(1, 1)
            assert first + await reader.readexactly(len(caps) - 1) == caps
            writer.write(management(1, 4, bytes([1, 0, 5, 3])))
            for request, reply in (
                (management(2, 0, b"\x01"), management(2, 3, b"\x01")),
                (management(9, 0, b"\0"), management(9, 3, b"\0")),
                (
                    serialize(Packet(1, b"\x01\x01" + body)),
                    serialize(Packet(1, b"\x02\x01\0")),
                ),
                (management(2, 0, b"\0"), management(2, 3, b"\0")),
            ):
                assert await reader.readexactly(len(request)) == request
                writer.write(reply)

        async with listen(app) as endpoint, asyncio.timeout(3):
            stdout = StringIO()
            args = build_parser().parse_args(
                [
                    "send",
                    "1",
                    body.hex(),
                    "--tcp",
                    f"{endpoint.address}:{endpoint.port}",
                    "--baudrate",
                    "9600",
                    "--json",
                ],
            )
            assert await run(args, stdout=stdout) == 0
            assert json.loads(stdout.getvalue()) == {
                "tx_result": {"port": 1, "status": "ACK", "reason": None},
            }

    async def test_vendor_opt_in_never_probes_rom(self) -> None:
        async def app(
            reader: asyncio.StreamReader,
            _writer: asyncio.StreamWriter,
        ) -> None:
            assert not await reader.read()

        async with (
            listen(app) as endpoint,
            open_gateway(endpoint, vendor=True) as client,
        ):
            assert isinstance(client, Tekmar482Client)

    async def test_normal_setting_updates_return_device_readback(self) -> None:
        def packet(service: int, method: int, body: bytes) -> bytes:
            return serialize(Packet(6, struct.pack("<BI", service, method) + body))

        async def app(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            mode = management(2, 0, bytes(1))
            assert await reader.readexactly(len(mode)) == mode
            writer.write(management(2, 3, bytes(1)))
            for method, request, actual in [
                (0x127, bytes.fromhex("010002"), bytes.fromhex("010001")),
                (0x13E, bytes.fromhex("0100074906"), bytes.fromhex("0100034606")),
                (0x13F, bytes.fromhex("01000762"), bytes.fromhex("01000360")),
                (0x147, bytes.fromhex("01000662"), bytes.fromhex("010006ff")),
                (0x14F, bytes.fromhex("01000762"), bytes.fromhex("01000260")),
                (0x157, bytes.fromhex("01000632"), bytes.fromhex("01000628")),
                (0x152, bytes.fromhex("010023"), bytes.fromhex("010020")),
                (0x151, bytes.fromhex("010032"), bytes.fromhex("0100ff")),
            ]:
                update = packet(0, method, request)
                assert await reader.readexactly(len(update)) == update
                writer.write(packet(2, method, request))
                writer.write(packet(3, method, b"\x02" + actual[1:]))
                writer.write(packet(3, method, actual))
                await writer.drain()
            await reader.read()

        async with (
            listen(app) as endpoint,
            endpoint.connect() as wire,
            BridgeClient(wire).normal() as client,
        ):
            mode = await client.set_mode(1, 2, timeout=1)
            assert mode is not None and mode.require("mode") == 1
            target = await client.set_latest_setpoint_device(1, 0x649, timeout=1)
            assert target is not None and target.require("setback") == 3
            assert target.require("temp") == 0x646
            heat = await client.set_heat_setpoint(1, 98, timeout=1)
            assert (
                heat is not None
                and heat.require("setback") == 3
                and heat.require("setpoint") == 96
            )
            cool = await client.set_latest_cool_setpoint(
                1,
                98,
                setback=6,
                confirm=True,
                timeout=1,
            )
            assert cool is not None and cool.require("setpoint") == 255
            slab = await client.set_slab_setpoint(1, 98, timeout=1)
            assert slab is not None and slab.require("setpoint") == 96
            fan = await client.set_latest_fan_percent(1, 50, setback=6, timeout=1)
            assert fan is not None and fan.require("percent") == 40
            minimum = await client.set_humidity_min(1, 35, timeout=1)
            assert minimum is not None and minimum.require("percent") == 32
            maximum = await client.set_humidity_max(1, 50, timeout=1)
            assert maximum is not None and maximum.require("percent") == 255

    async def test_wrong_rom_id_fails_before_unlock(self) -> None:
        async def rom(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            await self.sync(reader, writer, part=123)
            assert not await reader.read()

        async with listen(rom) as endpoint:
            with self.assertRaises(ProtocolError):
                async with open_gateway(endpoint):
                    self.fail("wrong MCU accepted")

    async def test_unsolicited_report_is_not_an_isp_banner(self) -> None:
        report = serialize(Packet(6, bytes.fromhex("023701000001000a06")))

        async def app(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            first = await reader.readexactly(1)
            if first == b"?":
                writer.write(report)
                assert not await reader.read()
                return
            caps = b"\xca" * 301 + management(1, 1)
            assert first + await reader.readexactly(len(caps) - 1) == caps
            writer.write(report + management(1, 4, bytes([1, 0, 5, 3])))
            mode = management(2, 0, b"\0")
            assert await reader.readexactly(len(mode)) == mode
            writer.write(management(2, 3, b"\0"))
            req = serialize(Packet(6, bytes.fromhex("013701000001000000")))
            assert await reader.readexactly(len(req)) == req
            writer.write(
                report + serialize(Packet(6, bytes.fromhex("043701000001000a06"))),
            )

        async with listen(app) as endpoint:
            stdout = StringIO()
            args = build_parser().parse_args(
                [
                    "request",
                    "CurrentTemperature",
                    "address=1",
                    "--tcp",
                    f"{endpoint.address}:{endpoint.port}",
                    "--baudrate",
                    "187500",
                    "--json",
                ],
            )
            assert await run(args, stdout=stdout) == 0
            assert json.loads(stdout.getvalue())["response"]["body"] == {
                "address": 1,
                "temp": 1546,
            }

    async def sync(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        part: int,
    ) -> None:
        assert await reader.readexactly(1) == b"?"
        writer.write(b"Synchronized\r\n")
        for expected, ack in (
            (b"Synchronized", b"OK"),
            (b"12000", b"OK"),
            (b"A 0", b"0"),
        ):
            assert await reader.readline() == expected + b"\r\n"
            writer.write(expected + b"\r\n" + ack + b"\r\n")
        assert await reader.readline() == b"J\r\n"
        writer.write(f"0\r\n{part}\r\n".encode())

    async def app_revision(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        revision: str,
    ) -> None:
        assert await reader.readexactly(6) == b"?\r\nJ\r\n"
        await self.booted(reader, writer, revision)

    async def test_matching_build_skips_isp(self) -> None:
        async def app(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            await self.app_revision(reader, writer, FirmwareBundle.load().revision)
            assert not await reader.read()

        async with listen(app) as endpoint:
            assert not await install_firmware(endpoint)

    async def test_rom_auto_programs_and_verifies_boot(self) -> None:
        await self.check_auto_program(None)

    async def test_full_flash_window_and_first_oversize_byte(self) -> None:
        # Distinct page contents detect a misplaced copy within the large sector.
        image = bytearray(b"".join(bytes([n]) * 4096 for n in range(16)))
        header = self.image_path.read_bytes()
        image[: len(header)] = header
        self.image_path.write_bytes(image)
        await self.check_auto_program(None)
        self.image_path.write_bytes(image + b"\x00")
        with self.assertRaisesRegex(ProtocolError, "invalid bundled flash image"):
            FirmwareBundle.load()

    async def test_different_build_auto_programs_and_verifies_boot(self) -> None:
        await self.check_auto_program("f" * 40)

    async def test_unversioned_build_auto_programs_and_verifies_boot(self) -> None:
        await self.check_auto_program("unversioned")

    async def enter_rom(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        running: str | None,
    ) -> None:
        if running is not None:
            await self.app_revision(reader, writer, running)
            request = management(5, 0)
            assert await reader.readexactly(len(request)) == request
            writer.write(management(5, 3))
        await self.sync(reader, writer, part=0x0402FF23)

    async def check_auto_program(self, running: str | None) -> None:
        bundle = FirmwareBundle.load()
        page = 4096
        expected = bundle.image + b"\xff" * (-len(bundle.image) % page)
        programmed = bytearray(len(expected))
        copied = []
        started = False

        async def rom(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            nonlocal started
            if started:
                await self.booted(reader, writer, bundle.revision)
                return
            await self.enter_rom(reader, writer, running)
            unlocked = False
            prepared = None
            staged = bytearray()
            while cmd := await reader.readline():
                op, *args = cmd.strip().split()
                if op == b"W":
                    assert args == [b"1073745920", b"4096"]
                    staged = await self.page_data(reader, writer)
                    continue
                if op == b"U":
                    assert args == [b"23130"] and len(staged) == page
                    unlocked = True
                elif op == b"P":
                    assert unlocked
                    prepared = tuple(map(int, args))
                elif op == b"E":
                    assert (
                        prepared
                        == tuple(map(int, args))
                        == (0, _PAGE_SECTORS[len(expected) // page - 1])
                    )
                    prepared = None
                elif op == b"C":
                    addr, source, length = map(int, args)
                    sector = _PAGE_SECTORS[addr // page]
                    assert prepared == (sector, sector)
                    assert source == 0x40001000 and length == page
                    programmed[addr : addr + page] = staged
                    copied.append(addr)
                    prepared = None
                elif op == b"M":
                    addr, source, length = map(int, args)
                    skip = 0 if copied[-1] else 64
                    assert addr == copied[-1] + skip and source == 0x40001000 + skip
                    assert (
                        length == page - skip
                        and programmed[addr : addr + length] == staged[skip:]
                    )
                elif op == b"G":
                    assert (
                        args == [b"0", b"A"]
                        and programmed == expected
                        and copied[-1] == 0
                    )
                    started = True
                    return
                else:
                    self.fail(f"unexpected ISP command {op!r}")
                writer.write(b"0\r\n")

        async with (
            listen(rom) as endpoint,
            open_gateway(endpoint, auto_update=running is not None) as client,
        ):
            assert isinstance(client, BridgeClient) and started
