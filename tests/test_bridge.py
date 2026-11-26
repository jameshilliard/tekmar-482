from __future__ import annotations

import asyncio
import binascii
import json
import struct
import unittest
from contextlib import asynccontextmanager
from functools import partial
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


RAW_CAPABILITIES = management(1, 4, bytes([2, 0, 5, 3]))


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
        writer.write(RAW_CAPABILITIES)
        request = management(4, 1)
        if first := await reader.read(1):
            assert first + await reader.readexactly(len(request) - 1) == request
            writer.write(management(4, 4, revision.encode()))

    async def test_custom_panel_stream_and_echo(self) -> None:
        high = 1 << 40

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
            writer.write(RAW_CAPABILITIES)
            for method, request, reply in [
                (
                    7,
                    management(7, 1),
                    management(
                        7,
                        4,
                        struct.pack("<2I2Q3I", 1, 1, high, high + 1, 0, 0, 0),
                    ),
                ),
                (
                    10,
                    management(10, 1),
                    management(
                        10,
                        4,
                        struct.pack("<2I5Q5I", 0xFFFFFFFF, 4, *([3] * 5), *([0] * 5)),
                    ),
                ),
                (
                    10,
                    management(10, 1),
                    management(
                        10,
                        4,
                        struct.pack("<2I5Q5I", 2, 9, 3, 3, high + 3, 3, 3, *range(5)),
                    ),
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
                writer.write(
                    management(3, 4, struct.pack("<BQQ", port, high + port, high + 1)),
                )

        async with listen(app) as endpoint, open_gateway(endpoint) as client:
            assert isinstance(client, BridgeClient)
            panel = await client.panel()
            assert (panel.inputs, panel.test_presses, panel.dip_changes) == (
                1,
                high,
                high + 1,
            )
            assert await client.probe(duration=0.001) == (3, 5, 4, 0, 1, 2, 3, 4)
            async with client.stream() as stream:
                result = await stream.send(0, bytes.fromhex("12300001a5"))
                assert result == TxResult(0, TxStatus.ACK)
                frame = await stream.read()
                assert (
                    isinstance(frame, NativeFrame)
                    and frame.message_id == 0x123
                    and frame.decoded == {}
                )
            assert stream.diagnostics[0].invalid_dropped == high + 1
            assert stream.diagnostics[4].valid_dropped == high + 4
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
                    RAW_CAPABILITIES,
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
            writer.write(
                management(6, 3, struct.pack("<IB", 0x544BF002, 1))
                + serialize(Packet(1, b"\x02\x01\0")),
            )
            assert await reader.readexactly(len(normal)) == normal
            writer.write(management(2, 3, b"\0"))
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
            writer.write(RAW_CAPABILITIES)
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

    async def test_cli_read_block_reports_the_selected_sensor_over_tcp(self) -> None:
        request = bytes.fromhex("6d70000b c60012 04000000 05000000")
        report = bytes.fromhex("6e7128c6 c60007 053412")

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
            writer.write(RAW_CAPABILITIES)
            for cmd, reply in (
                (management(2, 0, b"\x01"), management(2, 3, b"\x01")),
                (management(9, 0, b"\x04"), management(9, 3, b"\x04")),
                (
                    serialize(Packet(1, b"\x01\x02" + request)),
                    serialize(Packet(1, b"\x02\x02\0"))
                    + serialize(Packet(1, bytes.fromhex("000207000000") + report)),
                ),
                (management(2, 0, b"\0"), management(2, 3, b"\0")),
            ):
                assert await reader.readexactly(len(cmd)) == cmd
                writer.write(reply)

        async with listen(app) as endpoint, asyncio.timeout(3):
            stdout = StringIO()
            args = build_parser().parse_args(
                [
                    "read-block",
                    "2",
                    "0x12",
                    "3",
                    "0xc6",
                    "--selector",
                    "5",
                    "--ack",
                    "--tcp",
                    f"{endpoint.address}:{endpoint.port}",
                    "--json",
                ],
            )
            assert args.timeout == 20
            assert await run(args, stdout=stdout) == 0
            result = json.loads(stdout.getvalue())
            assert result["port"] == 2 and result["timestamp_ms"] == 7
            assert result["body"] == report.hex()
            assert result["fields"]["sensor_id"] == 5
            assert result["fields"]["slab_high_raw"] == 0x1234

    async def test_cli_write_previews_by_default_and_returns_actual_readback(
        self,
    ) -> None:
        request = bytes.fromhex("6d70000b 050012 04000000 00000000")
        update = bytes.fromhex("6df00009 1203 0500 112233442f")
        before = bytes.fromhex("6e7128c8 05001f 1122334455")
        clamped = bytes.fromhex("6e7128c8 05001f 112233442e")
        ack = serialize(Packet(1, bytes.fromhex("020100")))

        def capture(body: bytes) -> bytes:
            return serialize(Packet(1, bytes.fromhex("000107000000") + body))

        async def exchange(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
            *,
            confirm: bool,
        ) -> None:
            if (first := await reader.readexactly(1)) == b"?":
                assert await reader.readexactly(5) == b"\r\nJ\r\n"
                assert not await reader.read()
                return
            caps = management(1, 1)
            assert first + await reader.readexactly(len(caps) - 1) == caps
            writer.write(RAW_CAPABILITIES)
            steps = [
                (management(2, 0, b"\x01"), management(2, 3, b"\x01")),
                (management(9, 0, b"\0"), management(9, 3, b"\0")),
                (serialize(Packet(1, b"\x01\x01" + request)), ack + capture(before)),
            ]
            if confirm:
                steps.extend(
                    (
                        (serialize(Packet(1, b"\x01\x01" + update)), ack),
                        (
                            serialize(Packet(1, b"\x01\x01" + request)),
                            capture(clamped) + ack,
                        ),
                    ),
                )
            steps.append((management(2, 0, b"\0"), management(2, 3, b"\0")))
            for outgoing, incoming in steps:
                assert await reader.readexactly(len(outgoing)) == outgoing
                writer.write(incoming)

        for confirm in (False, True):
            async with (
                listen(partial(exchange, confirm=confirm)) as endpoint,
                asyncio.timeout(3),
            ):
                stdout = StringIO()
                args = build_parser().parse_args(
                    [
                        "write-block",
                        "1",
                        "0x12",
                        "3",
                        "0x05",
                        "away=47",
                        "--tcp",
                        f"{endpoint.address}:{endpoint.port}",
                        "--json",
                        *(["--confirm"] if confirm else []),
                    ],
                )
                assert await run(args, stdout=stdout) == int(confirm)
                result = json.loads(stdout.getvalue())
                assert result["proposed"] == update.hex()
                assert result["before"]["body"] == before.hex()
                if confirm:
                    assert (
                        result["status"] == "different" and result["tx_status"] == "ACK"
                    )
                    assert result["readback"]["reports"][0]["body"] == clamped.hex()
                else:
                    assert result["status"] == "preview"
                    assert result["tx_status"] is None and result["readback"] is None

    async def test_cli_snowmelt_reads_configuration_and_reports_missing_status_keys(
        self,
    ) -> None:
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
            writer.write(RAW_CAPABILITIES)
            mode = management(2, 0, b"\x01"), management(2, 3, b"\x01")
            mask = management(9, 0, b"\0"), management(9, 3, b"\0")
            steps = [mode, mask]
            for outgoing, report in (
                ("6d70000b d00014 04000000 00000000", "6e7148c5 d000030400"),
                ("6d70000b c10014 04000000 00000000", "6e7148c4 c1000108"),
                ("17f00004 3a021922", "57e148c5 4201190200"),
            ):
                steps.append(
                    (
                        serialize(Packet(1, bytes([1, 1]) + bytes.fromhex(outgoing))),
                        serialize(Packet(1, bytes([2, 1, 0])))
                        + serialize(
                            Packet(
                                1,
                                bytes([0, 1, 1, 0, 0, 0]) + bytes.fromhex(report),
                            ),
                        ),
                    ),
                )
            steps.append((management(2, 0, b"\0"), management(2, 3, b"\0")))
            for command, reply in steps:
                assert await reader.readexactly(len(command)) == command
                writer.write(reply)

        async with listen(app) as endpoint, asyncio.timeout(4):
            out = StringIO()
            args = build_parser().parse_args(
                [
                    "read-status",
                    "snowmelt",
                    "1",
                    "0x14",
                    "3",
                    "--keys",
                    "1922",
                    "--timeout",
                    "0.12",
                    "--json",
                    "--tcp",
                    f"{endpoint.address}:{endpoint.port}",
                ],
            )
            assert await run(args, stdout=out) == 1
            answer = json.loads(out.getvalue())["status_read"]
            assert not answer["answered"] and answer["missing_keys"] == [0x22]
            assert answer["tx_status"] == "ACK" and answer["service"] == "snowmelt"
            assert answer["reports"][0]["fields"]["pair0_water_detect_state"] == 2

    async def test_cli_io_directory_queries_exact_payload_without_a_block_address(
        self,
    ) -> None:
        async def app(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            first = await reader.readexactly(1)
            if first == b"?":
                assert await reader.readexactly(5) == b"\r\nJ\r\n"
                assert not await reader.read()
                return
            caps = management(1, 1)
            assert first + await reader.readexactly(len(caps) - 1) == caps
            writer.write(RAW_CAPABILITIES)
            request = serialize(Packet(1, bytes.fromhex("0103 1f600002 43a5")))
            response = serialize(
                Packet(
                    1,
                    bytes.fromhex(
                        "0003 22000000 5f58100e 43a5 e701 410042caff00 08 3412 e4",
                    ),
                ),
            )
            for outgoing, incoming in (
                (management(2, 0, b"\x01"), management(2, 3, b"\x01")),
                (management(9, 0, b"\0"), management(9, 3, b"\0")),
                (request, response + serialize(Packet(1, bytes([2, 3, 0])))),
                (management(2, 0, b"\0"), management(2, 3, b"\0")),
            ):
                assert await reader.readexactly(len(outgoing)) == outgoing
                writer.write(incoming)

        async with listen(app) as endpoint, asyncio.timeout(5):
            for command in (
                [
                    "read-status",
                    "io-discovery",
                    "3",
                    "0x43",
                    "0xa5",
                    "--peer",
                    "0x81",
                    "0",
                ],
                ["read-device", "3", "0x81", "0", "--io-endpoint", "0x43", "0xa5"],
            ):
                out = StringIO()
                args = build_parser().parse_args(
                    [
                        *command,
                        "--json",
                        "--tcp",
                        f"{endpoint.address}:{endpoint.port}",
                    ],
                )
                assert await run(args, stdout=out) == 0
                rows = [json.loads(line) for line in out.getvalue().splitlines()]
                view = rows[0]["status_read"]["io_directory"]
                assert view["complete"] and view["endpoint"] == {
                    "subnet": 0x43,
                    "node": 0xA5,
                }
                (only,) = view["channels"]
                assert (only["channel_id"], only["io_value"], only["label_hex"]) == (
                    0xE7,
                    0x1234,
                    "410042caff00",
                )
                assert only["body"].endswith("e4")
                if command[0] == "read-device":
                    assert rows[1]["io_directory"] == {
                        "port": 3,
                        "subnet": 0x81,
                        "node": 0,
                        **view,
                    }

    async def test_cli_discovery_probes_quiet_subnet_and_preserves_ack_mask(
        self,
    ) -> None:
        request = bytes.fromhex("6d70000b 000016 ffffff7f 00000000")
        report = (
            bytes.fromhex("6e7161c8 00001f")
            + (101901).to_bytes(3, "little")
            + (1145).to_bytes(2, "little")
        )

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
            writer.write(RAW_CAPABILITIES)
            for command, response in (
                (management(2, 0, b"\x01"), management(2, 3, b"\x01")),
                (management(9, 0, b"\x12"), management(9, 3, b"\x12")),
                (
                    serialize(Packet(1, b"\x01\x04" + request)),
                    serialize(Packet(1, bytes.fromhex("00040a000000") + report))
                    + serialize(Packet(1, b"\x02\x04\0")),
                ),
                (management(2, 0, b"\0"), management(2, 3, b"\0")),
            ):
                assert await reader.readexactly(len(command)) == command
                writer.write(response)
            for port in range(5):
                query = management(3, 1, bytes([port]))
                assert await reader.readexactly(len(query)) == query
                writer.write(management(3, 4, bytes([port]) + bytes(16)))

        async with listen(app) as endpoint, asyncio.timeout(5):
            stdout = StringIO()
            args = build_parser().parse_args(
                [
                    "discover",
                    "--probe-subnet",
                    "4:0x16",
                    "--ack-port",
                    "1",
                    "--ack-port",
                    "4",
                    "--duration",
                    "0.2",
                    "--tcp",
                    f"{endpoint.address}:{endpoint.port}",
                    "--json",
                ],
            )
            assert await run(args, stdout=stdout) == 0
            summary = json.loads(stdout.getvalue())
            assert summary["observed_nodes"] == [[4, 0x16, 7]]
            assert summary["devices"][0]["model"] == "481"
            assert summary["devices"][0]["reported_blocks"] == [0]
            assert summary["identity_probes"] == [
                {"port": 4, "subnet": 0x16, "tx_status": "ACK", "reason": None},
            ]

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

    async def test_native_dump_requests_catalog_blocks_only_when_active(self) -> None:
        prefix = bytes.fromhex("00030a000000")
        identity = (
            bytes.fromhex("6e7161c8 00001f")
            + (101901).to_bytes(3, "little")
            + (1145).to_bytes(2, "little")
        )
        rx_identity, rx_other, rx_fault, rx_group = (
            serialize(Packet(1, prefix + body))
            for body in (
                identity,
                bytes.fromhex("6e7161c4 770001 13"),
                bytes.fromhex("7d7161c2 8fe1"),
                bytes.fromhex("60f161c3 0410ca"),
            )
        )
        result = serialize(Packet(1, bytes([2, 3, 0])))
        mode = management(2, 0, b"\x01"), management(2, 3, b"\x01")
        stop = management(2, 0, b"\0"), management(2, 3, b"\0")
        ack = management(9, 0, b"\x08"), management(9, 3, b"\x08")
        stats = [
            (management(3, 1, bytes([p])), management(3, 4, bytes([p]) + bytes(16)))
            for p in range(5)
        ]

        def request(block: int, mask: int) -> bytes:
            body = (
                bytes.fromhex("6d70000b")
                + bytes([block, 0, 0x16])
                + mask.to_bytes(4, "little")
                + bytes(4)
            )
            return serialize(Packet(1, bytes([1, 3]) + body))

        async def simulate(*, passive: bool) -> list[dict[str, object]]:
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
                writer.write(RAW_CAPABILITIES)
                steps = [
                    (management(7, 1), management(7, 4, bytes(36))),
                    (management(4, 1), management(4, 4, b"1" * 40)),
                    mode,
                    (ack[0], ack[1] + rx_identity + rx_other + rx_fault + rx_group),
                ]
                if not passive:
                    steps.append((request(0, (1 << 31) - 1), result))
                steps.extend([stop, *stats])
                if not passive:
                    steps.extend(
                        [
                            mode,
                            ack,
                            (request(0, 64), rx_identity + result),
                            (request(0x77, 64), result + rx_other),
                        ],
                    )
                    for query, report_hex in (
                        ("3f700000", "7ff161c2 1604"),
                        ("32700000", "72f161c2 2204"),
                        ("31700000", "71f161c5 2241040019"),
                        ("30700000", "70f161c1 02"),
                        ("01100000", "410161c3 000102"),
                        ("20000001 01", "600161c1 02"),
                    ):
                        steps.append(
                            (
                                serialize(
                                    Packet(1, bytes([1, 3]) + bytes.fromhex(query)),
                                ),
                                result
                                + serialize(
                                    Packet(1, prefix + bytes.fromhex(report_hex)),
                                ),
                            ),
                        )
                    steps.extend([stop, *stats])
                for command, response in steps:
                    assert await reader.readexactly(len(command)) == command
                    writer.write(response)

            async with listen(app) as endpoint, asyncio.timeout(4):
                args = build_parser().parse_args(
                    [
                        "dump",
                        "--ack-port",
                        "3",
                        "--duration",
                        "0.10",
                        "--block-timeout",
                        "0.2",
                        "--tcp",
                        f"{endpoint.address}:{endpoint.port}",
                        "--json",
                        *(["--passive"] if passive else []),
                    ],
                )
                out = StringIO()
                assert await run(args, stdout=out) == 0
                faults = [
                    f
                    for line in out.getvalue().splitlines()
                    for f in json.loads(line).get("network_fault_reports", [])
                ]
                assert [(f["port"], f["node"], f["raw"]) for f in faults] == [
                    (3, 7, 0xE18F),
                ]
                windows = [
                    row["line_reports"]
                    for row in map(json.loads, out.getvalue().splitlines())
                    if "line_reports" in row
                ]
                assert len(windows) == (1 if passive else 2)
                (group,) = windows[0]
                assert (
                    group["group_id"],
                    group["actual_enable"],
                    group["extension_hex"],
                ) == (4, 1, "ca")
                if not passive:
                    assert {r["kind"] for r in windows[1]} == {
                        "outdoor",
                        "wwsd",
                        "zone_sync",
                        "clock",
                        "gateway_access",
                    }
                telemetry = [
                    json.loads(line)["status_read"]
                    for line in out.getvalue().splitlines()
                    if "status_read" in line
                ]
                assert len(telemetry) == (0 if passive else 6)
                assert all(
                    entry["answered"] and entry["reports"] for entry in telemetry
                )
                return [
                    json.loads(line)["block_read"]
                    for line in out.getvalue().splitlines()
                    if "block_read" in line
                ]

        assert not await simulate(passive=True)
        reads = await simulate(passive=False)
        assert [r["block"] for r in reads] == [0, 0x77]
        assert all(r["missing_nodes"] == [] and r["tx_status"] == "ACK" for r in reads)

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

    async def test_retired_custom_wire_version_is_not_programmed(self) -> None:
        async def app(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            assert await reader.readexactly(6) == b"?\r\nJ\r\n"
            probe = b"\xca" * 301 + management(1, 1)
            assert await reader.readexactly(len(probe)) == probe
            writer.write(management(1, 4, bytes([1, 0, 5, 7])))
            assert not await reader.read()

        async with listen(app) as endpoint:
            with self.assertRaisesRegex(
                ProtocolError,
                "unsupported custom bridge wire version 1",
            ):
                await install_firmware(endpoint)

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
            writer.write(report + RAW_CAPABILITIES)
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
