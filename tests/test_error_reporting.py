import asyncio
import json
import unittest
from io import StringIO

from tekmar_482 import (
    BridgeClient,
    BridgeManagement,
    ConnectionClosedError,
    ErrorReason,
    ErrorReporting,
    ErrorSource,
    GatewayErrorCounter,
    NativeFrame,
    Packet,
    ProtocolError,
    Tekmar482Client,
    TxResult,
    TxStatus,
)
from tekmar_482.cli import build_parser, run
from tekmar_482.tpck import serialize

from .test_bridge import listen, management
from .test_management import Script

_NORMAL = ("0101f04b54", "0401f04b540300050f")
_RAW = ("0101f04b54", "0401f04b540301050f")
_STATUS = ("010cf04b54", "040cf04b5404")
_STOP = ("000cf04b5400", "030cf04b5400")
_RESTORE = ("000cf04b5404", "030cf04b5404")
_CRC = "0102ffffffff0100000001000000"
_BREAK = "0583000000000100000000000000"


class ErrorReportingTests(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_snapshot_and_explicit_selection_in_either_mode(
        self,
    ) -> None:
        for caps in (_NORMAL, _RAW):
            script = Script(
                caps,
                _STATUS,
                caps,
                ("010cf04b540102", "040cf04b54" + _CRC),
                caps,
                _STOP,
            )
            async with Tekmar482Client(script) as client:
                errors = ErrorReporting(client)
                assert await errors.sources() == {ErrorSource.PORT_2}
                assert await errors.counter(
                    ErrorSource.PORT_1,
                    ErrorReason.CRC,
                ) == GatewayErrorCounter(
                    ErrorSource.PORT_1,
                    ErrorReason.CRC,
                    0xFFFFFFFF,
                    (1 << 32) + 1,
                )
                with self.assertRaises(ValueError):
                    await errors.counter(ErrorSource.GATEWAY, ErrorReason.CRC)
                await errors.set_sources([])
            assert not script.rows

    async def test_watch_fences_previous_reports_and_coalesces_only_matching_keys(
        self,
    ) -> None:
        report = "020cf04b54"
        later_crc = "0102000000000200000001000000"
        before = "0101010000000100000000000000"
        other_port = "0201010000000100000000000000"
        script = Script(
            _NORMAL,
            _STATUS,
            (
                "000cf04b5422",
                " ".join(
                    (
                        report + before,
                        "030cf04b5422",
                        report + _CRC,
                        report + _BREAK,
                        report + later_crc,
                        report + other_port,
                    ),
                ),
            ),
            _NORMAL,
            ("010cf04b540583", report + _BREAK + " 040cf04b54" + _BREAK),
            _RESTORE,
        )
        async with Tekmar482Client(script) as client, asyncio.timeout(2):
            errors = BridgeManagement(client).error_reporting
            async with errors.watch(
                [ErrorSource.PORT_1, ErrorSource.GATEWAY],
            ) as events:
                # The unsolicited Report for the very same key is not the reply.
                snapshot = await errors.counter(
                    ErrorSource.GATEWAY,
                    ErrorReason.UART_BREAK,
                )
                assert (await anext(events)).count == (1 << 32) + 2
                assert await anext(events) == snapshot
                with self.assertRaisesRegex(ProtocolError, "already has an owner"):
                    await ErrorReporting(client).set_sources([])
            with self.assertRaises(StopAsyncIteration):
                await anext(events)
        assert not script.rows

    async def test_cancel_restores_but_reconnect_never_writes_to_the_new_gateway(
        self,
    ) -> None:
        start = (_NORMAL, _STATUS, ("000cf04b5401", "030cf04b5401"))
        script = Script(*start, _RESTORE, *start)
        async with Tekmar482Client(script) as client, asyncio.timeout(2):
            errors = ErrorReporting(client)
            ready = asyncio.Event()

            async def watch() -> None:
                async with errors.watch([ErrorSource.PORT_0]) as reports:
                    ready.set()
                    await anext(reports)

            task = asyncio.create_task(watch())
            await ready.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            with self.assertRaises(ConnectionClosedError):
                async with errors.watch([ErrorSource.PORT_0]) as reports:
                    await client.close()
                    await client.open()
                    with self.assertRaises(StopAsyncIteration):
                        await anext(reports)
        assert not script.rows

    async def test_malformed_counters_do_not_kill_the_session_or_match_other_keys(
        self,
    ) -> None:
        script = Script(
            _NORMAL,
            _STATUS,
            ("000cf04b5402", "030cf04b5402 020cf04b5405ff"),
            _NORMAL,
            ("010cf04b540102", "040cf04b54" + _BREAK),
            _RESTORE,
        )
        async with Tekmar482Client(script) as client, asyncio.timeout(2):
            errors = ErrorReporting(client)
            async with errors.watch([ErrorSource.PORT_1]) as reports:
                with self.assertRaisesRegex(
                    ProtocolError,
                    "invalid gateway error counter",
                ):
                    await anext(reports)
                with self.assertRaisesRegex(ProtocolError, "different error counter"):
                    await errors.counter(ErrorSource.PORT_1, ErrorReason.CRC)
        for invalid in ("", "0502" + "00" * 12, "0180" + "00" * 12):
            with self.assertRaises(ProtocolError):
                GatewayErrorCounter.decode(bytes.fromhex(invalid))
        assert not script.rows

    async def test_firmware_without_the_feature_receives_only_a_probe(self) -> None:
        for reply in ("0400000000", "0401f04b5403010507"):
            script = Script(("0101f04b54", reply))
            async with Tekmar482Client(script) as client:
                with self.assertRaisesRegex(ProtocolError, "does not advertise"):
                    async with ErrorReporting(client).watch():
                        self.fail("unsupported subscription")
            assert not script.rows

    async def test_raw_capture_tx_result_and_diagnostic_share_one_connection(
        self,
    ) -> None:
        body = bytes.fromhex("12300000")
        caps = management(1, 1)
        frame = serialize(Packet(1, bytes(6) + body))
        report = management(12, 2, bytes.fromhex(_CRC))
        result = serialize(Packet(1, bytes.fromhex("020100")))

        async def app(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            for request, reply in (
                (b"\xca" * 301 + caps, management(1, 4, bytes.fromhex("0300050f"))),
                (management(2, 0, b"\x01\x01"), management(2, 3, b"\x01\x01")),
                (management(9, 0, b"\0"), management(9, 3, b"\0")),
                (caps, management(1, 4, bytes.fromhex("0301050f"))),
                (management(12, 1), management(12, 4, b"\0")),
                (management(12, 0, b"\x02"), management(12, 3, b"\x02")),
                (
                    serialize(Packet(1, b"\0\x01" + bytes(4) + body)),
                    frame + report + result,
                ),
                (management(12, 0, b"\0"), management(12, 3, b"\0")),
                (management(2, 0, bytes(2)), management(2, 3, bytes(2))),
            ):
                assert await reader.readexactly(len(request)) == request
                writer.write(reply)

        async with (
            listen(app) as endpoint,
            endpoint.connect() as wire,
            asyncio.timeout(3),
        ):
            bridge = BridgeClient(wire)
            await bridge.identify()
            async with (
                bridge.stream(diagnostics=False) as stream,
                stream.error_reporting.watch([ErrorSource.PORT_1]) as events,
            ):
                assert await stream.send(1, body) == TxResult(1, TxStatus.ACK)
                assert await stream.read() == NativeFrame(0, 0, body)
                assert (await anext(events)).count == (1 << 32) + 1

    async def test_cli_attaches_to_raw_without_selecting_mode_or_setting_acks(
        self,
    ) -> None:
        caps = management(1, 1)
        reply = management(1, 4, bytes.fromhex("0301050f"))

        async def app(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            first = await reader.readexactly(1)
            if first == b"?":
                assert await reader.readexactly(5) == b"\r\nJ\r\n"
                assert not await reader.read()
                return
            assert first + await reader.readexactly(len(caps) - 1) == caps
            writer.write(reply)
            for req, response in (
                (caps, reply),
                (management(12, 1), management(12, 4, b"\x01")),
                (
                    management(12, 0, b"\x20"),
                    management(12, 3, b"\x20")
                    + management(12, 2, bytes.fromhex(_BREAK)),
                ),
                (management(12, 0, b"\x01"), management(12, 3, b"\x01")),
            ):
                assert await reader.readexactly(len(req)) == req
                writer.write(response)

        async with listen(app) as endpoint, asyncio.timeout(3):
            args = build_parser().parse_args(
                [
                    "errors",
                    "--tcp",
                    f"127.0.0.1:{endpoint.port}",
                    "--duration",
                    "0.05",
                    "--source",
                    "gateway",
                    "--json",
                ],
            )
            stdout = StringIO()
            assert await run(args, stdout=stdout) == 0
            assert json.loads(stdout.getvalue()) == {
                "source": "gateway",
                "reason": "uart_break",
                "last_seen_ms": 0,
                "count": 1,
            }
