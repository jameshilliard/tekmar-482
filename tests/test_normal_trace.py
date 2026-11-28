import asyncio
import json
import struct
import unittest
from io import StringIO

from tekmar_482 import (
    BridgeManagement,
    ConnectionClosedError,
    GatewayTraceGap,
    NativeFrame,
    NormalTrace,
    NormalTraceState,
    ProtocolError,
    RxFlag,
    Tekmar482Client,
    TraceOrigin,
    TracePacket,
    TraceRx,
    TraceTx,
    TrpcPacket,
)
from tekmar_482.cli import build_parser, run
from tekmar_482.packet import Packet
from tekmar_482.raw import (
    CaptureGap,
    FailureReason,
    InvalidCapture,
    InvalidReason,
    decode_raw_packet,
)
from tekmar_482.tpck import serialize

from .test_bridge import listen, management
from .test_management import Script


class NormalTraceTests(unittest.IsolatedAsyncioTestCase):
    def test_decode_independent_wire_vectors_without_losing_unknown_bytes(self) -> None:
        def decode(
            data: str,
        ) -> TraceRx | TraceTx | InvalidCapture | GatewayTraceGap | TrpcPacket | None:
            return TracePacket(
                42,
                outbound=False,
                packet=Packet(0xD0, bytes.fromhex(data)),
            ).decoded

        capture = "00 02 01000000 76f00608 00032b0a06000000 03"
        rx = decode(capture)
        assert isinstance(rx, TraceRx) and rx.flags == RxFlag.ACK | RxFlag.EARLY_START
        assert rx.frame.port == 2 and rx.frame.timestamp_ms == 1
        assert rx.frame.message_id == 0x36F
        raw = decode_raw_packet(Packet(1, bytes.fromhex(capture)))
        assert isinstance(raw, NativeFrame)
        assert raw == rx.frame and raw.rx_flags == rx.flags
        base = decode_raw_packet(Packet(1, bytes.fromhex(capture)[:-1]))
        assert isinstance(base, NativeFrame) and base == raw and base.rx_flags is None
        tx = decode("01 00 ffffffff 6d700007 03000000008000 03 02")
        assert isinstance(tx, TraceTx) and tx.origin is TraceOrigin.PROBE
        assert tx.failure is FailureReason.ARBITRATION_LOST
        assert tx.frame.message_id == 0x2D7 and tx.frame.timestamp_ms == 0xFFFFFFFF
        ack = decode("01 00 00000000 61f00002 0c01 01 00")
        assert isinstance(ack, TraceTx) and ack.failure is None
        invalid = "04 03 02000000 02 0900 8080"
        assert decode(invalid) == decode_raw_packet(Packet(1, bytes.fromhex(invalid)))
        assert decode(invalid) == InvalidCapture(
            3,
            2,
            InvalidReason.CRC,
            9,
            bytes.fromhex("8080"),
        )
        assert decode("05 15 ffffffffffffffff") == GatewayTraceGap((1 << 64) - 1, 0x15)
        for data in (
            "00",
            "00 05 00000000 76f00600 00",
            capture[:-3],
            capture + " 00",
            "00 02 01000000 03 76f00608 00032b0a06000000",  # Retired prefix flags.
            "ff 00 00000000",
            "04 03 02000000 02 0900 80",
            "04 03 02000000 02 0900 8081",
            "05 00 0100000000000000",
            "05 20 0100000000000000",
            "05 11 0100000000000000",
            "05 01 0000000000000000",
            "05 01 01000000",
            "05 01 010000000000000000",
            "01 02 00000000 12300004 00 00",
        ):
            assert decode(data) is None

    async def test_bounded_observer_drains_before_disconnect(self) -> None:
        trace = NormalTrace(1)
        sample = Packet(6, bytes.fromhex("0207010000 0000"))
        trace.record(sample, outbound=False)
        trace.record(sample, outbound=True)
        trace.finish(ConnectionError("wire lost"))
        assert await anext(trace) == CaptureGap(1) and trace.host_drops == 1
        row = await anext(trace)
        assert isinstance(row, TracePacket) and row.outbound and row.packet == sample
        with self.assertRaisesRegex(ConnectionError, "wire lost"):
            await anext(trace)

    async def test_refuses_stock_old_and_raw_without_changing_modes(self) -> None:
        for caps, reason in (
            ("0400000000", "Normal trace requires"),
            ("0401f04b5404000503", "Normal trace requires"),
            ("0401f04b5404010507", "Normal trace requires"),
            ("0401f04b5403000507", "unsupported custom bridge wire version"),
            ("0401f04b5401000507", "unsupported custom bridge wire version"),
        ):
            script = Script(("0101f04b54", caps))
            async with Tekmar482Client(script) as client:
                with self.assertRaisesRegex(ProtocolError, reason):
                    async with BridgeManagement(client).trace():
                        self.fail("unexpected trace")
            assert not script.rows

    async def test_owned_scope_restores_previous_mask_on_cancellation(self) -> None:
        state = "040bf04b5402" + "00" * 40
        script = Script(
            ("0101f04b54", "0401f04b5404000507"),
            ("010bf04b54", state),
            ("000bf04b5411", "030bf04b5411"),
            ("000bf04b5402", "030bf04b5402"),
            ("010bf04b54", state),
        )
        ready = asyncio.Event()
        async with Tekmar482Client(script) as client:
            management = BridgeManagement(client)

            async def debugger() -> None:
                async with management.trace([0, 4]):
                    ready.set()
                    await asyncio.Event().wait()

            task = asyncio.create_task(debugger())
            async with asyncio.timeout(1):
                await ready.wait()
            with self.assertRaisesRegex(ProtocolError, "already owns"):
                async with management.trace([0]):
                    self.fail("overlap admitted")
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        assert not script.rows

    async def test_inspect_and_stop_orphaned_trace_without_switching_modes(
        self,
    ) -> None:
        high = 1 << 42
        counters = struct.pack("<5Q", 4, 0, high, 0, 8).hex()
        caps = ("0101f04b54", "0401f04b5404000507")
        script = Script(
            caps,
            ("010bf04b54", "040bf04b5411" + counters),
            caps,
            ("000bf04b5400", "030bf04b5400"),
            ("010bf04b54", "040bf04b5400" + counters),
        )
        async with Tekmar482Client(script) as client:
            manager = BridgeManagement(client)
            assert await manager.trace_state() == NormalTraceState(
                17,
                (4, 0, high, 0, 8),
            )
            assert await manager.stop_trace() == NormalTraceState(0, (4, 0, high, 0, 8))
        assert not script.rows

    async def test_wide_trace_delta_is_not_truncated_or_wrapped_on_reset(self) -> None:
        high = 1 << 48
        for last in (9, high * 2):
            script = Script(
                ("0101f04b54", "0401f04b5404000507"),
                (
                    "010bf04b54",
                    "040bf04b5400" + struct.pack("<5Q", high, 0, 0, 0, 0).hex(),
                ),
                ("000bf04b5401", "030bf04b5401"),
                ("000bf04b5400", "030bf04b5400"),
                (
                    "010bf04b54",
                    "040bf04b5400" + struct.pack("<5Q", last, 0, 0, 0, 0).hex(),
                ),
            )
            async with Tekmar482Client(script) as client:
                manager = BridgeManagement(client)
                if last < high:
                    with self.assertRaisesRegex(ProtocolError, "decreased"):
                        async with manager.trace([0]):
                            pass
                else:
                    async with manager.trace([0]) as trace:
                        pass
                    assert trace.gateway_drops == (high, 0, 0, 0, 0)
            assert not script.rows

    async def test_wide_firmware_rejects_truncated_trace_counters(self) -> None:
        script = Script(
            ("0101f04b54", "0401f04b5404000507"),
            ("010bf04b54", "040bf04b54" + "00" * 21),
        )
        async with Tekmar482Client(script) as client:
            with self.assertRaisesRegex(ProtocolError, "diagnostic length"):
                await BridgeManagement(client).trace_state()

    async def test_reconnect_does_not_restore_a_mask_to_the_new_gateway(self) -> None:
        script = Script(
            ("0101f04b54", "0401f04b5404000507"),
            ("010bf04b54", "040bf04b54" + "00" * 41),
            ("000bf04b5401", "030bf04b5401"),
        )
        async with Tekmar482Client(script) as client:
            with self.assertRaises(ConnectionClosedError):
                async with BridgeManagement(client).trace([0]):
                    await client.close()
                    await client.open()
        assert not script.rows

    async def test_cli_trace_shares_normal_replies_and_drains_both_directions(
        self,
    ) -> None:
        for request in (False, True):

            async def app(
                reader: asyncio.StreamReader,
                writer: asyncio.StreamWriter,
                *,
                requesting: bool = request,
            ) -> None:
                first = await reader.readexactly(1)
                if first == b"?":
                    assert await reader.readexactly(5) == b"\r\nJ\r\n"
                    assert not await reader.read()
                    return
                caps = management(1, 1)
                assert first + await reader.readexactly(len(caps) - 1) == caps
                caps_reply = management(1, 4, bytes.fromhex("04000507"))
                writer.write(caps_reply)
                before = bytes(1) + struct.pack("<5Q", 0, 0, 0xFFFFFFFF, 0, 0)
                after = bytes(1) + struct.pack("<5Q", 0, 0, 0x100000001, 0, 0)
                rx = serialize(
                    Packet(
                        0xD0,
                        bytes.fromhex("00 02 07000000 76f00608 00032b0a06000000 01"),
                    ),
                )
                tx = serialize(
                    Packet(
                        0xD0,
                        bytes.fromhex("01 02 08000000 6d700007 03000000008000 03 00"),
                    ),
                )
                report = serialize(Packet(6, bytes.fromhex("023f010000 0100002b")))
                steps = [
                    (caps, caps_reply),
                    (management(11, 1), management(11, 4, before)),
                    (
                        management(11, 0, b"\x04"),
                        management(11, 3, b"\x04") + rx + report,
                    ),
                ]
                if requesting:
                    steps.append(
                        (
                            serialize(Packet(6, bytes.fromhex("013f010000 01000700"))),
                            serialize(Packet(6, bytes.fromhex("043f010000 0100072b"))),
                        ),
                    )
                steps.extend(
                    [
                        (management(11, 0, b"\0"), tx + management(11, 3, b"\0")),
                        (management(11, 1), management(11, 4, after)),
                    ],
                )
                for expected, reply in steps:
                    assert await reader.readexactly(len(expected)) == expected
                    writer.write(reply)
                await reader.read()

            async with listen(app) as endpoint, asyncio.timeout(4):
                args = build_parser().parse_args(
                    [
                        *(
                            [
                                "request",
                                "HeatSetpoint",
                                "address=1",
                                "setback=7",
                                "--trace-port",
                                "2",
                            ]
                            if request
                            else ["trace-normal", "--port", "2", "--duration", "0.03"]
                        ),
                        "--tcp",
                        f"{endpoint.address}:{endpoint.port}",
                        "--json",
                    ],
                )
                output = StringIO()
                assert await run(args, stdout=output) == 0
            rows = [json.loads(line) for line in output.getvalue().splitlines()]
            assert rows[-1] == {
                "trace_drops": {"gateway_by_port": [0, 0, 2, 0, 0], "host": 0},
            }
            native = [r for r in rows if r.get("layer") == "tn4"]
            assert [(r["direction"], r["message_id"]) for r in native] == [
                ("rx", 0x36F),
                ("tx", 0x2D7),
            ]
            assert (
                native[0]["ack"]
                and native[1]["origin"] == "probe"
                and native[1]["outcome"] == "ack"
            )
            sides = [
                r["direction"]
                for r in rows
                if r.get("layer") == "trpc" and r["method"] == "HeatSetpoint"
            ]
            assert sides == (
                ["host_rx", "host_tx", "host_rx"] if request else ["host_rx"]
            )
