import asyncio
import json
import unittest
from io import StringIO

from tekmar_482 import (
    FailureReason,
    NativeAddress,
    NativeFrame,
    NativeMessage,
    NativePortBusyError,
    NativeScene,
    NativeWriteStatus,
    Packet,
    RejectReason,
    ScenePortResult,
    SceneReport,
    SceneResponse,
    SceneSelection,
    SceneSendIssue,
    SceneSetup,
    TxResult,
    TxStatus,
    write_scene_response,
)
from tekmar_482.cli import CliError, build_parser, run
from tekmar_482.raw import RawDispatcher
from tekmar_482.tpck import serialize

from .test_bridge import RAW_CAPABILITIES, listen, management

_HOST = NativeAddress(0, 0)
_PEER = NativeAddress(0x13, 7)
_TEMP = NativeScene.TEMPORARY_OCCUPIED
_SCENE_BODY = bytes.fromhex("2ff00003 05b400")


class NativeSceneTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tx: asyncio.Queue[bytes] = asyncio.Queue()
        rx: asyncio.Queue[Packet] = asyncio.Queue()
        self.stream = RawDispatcher(self.tx.put, 16)
        await self.enterAsyncContext(self.stream.receiving(rx.get))

    async def outgoing(self) -> bytes:
        async with asyncio.timeout(1):
            return await self.tx.get()

    def result(self, port: int, *codes: int) -> None:
        self.stream.feed(Packet(1, bytes([2, port, *codes])))

    def capture(
        self,
        mid: int,
        data: str,
        *,
        peer: NativeAddress = _PEER,
    ) -> NativeFrame:
        body = NativeMessage(mid, peer, bytes.fromhex(data)).to_bytes()
        self.stream.feed(Packet(1, bytes([0, 2, 3, 0, 0, 0]) + body + bytes(1)))
        return NativeFrame(2, 3, body)

    async def test_independent_port_results_and_capture_of_own_echo(self) -> None:
        selection = SceneSelection(_TEMP, (3, 1, 2, 0), _HOST)
        busy = asyncio.create_task(
            self.stream.send(1, NativeMessage.request_clock(source=_HOST)),
        )
        await self.outgoing()
        task = asyncio.create_task(selection.send(self.stream))
        self.assertCountEqual(
            [await self.outgoing() for _ in range(3)],
            [bytes([0, p]) + bytes(4) + _SCENE_BODY for p in (3, 2, 0)],
        )
        echo = self.capture(0x2FF, "05b400", peer=_HOST)
        self.result(2, TxStatus.FAILED, FailureReason.NO_ACK)
        self.result(3, TxStatus.ACK)
        self.result(0, TxStatus.REJECTED, RejectReason.PIN_UNAVAILABLE)
        assert await task == (
            ScenePortResult(3, TxResult(3, TxStatus.ACK)),
            ScenePortResult(1, SceneSendIssue.PORT_BUSY),
            ScenePortResult(2, TxResult(2, TxStatus.FAILED, FailureReason.NO_ACK)),
            ScenePortResult(
                0,
                TxResult(0, TxStatus.REJECTED, RejectReason.PIN_UNAVAILABLE),
            ),
        )
        assert await self.stream.read() == echo
        assert echo.scene_report == SceneReport(5, 180)
        self.result(1, TxStatus.ACK)
        assert (await busy).status is TxStatus.ACK
        assert self.tx.empty()

    async def test_timeout_and_cancellation_do_not_retry_or_release_wire_slot(
        self,
    ) -> None:
        one = SceneSelection(NativeScene.NORMAL, (1,), _HOST)
        for cancel in (True, False):
            task = asyncio.create_task(one.send(self.stream, timeout=0.05))
            assert await self.outgoing() == bytes.fromhex(
                "0001 00000000 2ff00003 000000",
            )
            if cancel:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            else:
                assert await task == (ScenePortResult(1, SceneSendIssue.DEADLINE),)
            with self.assertRaises(NativePortBusyError):
                await self.stream.send(1, one.message)
            self.result(1, TxStatus.ACK)
            self.stream.check_drained()
        assert self.tx.empty()

    async def test_unknown_partial_and_unsupported_scene_fields(self) -> None:
        assert self.capture(0x2FF, "054500ab").scene_report == SceneReport(
            5,
            69,
            b"\xab",
        )
        unknown = self.capture(0x2FF, "b00107").scene_report
        assert (
            unknown is not None
            and unknown.scene is None
            and unknown.duration_minutes == 1793
        )
        assert self.capture(0x2FF, "05").scene_report is None
        assert self.capture(0x31F, "0008010000").scene_report is None
        partial = SceneSetup(self.capture(0x2E7, "0e008c 05e803"))
        assert partial.enabled is None
        assert partial.configured_response(NativeScene.NORMAL) is SceneResponse.SCHEDULE
        assert partial.configured_response(NativeScene.PERMANENT_4) == 0xE8
        assert partial.configured_response(NativeScene.PERMANENT_5) is None
        assert partial.configured_response(NativeScene.AWAY) is None
        assert NativeScene.AWAY.fixed_response is SceneResponse.AWAY
        with self.assertRaisesRegex(ValueError, "scene setup report"):
            SceneSetup(self.capture(0x2E7, "0e00c1 00000000"))
        request = asyncio.create_task(
            SceneSetup.read(self.stream, 2, _PEER, source=_HOST),
        )
        assert await self.outgoing() == bytes.fromhex(
            "0002 00000000 6d70000b 0e0013 40000000 00000000",
        )
        self.result(2, TxStatus.ACK)
        self.capture(0x2E7, "0e0080 0000000000000001")
        setup = await request
        assert setup.enabled and setup.configured_response(NativeScene.NORMAL) is None

    async def test_response_edit_preserves_siblings_and_requires_target_readback(
        self,
    ) -> None:
        for scene in (NativeScene.AWAY, NativeScene.UNOCCUPIED, _TEMP):
            with self.assertRaisesRegex(ValueError, "fixed response"):
                await write_scene_response(
                    self.stream,
                    2,
                    _PEER,
                    scene,
                    SceneResponse.WAKE,
                    source=_HOST,
                )
        task = asyncio.create_task(
            write_scene_response(
                self.stream,
                2,
                _PEER,
                NativeScene.PERMANENT_4,
                SceneResponse.SCHEDULE,
                source=_HOST,
            ),
        )
        request = await self.outgoing()
        self.result(2, TxStatus.ACK)
        self.capture(0x2E7, "0e0008 aabbcc04ef987601")
        assert await self.outgoing() == bytes.fromhex(
            "0002 00000000 6df0000c 13070e00 aabbcc05ef987601",
        )
        self.result(2, TxStatus.ACK)
        assert await self.outgoing() == request
        self.result(2, TxStatus.ACK)
        self.capture(0x2E7, "0e0008 01")  # Device retained a different policy.
        assert (await task).status is NativeWriteStatus.DIFFERENT
        assert self.tx.empty()

    async def test_offline_preview_and_scope_validation(self) -> None:
        parser = build_parser()
        out = StringIO()
        args = [
            "select-scene",
            "8",
            "--raw",
            "--ports",
            "2",
            "4",
            "--source-subnet",
            "0xff",
            "--source-node",
            "63",
        ]
        assert await run(parser.parse_args([*args, "--json"]), stdout=out) == 0
        assert json.loads(out.getvalue()) == {
            "preview": {
                "scene": 8,
                "ports": [2, 4],
                "duration_minutes": 480,
                "body": "2fffffc307e001",
            },
        }
        with self.assertRaisesRegex(CliError, "specify --serial or --tcp"):
            await run(parser.parse_args([*args, "--confirm"]))
        with self.assertRaisesRegex(CliError, "unavailable|not available"):
            await run(parser.parse_args(["--vendor", *args]))
        for ports in ((), (1, 1), (0, 5)):
            with self.assertRaisesRegex(ValueError, "physical ports"):
                SceneSelection(_TEMP, ports, _HOST)
        with self.assertRaisesRegex(ValueError, "timeout"):
            await SceneSelection(_TEMP, (1,), _HOST).send(self.stream, timeout=0)
        assert self.tx.empty()

    async def test_explicit_raw_cli_broadcast_over_tcp_reports_each_link(self) -> None:
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
            for method, data in ((2, b"\x01\x01"), (9, b"\0")):
                command = management(method, 0, data)
                assert await reader.readexactly(len(command)) == command
                writer.write(management(method, 3, data))
            sends = [
                serialize(Packet(1, bytes([0, port]) + bytes(4) + _SCENE_BODY))
                for port in (1, 3)
            ]
            self.assertCountEqual(
                [await reader.readexactly(len(sends[0])) for _ in sends],
                sends,
            )
            writer.write(
                serialize(Packet(1, bytes([2, 3, 2, 1])))
                + serialize(Packet(1, bytes([2, 1, 0]))),
            )
            leave = management(2, 0, bytes(2))
            assert await reader.readexactly(len(leave)) == leave
            writer.write(management(2, 3, bytes(2)))

        async with listen(app) as endpoint, asyncio.timeout(5):
            out = StringIO()
            args = build_parser().parse_args(
                [
                    "select-scene",
                    "6",
                    "--raw",
                    "--ports",
                    "1",
                    "3",
                    "--tcp",
                    f"{endpoint.address}:{endpoint.port}",
                    "--json",
                    "--confirm",
                ],
            )
            assert await run(args, stdout=out) == 1
            assert [json.loads(line) for line in out.getvalue().splitlines()] == [
                {"port": 1, "tx_status": "ACK", "tx_reason": None},
                {"port": 3, "tx_status": "FAILED", "tx_reason": 1},
            ]
