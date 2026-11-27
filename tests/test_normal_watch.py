import asyncio
import unittest
from io import StringIO
from unittest.mock import MagicMock, patch

from tekmar_482 import (
    BridgeClient,
    BridgeManagement,
    CaptureGap,
    ConnectionClosedError,
    NativeAddress,
    NativeBlockQuery,
    NativeFrame,
    NativePath,
    NativePortBusyError,
    NativeStatusQuery,
    NormalPeerCapture,
    Packet,
    ProtocolError,
    StatusRequest,
    Tekmar482Client,
    ThermostatMode,
    watch_climate,
)
from tekmar_482.cli import CliError, build_parser, run

from .native_fixtures import TEST_PATH, block, frame, identity
from .test_normal_block import Wire
from .test_peer_capture import restore, start, trace

_CAP = "0101f04b54"
_SUPPORT = "0401f04b54020005bf3e"
_ID = "03354000000000000000"
_MODE = "03354000000003000000"


def record(peer: NativeFrame) -> Packet:
    return trace("00 03 01000000 01 " + peer.body.hex())


def end(wire: Wire, peer: NativeFrame) -> None:
    wire.feed("020ff04b54 030000 01000000 " + peer.body.hex())


class NormalWatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_background_block_read_waits_its_turn_on_the_same_port(self) -> None:
        wire = Wire()
        async with Tekmar482Client(wire) as client:
            manager = BridgeManagement(client)
            async with client.session.normal_edit_locks[3]:
                waiting = asyncio.create_task(
                    manager.read_block_group(
                        3,
                        NativeBlockQuery(TEST_PATH.address, 0),
                        wait_for_edits=True,
                    ),
                )
                await asyncio.sleep(0)
                with self.assertRaises(NativePortBusyError):
                    await manager.read_status(
                        3,
                        NativeStatusQuery(StatusRequest.MASTER),
                    )
                assert wire.out.empty() and not waiting.done()
            await wire.until_read(_ID)
            wire.feed("040ff04b54" + _ID)
            end(wire, identity())
            assert (await waiting).require_report().body == identity().body
            assert not client.session.normal_edit_locks[3].locked()

    async def test_watermark_accounts_for_loss_and_does_not_chase_new_traffic(
        self,
    ) -> None:
        fence = Packet(6, b"start")
        peers = NormalPeerCapture(1, 8, fence)
        peers.record(fence, outbound=False)
        peers.record(record(identity()), outbound=False)
        wait = asyncio.create_task(peers.wait_consumed())
        await asyncio.sleep(0)
        assert not wait.done()
        peers.record(record(block(3, bytes.fromhex("0101"))), outbound=False)
        assert await peers.read() == CaptureGap(1)
        async with asyncio.timeout(1):
            await wait
        closed = asyncio.create_task(peers.wait_consumed())
        await asyncio.sleep(0)
        peers.finish()
        with self.assertRaises(ConnectionClosedError):
            await closed
        assert isinstance(await peers.read(), NativeFrame)
        await peers.wait_consumed()

    async def test_normal_mode_read_accepts_only_current_path_after_trace(self) -> None:
        for change in ("none", "gap", "move"):
            with self.subTest(change=change):
                await self._watch(change)

    async def _watch(self, change: str) -> None:
        wire = Wire()
        heat = block(3, bytes.fromhex("07010904"))

        async def gateway() -> None:
            await start(wire, "08")
            await wire.expect(_CAP)
            wire.feed(_SUPPORT)
            await wire.until_read(_ID)
            wire.feed("040ff04b54" + _ID)
            wire.queue_packet(record(identity()))
            end(wire, identity())
            await wire.until_read(_MODE)
            wire.feed("040ff04b54" + _MODE)
            if change != "gap":
                wire.queue_packet(record(heat))
            if change == "gap":
                wire.queue_packet(trace("05 08 0100000000000000"))
            elif change == "move":
                move = frame(0x3E7, bytes.fromhex("3507"), NativePath(3, 0x35, 8))
                wire.queue_packet(record(move))
            end(wire, heat)
            if change != "none":
                await wire.until_read(_ID)
                wire.feed("040ff04b54" + _ID)
                wire.feed("020ff04b54 0301ff")
            await restore(wire)

        async with Tekmar482Client(wire) as client, asyncio.TaskGroup() as tasks:
            tasks.create_task(gateway())
            async with BridgeManagement(client).native_session([3]) as link:
                result = await watch_climate(link, [TEST_PATH], duration=0.15)
        if change == "none":
            assert [b.query.block_id for b in result.latest_reads] == [0, 3]
            assert result.thermostats[0].setup.mode is ThermostatMode.HEAT
        else:
            assert not result.thermostats and result.missing_mode_reports == (
                TEST_PATH,
            )
            assert [b.query.block_id for b in result.latest_reads] == [0]
            assert result.latest_reads[0].expired_unsent
            assert result.dropped == int(change == "gap")

    async def test_refuses_wrong_source_unobserved_port_and_unordered_firmware(
        self,
    ) -> None:
        wire = Wire()

        async def gateway() -> None:
            await start(wire, "08")
            await wire.expect(_CAP)
            wire.feed("0401f04b54020005bf1e")
            await restore(wire)

        async with Tekmar482Client(wire) as client, asyncio.TaskGroup() as tasks:
            tasks.create_task(gateway())
            async with BridgeManagement(client).native_session([3]) as link:
                with self.assertRaisesRegex(ValueError, "gateway source"):
                    await watch_climate(link, [TEST_PATH], source=NativeAddress(1, 1))
                with self.assertRaisesRegex(ValueError, "captured ports"):
                    await watch_climate(link, [NativePath(1, 3, 5)])
                with self.assertRaisesRegex(ProtocolError, "ordered native results"):
                    await watch_climate(link, [TEST_PATH])
            with self.assertRaises(ConnectionClosedError):
                await watch_climate(link, [TEST_PATH])

    async def test_cli_watch_native_uses_normal_by_default_without_link_ack(
        self,
    ) -> None:
        wire = Wire()

        async def gateway() -> None:
            await start(wire, "08")
            await wire.expect(_CAP)
            wire.feed(_SUPPORT)
            await wire.until_read(_ID)
            wire.feed("040ff04b54" + _ID)
            wire.feed("020ff04b54 0301ff")
            await restore(wire)

        parser = build_parser()
        args = [
            "watch-native",
            "--device",
            "3",
            "0x35",
            "7",
            "--duration",
            "0.1",
            "--tcp",
            "localhost:4567",
        ]
        async with Tekmar482Client(wire) as client, asyncio.TaskGroup() as tasks:
            bridge = MagicMock(spec=BridgeClient)
            bridge.normal.return_value.__aenter__.return_value = client
            with patch("tekmar_482.cli.open_gateway") as opened:
                opened.return_value.__aenter__.return_value = bridge
                with self.assertRaisesRegex(CliError, "custom source addresses"):
                    await run(parser.parse_args([*args, "--source-node", "3"]))
                opened.assert_not_called()
                tasks.create_task(gateway())
                await run(parser.parse_args(args), stdout=StringIO())
            bridge.normal.assert_called_once_with(select_mode=False)
            bridge.stream.assert_not_called()
