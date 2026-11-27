import asyncio
import io
import json
import unittest
from unittest.mock import MagicMock, patch

from tekmar_482 import (
    BridgeClient,
    BridgeManagement,
    NativeAddress,
    NativeBlockQuery,
    NativeTargets,
    NativeTransmitError,
    NullTransport,
    ProtocolError,
    Tekmar482Client,
)
from tekmar_482.cli import CliError, build_parser, run
from tekmar_482.packet import TYPE_TRPC, Packet

from .test_management import Script

_CAP = "0101f04b54"
_SUPPORT = "0401f04b540200053f01"
_READ = "010ff04b54"
_ADMIT = "040ff04b54"
_EVENT = "020ff04b54"
_KEY = "02120138000703"
_PEER = "6e712845380003f322"
_TIME = "34120000"
_QUERY = NativeBlockQuery(NativeAddress(0x12, 33), 0x38, 3)


class Wire(NullTransport):
    def __init__(self) -> None:
        super().__init__()
        self.out: asyncio.Queue[bytes] = asyncio.Queue()

    async def write_packet(self, packet: Packet) -> None:
        assert packet.type == TYPE_TRPC
        self.out.put_nowait(packet.data)

    async def expect(self, hex_data: str) -> None:
        async with asyncio.timeout(1):
            assert await self.out.get() == bytes.fromhex(hex_data)

    def feed(self, hex_data: str) -> None:
        self.queue_packet(Packet(TYPE_TRPC, bytes.fromhex(hex_data)))

    async def until_read(self, key: str = _KEY) -> None:
        await self.expect(_CAP)
        self.feed(_SUPPORT)
        await self.expect(_READ + key)


class NormalBlockTests(unittest.IsolatedAsyncioTestCase):
    async def test_cli_uses_normal_without_switching_or_enabling_link_ack(self) -> None:
        script = Script(
            (_CAP, _SUPPORT),
            (_READ + _KEY, _ADMIT + _KEY + " " + _EVENT + "020000" + _TIME + _PEER),
        )
        args = [
            "read-block",
            "2",
            "0x12",
            "33",
            "0x38",
            "--selector",
            "3",
            "--json",
            "--tcp",
            "localhost:1234",
        ]
        stdout = io.StringIO()
        parser = build_parser()
        async with Tekmar482Client(script) as client:
            bridge = MagicMock(spec=BridgeClient)
            bridge.normal.return_value.__aenter__.return_value = client
            with patch("tekmar_482.cli.open_gateway") as opened:
                opened.return_value.__aenter__.return_value = bridge
                with self.assertRaisesRegex(CliError, "require read-block --raw"):
                    await run(parser.parse_args([*args, "--ack"]), stdout=stdout)
                opened.assert_not_called()
                assert await run(parser.parse_args(args), stdout=stdout) == 0
            bridge.normal.assert_called_once_with(select_mode=False)
            bridge.stream.assert_not_called()
        assert json.loads(stdout.getvalue())["fields"]["schedule_id"] == 3
        assert parser.parse_args([*args, "--raw", "--ack"]).raw

    async def test_stale_before_admission_and_unrelated_ports_cannot_satisfy(
        self,
    ) -> None:
        reply = _EVENT + "020001" + _TIME + _PEER
        script = Script(
            (_CAP, _SUPPORT),
            (
                _READ + _KEY,
                " ".join((_EVENT + "0201", _ADMIT + _KEY, _EVENT + "0101", reply)),
            ),
        )
        async with Tekmar482Client(script) as client:
            frame = await BridgeManagement(client).read_block(2, _QUERY)
            assert (frame.port, frame.timestamp_ms, frame.node) == (2, 0x1234, 33)
            assert frame.body == bytes.fromhex(_PEER)
            assert frame.decoded["schedule_id"] == 3
            assert client.session.report_subscription_count == 0
        assert not script.rows

    async def test_errors_never_become_an_empty_or_cached_block(self) -> None:
        for tail, exc in (
            ("01ff", TimeoutError),
            ("02fe", NativeTransmitError),
            ("0305", NativeTransmitError),
            ("ff", ProtocolError),
            ("0103", ProtocolError),
            ("00", ProtocolError),
            ("0000" + _TIME + _PEER[:-2], ProtocolError),
            ("0001" + _TIME + _PEER.replace("f322", "f422"), ProtocolError),
            ("0002" + _TIME + _PEER, ProtocolError),
        ):
            with self.subTest(tail=tail):
                script = Script(
                    (_CAP, _SUPPORT),
                    (_READ + _KEY, _ADMIT + _KEY + " " + _EVENT + "02" + tail),
                )
                async with Tekmar482Client(script) as client:
                    with self.assertRaises(exc):
                        await BridgeManagement(client).read_block(2, _QUERY)
                    assert not client.session.report_subscription_count
        for caps in ("0400000000", "0401f04b540200053f", "0401f04b540201057f"):
            script = Script((_CAP, caps))
            async with Tekmar482Client(script) as client:
                with self.assertRaisesRegex(ProtocolError, "does not support"):
                    await BridgeManagement(client).read_block(2, _QUERY)
        async with Tekmar482Client(Script()) as client:
            with self.assertRaises(ValueError):
                await BridgeManagement(client).read_block(
                    2,
                    _QUERY,
                    timeout=float("nan"),
                )
            with self.assertRaises(ValueError):
                await BridgeManagement(client).read_block(5, _QUERY)
            with self.assertRaises(ValueError):
                await BridgeManagement(client).read_block(
                    2,
                    NativeBlockQuery(NativeTargets(1, 3), 0),
                )

    async def test_two_ports_await_peers_without_owning_the_normal_session(
        self,
    ) -> None:
        wire = Wire()
        async with Tekmar482Client(wire) as client:
            manager = BridgeManagement(client)
            a = asyncio.create_task(manager.read_block(2, _QUERY))
            await wire.until_read()
            wire.feed(_ADMIT + _KEY)
            other = NativeBlockQuery(NativeAddress(0, 31), 0xABCD)
            key = "03001fcdab0000"
            b = asyncio.create_task(BridgeManagement(client).read_block(3, other))
            await wire.until_read(key)
            wire.feed(_ADMIT + key)
            revision = asyncio.create_task(manager.build_revision())
            await wire.expect("0104f04b54")
            wire.feed("0404f04b54" + "61" * 40)
            assert await revision == "a" * 40
            wire.feed(_EVENT + "030001" + _TIME + "6e7007c3cdab00")
            assert (await b).decoded["support_mask"] == "0x00" and not a.done()
            wire.feed(_EVENT + "020000" + _TIME + _PEER)
            assert (await a).body == bytes.fromhex(_PEER)

    async def test_cancelled_admission_drains_before_same_query_reuse(self) -> None:
        wire = Wire()
        async with Tekmar482Client(wire) as client:
            manager = BridgeManagement(client)
            a = asyncio.create_task(manager.read_block(2, _QUERY))
            await wire.until_read()
            a.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await a
            b = asyncio.create_task(BridgeManagement(client).read_block(2, _QUERY))
            await wire.expect(_CAP)
            wire.feed(_ADMIT + _KEY)  # Late acceptance of the cancelled request.
            wire.feed(_SUPPORT)
            await wire.expect(_READ + _KEY)
            wire.feed(
                _EVENT + "0202",
            )  # Its terminal report precedes read B's admission.
            wire.feed(_ADMIT + _KEY)
            wire.feed(_EVENT + "020000" + _TIME + _PEER)
            assert (await b).timestamp_ms == 0x1234
            assert not client.session.report_subscription_count

    async def test_busy_then_local_timeout_release_subscriptions(self) -> None:
        script = Script(
            (_CAP, _SUPPORT),
            (_READ + _KEY, "0406f04b540ff04b5401"),
            (_CAP, _SUPPORT),
            (_READ + _KEY, _ADMIT + _KEY),
        )
        async with Tekmar482Client(script) as client:
            manager = BridgeManagement(client)
            with self.assertRaisesRegex(ProtocolError, "busy"):
                await manager.read_block(2, _QUERY)
            with self.assertRaises(TimeoutError):
                await manager.read_block(2, _QUERY, timeout=0.01)
            assert not client.session.report_subscription_count and not script.rows
