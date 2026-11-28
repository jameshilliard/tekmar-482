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
from tekmar_482.native_discovery import NativeDevice
from tekmar_482.native_snapshot import read_native_snapshot
from tekmar_482.packet import TYPE_TRPC, Packet

from .test_management import Script

_CAP = "0101f04b54"
_SUPPORT = "0401f04b540400053f04"
_READ = "010ff04b54"
_ADMIT = "040ff04b54"
_EVENT = "020ff04b54"
_KEY = "02120100000038000703"
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

    async def test_cli_group_returns_partial_evidence_without_selecting_raw(
        self,
    ) -> None:
        key = "02120100004038000703"
        script = Script(
            (_CAP, _SUPPORT),
            (
                _READ + key,
                " ".join(
                    (
                        _ADMIT + key,
                        _EVENT + "020401" + _TIME + _PEER,
                        _EVENT + "020101",
                    ),
                ),
            ),
        )
        args = build_parser().parse_args(
            [
                "read-block",
                "2",
                "0x12",
                "1",
                "0x38",
                "--selector",
                "3",
                "--also-node",
                "31",
                "--json",
                "--tcp",
                "localhost:1234",
            ],
        )
        out = io.StringIO()
        async with Tekmar482Client(script) as client:
            bridge = MagicMock(spec=BridgeClient)
            bridge.normal.return_value.__aenter__.return_value = client
            with patch("tekmar_482.cli.open_gateway") as opened:
                opened.return_value.__aenter__.return_value = bridge
                assert await run(args, stdout=out) == 1
            bridge.normal.assert_called_once_with(select_mode=False)
            bridge.stream.assert_not_called()
        result = json.loads(out.getvalue())["block_read"]
        assert result["missing_nodes"] == [31]
        assert result["reports"][0]["fields"]["schedule_id"] == 3

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
        for caps in ("0400000000", "0401f04b540400053f01", "0401f04b540401053f04"):
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
            key = "030000000040cdab0000"
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

    async def test_one_bitmap_includes_distinct_peers_and_retains_partial_evidence(
        self,
    ) -> None:
        query = NativeBlockQuery(NativeTargets(0x12, 0x40000001), 0x38, 3)
        key = "02120100004038000703"
        last_node = "6e7127c5380003f322"
        part = _EVENT + "020401" + _TIME + last_node
        for terminal, missing, nodes in (
            ("0001" + _TIME + _PEER, (), [33, 31]),
            ("0101", (1,), [31]),
        ):
            with self.subTest(terminal=terminal):
                script = Script(
                    (_CAP, _SUPPORT),
                    (
                        _READ + key,
                        " ".join((_ADMIT + key, part, _EVENT + "02" + terminal)),
                    ),
                )
                async with Tekmar482Client(script) as client:
                    batch = await BridgeManagement(client).read_block_group(2, query)
                    assert batch.missing_nodes == missing
                    assert [f.node for f in batch.reports] == nodes
                    assert (
                        batch.tx_result is not None and batch.tx_result.completed_frame
                    )
                    assert (
                        not client.session.report_subscription_count and not script.rows
                    )

        for events in (
            [_EVENT + "020001" + _TIME + last_node],  # premature terminal
            [part, _EVENT + "020401" + _TIME + _PEER],  # too many parts
            [part, _EVENT + "020001" + _TIME + last_node.replace("27c5", "2fc5")],
            [part, _EVENT + "020100"],  # two outcomes for one wire request
            [part, _EVENT + "020501"],  # lost control evidence, not a silent node
        ):
            with self.subTest(events=events):
                script = Script(
                    (_CAP, _SUPPORT),
                    (_READ + key, " ".join((_ADMIT + key, *events))),
                )
                async with Tekmar482Client(script) as client:
                    with self.assertRaises(ProtocolError):
                        await BridgeManagement(client).read_block_group(2, query)
                    assert (
                        not client.session.report_subscription_count and not script.rows
                    )

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

    async def test_normal_snapshot_retries_only_silent_nodes_and_reuses_unsent_slot(
        self,
    ) -> None:
        def exchange(key: str, *records: str) -> tuple[str, str]:
            return _READ + key, " ".join((_ADMIT + key, *(_EVENT + r for r in records)))

        script = Script(
            (_CAP, _SUPPORT),
            exchange(
                "02120300000000000000",
                "020401010000006e712086000007332211",
                "020101",
            ),
            (_CAP, _SUPPORT),
            exchange("02120100000000000000", "0201ff"),
            (_CAP, _SUPPORT),
            exchange("02120200000034120000", "020000020000006e712083341200"),
        )
        devices = (
            NativeDevice(2, NativeAddress(0x12, 1)),
            NativeDevice(
                2,
                NativeAddress(0x12, 2),
                reported_blocks=frozenset({0x1234}),
            ),
        )
        async with Tekmar482Client(script) as client:
            manager = BridgeManagement(client)
            with self.assertRaisesRegex(ValueError, "gateway source"):
                await manager.read_block_group(2, _QUERY, source=NativeAddress(0, 1))
            snapshot = await read_native_snapshot(manager, devices)
            first, retry, observed = snapshot.blocks
            assert first.missing_nodes == retry.missing_nodes == (1,)
            assert retry.expired_unsent and not retry.transmit_pending
            assert observed.reports[0].node == 2 and observed.query.block_id == 0x1234
            assert not script.rows and not client.session.report_subscription_count
