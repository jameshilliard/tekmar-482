from __future__ import annotations

import asyncio
import json
import unittest
from io import StringIO
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

from tekmar_482 import (
    BridgeClient,
    BridgeManagement,
    FailureReason,
    NativeFrame,
    NativePath,
    NormalPeerCapture,
    Packet,
    Tekmar482Client,
    read_water,
)
from tekmar_482.cli import CliError, build_parser, run

from .native_fixtures import TEST_PATH, block, frame, identity
from .test_normal_block import Wire
from .test_peer_capture import restore, start, trace

if TYPE_CHECKING:
    from tekmar_482 import WaterRead

_CAP = "0101f04b54"
_FEATURES = "0401f04b54020005bf3e"
_MASTER = "03000000f703"
_ID = "03354000000000000000"
_PUMP_SETUP = "03354000000041000000"
_MIX_PUMPS = "03354000000050000000"
_WATER = bytes.fromhex("bc07 00c0 9828 2211 fe")


def peer(f: NativeFrame) -> Packet:
    return trace(
        f"00 {f.port:02x} "
        + f.timestamp_ms.to_bytes(4, "little").hex()
        + " 00 "
        + f.body.hex(),
    )


async def master_request(wire: Wire) -> None:
    await start(wire, "08")
    await wire.expect(_CAP)
    wire.feed(_FEATURES)
    await wire.expect(_CAP)
    wire.feed(_FEATURES)
    await wire.expect("0110f04b54" + _MASTER)
    wire.feed("0410f04b54" + _MASTER)


def complete_master(wire: Wire, claim: NativeFrame) -> None:
    wire.feed("0210f04b54 0300 01000000 " + claim.body.hex())
    wire.feed("0210f04b54 030101")  # No link ACK, but the peer answered.


async def block_reply(wire: Wire, key: str, reply: NativeFrame) -> None:
    await wire.until_read(key)
    wire.feed("040ff04b54" + key)
    wire.queue_packet(peer(reply))
    wire.feed("020ff04b54 030000 01000000 " + reply.body.hex())


class NormalWaterTests(unittest.IsolatedAsyncioTestCase):
    async def test_busy_port_expires_without_discarding_other_ports(self) -> None:
        wire = Wire()
        healthy = NativePath(2, TEST_PATH.subnet, 0)

        async def gateway() -> None:
            await start(wire, "0c")
            await wire.expect(_CAP)
            wire.feed(_FEATURES)
            for port in (2, 3):
                await wire.expect(_CAP)
                wire.feed(_FEATURES)
                request = f"{port:02x}000000f703"
                await wire.expect("0110f04b54" + request)
                if port == healthy.port:
                    wire.feed("0410f04b54" + request)
                else:
                    wire.feed("0406f04b54 10f04b54 01")
            claim = frame(0x3FF, _WATER, healthy)
            wire.queue_packet(peer(claim))
            wire.feed("0210f04b54 020001000000" + claim.body.hex())
            wire.feed("0210f04b54 020100")
            # The old operation on port 3 never completes within this scan.
            await restore(wire)

        async with Tekmar482Client(wire) as client, asyncio.TaskGroup() as tasks:
            tasks.create_task(gateway())
            async with BridgeManagement(client).native_session([2, 3]) as link:
                result = await read_water(link, [2, 3], duration=0.15)
            assert not client.session.report_subscription_count
        assert result.unfinished_ports == (3,)
        assert [batch.port for batch in result.master_queries] == [2]
        assert [(s.port, s.address) for s in result.sources] == [(2, healthy.address)]

    async def test_identifies_plant_and_reads_pump_setup_before_window_ends(
        self,
    ) -> None:
        wire = Wire()

        async def gateway() -> None:
            await master_request(wire)
            claim = frame(0x3FF, bytes.fromhex("35"))
            wire.queue_packet(peer(claim))
            wire.queue_packet(peer(frame(0x3FF, b"", stamp=2)))
            complete_master(wire, claim)
            await block_reply(wire, _ID, identity(104103))
            await block_reply(wire, _PUMP_SETUP, block(0x41, bytes.fromhex("01 05")))
            await wire.until_read(_MIX_PUMPS)
            wire.feed("040ff04b54" + _MIX_PUMPS)
            wire.queue_packet(peer(frame(0x3CF, _WATER)))
            await restore(wire)

        async with Tekmar482Client(wire) as client, asyncio.TaskGroup() as tasks:
            tasks.create_task(gateway())
            async with BridgeManagement(client).native_session([3]) as link:
                result = await read_water(link, [3], duration=0.18)
        (query,) = result.master_queries
        assert (
            query.answered
            and query.tx_result
            and query.tx_result.reason is FailureReason.NO_ACK
        )
        (water,) = result.sources
        assert water.master and water.master.value.supply_f == 113
        (plant,) = result.plant_configurations
        assert plant[0] == TEST_PATH and [r.block for r in plant[1].reports] == [0x41]
        assert [b.query.block_id for b in result.block_reads] == [0, 0x41]
        assert result.unfinished_ports == (3,) and result.dropped == 0

    async def test_no_follow_up_after_gap_move_reassignment_or_missing_trace(
        self,
    ) -> None:
        async def scan(manager: BridgeManagement) -> WaterRead:
            async with manager.native_session([3]) as link:
                return await read_water(link, [3], duration=0.08)

        claim = frame(0x3FF, bytes.fromhex("35"))
        for case in ("gap", "move", "reassigned", "unseen"):
            with self.subTest(case=case):
                wire = Wire()
                async with (
                    Tekmar482Client(wire) as client,
                    asyncio.TaskGroup() as tasks,
                ):
                    task = tasks.create_task(scan(BridgeManagement(client)))
                    await master_request(wire)
                    if case != "unseen":
                        wire.queue_packet(peer(claim))
                    if case == "gap":
                        wire.queue_packet(trace("05 08 0100000000000000"))
                    elif case == "move":
                        wire.queue_packet(
                            peer(frame(0x3E7, b"\x35\x07", NativePath(3, 0x35, 8))),
                        )
                    elif case == "reassigned":
                        wire.queue_packet(peer(frame(0x3FF, b"\x24")))
                    complete_master(wire, claim)
                    await restore(wire)
                result = task.result()
                assert result.master_queries[0].answered
                assert not result.block_reads and not result.unfinished_ports
                assert result.gateway_trace_records_dropped == int(case == "gap")

    async def test_host_loss_is_unscoped_gateway_loss_is_per_wire(self) -> None:
        for mask, remaining in ((0, ()), (4, (3,)), (12, ())):
            with self.subTest(mask=mask):
                fence = Packet(6, b"start")
                peers = NormalPeerCapture(2, 12, fence)
                peers.record(fence, outbound=False)
                for p in (2, 3):
                    f = frame(
                        0x3CF,
                        _WATER,
                        NativePath(p, TEST_PATH.subnet, TEST_PATH.node),
                    )
                    peers.record(peer(f), outbound=False)
                task = asyncio.create_task(
                    read_water(peers, [2, 3], source=None, duration=0.06),
                )
                async with asyncio.timeout(1):
                    await peers.wait_consumed()
                    if mask:
                        peers.record(
                            trace(f"05 {mask:02x} 0300000000000000"),
                            outbound=False,
                        )
                    else:
                        for _ in range(3):
                            peers.record(
                                trace("04 03 02000000 02 0900 8080"),
                                outbound=False,
                            )
                    result = await task
                assert tuple(s.port for s in result.sources) == remaining
                assert (
                    result.host_records_dropped,
                    result.gateway_trace_records_dropped,
                ) == ((0, 3) if mask else (1, 0))
                assert not result.master_queries and not result.unfinished_ports

    async def test_cli_passive_normal_limits_trace_to_selected_port(self) -> None:
        args = [
            "read-water",
            "3",
            "--passive",
            "--tcp",
            "localhost:4321",
            "--duration",
            "0.06",
            "--json",
        ]
        parser = build_parser()
        output = StringIO()
        wire = Wire()

        async def gateway() -> None:
            await start(wire, "08")
            wire.queue_packet(peer(frame(0x3CF, _WATER)))
            await restore(wire)

        async with Tekmar482Client(wire) as client, asyncio.TaskGroup() as tasks:
            bridge = MagicMock(spec=BridgeClient)
            bridge.normal.return_value.__aenter__.return_value = client
            with patch("tekmar_482.cli.open_gateway") as opened:
                opened.return_value.__aenter__.return_value = bridge
                for option in ("--ack-port", "--source-node"):
                    with self.assertRaisesRegex(CliError, "--raw"):
                        await run(
                            parser.parse_args([*args, option, "3"]),
                            stdout=output,
                        )
                opened.assert_not_called()
                tasks.create_task(gateway())
                assert await run(parser.parse_args(args), stdout=output) == 0
            bridge.normal.assert_called_once_with(select_mode=False)
            bridge.stream.assert_not_called()
        data = json.loads(output.getvalue())
        assert data["water_sources"][0]["port"] == 3 and not data["master_queries"]
        assert (
            data["gateway_trace_records_dropped"] == data["host_records_dropped"] == 0
        )
        assert not data["unfinished_ports"]

    async def test_cli_combines_claims_only_for_an_explicit_shared_network(
        self,
    ) -> None:
        mix = NativePath(2, 0x46, 19)
        slots = bytes([TEST_PATH.subnet, mix.subnet])

        async def gateway(wire: Wire) -> None:
            await start(wire, "0c")
            for path in (mix, TEST_PATH):
                wire.queue_packet(peer(frame(0x3FF, slots, path)))
                wire.queue_packet(peer(frame(0x3CF, _WATER, path)))
            await restore(wire)

        parser = build_parser()
        for shared in (False, True):
            with self.subTest(shared=shared):
                args = parser.parse_args(
                    [
                        "read-water",
                        "2",
                        "3",
                        "--passive",
                        "--tcp",
                        "localhost:4321",
                        "--duration",
                        "0.06",
                        "--json",
                        *(["--shared-network"] if shared else []),
                    ],
                )
                output, wire = StringIO(), Wire()
                async with (
                    Tekmar482Client(wire) as client,
                    asyncio.TaskGroup() as tasks,
                ):
                    bridge = MagicMock(spec=BridgeClient)
                    bridge.normal.return_value.__aenter__.return_value = client
                    tasks.create_task(gateway(wire))
                    with patch("tekmar_482.cli.open_gateway") as opened:
                        opened.return_value.__aenter__.return_value = bridge
                        assert await run(args, stdout=output) == 0

                data = json.loads(output.getvalue())
                systems = data["water_systems"]
                assert len(data["water_sources"]) == 2 and not data["master_queries"]
                if shared:
                    (system,) = systems
                    assert system["ports"] == [2, 3]
                    assert [loop["source"]["port"] for loop in system["loops"]] == [
                        3,
                        2,
                    ]
                else:
                    assert [system["ports"] for system in systems] == [[2], [3]]
                    assert systems[0]["loops"][0]["source"] is None
                    assert systems[1]["loops"][1]["source"] is None
