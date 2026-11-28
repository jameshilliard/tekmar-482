from __future__ import annotations

import asyncio
import json
import unittest
from contextlib import suppress
from io import StringIO
from typing import TYPE_CHECKING
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
    SnowmeltGroups,
    StatusRequest,
    Tekmar482Client,
    ThermostatMode,
    watch_climate,
    watch_native,
)
from tekmar_482.cli import CliError, build_parser, run

from .native_fixtures import TEST_PATH, block, frame, identity
from .test_normal_block import Wire
from .test_peer_capture import restore, start, trace

if TYPE_CHECKING:
    from tekmar_482 import NativeState

_CAP = "0101f04b54"
_SUPPORT = "0401f04b54050005bf2e"  # Ordered active reads without subnet discovery.
_ID = "03354000000000000000"
_MODE = "03354000000003000000"
_PLANT = 104103


def record(peer: NativeFrame) -> Packet:
    return trace("00 03 01000000 " + peer.body.hex() + " 01")


def end(wire: Wire, peer: NativeFrame) -> None:
    wire.feed("020ff04b54 030000 01000000 " + peer.body.hex())


async def admit_read(wire: Wire, keys: set[tuple[int, int]]) -> tuple[int, int]:
    await wire.expect(_CAP)
    wire.feed(_SUPPORT)
    async with asyncio.timeout(1):
        request = await wire.out.get()
    assert request[:5].hex() == "010ff04b54"
    port = request[5]
    block_id = int.from_bytes(request[11:13], "little")
    keys.remove((port, block_id))
    assert request[6:].hex() == f"3540000000{block_id:02x}000000"
    wire.feed("040ff04b54" + request[5:].hex())
    return port, block_id


def plant_report(wire: Wire, f: NativeFrame, *, traced: bool = True) -> None:
    if traced:
        wire.queue_packet(trace(f"00 {f.port:02x} 01000000 {f.body.hex()} 01"))
    wire.feed(f"020ff04b54 {f.port:02x}0000 01000000 " + f.body.hex())


class NormalWatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_snowmelt_pair_recovers_from_missed_timing_on_peer_report(
        self,
    ) -> None:
        wire = Wire()
        key = "030135077f0111022526"

        async def paired_read() -> None:
            await wire.expect(_CAP)
            wire.feed(_SUPPORT)
            await wire.expect("0110f04b54" + key)
            wire.feed("0410f04b54" + key)

        async def gateway() -> None:
            await start(wire, "08")
            await wire.expect(_CAP)
            wire.feed(_SUPPORT)
            await admit_read(wire, {(3, 0)})
            for b, data in (
                (0xD0, "0103"),  # Electric application and configured zone 3.
                (0xC0, "0103"),
                (0xC1, "0103"),
                (0xC4, "00"),
                (0x71, "030006"),
                (0x72, "030000"),
                (0x78, "030000"),
            ):
                wire.queue_packet(record(block(b, bytes.fromhex(data))))
            wire.queue_packet(
                record(
                    frame(0x17E, bytes.fromhex("19051900002200001b0002230000290000")),
                ),
            )
            plant_report(wire, identity(105801))
            await paired_read()
            wire.feed("0210f04b54 030405")  # Transmit fault: missed timing.
            for b in (0xC2, 0xC3):
                await admit_read(wire, {(3, b)})
                wire.feed("020ff04b54 030305")
            wire.queue_packet(record(frame(0x17E, bytes.fromhex("1a01190000"))))
            await paired_read()
            groups = frame(0x17E, bytes.fromhex("1902250400260600"))
            wire.queue_packet(record(groups))
            wire.feed("0210f04b54 030001000000 " + groups.body.hex())
            wire.feed("0210f04b54 030100")
            await restore(wire)

        async with Tekmar482Client(wire) as client, asyncio.TaskGroup() as tasks:
            tasks.create_task(gateway())
            async with BridgeManagement(client).native_session([3]) as link:
                result = await watch_native(
                    link,
                    [TEST_PATH],
                    duration=0.2,
                    schedules=False,
                )
        assert result.reads == 5 and wire.out.empty()
        (device,) = result.state.snowmelt_devices
        assert (
            device.reported_groups == SnowmeltGroups(4, 6)
            and device.setup.groups is None
        )

    async def test_plant_probe_does_not_overtake_a_queued_mode_read(self) -> None:
        wire = Wire()
        plant = NativePath(3, 0x56, 0)
        mode = block(3, bytes.fromhex("0101"))

        async def gateway() -> None:
            await start(wire, "08")
            await wire.expect(_CAP)
            wire.feed(_SUPPORT)
            wire.queue_packet(record(frame(0x3FF, b"")))
            await admit_read(wire, {(3, 0)})
            identify = identity(102301)  # 527 thermostat.
            wire.queue_packet(record(identify))
            wire.queue_packet(
                record(frame(0x3CF, bytes.fromhex("bc07 00c0 9828 2211 fe"), plant)),
            )
            plant_report(wire, identify, traced=False)
            await admit_read(wire, {(3, 3)})
            plant_report(wire, mode)
            await wire.expect(_CAP)
            wire.feed(_SUPPORT)
            await wire.expect("0110f04b54 03000000f703")
            wire.feed("0410f04b54 03000000f703")
            claim = frame(0x3FF, bytes([plant.subnet]), plant)
            wire.queue_packet(record(claim))
            wire.feed("0210f04b54 030001000000 " + claim.body.hex())
            wire.feed("0210f04b54 030100")
            await restore(wire)

        async with Tekmar482Client(wire) as client, asyncio.TaskGroup() as tasks:
            tasks.create_task(gateway())
            async with BridgeManagement(client).native_session([3]) as link:
                result = await watch_native(
                    link,
                    duration=0.1,
                    schedules=False,
                    thermostat_targets=False,
                    thermostat_sensors=False,
                )
        assert result.state.has_master_announcement(3) and result.reads == 3
        climate = result.state.climate(TEST_PATH)
        assert climate and climate.setup.mode is ThermostatMode.HEAT
        assert wire.out.empty()

    async def test_sensor_watch_drops_retired_sample_without_polling(self) -> None:
        wire = Wire()
        room_samples: set[int | None] = set()

        def observe(path: NativePath, state: NativeState) -> None:
            climate = state.climate(path)
            if climate is not None and climate.sensors.room is not None:
                room_samples.add(climate.sensors.room.average_raw)

        async def gateway() -> None:
            await start(wire, "08")
            await wire.expect(_CAP)
            wire.feed(_SUPPORT)
            for b, value in (
                (0, identity(107201)),  # 532: room statistics and two external inputs.
                (3, block(3, bytes.fromhex("0101"))),
                (0x0D, block(0x0D, bytes.fromhex("030102"))),
            ):
                await admit_read(wire, {(3, b)})
                plant_report(wire, value)
            await admit_read(wire, {(3, 0x15)})
            wire.queue_packet(record(block(0x0D, bytes.fromhex("030103"))))
            wire.queue_packet(record(frame(0x36F, bytes.fromhex("01012a0006000000"))))
            plant_report(wire, block(0x15, bytes.fromhex("030006")))
            await admit_read(wire, {(3, 3)})  # Mode refresh takes the next turn.
            plant_report(wire, block(3, bytes.fromhex("0101")))
            remaining = {(3, b) for b in (0x15, 0x17, 0x18)}
            payloads = {0x15: "030106", 0x17: "0703aa05", 0x18: "00"}
            while remaining:
                _, b = await admit_read(wire, remaining)
                plant_report(wire, block(b, bytes.fromhex(payloads[b])))
            # Normal sensor traffic must not schedule a new read.
            wire.queue_packet(record(block(0x17, bytes.fromhex("0703ab05"))))
            await restore(wire)

        async with Tekmar482Client(wire) as client, asyncio.TaskGroup() as tasks:
            tasks.create_task(gateway())
            async with BridgeManagement(client).native_session([3]) as link:
                answer = await watch_native(
                    link,
                    [TEST_PATH],
                    duration=0.2,
                    schedules=False,
                    thermostat_targets=False,
                    on_change=observe,
                )
        assert answer.reads == 8 and room_samples == {0x601}
        assert not answer.state.missing_sensor_reads(TEST_PATH)
        assert wire.out.empty()

    async def test_automatic_watch_follows_only_observed_ports_and_retires_a_move(
        self,
    ) -> None:
        wire = Wire()
        other = NativePath(2, TEST_PATH.subnet, TEST_PATH.node)
        moved = NativePath(3, TEST_PATH.subnet, 8)
        product = 200003  # An identified product without a known configuration profile.

        def announce(f: NativeFrame) -> None:
            wire.queue_packet(trace(f"00 {f.port:02x} 01000000 {f.body.hex()} 01"))

        async def gateway() -> None:
            await start(
                wire,
                "0e",
            )  # Capture includes a port the watcher does not select.
            await wire.expect(_CAP)
            wire.feed(_SUPPORT)
            announce(frame(0x3FF, b"", NativePath(1, TEST_PATH.subnet, 7)))
            announce(frame(0x3FF, b"", NativePath(2, TEST_PATH.subnet, 0)))
            announce(frame(0x17E, b"\x09\x01", NativePath(3, TEST_PATH.subnet, 5)))
            announce(frame(0x3FF, b"\x35", TEST_PATH))
            announce(frame(0x3FF, b"\x35", other))
            announce(frame(0x3FF, b"\x35", TEST_PATH))
            remaining = {(2, 0), (3, 0)}
            await admit_read(wire, remaining)
            await admit_read(wire, remaining)
            announce(frame(0x3E7, bytes.fromhex("3507"), moved))
            plant_report(wire, identity(product, path=TEST_PATH))
            plant_report(wire, identity(product, path=other))
            key = "03358000000000000000"
            await wire.until_read(key)
            wire.feed("040ff04b54" + key)
            plant_report(wire, identity(product, path=moved))
            await restore(wire)

        async with Tekmar482Client(wire) as client, asyncio.TaskGroup() as tasks:
            tasks.create_task(gateway())
            async with BridgeManagement(client).native_session([1, 2, 3]) as link:
                result = await watch_native(link, ports=[2, 3], duration=0.23)
        assert result.state.paths == (NativePath(2, TEST_PATH.subnet, 0), other, moved)
        assert result.state.identity(TEST_PATH) is None
        assert result.state.identity(other) == result.state.identity(moved) is not None
        assert {
            (b.port, b.query.target.request_mask()) for b in result.latest_blocks
        } == {(2, 1 << 6), (3, 1 << 7)}
        assert result.reads == 3

    async def test_idle_expiry_does_not_probe_or_count_a_drop(self) -> None:
        fence = Packet(6, b"start")
        peers = NormalPeerCapture(8, 8, fence)
        peers.record(fence, outbound=False)
        appeared = asyncio.Event()
        expired = asyncio.Event()
        events: list[int | None] = []

        def changed(path: NativePath, state: NativeState) -> None:
            assert path == TEST_PATH and state.dropped == 0
            if (product := state.identity(path)) is not None:
                events.append(product.product_number)
                appeared.set()
            else:
                assert path not in state.paths
                events.append(None)
                expired.set()

        task = asyncio.create_task(
            watch_native(peers, source=None, stale_after=0.03, on_change=changed),
        )
        try:
            async with asyncio.timeout(1):
                peers.record(record(identity(200003)), outbound=False)
                await appeared.wait()
                await expired.wait()
                appeared.clear()
                peers.record(record(identity(200004)), outbound=False)
                await appeared.wait()
            assert events == [200003, None, 200004]
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

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

    async def test_port_gap_retires_only_that_ports_in_flight_reads(self) -> None:
        wire = Wire()
        paths = {p: NativePath(p, TEST_PATH.subnet, TEST_PATH.node) for p in (2, 3)}
        changes: list[tuple[NativePath, bool]] = []

        async def gateway() -> None:
            await start(wire, "0c")
            await wire.expect(_CAP)
            wire.feed(_SUPPORT)
            for block_id in (0, 0x41):
                remaining = {(p, block_id) for p in paths}
                await admit_read(wire, remaining)
                await admit_read(wire, remaining)
                if block_id == 0:
                    for path in paths.values():
                        plant_report(wire, identity(_PLANT, path=path))
            wire.queue_packet(trace("05 04 0300000000000000"))
            plant_report(wire, block(0x41, b"\x01\x04", paths[2]), traced=False)
            plant_report(wire, block(0x41, b"\x01\x05", paths[3]))
            remaining = {(2, 0), (3, 0x50)}  # Reidentify port 2, continue port 3.
            await admit_read(wire, remaining)
            await admit_read(wire, remaining)
            await restore(wire)

        async with Tekmar482Client(wire) as client, asyncio.TaskGroup() as tasks:
            tasks.create_task(gateway())
            async with BridgeManagement(client).native_session(paths) as link:
                result = await watch_native(
                    link,
                    paths.values(),
                    duration=0.25,
                    on_change=lambda p, state: changes.append(
                        (p, state.identity(p) is not None),
                    ),
                )
        state = result.state
        assert state.dropped == 3 and state.paths == (paths[3],)
        assert state.identity(paths[2]) is None and state.identity(paths[3]) is not None
        assert (
            state.block(paths[2], 0x41) is None
            and state.block(paths[3], 0x41) is not None
        )
        assert {(b.port, b.query.block_id) for b in result.latest_blocks} == {
            (3, 0),
            (3, 0x41),
        }
        assert (paths[2], False) in changes and (paths[3], False) not in changes

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
            wire.feed("0401f04b54050005bf1e")
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

    async def test_cli_autodiscovers_without_an_address_list(self) -> None:
        wire = Wire()

        async def gateway() -> None:
            await start(wire, "08")
            await wire.expect(_CAP)
            wire.feed(_SUPPORT)
            wire.queue_packet(record(identity(200003)))
            await restore(wire)

        parser = build_parser()
        args = [
            "watch-native",
            "--port",
            "3",
            "--duration",
            "0.1",
            "--tcp",
            "localhost:4567",
            "--json",
        ]
        output = StringIO()
        async with Tekmar482Client(wire) as client, asyncio.TaskGroup() as tasks:
            bridge = MagicMock(spec=BridgeClient)
            bridge.normal.return_value.__aenter__.return_value = client
            with patch("tekmar_482.cli.open_gateway") as opened:
                opened.return_value.__aenter__.return_value = bridge
                with self.assertRaisesRegex(CliError, "--stale-after"):
                    await run(parser.parse_args([*args, "--stale-after", "nan"]))
                opened.assert_not_called()
                tasks.create_task(gateway())
                assert await run(parser.parse_args(args), stdout=output) == 0
            bridge.normal.assert_called_once_with(select_mode=False)
            bridge.stream.assert_not_called()
        identity_event, summary = map(json.loads, output.getvalue().splitlines())
        assert identity_event["native_identity"]["product_number"] == 200003
        assert summary["observed_paths"] == [{"port": 3, "subnet": 0x35, "node": 7}]
        assert not summary["missing_identities"]
