"""Exercise the public client against the pinned production ARM firmware."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import unittest
from calendar import Day
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime, time
from pathlib import Path
from typing import TYPE_CHECKING

from tekmar_482 import (
    BridgeClient,
    BridgeFeature,
    BridgeManagement,
    GatewayEndpoint,
    IoDirectory,
    IoEndpoint,
    LineBroadcast,
    LocalScheduleOverride,
    NativeAddress,
    NativeBlockQuery,
    NativeField,
    NativeIdentity,
    NativeIdentityMismatchError,
    NativeMessage,
    NativePath,
    NativeScene,
    NativeSnapshot,
    NativeState,
    NativeStatusQuery,
    NativeWriteStatus,
    PeerBusErrors,
    PeerBusEvents,
    PeerLinkCounters,
    SceneResponse,
    SceneSetup,
    ScheduleBinding,
    ScheduleChange,
    ScheduleCycle,
    ScheduleEvent,
    ScheduleSetupChange,
    ScheduleTarget,
    SensorSlot,
    SetpointApplication,
    SetpointSlot,
    SetpointTargetChange,
    SnowmeltApplication,
    SnowmeltBlock,
    SnowmeltGroupStatus,
    SnowmeltKey,
    SnowmeltOperatingState,
    SnowmeltOperation,
    StageCommand,
    StageSelection,
    StatusRequest,
    TargetSlot,
    ThermostatMode,
    ThermostatSensorMonitor,
    ThermostatTarget,
    ThermostatTargetChange,
    TraceOrigin,
    TracePacket,
    TraceTx,
    TxStatus,
    WaterDetection,
    WritableBlock,
    read_io_directory,
    read_thermostat_sensors,
    read_weekly_schedule,
    watch_native,
)
from tekmar_482.native_scene import scene_response_fields
from tekmar_482.wire_trace import observe_bridge_packets

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

    from tekmar_482 import Packet

type ScheduledReply = str | tuple[int, str]
type Exchange = tuple[int, str, tuple[ScheduledReply, ...]]
_IMAGE = os.environ.get("TEKMAR_TEST_FLASH_IMAGE")
_EMULATOR = Path(__file__).resolve().parents[1] / "firmware" / "tests" / "smoke.py"


async def _uart_step(
    incoming: asyncio.StreamReader,
    outgoing: asyncio.StreamWriter,
    program: asyncio.StreamReader,
    control: asyncio.StreamWriter,
) -> None:
    while True:
        try:
            async with asyncio.timeout(0.002):
                data = await incoming.read(4096)
            if not data:
                return
        except TimeoutError:
            data = b""  # Advance bus timers while the client awaits a reply.
        control.write(data.hex().encode("ascii") + b"\n")
        await control.drain()
        row = await program.readline()
        if not row:
            msg = "firmware emulator stopped before the client closed the UART"
            raise RuntimeError(msg)
        reply = bytes.fromhex(row.decode("ascii"))
        if reply:
            outgoing.write(reply)
            await outgoing.drain()


@asynccontextmanager
async def _connected(
    program: asyncio.StreamReader,
    control: asyncio.StreamWriter,
) -> AsyncIterator[BridgeManagement]:
    accepted: asyncio.Queue[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = (
        asyncio.Queue()
    )

    def accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        accepted.put_nowait((reader, writer))

    async def serve() -> None:
        reader, writer = await accepted.get()
        try:
            await _uart_step(reader, writer, program, control)
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(accept, "127.0.0.1", 0)
    endpoint = GatewayEndpoint.tcp("127.0.0.1", server.sockets[0].getsockname()[1])
    async with server, asyncio.TaskGroup() as tasks:
        tasks.create_task(serve())
        async with endpoint.connect() as wire:
            gateway = BridgeClient(wire)
            await gateway.identify()
            async with gateway.normal(select_mode=False) as client:
                yield BridgeManagement(client)


@asynccontextmanager
async def _gateway(script: Sequence[Exchange]) -> AsyncIterator[BridgeManagement]:
    uv = shutil.which("uv")
    assert uv is not None and _IMAGE is not None
    process = await asyncio.create_subprocess_exec(
        uv,
        "run",
        "--script",
        str(_EMULATOR),
        "--client-port",
        _IMAGE,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    try:
        program, control = process.stdout, process.stdin
        assert program is not None and control is not None
        async with asyncio.timeout(60):
            control.write(json.dumps(script).encode("ascii") + b"\n")
            await control.drain()
            assert await program.readline() == b"\n", "emulator did not boot"
            async with _connected(program, control) as manager:
                yield manager
            control.close()
            await control.wait_closed()
            assert await process.wait() == 0, "firmware bus script did not complete"
    finally:
        if process.stdin is not None:
            process.stdin.close()
        if process.returncode is None:
            # uv starts a child that inherits the emulator's control pipes.
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            await process.wait()


def _block(
    port: int,
    route: str,
    block: str,
    reply: str,
    selector: str = "00",
) -> Exchange:
    return port, f"6d70000b {block} {route} {selector}000000", (reply,)


_THERMOSTAT = NativeAddress(0x23, 5)
_SNOW = NativeAddress(0x14, 3)
_PLANT = NativeAddress(0x2A, 8)
_SETPOINT = NativeAddress(0x31, 7)


@unittest.skipUnless(_IMAGE, "set TEKMAR_TEST_FLASH_IMAGE to run the ARM interop suite")
class FirmwareInteropTests(unittest.IsolatedAsyncioTestCase):
    async def test_normal_watch_discovers_io_after_identifying_a_quiet_553(
        self,
    ) -> None:
        path = NativePath(3, _THERMOSTAT.subnet, _THERMOSTAT.node)
        endpoint = IoEndpoint(path.subnet, path.node)
        script = (
            _block(3, "23 10000000", "0000", "6e723149 00003f 8e9a01 c60402"),
            _block(3, "23 10000000", "0300", "6e723144 030001 01"),
            _block(3, "23 10000000", "0400", "6e723143 040000"),
            _block(3, "23 10000000", "9700", "6e723143 970000"),
            (
                3,
                "1f600002 2305",
                tuple(
                    f"1f52314d 2305 {ch:02x}06 533100000000 01 0100" for ch in range(6)
                ),
            ),
        )
        complete = asyncio.Event()

        def observe(updated: NativePath, state: NativeState) -> None:
            if (
                updated == path
                and (directory := state.io_directory(path, endpoint))
                and directory.complete
            ):
                assert {c.sample.channel for c in directory.channels} == set(range(6))
                complete.set()

        async with (
            _gateway(script) as manager,
            manager.native_session([path.port]) as capture,
            asyncio.TaskGroup() as tasks,
        ):
            watch = tasks.create_task(
                watch_native(
                    capture,
                    [path],
                    schedules=False,
                    thermostat_targets=False,
                    thermostat_sensors=False,
                    plant_settings=False,
                    setpoint_settings=False,
                    on_change=observe,
                ),
            )
            await complete.wait()
            watch.cancel()

    async def test_normal_watch_reads_known_io_endpoint_from_a_node_zero_peer(
        self,
    ) -> None:
        path = NativePath(3, 0x23, 0)
        endpoint = IoEndpoint(0x43, 0xA5)
        script = (
            (
                3,
                "1f600002 43a5",
                (
                    "1f52300d 43a5 0802 473100000000 01 0000",
                    "1f52300d 43a5 f202 503200000000 09 1900",
                ),
            ),
        )
        complete = asyncio.Event()

        def observe(updated: NativePath, state: NativeState) -> None:
            if (
                updated == path
                and (directory := state.io_directory(path, endpoint))
                and directory.complete
            ):
                assert [c.sample.channel for c in directory.channels] == [8, 242]
                complete.set()

        async with (
            _gateway(script) as manager,
            manager.native_session([path.port]) as capture,
            asyncio.TaskGroup() as tasks,
        ):
            watch = tasks.create_task(
                watch_native(
                    capture,
                    [path],
                    io_endpoints={path: (endpoint,)},
                    on_change=observe,
                ),
            )
            async with asyncio.timeout(5):
                await complete.wait()
            watch.cancel()

    async def test_io_recovery_requests_only_the_missing_channel_in_normal_mode(
        self,
    ) -> None:
        script = (
            (
                3,
                "1f600002 2305",
                tuple(
                    f"1f52314d 2305 {ch:02x}06 533100000000 01 0100" for ch in range(5)
                ),
            ),
            (3, "1f400003 230505", ("1f323146 2305 05 00 0000",)),
        )
        async with _gateway(script) as manager:
            read = await read_io_directory(
                manager,
                3,
                IoEndpoint(0x23, 5),
                peer=_THERMOSTAT,
                identity=NativeIdentity(105102, 1222, 1, 1),
            )
            assert not read.discovery.answered
            assert read.discovery.covered == set(range(5))
            (channel,) = read.supplemental
            assert channel.answered and channel.covered == {5}
            assert read.directory.complete
            assert read.directory.channels[-1].sample.label is None

    async def test_io_recovery_continues_after_a_completed_unanswered_channel(
        self,
    ) -> None:
        script = (
            (
                3,
                "1f600002 2305",
                tuple(
                    f"1f52314d 2305 {ch:02x}06 410042000000 01 0100" for ch in range(4)
                ),
            ),
            (3, "1f400003 230504", ()),
            (3, "1f400003 230505", ("1f323146 2305 05 00 0000",)),
        )
        async with _gateway(script) as manager:
            read = await read_io_directory(
                manager,
                3,
                IoEndpoint(0x23, 5),
                peer=_THERMOSTAT,
                identity=NativeIdentity(105102, 1222, 1, 1),
            )
            missed, later = read.supplemental
            assert missed.tx_result and missed.tx_result.status is TxStatus.ACK
            assert not missed.answered and not missed.reports
            assert later.answered and later.covered == {5}
            directory = read.directory
            assert not directory.complete
            assert [c.sample.channel for c in directory.channels] == [0, 1, 2, 3, 5]
            assert directory.channels[0].sample.label_text == "AB"
            assert directory.channels[-1].sample.label is None

    async def test_io_discovery_collects_a_reply_train_longer_than_ten_seconds(
        self,
    ) -> None:
        parts = tuple(
            (1850, f"1f52314d 2305 {channel:02x}06 533100000000 01 0100")
            for channel in range(6)
        )
        async with _gateway(((3, "1f600002 2305", parts),)) as manager:
            batch = await manager.read_status(
                3,
                NativeStatusQuery.io(0x23, 5),
                peer=_THERMOSTAT,
            )
            assert batch.answered
            started = batch.reports[0].timestamp_ms
            assert batch.reports[-1].timestamp_ms - started > 10_000
            book = IoDirectory.from_batch(batch)
            assert book.complete
            assert tuple(c.sample.channel for c in book.channels) == tuple(range(6))

    async def test_stage_commands_keep_exact_targets_and_normal_tx_evidence(
        self,
    ) -> None:
        script = (
            (1, "39f00005 abf5018080", ()),
            (4, "3af00005 2300000080", ()),
        )
        purge = StageSelection.from_nodes(0xAB, (1, 16, 24), stage=1, output_level=117)
        override = StageSelection.from_nodes(0x23, (24,), stage=0, output_level=0)
        async with _gateway(script) as manager, manager.trace((1, 4)) as trace:
            first, second = await asyncio.gather(
                manager.send_stage_command(1, StageCommand.START_PURGE, purge),
                manager.send_stage_command(4, StageCommand.END_OVERRIDE_OFF, override),
            )
            assert first is not None and second is not None
            assert (first.port, first.status) == (1, TxStatus.ACK)
            assert (second.port, second.status) == (4, TxStatus.ACK)
            attempts = sorted(
                (tx.frame.port, tx.frame.body)
                for record in trace.drain()
                if isinstance(record, TracePacket)
                and isinstance(tx := record.decoded, TraceTx)
                and tx.origin is TraceOrigin.STAGE_COMMAND
            )
            assert attempts == [(p, bytes.fromhex(body)) for p, body, _ in script]
            target = await manager.traffic.device(NativePath(4, 0x23, 24))
            assert target is not None and target.counts.tx_ack == 1
            for node in (1, 16, 24):
                assert await manager.traffic.device(NativePath(1, 0xAB, node)) is None
        assert trace.gateway_drops == (0,) * 5 and not trace.host_drops

    async def test_normal_service_frames_preserve_explicit_headers_and_trace(
        self,
    ) -> None:
        script = (
            (0, "687431c1 03", ()),
            (1, "b57431c3 cffe63", ()),
            (2, "b3f431c2 0320", ()),
            (3, "b87431c2 3412", ()),
            (4, "b8f431c3 025634", ()),
        )
        async with _gateway(script) as manager, manager.trace() as trace:
            outcomes = await asyncio.gather(
                *(
                    manager.send_service_frame(
                        p,
                        NativeMessage.from_bytes(bytes.fromhex(body)),
                    )
                    for p, body, _ in script
                ),
            )
            assert all(tx is not None and tx.status is TxStatus.ACK for tx in outcomes)
            attempts = sorted(
                (tx.frame.port, tx.frame.body)
                for record in trace.drain()
                if isinstance(record, TracePacket)
                and isinstance(tx := record.decoded, TraceTx)
                and tx.origin is TraceOrigin.SERVICE_FRAME
            )
            assert attempts == [(p, bytes.fromhex(body)) for p, body, _ in script]
        assert trace.gateway_drops == (0,) * 5 and not trace.host_drops

    async def test_link_timing_reads_from_the_production_image(self) -> None:
        async with _gateway(()) as manager:
            caps = await manager.capabilities()
            assert caps is not None and caps.features & BridgeFeature.LINK_TIMING
            service = await manager.link_service_timing()
            assert service.max_pass_us >= service.max_receive_us > 0
            assert service.max_line_us > 0
            for port in range(caps.ports):
                sample = await manager.port_transmit_timing(port)
                assert sample.port == port
                assert sample.max_poll_gap_us > 0  # Cold-start discovery.
                assert sample.last_miss is None

    async def test_watch_reads_cataloged_thermostat_input_samples(self) -> None:
        script = tuple(
            _block(3, "23 10000000", block, report)
            for block, report in (
                ("0000", "6e723149 00003f c1a201 d20405"),  # 532
                ("0300", "6e723146 030007 010409"),
                ("0d00", "6e723145 0d0003 0102"),
                ("1500", "6e723149 15003f 0006 e405 1006"),
                ("1700", "6e72314a 17007f 02 f505 e405 0806"),
                ("1800", "6e723143 180000"),
            )
        )
        path = NativePath(3, _THERMOSTAT.subnet, _THERMOSTAT.node)
        complete = asyncio.Event()

        def observe(updated: NativePath, state: NativeState) -> None:
            assert updated == path
            climate = state.climate(path)
            if climate is not None and not state.missing_sensor_reads(path):
                room = climate.sensors.room
                assert room is not None and room.average_raw == 0x600
                assert climate.sensors.average_floor_f == 67.5
                assert climate.sensors.inputs[-1].current_raw is None
                complete.set()

        async with (
            _gateway(script) as manager,
            manager.native_session([3]) as capture,
            asyncio.TaskGroup() as tasks,
        ):
            watch = tasks.create_task(
                watch_native(
                    capture,
                    [path],
                    thermostat_targets=False,
                    schedules=False,
                    on_change=observe,
                ),
            )
            await complete.wait()
            watch.cancel()

    async def test_sensor_readback_and_zero_mask_through_normal_firmware(self) -> None:
        script = tuple(
            _block(3, "23 10000000", block, report)
            for block, report in (
                ("0000", "6e723149 00003f c1a201 d20405"),
                ("0d00", "6e723145 0d0003 0102"),
                ("1500", "6e723145 150003 0006"),
                ("1700", "6e723146 170007 02f505"),
                ("1800", "6e723143 180000"),
                ("1700", "6e723143 170000"),
            )
        )
        async with _gateway(script) as manager:
            result = await read_thermostat_sensors(manager, 3, _THERMOSTAT)
            assert not result.unanswered_blocks
            monitor = ThermostatSensorMonitor.from_batches(result.batches)
            assert monitor.devices == (result.device,)
            inputs = {item.slot: item for item in monitor.devices[0].sensors.inputs}
            assert inputs[SensorSlot.EXTERNAL_1].current_raw == 0x05F5
            replacement = await manager.read_block_group(
                3,
                NativeBlockQuery(_THERMOSTAT, 0x17),
            )
            assert monitor.observe(replacement.require_report())
            inputs = {item.slot: item for item in monitor.devices[0].sensors.inputs}
            assert inputs[SensorSlot.EXTERNAL_1].current_raw is None
            snapshot = NativeSnapshot((*result.batches, replacement), ())
            (read_sensors,) = snapshot.thermostat_sensors
            (read_climate,) = snapshot.thermostats
            assert (
                read_sensors.sensors
                == read_climate.sensors
                == monitor.devices[0].sensors
            )

    async def test_thermostat_equipment_blocks_through_normal_firmware(self) -> None:
        script = tuple(
            _block(3, "23 10000000", block, report)
            for block, report in (
                ("0000", "6e723149 00003f 8e9a01 c60403"),
                ("0100", "6e723148 01001f 02 8b 00 0d cb"),
                ("0400", "6e723149 040031 02 aabbcc 01 03"),
                ("0800", "6e723147 080017 04 00 23 07"),
                ("5800", "6e723144 580001 01"),
            )
        )
        async with _gateway(script) as manager:
            batches = [
                await manager.read_block_group(3, NativeBlockQuery(_THERMOSTAT, b))
                for b in (0, 1, 4, 8, 0x58)
            ]
            assert all(not batch.missing_nodes for batch in batches)
            (thermostat,) = NativeSnapshot(tuple(batches), ()).thermostats
            setup = thermostat.setup
            assert setup.product.model == "553"
            assert setup.stage_setup and setup.stage_setup.heating_stage1.pump_required
            assert setup.floor_cooling_enabled is True
            assert (
                setup.cooling_setup and setup.cooling_setup.minimum_off_time_raw is None
            )
            fan = thermostat.adjustments.fan_setup
            assert fan and fan.delay_raw == 7 and fan.purge_raw is None

    async def test_thermostat_monitor_blocks_through_normal_firmware(self) -> None:
        script = tuple(
            _block(3, "23 10000000", block, report)
            for block, report in (
                ("0000", "6e723149 00003f 8e9a01 c60403"),
                ("0a00", "6e723146 0a000d 81 3412"),
                ("1b00", "6e723147 1b0017 01000002"),
                ("9900", "6e723145 990003 7856"),
                ("9a00", "6e723145 9a0003 0000"),
                ("a800", "6e723144 a80001 00"),
            )
        )
        async with _gateway(script) as manager:
            batches = [
                await manager.read_block_group(3, NativeBlockQuery(_THERMOSTAT, b))
                for b in (0, 0x0A, 0x1B, 0x99, 0x9A, 0xA8)
            ]
            assert all(not batch.missing_nodes for batch in batches)
            (thermostat,) = NativeSnapshot(tuple(batches), ()).thermostats
            assert thermostat.adjustments.onboard_humidity_enabled is False
            assert thermostat.adjustments.humidity is None
            equipment = thermostat.equipment
            assert (
                equipment.filter_usage
                and equipment.filter_usage.value.interval_raw is None
            )
            assert equipment.filter_usage.value.elapsed_raw == 0x1234
            assert equipment.relay_status and equipment.relay_status.value.fan1 == 2
            assert equipment.relay_status.value.fan2 is None
            assert [(r.block.value, r.hours) for r in equipment.runtimes] == [
                (0x99, 0x5678),
                (0x9A, 0),
            ]

    async def test_operating_policy_blocks_through_normal_firmware(self) -> None:
        script = tuple(
            _block(3, "23 10000000", block, report)
            for block, report in (
                ("0000", "6e723149 00003f 8e9a01 c60403"),
                ("0b00", "6e723149 0b0013 02 4a aabb 39 cc"),
                ("0c00", "6e723144 0c0001 20"),
                ("a900", "6e723145 a90009 00ff"),
                ("aa00", "6e723144 aa0001 fe"),
                ("ad00", "6e723144 ad0001 05"),
            )
        )
        async with _gateway(script) as manager:
            batches = [
                await manager.read_block_group(3, NativeBlockQuery(_THERMOSTAT, b))
                for b in (0, 0x0B, 0x0C, 0xA9, 0xAA, 0xAD)
            ]
            assert all(not batch.missing_nodes for batch in batches)
            ((path, policy),) = NativeSnapshot(tuple(batches), ()).operating_policies
            assert path == NativePath(3, _THERMOSTAT.subnet, _THERMOSTAT.node)
            assert policy.shutdown_source_raw == 2
            assert policy.warm_shutdown and policy.warm_shutdown.slots == (
                74,
                None,
                None,
                57,
                None,
            )
            assert (
                policy.cold_shutdown and policy.cold_shutdown.raw(TargetSlot.WAKE) == 32
            )
            assert (
                policy.balance_point and policy.balance_point.raw(TargetSlot.WAKE) == 0
            )
            assert policy.balance_point.raw(TargetSlot.SLEEP) == 255
            assert policy.balance_point_schedule_raw == 254
            assert policy.compressor_during_backup_raw == 5

    async def test_watch_resumes_busy_wire_after_an_abandoned_firmware_read(
        self,
    ) -> None:
        blocked = NativePath(3, _THERMOSTAT.subnet, _THERMOSTAT.node)
        healthy = NativePath(4, _PLANT.subnet, _PLANT.node)
        script = (
            (3, "6d70000b 4100 23 10000000 00000000", ()),
            _block(3, "23 10000000", "0000", "6e723149 00003f 430d03 c60402"),
            _block(4, "2a 80000000", "0000", "6e72a209 00003f 440d03 b60402"),
        )
        evidence: list[NativePath] = []
        complete = asyncio.Event()
        rejections = 0

        def on_packet(packet: Packet, outbound: bool) -> None:  # noqa: FBT001
            nonlocal rejections
            if (
                not outbound
                and packet.type == 6
                and packet.data == bytes.fromhex("0406f04b54 0ff04b54 01")
            ):
                rejections += 1

        def observe(path: NativePath, state: NativeState) -> None:
            if path not in evidence and state.identity(path) is not None:
                evidence.append(path)
                if len(evidence) == 2:
                    complete.set()

        with observe_bridge_packets(on_packet):
            async with (
                _gateway(script) as manager,
                manager.native_session([3, 4]) as capture,
                asyncio.TaskGroup() as tasks,
            ):
                with self.assertRaises(TimeoutError):
                    await manager.read_block(
                        3,
                        NativeBlockQuery(_THERMOSTAT, 0x41),
                        timeout=0.02,
                    )
                watch = tasks.create_task(
                    watch_native(capture, [blocked, healthy], on_change=observe),
                )
                await complete.wait()
                assert evidence == [healthy, blocked] and rejections == 1
                watch.cancel()

    async def test_passive_watch_publishes_unsolicited_mode_without_querying_it(
        self,
    ) -> None:
        request = "6d70000b 0000 23 10000000 00000000"
        identity = "6e723149 00003f 8e9a01 c60402"
        script = (
            (3, request, (identity,)),
            (
                3,
                request,
                (
                    "6e723146 030007 000409",
                    "6e723146 030007 010409",
                    identity,
                ),
            ),
        )
        path = NativePath(3, _THERMOSTAT.subnet, _THERMOSTAT.node)
        modes: list[ThermostatMode] = []
        changed = asyncio.Event()

        def observe(updated: NativePath, state: NativeState) -> None:
            assert updated == path
            if (climate := state.climate(path)) and climate.setup.mode is not None:
                modes.append(climate.setup.mode)
                if climate.setup.mode is ThermostatMode.HEAT:
                    changed.set()

        async with (
            _gateway(script) as manager,
            manager.native_session([3]) as capture,
            asyncio.TaskGroup() as tasks,
        ):
            watch = tasks.create_task(
                watch_native(capture, source=None, on_change=observe),
            )
            # A second identity exchange lets the peer publish its mode changes
            # before replying, without competing with autonomous discovery.
            await manager.read_block(3, NativeBlockQuery(_THERMOSTAT, 0))
            await manager.read_block(3, NativeBlockQuery(_THERMOSTAT, 0))
            async with asyncio.timeout(5):
                await changed.wait()
            assert modes == [ThermostatMode.OFF, ThermostatMode.HEAT]
            watch.cancel()

    async def test_air_group_announcement_refreshes_setup_via_firmware(self) -> None:
        info = "6e723149 00003f 8e9a01 c60402"
        script = (
            _block(3, "23 10000000", "0000", info),
            _block(3, "23 10000000", "0300", "6e723146 030007 010403"),
            (
                3,
                "6d70000b 0000 23 10000000 00000000",
                ("76a23142 8583", "76a23142 e5a3", info),
            ),
            (3, "6d700007 0300 23 10000000", ("6e723146 030007 010405",)),
        )
        path = NativePath(3, _THERMOSTAT.subnet, _THERMOSTAT.node)
        observations: list[tuple[int | None, tuple[int, bool]]] = []
        confirmed = asyncio.Event()

        def observe(updated: NativePath, state: NativeState) -> None:
            if (
                updated == path
                and (device := state.climate(path))
                and (role := device.air_group_configuration)
            ):
                group = device.setup.air_group_id
                observations.append((group, role.role))
                if group == 5:
                    confirmed.set()

        async with (
            _gateway(script) as manager,
            manager.native_session([3]) as capture,
            asyncio.TaskGroup() as tasks,
        ):
            watch = tasks.create_task(
                watch_native(capture, source=None, on_change=observe),
            )
            identify = NativeBlockQuery(_THERMOSTAT, 0)
            await manager.read_block(3, identify)
            await manager.read_block(3, NativeBlockQuery(_THERMOSTAT, 3))
            await manager.read_block(3, identify)
            async with asyncio.timeout(5):
                await confirmed.wait()
            assert observations[0] == (3, (5, True))
            assert observations[-1] == (5, (5, True))
            watch.cancel()

    async def test_local_override_and_unavailable_mask_through_normal_firmware(
        self,
    ) -> None:
        script = (
            _block(3, "23 10000000", "0000", "6e723149 00003f 078701 8a0401"),
            _block(3, "23 10000000", "3700", "6e723144 370001 07"),
            _block(3, "23 10000000", "3700", "6e723143 370000"),
        )
        path = NativePath(3, _THERMOSTAT.subnet, _THERMOSTAT.node)
        async with _gateway(script) as manager:
            identity = await manager.read_block_group(
                3,
                NativeBlockQuery(_THERMOSTAT, 0),
            )
            query = NativeBlockQuery(_THERMOSTAT, 0x37)
            override = await manager.read_block_group(3, query)
            missing = await manager.read_block_group(3, query)
            assert not override.missing_nodes and not missing.missing_nodes
            state = NativeSnapshot((identity, override), ()).observed_state()
            calendar = state.schedule(path)
            assert calendar is not None and calendar.local_override is not None
            assert calendar.local_override.mode is LocalScheduleOverride.AUTO_4
            assert not calendar.schedules and not calendar.signals
            assert state.observe(missing.require_report()) == (path,)
            updated = state.schedule(path)
            assert updated is not None and updated.local_override is None

    async def test_humidity_status_through_normal_firmware(self) -> None:
        script = (
            _block(3, "23 10000000", "0000", "6e723149 00003f 8e9a01 c60403"),
            (
                3,
                "32200005 2310000000",
                ("32323147 02812c34125518", "3232314b 02812c34125518262d39ca"),
            ),
        )
        async with _gateway(script) as manager:
            identity = await manager.read_block_group(
                3,
                NativeBlockQuery(_THERMOSTAT, 0),
            )
            batch = await manager.read_status(
                3,
                NativeStatusQuery.humidity(0x23, 0x10),
                peer=_THERMOSTAT,
            )
            assert batch.answered
            (report,) = batch.reports
            assert report.message.payload == bytes.fromhex("02812c34125518262d39ca")
            assert report.decoded["extension_hex"] == "ca"
            (thermostat,) = NativeSnapshot((identity,), (batch,)).thermostats
            sample = thermostat.adjustments.humidity
            assert sample is not None and sample.frame is report
            assert sample.value.relative_percent == 44
            assert sample.value.sensor_bytes == bytes.fromhex("1826")
            assert sample.value.humidify_target_raw == 45
            assert sample.value.dehumidify_target_raw == 57

    async def test_thermostat_mode_target_and_weekend_schedule(self) -> None:
        def read(block: str, reply: str) -> Exchange:
            return _block(3, "23 10000000", block, reply)

        identify = read("0000", "6e723149 00003f 8e9a01 c60402")
        off = read("0300", "6e723146 030007 000409")
        heat = read("0300", "6e723146 030007 010409")
        target = read("0500", "6e723148 05001f 2a212a211e")
        retained = "6e723148 05001f 2d212a211e"
        cycle = read("0f00", "6e723146 0f000d 010101")
        weekend = read("1300", "6e72314a 13007f 0306090c0f1215")
        script = (
            identify,
            off,
            off,
            (3, "6df00007 2305 0300 010409", ()),
            # Firmware independently refreshes the legacy ModeSetting cache.
            (3, "6d700007 0300 23 10000000", heat[2]),
            heat,
            identify,
            target,
            target,
            (3, "6df00009 2305 0500 2e212a211e", ()),
            # The thermostat retained a different value despite ACKing the write.
            read("0500", retained),
            identify,
            cycle,
            weekend,
            weekend,
            (3, "6df0000b 2305 1300 0306090c0f3030", ()),
            read("1300", "6e723145 130060 3030"),  # Compact Sat/Sun readback.
            identify,
            cycle,
            read("1000", "6e72314a 10007f 2a2a2a2a2a3636"),
            read("1300", "6e72314a 13007f 0306090c0f3030"),
        )
        async with _gateway(script) as manager:
            expected = NativeIdentity(105102, 1222, 2, None)
            mode = await manager.write_thermostat_mode(
                3,
                _THERMOSTAT,
                ThermostatMode.HEAT,
                expected_identity=expected,
                preview=False,
            )
            assert mode.status is NativeWriteStatus.CONFIRMED
            assert mode.readback is not None
            assert mode.readback.require_report().decoded["mode_setting"] == 1
            value = await manager.write_thermostat_target(
                3,
                _THERMOSTAT,
                ThermostatTargetChange(ThermostatTarget.HEAT, TargetSlot.WAKE, 23),
                expected_identity=expected,
                preview=False,
            )
            assert value.tx_result and value.tx_result.status is TxStatus.ACK
            assert value.status is NativeWriteStatus.DIFFERENT
            assert value.readback is not None
            assert value.readback.require_report().body == bytes.fromhex(retained)
            row = await manager.write_schedule_time(
                3,
                ScheduleTarget(_THERMOSTAT),
                ScheduleChange(ScheduleEvent.UNOCCUPIED, Day.SUNDAY, time(8)),
                expected_identity=expected,
                preview=False,
            )
            assert row.status is NativeWriteStatus.CONFIRMED
            week = await read_weekly_schedule(
                manager,
                3,
                ScheduleTarget(_THERMOSTAT),
                source=NativeAddress(0, 0),
            )
            assert week.table_complete
            occupied, unoccupied = week.rows
            assert occupied.display_event is ScheduleEvent.OCCUPIED
            assert unoccupied.display_event is ScheduleEvent.UNOCCUPIED
            assert unoccupied.days == (3, 6, 9, 12, 15, 48, 48)

    async def test_scene_and_clock_broadcasts_and_verified_response_edit(self) -> None:
        port = 3
        before = _block(port, "23 10000000", "0e00", "6e72314b 0e00ff 000a050304030001")
        script = (
            (3, "2ff00003 05b400", ()),
            (1, "31f00005 1e01060e1e", ()),
            _block(port, "23 10000000", "0000", "6e723149 00003f 8e9a01 c60402"),
            before,
            before,
            (port, "6df0000c 2305 0e00 000a050504030000", ()),
            _block(port, "23 10000000", "0e00", "6e723145 0e0088 0500"),
        )
        async with _gateway(script) as manager:
            (scene,) = await LineBroadcast.scene(
                (3,),
                NativeScene.TEMPORARY_OCCUPIED,
            ).send(manager)
            (clock,) = await LineBroadcast.clock(
                (1,),
                datetime(2030, 7, 15, 0, 30, tzinfo=UTC),
            ).send(manager)
            assert scene.port == 3 and scene.link_acknowledged
            assert clock.port == 1 and clock.link_acknowledged
            changes = scene_response_fields(
                NativeScene.PERMANENT_4,
                SceneResponse.SCHEDULE,
            ) | {
                NativeField.SCENES_ENABLED: 0,
            }
            result = await manager.write_block(
                port,
                NativeBlockQuery(_THERMOSTAT, WritableBlock.SCENE_SETUP),
                changes,
                expected_identity=NativeIdentity(105102, 1222, 2, None),
                preview=False,
            )
            assert (
                result.status is NativeWriteStatus.CONFIRMED
                and result.readback is not None
            )
            verified = SceneSetup(result.readback.require_report())
            assert verified.enabled is False
            assert (
                verified.configured_response(NativeScene.PERMANENT_4)
                is SceneResponse.SCHEDULE
            )

    async def test_peer_address_move_aborts_an_admitted_edit(self) -> None:
        port = 3
        initial = _block(port, "23 10000000", "0500", "6e723148 05001f 2a212a211e")
        guard: Exchange = (port, initial[1], ("7e736182 2305",))
        # A subsequent read of the new address can use the released port.
        moved = NativeAddress(0x36, 6)
        script = (
            _block(port, "23 10000000", "0000", "6e723149 00003f 8e9a01 c60402"),
            initial,
            guard,
            _block(port, "36 20000000", "0000", "6e736189 00003f 8e9a01 c60402"),
        )
        async with _gateway(script) as manager:
            result = await manager.write_block(
                port,
                NativeBlockQuery(_THERMOSTAT, 5),
                {NativeField.WAKE: 46},
                expected_identity=NativeIdentity(105102, 1222, 2, None),
                preview=False,
            )
            assert result.status is NativeWriteStatus.TARGET_MOVED
            assert result.guard_result and result.guard_result.completed_frame
            assert result.tx_result is None and result.readback is None
            report = await manager.read_block(port, NativeBlockQuery(moved, 0))
            assert report.message.source == moved

    async def test_schedule_setup_stages_a_row_then_enables_a_553(self) -> None:
        def read(block: str, answer: str) -> Exchange:
            return _block(3, "23 10000000", block, answer)

        identify = read("0000", "6e723149 00003f 8e9a01 c60402")
        setup = read("0f00", "6e723148 0f001d 00ed030100")
        wake = read("1000", "6e72314a 10007f 0306090c0f1215")
        sleep = read("1300", "6e72314a 13007f 30303030303030")
        script = (
            identify,
            setup,
            identify,
            setup,
            wake,
            sleep,
            identify,
            setup,
            wake,
            wake,
            (3, "6df0000b 2305 1000 2d06090c0f1215", ()),
            read("1000", "6e723144 100001 2d"),
            setup,
            setup,
            (3, "6df0000a 2305 0f00 01ed03010000", ()),
            read("0f00", "6e723148 0f001d 01ed030100"),
        )
        async with _gateway(script) as manager:
            target = ScheduleTarget(_THERMOSTAT)
            before = await read_weekly_schedule(
                manager,
                3,
                target,
                source=NativeAddress(0, 0),
            )
            result = await manager.configure_schedule(
                3,
                target,
                ScheduleSetupChange(
                    binding=ScheduleBinding.ZONE,
                    times=(
                        ScheduleChange(ScheduleEvent.OCCUPIED, Day.MONDAY, time(7, 30)),
                    ),
                ),
                expected_config=before.config,
                expected_identity=NativeIdentity(105102, 1222, 2, None),
                preview=False,
            )
            assert not result.remaining and result.status is NativeWriteStatus.CONFIRMED
            assert all(
                s.guard_result and s.guard_result.completed_frame for s in result.steps
            )
            assert [s.proposed.payload[2:4].hex() for s in result.steps] == [
                "1000",
                "0f00",
            ]

    async def test_timer_format_preserves_index_and_master_in_all_four_rows(
        self,
    ) -> None:
        timer = NativeAddress(0x45, 5)
        identity = _block(3, "45 10000000", "0000", "6e745149 00003f ef8a01 950402")

        def row(block: str, header: str) -> Exchange:
            return _block(
                3,
                "45 10000000",
                block,
                f"6e74514b {block}ff {header} 2a2a2a2a2a2a2a",
                "02",
            )

        wake = row("3800", "7a")
        script = [
            identity,
            wake,
            *(row(b, "7a") for b in ("3900", "3a00", "3b00")),
            identity,
            wake,
        ]
        for b in ("3900", "3a00", "3b00", "3800"):
            before = row(b, "7a")
            script.extend(
                (
                    before,
                    before,
                    (3, f"6df0000c 4505 {b} 5a2a2a2a2a2a2a2a", ()),
                    row(b, "5a"),
                ),
            )
        async with _gateway(script) as manager:
            result = await manager.configure_schedule(
                3,
                ScheduleTarget(timer, system=3),
                ScheduleSetupChange(cycle=ScheduleCycle.FIVE_TWO),
                expected_binding=ScheduleBinding.MASTER_3,
                preview=False,
            )
            assert result.status is NativeWriteStatus.CONFIRMED and not result.remaining
            assert len(result.steps) == 4
            assert all(s.proposed.payload[4] == 0x5A for s in result.steps)

    async def test_indexed_timer_edit_uses_its_resolved_owner(self) -> None:
        local = NativePath(3, 0x45, 5)
        mirror = NativePath(2, local.subnet, local.node)
        ports = (local.port, mirror.port)
        identify = "6e745149 00003f ef8a01 950402"  # 033 timer.
        past = "6e74514b 3800ff 7a 0106090c0f1215"
        before = "6e74514b 3800ff 7a 0306090c0f1215"
        readback = "6e745145 380003 7a 30"

        def read(port: int, block: str, answer: str, selector: str = "00") -> Exchange:
            return _block(port, "45 10000000", block, answer, selector)

        script = (
            (3, "3f700000", ("7ff457c0",)),
            (2, "3f700000", ("7ff677c0",)),
            read(2, "0000", identify),
            read(2, "3800", past, "02"),
            read(3, "0000", identify),
            read(3, "3800", before, "02"),
            read(3, "0000", identify),
            read(3, "3800", before, "02"),
            read(3, "3800", before, "02"),
            (3, "6df0000c 4505 3800 7a 3006090c0f1215", ()),
            read(3, "3800", readback, "02"),
        )
        state = NativeState()
        async with _gateway(script) as manager, manager.peer_capture(ports) as capture:
            for port in ports:
                present = await manager.read_status(
                    port,
                    NativeStatusQuery(StatusRequest.MASTER),
                )
                assert present.answered
            query = NativeBlockQuery(local.address, 0x38, selector=2)
            for path in (mirror, local):
                await manager.read_block(path.port, NativeBlockQuery(path.address, 0))
                await manager.read_block(path.port, query)
            async with asyncio.timeout(5):
                while any(state.block(p, 0x38, 2) is None for p in (local, mirror)):
                    state.observe(await capture.read())
            owner = state.system_schedule(3, ports=ports).owner
            assert owner and owner.config.setup.body == bytes.fromhex(before)
            assert owner.config.setup.port == local.port
            expected = state.identity(local)
            assert expected is not None
            result = await manager.write_schedule_time(
                owner.config.setup.port,
                owner.config.target,
                ScheduleChange(ScheduleEvent.OCCUPIED, Day.MONDAY, time(8)),
                expected_identity=expected,
                preview=False,
            )
            assert result.status is NativeWriteStatus.CONFIRMED
            assert result.readback
            assert result.readback.require_report().body == bytes.fromhex(readback)

    async def test_snowmelt_uses_configured_zone_and_separate_group(self) -> None:
        def read(block: str, reply: str) -> Exchange:
            return _block(2, "14 04000000", block, reply)

        app = read("d000", "6e7140c5 d00003 0302")
        zone = read("c000", "6e7140c4 c00001 03")
        script = (
            read("0000", "6e7140c9 00003f 499d01 ca0402"),
            app,
            zone,
            (2, "17f00004 11022526", ("57e140c8 1902250400260600",)),
            (2, "21f00002 0401", ()),
            (2, "17f00003 11011b", ("57e140c5 19011b0304",)),
            app,
            zone,
            (
                2,
                "17f00004 11021922",
                ("57e140c5 210122ff05", "57e140c5 1901190200", "57e140c5 1901220006"),
            ),
        )
        async with _gateway(script) as manager:
            result = await manager.set_snowmelt_group(
                2,
                _SNOW,
                SnowmeltOperation.MELT,
                enabled=True,
                expected_group=4,
                expected_zone=3,
                preview=False,
            )
            assert (result.group, result.zone) == (4, 3)
            assert result.status is SnowmeltGroupStatus.STATE_REPORTED
            assert result.observed_state is SnowmeltOperatingState.MELTING
            reading = await manager.read_snowmelt_status(2, _SNOW, keys=b"\x19\x22")
            assert reading.answered and len(reading.reports) == 2
            assert all(report.message.source == _SNOW for report in reading.reports)
            water = reading.snowmelt_pair(SnowmeltKey.WATER_DETECTION)
            slab = reading.snowmelt_pair(SnowmeltKey.SLAB_TEMPERATURE)
            assert water and water.water_detection is WaterDetection.DRY
            assert slab and slab.raw == 0x600

    async def test_snowmelt_idle_target_off_does_not_enable_a_group(self) -> None:
        def read(block: str, reply: str) -> Exchange:
            return _block(2, "14 04000000", block, reply)

        baseline = read("7200", "6e7140c5 720003 b004")
        script = (
            read("0000", "6e7140c9 00003f 499d01 ca0402"),
            read("d000", "6e7140c5 d00003 0302"),
            baseline,
            baseline,
            (2, "6df00006 1403 7200 0000", ()),
            read("7200", "6e7140c5 720003 0000"),
        )
        async with _gateway(script) as manager:
            result = await manager.write_snowmelt_setpoint(
                2,
                _SNOW,
                SnowmeltBlock.IDLE_SETPOINT,
                None,
                expected_application=SnowmeltApplication.ELECTRIC,
                preview=False,
            )
            assert result.status is NativeWriteStatus.CONFIRMED
            assert result.readback is not None
            assert result.readback.require_report().decoded["idle_setpoint_raw"] == 0

    async def test_indexed_plant_io_and_setpoint_use_independent_ports(self) -> None:
        old = _block(1, "31 40000000", "2c00", "6e7311cb 2c00ff 0008 8c07 0000 6009")
        script = (
            _block(4, "2a 80000000", "0000", "6e72a209 00003f a79601 b60402"),
            _block(4, "2a 80000000", "4400", "6e72a206 440007 81 4523", "81"),
            (
                4,
                "1f600002 43a5",
                (
                    "5f52a20d 43a5e702 410042caff00 08 3412",
                    "5f52a20e 43a5a302 580059350000 01 0100 e4",
                ),
            ),
            _block(1, "31 40000000", "0000", "6e7311c9 00003f ee8a01 760402"),
            _block(1, "31 40000000", "2600", "6e7311c4 260001 02"),
            old,
            old,
            (1, "6df0000c 3107 2c00 7008 8c07 0000 6009", ()),
            _block(1, "31 40000000", "2c00", "6e7311c5 2c0003 7008"),
        )
        async with _gateway(script) as manager:

            async def change_setpoint() -> None:
                edit = await manager.write_setpoint_target(
                    1,
                    _SETPOINT,
                    SetpointTargetChange(
                        SetpointApplication.DHW,
                        SetpointSlot.WAKE_OCCUPIED,
                        55,
                    ),
                    preview=False,
                )
                assert edit.status is NativeWriteStatus.CONFIRMED
                assert edit.readback is not None and edit.readback.port == 1
                assert (
                    edit.readback.require_report().decoded["wake_occupied_raw"] == 2160
                )

            product, _ = await asyncio.gather(
                manager.read_block_group(4, NativeBlockQuery(_PLANT, 0)),
                change_setpoint(),
            )
            pump = await manager.read_block_group(
                4,
                NativeBlockQuery(_PLANT, 0x44, 0x81),
            )
            directory = await manager.read_status(
                4,
                NativeStatusQuery.io(0x43, 0xA5),
                peer=_PLANT,
            )
            state = NativeSnapshot((product, pump), (directory,)).observed_state()
            plant = state.plant_registers(NativePath(4, _PLANT.subnet, _PLANT.node))
            assert plant is not None
            assert [(r.selector, r.hours) for r in plant.runtimes] == [(0x81, 0x2345)]
            book = IoDirectory.from_batch(directory)
            assert book.complete and book.advertised_count == 2
            assert [(c.sample.channel, c.sample.value) for c in book.channels] == [
                (0xA3, 1),
                (0xE7, 0x1234),
            ]
            assert book.channels[0].frame.decoded["extension_hex"] == "e4"

    async def test_peer_counters_use_the_device_local_selector(self) -> None:
        script = (
            _block(4, "2a 80000000", "0000", "6e72a209 00003f aa9601 b90402"),
            _block(4, "2a 80000000", "4800", "6e72a206 480085 01fe07", "01"),
            _block(4, "2a 80000000", "4900", "6e72a208 49001f 01 00 09 04 02", "01"),
            _block(4, "2a 80000000", "6200", "6e72a207 62000f 34560080"),
        )
        async with _gateway(script) as manager:
            identity = await manager.read_block_group(4, NativeBlockQuery(_PLANT, 0))
            assert identity.require_report().decoded["model"] == "406"
            reports = [
                await manager.read_block_group(
                    4,
                    NativeBlockQuery(_PLANT, block, selector),
                )
                for block, selector in ((0x48, 1), (0x49, 1), (0x62, None))
            ]
            assert all(not report.missing_nodes for report in reports)
            assert NativeSnapshot((identity, *reports), ()).peer_links == (
                (
                    NativePath(4, _PLANT.subnet, _PLANT.node),
                    PeerLinkCounters(
                        (PeerBusErrors(1, None, 254, None, None, None, None, 7),),
                        (PeerBusEvents(1, 0, 9, 4, 2),),
                        0x80005634,
                    ),
                ),
            )

    async def test_setpoint_refuses_changed_application_and_checks_off_readback(
        self,
    ) -> None:
        def read(block: str, reply: str) -> Exchange:
            return _block(1, "31 40000000", block, reply)

        identify = read("0000", "6e7311c9 00003f ee8a01 760402")
        secondary = read("2b00", "6e7311c7 2b000f 4e07 f406")
        script = (
            identify,
            read("2600", "6e7311c4 260001 02"),  # Now a DHW controller, not a hot tub.
            identify,
            read("2600", "6e7311c4 260001 01"),
            read("2a00", "6e7311cb 2a00ff 4e07 f406 4605 6207"),
            secondary,
            secondary,
            (1, "6df00008 3107 2b00 4e07 d804", ()),
            read("2b00", "6e7311c5 2b000c f406"),  # ACK without applying Off.
        )
        async with _gateway(script) as manager:
            change = SetpointTargetChange(
                SetpointApplication.HOT_TUB,
                SetpointSlot.SLEEP,
                None,
            )
            with self.assertRaisesRegex(ValueError, "confirm the selected application"):
                await manager.write_setpoint_target(1, _SETPOINT, change, preview=False)
            result = await manager.write_setpoint_target(
                1,
                _SETPOINT,
                change,
                preview=False,
            )
            assert result.tx_result and result.tx_result.status is TxStatus.ACK
            assert result.status is NativeWriteStatus.DIFFERENT
            assert (
                result.readback
                and result.readback.require_report().decoded["sleep_raw"] == 1780
            )

    async def test_plant_topology_resolves_each_loop_with_forwarded_reports(
        self,
    ) -> None:
        boiler = NativePath(3, 0x45, 31)
        mix = NativePath(2, 0x56, 31)
        statuses = (
            "7cf457c9 bc07 00c0 9828 2211 fe",
            "7cf567c9 0807 00c0 1426 4400 01",
        )
        script = (
            (3, "3f700000", ("7ff457c2 4556", *statuses)),
            (2, "3f700000", ("7ff567c2 4556", *statuses)),
            _block(3, "45 00000040", "0000", "6e7457c9 00003f a79601 b60402"),
            _block(2, "56 00000040", "0000", "6e7567c9 00003f a79601 b60402"),
            _block(3, "45 00000040", "2400", "6e7457c5 240041 00 02"),
            _block(2, "56 00000040", "0200", "6e7567c5 020003 85 31"),
        )
        state = NativeState()
        async with _gateway(script) as manager, manager.peer_capture((2, 3)) as capture:
            query = NativeStatusQuery(StatusRequest.MASTER)
            for path, register in ((boiler, 0x24), (mix, 0x02)):
                response = await manager.read_status(path.port, query)
                assert (
                    response.answered
                    and response.reports[0].message.source == path.address
                )
                async with asyncio.timeout(3):
                    while sum(
                        s.port == path.port and s.master is not None
                        for s in state.water_sources
                    ) < len(statuses):
                        state.observe(await capture.read())
                for block in (0, register):
                    await manager.read_block(
                        path.port,
                        NativeBlockQuery(path.address, block),
                    )

            async with asyncio.timeout(3):
                while any(
                    state.block(path, block) is None
                    for path, block in ((boiler, 0x24), (mix, 0x02))
                ):
                    state.observe(await capture.read())

        assert len(state.water_sources) == 4 and state.dropped == 0
        (plant,) = state.water_systems(ports=(2, 3))
        first, second = plant.loops
        assert first.source and (first.source.port, first.source.address) == (
            boiler.port,
            boiler.address,
        )
        assert first.source.master and first.source.master.value.supply_f == 113
        assert second.source and (second.source.port, second.source.address) == (
            mix.port,
            mix.address,
        )
        assert second.source.master and second.source.master.value.supply_f == 95
        dhw = state.plant_registers(boiler)
        reset = state.plant_registers(mix)
        assert dhw and dhw.report(0x24) and dhw.report(0x02) is None
        assert reset and reset.report(0x02) and reset.report(0x24) is None

    async def test_cold_watch_finds_a_quiet_thermostat_from_the_master_subnet(
        self,
    ) -> None:
        path = NativePath(3, 0x45, 0)
        quiet = NativePath(3, 0x45, 5)
        script = (
            (3, "32700000", ("72f45002 fa05", "7cf45009 bc07 00c0 9828 2211 fe")),
            (3, "3f700000", ("7ff45001 45",)),
            (
                3,
                "6d70000b 0000 45 ffffff7f 00000000",
                ("6e745149 00003f 9d8f01 9c0402",),
            ),
            _block(3, "45 10000000", "0300", "6e745144 030001 01"),
        )
        observed: asyncio.Future[NativeState] = (
            asyncio.get_running_loop().create_future()
        )

        def change(peer: NativePath, state: NativeState) -> None:
            if (
                peer == quiet
                and (climate := state.climate(quiet)) is not None
                and climate.setup.mode is ThermostatMode.HEAT
                and not observed.done()
            ):
                observed.set_result(state)

        async with (
            _gateway(script) as manager,
            manager.native_session((3,)) as session,
            asyncio.TaskGroup() as tasks,
        ):
            watch = tasks.create_task(
                watch_native(
                    session,
                    schedules=False,
                    thermostat_targets=False,
                    thermostat_sensors=False,
                    on_change=change,
                ),
            )
            # Seed unsolicited loop telemetry on the running Normal link.
            outdoor = await manager.read_status(
                3,
                NativeStatusQuery(StatusRequest.OUTDOOR),
            )
            assert outdoor.answered
            async with asyncio.timeout(5):
                state = await observed
            (plant,) = state.water_systems(ports=(3,))
            source = plant.loops[0].source
            assert source and source.address == path.address
            assert source.master and source.master.value.supply_f == 113
            watch.cancel()

    async def test_header_only_presence_selects_the_local_thermostat_read_route(
        self,
    ) -> None:
        local = NativePath(3, 0x45, 5)
        mirror = NativePath(2, 0x45, 5)
        report = "6e745149 00003f 9d8f01 9c0402"  # 527 thermostat.
        partial = "6e745148 00001f 9d8f01 9c04"  # Z missing.
        different = "6e745149 00003f 8e9a01 c60402"  # 553 thermostat.
        script = (
            (3, "3f700000", ("7ff457c0",)),
            (2, "3f700000", ("7ff677c0",)),
            _block(2, "45 10000000", "0000", report),
            _block(3, "45 10000000", "0000", report),
            _block(3, "45 10000000", "0300", "6e745144 030001 01"),
            _block(3, "45 10000000", "0000", partial),
            _block(3, "45 10000000", "0000", different),
        )
        state = NativeState()
        async with _gateway(script) as manager, manager.peer_capture((2, 3)) as capture:
            for port in (3, 2):
                result = await manager.read_status(
                    port,
                    NativeStatusQuery(StatusRequest.MASTER),
                )
                assert result.answered and result.reports[0].message.payload == b""
            for path in (mirror, local):
                await manager.read_block(path.port, NativeBlockQuery(path.address, 0))

            async with asyncio.timeout(5):
                while any(state.identity(p) is None for p in (local, mirror)):
                    state.observe(await capture.read())

            (observed,) = state.device_routes(ports=(2, 3))
            preferred = observed.preferred
            assert preferred is not None and preferred == local
            assert observed.paths == (mirror, local)
            assert not state.has_master_announcement(preferred.port)  # No hydronic map.
            sample = await manager.read_block(
                preferred.port,
                NativeBlockQuery(preferred.address, 3),
            )
            assert sample.port == local.port and sample.decoded["mode_setting"] == 1
            expected = state.identity(preferred)
            assert expected is not None
            for seen in (
                NativeIdentity(102301, 1180, None, None),
                NativeIdentity(105102, 1222, 2, None),
            ):
                with self.assertRaises(NativeIdentityMismatchError) as raised:
                    await manager.write_thermostat_target(
                        preferred.port,
                        preferred.address,
                        ThermostatTargetChange(
                            ThermostatTarget.HEAT,
                            TargetSlot.WAKE,
                            23,
                        ),
                        expected_identity=expected,
                        preview=False,
                    )
                assert raised.exception.expected == expected
                assert raised.exception.observed == seen
