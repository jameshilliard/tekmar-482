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
from datetime import time
from pathlib import Path
from typing import TYPE_CHECKING

from tekmar_482 import (
    BridgeClient,
    BridgeManagement,
    GatewayEndpoint,
    IoDirectory,
    NativeAddress,
    NativeBlockQuery,
    NativeField,
    NativeIdentity,
    NativeIdentityMismatchError,
    NativePath,
    NativeSnapshot,
    NativeState,
    NativeStatusQuery,
    NativeWriteStatus,
    ScheduleChange,
    ScheduleEvent,
    ScheduleTarget,
    SetpointApplication,
    SetpointSlot,
    SetpointTargetChange,
    SnowmeltApplication,
    SnowmeltBlock,
    SnowmeltGroupStatus,
    SnowmeltKey,
    SnowmeltOperatingState,
    SnowmeltOperation,
    StatusRequest,
    TargetSlot,
    ThermostatMode,
    ThermostatTarget,
    ThermostatTargetChange,
    TxStatus,
    WaterDetection,
    read_weekly_schedule,
    watch_native,
)
from tekmar_482.wire_trace import observe_bridge_packets

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

    from tekmar_482 import Packet

type Exchange = tuple[int, str, tuple[str, ...]]
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
            read("c200", "6e7140c4 c20001 04"),
            read("c300", "6e7140c4 c30001 06"),
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
