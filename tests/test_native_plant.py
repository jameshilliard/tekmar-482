import asyncio
import json
import unittest
from typing import TYPE_CHECKING
from unittest.mock import patch

from tekmar_482 import (
    BoilerChillerOutput,
    CaptureGap,
    EquipmentBank,
    GatewayTraceGap,
    HeatPumpOutput,
    Modulation,
    NativeBlockQuery,
    NativeFrame,
    NativeMessage,
    NativePath,
    NativeState,
    Packet,
    PlantKind,
    PlantRegisters,
    RuntimeRegister,
    StageBits,
    StageCounts,
    WaterMonitor,
    read_water,
    watch_native,
)
from tekmar_482.cli import build_parser
from tekmar_482.cli_native_watch import watch_all_from_args
from tekmar_482.cli_status import plant_register_fields, water_fields
from tekmar_482.raw import RawDispatcher

from .native_fixtures import TEST_PATH, block, frame, identity

if TYPE_CHECKING:
    from tekmar_482.raw import CaptureRecord

_STAGED = bytes.fromhex("00 04 c2 65 34 56 f3")
_MODULATING = bytes.fromhex("00 06 82 19 0a 64 56 67 e3")
_OTHER = NativePath(2, TEST_PATH.subnet, TEST_PATH.node)
_274 = 102002
_402 = 104103


def _registers(state: NativeState, path: NativePath = TEST_PATH) -> PlantRegisters:
    result = state.plant_registers(path)
    assert result is not None
    return result


class PlantStateTests(unittest.TestCase):
    def test_output_instances_are_local_and_device_zero_is_valid(self) -> None:
        state = NativeState()
        for path in (TEST_PATH, _OTHER):
            state.observe(frame(0x24F, _MODULATING, path))
        for data in (
            _STAGED,
            bytes.fromhex("30 04 02 12 56 57 13"),
            bytes.fromhex("00 08 20 01 03"),
            bytes.fromhex("00 07 abcd"),
            bytes.fromhex("0a ff cafe"),
        ):
            assert state.observe(frame(0x24F, data)) == (TEST_PATH,)
        assert not state.observe(frame(0x24F, _STAGED, stamp=15))
        assert not state.observe(frame(0x24F, _STAGED[:-1]))
        peer = state.water_source(TEST_PATH)
        other = state.water_source(_OTHER)
        assert peer and other and peer.identity is None
        samples = peer.equipment.outputs
        assert len(other.equipment.outputs) == 1
        assert [s.value.key for s in samples] == [
            (4, 0),
            (4, 3),
            (6, 0),
            (7, 0),
            (8, 0),
            (255, 0),
        ]
        assert samples[0].timestamp_ms == 15 and samples[0].value.device_total == 0
        boiler = samples[0].value.body
        assert isinstance(boiler, BoilerChillerOutput) and isinstance(
            boiler.stages,
            StageBits,
        )
        assert boiler.stages.operating == (2, 3) and boiler.stages.available == (1, 3)
        mod = samples[2].value
        assert mod.kind is PlantKind.MODULATING_BOILER and isinstance(
            mod.body,
            BoilerChillerOutput,
        )
        assert mod.body.stages == Modulation(25, 10, 100)
        hp = samples[4].value.body
        assert isinstance(hp, HeatPumpOutput) and isinstance(hp.stages, StageCounts)
        assert hp.stages.percent_on == 100 / 3
        assert (
            samples[3].extension == bytes.fromhex("abcd")
            and samples[5].value.kind is None
        )
        assert samples[5].extension == bytes.fromhex("cafe")
        assert (
            not state.plant_configurations
        )  # Output identity is not block-zero product identification.
        assert '"extension_hex": "cafe"' in json.dumps(water_fields(peer))

    def test_stage_subnets_and_modes_do_not_infer_physical_output(self) -> None:
        state = NativeState()
        for data in ("15 21 c2 4506", "15 00 00", "25 01 80 7806"):
            state.observe(frame(0x367, bytes.fromhex(data)))
        state.observe(frame(0x367, bytes.fromhex("15 21 02 ca35")))
        state.observe(frame(0x363, bytes.fromhex("15 c0")))
        state.observe(frame(0x363, bytes.fromhex("15 40")))
        for mid, data in ((0x337, "ff"), (0x37F, "81"), (0x3C7, "25"), (0x3C7, "15")):
            state.observe(frame(mid, bytes.fromhex(data)))
        assert not state.observe(frame(0x35F, bytes.fromhex("15 81 01 00 00")))
        s = state.water_source(TEST_PATH)
        assert s
        e = s.equipment
        assert (
            not e.outputs
            and e.boiler_maximum
            and e.boiler_maximum.value.percent is None
        )
        assert e.cycle and e.cycle.value.fields() == {
            "cycle_length": 1,
            "hold_override": 1,
        }
        assert [d.value.subnet for d in e.relay_heartbeats] == [0x15, 0x25]
        assert len(e.general_demands) == 1 and e.general_demands[0].value.flags == 0x40
        assert [
            (v.value.subnet, v.value.stage, v.value.output_on, v.value.target_raw)
            for v in e.stage_demands
        ] == [(0x15, 0, False, None), (0x15, 1, False, None), (0x25, 1, True, 0x678)]
        assert e.stage_demands[1].extension == bytes.fromhex("ca35")

    def test_mixing_update_never_latches_command_bytes_as_state(self) -> None:
        monitor = WaterMonitor()
        report = frame(0x34F, bytes.fromhex("f7 3a c1 ed 01 02 03 aa"))
        cmd = frame(0x357, bytes.fromhex("fd fe 14 ca35"))
        assert cmd.mixing_update and cmd.mixing_update.target_raw == 0x3FE
        assert not monitor.observe(cmd) and not monitor.sources
        assert monitor.observe(report)
        assert monitor.observe(frame(0x347, bytes.fromhex("c1 14")))
        assert not monitor.observe(cmd)
        assert monitor.observe(frame(0x347, bytes.fromhex("c1 15")))
        mix = monitor.sources[0].equipment.mixing_report
        assert mix and (
            mix.value.target_raw,
            mix.value.supply_raw,
            mix.value.return_raw,
        ) == (0x101, 0x302, 0x203)
        assert mix.value.sensor_flags == 3 and mix.extension == b"\xaa"
        assert monitor.observe(frame(0x347, bytes.fromhex("c2 15")))
        assert monitor.sources[0].equipment.mixing_report is None
        assert not monitor.observe(frame(0x34F, bytes(6)))

    def test_move_gap_and_replacement_discard_previous_equipment(self) -> None:
        state = NativeState()
        state.observe(identity(_402))
        state.observe(frame(0x24F, _STAGED))
        moved = NativePath(TEST_PATH.port, 0x62, 4)
        taking = frame(0x3E7, bytes([TEST_PATH.subnet, TEST_PATH.node]), moved)
        state.observe(taking)
        assert not state.water_sources and not state.plant_configurations
        state.observe(frame(0x24F, _MODULATING, moved))
        state.observe(identity(_402, path=moved))
        assert (
            state.water_source(moved) is None
        )  # Stale pre-identification observations cannot follow a move.
        state.observe(frame(0x24F, _STAGED, moved))
        assert not state.observe(taking) and state.water_source(moved)
        state.observe(CaptureGap(3))
        state.observe(frame(0x24F, _MODULATING, moved))
        state.observe(identity(_274, path=moved))
        assert state.water_source(moved) is None and state.dropped == 3
        state.observe(frame(0x24F, _STAGED, moved))
        state.observe(identity(_402, path=moved))
        assert not state.water_sources

    def test_trace_gap_discards_only_the_affected_wires_native_samples(self) -> None:
        state = NativeState()
        for path in (TEST_PATH, _OTHER):
            state.observe(identity(_402, path=path))
            state.observe(frame(0x24F, _STAGED, path))
            state.observe(block(0x44, bytes.fromhex("07 81 4523"), path))
        prior = state.water_source(TEST_PATH)
        assert prior and prior.equipment.outputs
        assert state.observe(GatewayTraceGap(3, 1 << _OTHER.port)) == (_OTHER,)
        assert state.water_sources == (prior,) and state.identity(_OTHER) is None
        assert len(state.plant_configurations) == 1
        assert _registers(state).runtimes[0].hours == 0x2345
        assert state.observe(CaptureGap(2)) == (TEST_PATH,)
        assert not state.paths and state.dropped == 5

    def test_bank_masks_runtime_selector_and_complete_word_are_independent(
        self,
    ) -> None:
        state = NativeState()
        for p in (TEST_PATH, _OTHER):
            state.observe(identity(_274, path=p))
            assert (0x43, 0) not in _registers(state, p).missing
            state.observe(block(0x35, bytes.fromhex("03 05 01"), p))
            state.observe(block(0x43, bytes.fromhex("07 00 3412"), p))
            state.observe(block(0x43, bytes.fromhex("05 02 ab"), p))
        view = _registers(state)
        assert view.banks[0].bank is EquipmentBank.BOILERS and view.banks[
            0
        ].available_indices == (0, 2)
        assert [(r.register, r.selector, r.hours) for r in view.runtimes] == [
            (RuntimeRegister.BOILER, 0, 0x1234),
            (RuntimeRegister.BOILER, 2, None),
        ]
        state.observe(block(0x35, bytes.fromhex("03 04 04")))
        assert state.block(TEST_PATH, 0x43, 0) is None and state.block(_OTHER, 0x43, 0)
        assert state.block(TEST_PATH, 0x43, 2) is not None
        dest = bytes([TEST_PATH.subnet, TEST_PATH.node, 0x43, 0, 2, 0, 0])
        assert state.observe(frame(0x2DF, dest, NativePath(TEST_PATH.port, 0, 0))) == (
            TEST_PATH,
        )
        assert (0x43, 2) in _registers(state).missing and not _registers(state).runtimes
        state.observe(block(0x35, bytes.fromhex("01 00")))
        assert (
            _registers(state).banks[0].available_indices == ()
            and _registers(state).banks[0].enabled_mask is None
        )
        assert not [k for k in _registers(state).missing if k[0] == 0x43]
        state.observe(block(0x35, bytes.fromhex("02 05")))
        assert (
            _registers(state).banks[0].available_indices is None
            and (0x43, 0) in _registers(state).missing
        )
        state.observe(identity(_402))
        assert (0x44, 129) in _registers(state).missing and not _registers(state).banks
        state.observe(block(0x44, bytes.fromhex("07 81 4523")))
        assert not state.observe(block(0x44, bytes.fromhex("06 aabb")))
        assert _registers(state).runtimes[0].hours == 0x2345
        assert '"selector": 129' in json.dumps(plant_register_fields(_registers(state)))
        node0 = NativePath(TEST_PATH.port, 0x62, 0)
        state.observe(identity(_402, path=node0))
        assert not _registers(state, node0).missing

    def test_loop_label_waits_for_setup_report_after_addressed_change(self) -> None:
        state = NativeState()
        for path in (TEST_PATH, _OTHER):
            state.observe(frame(0x3FF, bytes([path.subnet]), path))
            state.observe(block(0x33, bytes([3, path.subnet, 2]), path))
            state.observe(frame(0x24F, _STAGED, path))
        host = NativePath(TEST_PATH.port, 0, 0)
        prefix = bytes([TEST_PATH.subnet, TEST_PATH.node, 0x33, 0, TEST_PATH.subnet, 3])
        assert not state.observe(frame(0x2DF, prefix, host))
        changed = frame(0x2DF, prefix + bytes(6), host)
        assert state.observe(changed) == (TEST_PATH,)
        source = state.water_source(TEST_PATH)
        elsewhere = state.water_source(_OTHER)
        assert source and source.assignment is None and not source.equipment.outputs
        assert (
            elsewhere
            and elsewhere.assignment
            and elsewhere.assignment.mix_number == 2
            and elsewhere.equipment.outputs
        )
        state.observe(frame(0x3FF, bytes([TEST_PATH.subnet])))
        state.observe(frame(0x24F, _MODULATING))
        state.observe(block(0x34, bytes([0])))
        pending = state.water_source(TEST_PATH)
        assert (
            pending and pending.pending_setup == {0x33} and pending.assignment is None
        )
        state.observe(block(0x33, bytes([3, TEST_PATH.subnet, 3])))
        verified = state.water_source(TEST_PATH)
        assert verified and verified.assignment and verified.assignment.mix_number == 3
        assert not verified.pending_setup and not verified.equipment.outputs


class PlantSessionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tx: asyncio.Queue[bytes] = asyncio.Queue()
        self.stream = RawDispatcher(self.tx.put, 256)
        idle: asyncio.Queue[Packet] = asyncio.Queue()
        await self.enterAsyncContext(self.stream.receiving(idle.get))

    def rx(self, f: NativeFrame) -> None:
        self.stream.feed(
            Packet(
                1,
                bytes([0, f.port]) + f.timestamp_ms.to_bytes(4, "little") + f.body,
            ),
        )

    def ack(self) -> None:
        self.stream.feed(Packet(1, bytes([2, TEST_PATH.port, 0])))

    async def sent(self) -> NativeMessage:
        async with asyncio.timeout(1):
            p = await self.tx.get()
        assert p[:2] == bytes([1, TEST_PATH.port])
        return NativeMessage.from_bytes(p[2:])

    async def query(self) -> tuple[int, int]:
        m = await self.sent()
        assert m.message_id == 0x2D7 and m.payload[2] == TEST_PATH.subnet
        assert (
            int.from_bytes(m.payload[3:7], "little") == TEST_PATH.address.request_mask()
        )
        return int.from_bytes(m.payload[:2], "little"), int.from_bytes(
            m.payload[7:],
            "little",
        )

    async def test_passive_water_cycles_are_per_source_not_a_host_hold(self) -> None:
        self.rx(frame(0x37F, bytes.fromhex("80"), TEST_PATH, stamp=100))
        self.rx(frame(0x37F, bytes.fromhex("7f"), _OTHER, stamp=101))
        self.rx(frame(0x24F, _MODULATING, _OTHER, stamp=102))
        result = await watch_native(self.stream, source=None, duration=0.02)
        first = result.state.water_source(TEST_PATH)
        other = result.state.water_source(_OTHER)
        assert first and (held := first.equipment.cycle)
        assert held.timestamp_ms == 100 and held.value.hold_override
        assert held.value.cycle_length == 0
        assert other and (ordinary := other.equipment.cycle) and other.equipment.outputs
        assert ordinary.value.cycle_length == 127 and not ordinary.value.hold_override
        assert not result.reads and self.tx.empty()

    async def serve_reconfiguration(self) -> set[tuple[int, int]]:
        assert await self.query() == (0, 0)
        self.ack()
        self.rx(identity(_274))
        seen = set()
        rotated = False
        while True:
            b, s = await self.query()
            assert (b, s) not in seen
            seen.add((b, s))
            self.ack()
            if b == 0x35:
                self.rx(block(b, b"\x03\x01\x01"))
            elif (b, s) == (0x43, 0):
                self.rx(block(0x35, b"\x03\x02\x02"))
                self.rx(block(b, bytes.fromhex("07 00 aa55")))
                self.rx(block(b, bytes.fromhex("07 00 aa55")))
                rotated = True
            elif (b, s) == (0x43, 1):
                assert rotated
                self.rx(block(b, bytes.fromhex("07 01 5623")))
                return seen
            else:
                self.rx(
                    block(
                        b,
                        bytes([1, s])
                        if b in (0x3C, 0x3D, 0x44, 0x45, 0x46)
                        else bytes(1),
                    ),
                )

    async def test_active_watch_rejects_late_output_bank_replies(self) -> None:
        all_frames: list[CaptureRecord] = []
        task = asyncio.create_task(
            watch_native(
                self.stream,
                [TEST_PATH],
                duration=0.32,
                timeout=0.035,
                schedules=False,
                on_record=all_frames.append,
            ),
        )
        await self.serve_reconfiguration()
        self.rx(frame(0x24F, _MODULATING))
        result = await task
        state = result.state
        assert not state.block(TEST_PATH, 0x43, 0)
        assert _registers(state).runtimes[0].hours == 0x2356
        assert (
            state.water_sources[0].equipment.outputs[0].value.kind
            is PlantKind.MODULATING_BOILER
        )
        assert block(0x43, bytes.fromhex("07 00 aa55")) in all_frames
        assert not any(
            b.query == NativeBlockQuery(TEST_PATH.address, 0x43, 0)
            for b in result.latest_blocks
        )

    async def test_finite_read_deadline_includes_waiting_for_transmit_idle(
        self,
    ) -> None:
        waiting = asyncio.Event()
        unavailable = asyncio.Event()
        busy = False

        async def wait_busy(port: int) -> None:
            assert port == TEST_PATH.port
            if busy:
                waiting.set()
                await unavailable.wait()

        with patch.object(self.stream, "wait_transmit_idle", wait_busy):
            async with asyncio.timeout(1):
                task = asyncio.create_task(
                    read_water(self.stream, [TEST_PATH.port], duration=0.12),
                )
                assert (await self.sent()).message_id == 0x3F7
                self.ack()
                self.rx(frame(0x3FF, bytes([TEST_PATH.subnet])))
                assert await self.query() == (0, 0)
                busy = True
                self.ack()
                self.rx(identity(_402))
                await waiting.wait()
                result = await task
        assert len(result.block_reads) == 1 and self.tx.empty()
        assert result.unfinished_ports == (TEST_PATH.port,)
        assert result.plant_configurations[0][1].missing

    async def test_read_water_uses_same_generation_and_keeps_partial_results(
        self,
    ) -> None:
        task = asyncio.create_task(
            read_water(self.stream, [TEST_PATH.port], duration=0.32, timeout=0.04),
        )
        assert (await self.sent()).message_id == 0x3F7
        self.ack()
        self.rx(frame(0x3FF, bytes([TEST_PATH.subnet])))
        await self.serve_reconfiguration()
        self.rx(frame(0x24F, _STAGED))
        result = await task
        assert result.master_queries[0].answered
        (entry,) = result.plant_configurations
        assert entry[0] == TEST_PATH and [
            (v.selector, v.hours) for v in entry[1].runtimes
        ] == [(1, 0x2356)]
        assert result.sources[0].equipment.outputs and entry[1].missing
        assert result.block_reads[-1].tx_result is None
        self.ack()

    async def test_passive_cli_and_disabled_plant_reads_never_probe_equipment(
        self,
    ) -> None:
        args = build_parser().parse_args(
            [
                "watch-native",
                "--passive",
                "--duration",
                "0.02",
                "--tcp",
                "localhost:3001",
            ],
        )
        self.rx(identity(_402))
        self.rx(frame(0x24F, _MODULATING))
        self.rx(block(0x44, bytes.fromhex("07 81 0034")))
        log: list[object] = []
        assert (
            await watch_all_from_args(self.stream, args, log.append) == 0
            and self.tx.empty()
        )
        assert any(
            isinstance(d, dict) and d.get("plant_registers", {}).get("runtimes")
            for d in log
        )
        assert any(
            isinstance(d, dict)
            and d.get("water_source", {}).get("equipment", {}).get("outputs")
            for d in log
        )
        task = asyncio.create_task(
            watch_native(
                self.stream,
                [TEST_PATH],
                schedules=False,
                plant_settings=False,
                duration=0.06,
            ),
        )
        assert await self.query() == (0, 0)
        self.ack()
        self.rx(identity(_402))
        assert (await task).reads == 1 and self.tx.empty()

    async def test_read_water_402_reads_distinct_pump_selectors_and_skips_empty_bank(
        self,
    ) -> None:
        task = asyncio.create_task(
            read_water(self.stream, [TEST_PATH.port], duration=0.2, timeout=0.1),
        )
        assert (await self.sent()).message_id == 0x3F7
        self.ack()
        self.rx(frame(0x3FF, bytes([TEST_PATH.subnet])))
        assert await self.query() == (0, 0)
        self.ack()
        self.rx(identity(_402))
        expected = {
            (b, 0) for b in (0x22, 0x23, 0x3C, 0x41, 0x43, 0x44, 0x4B, 0x4C, 0x4E, 0x50)
        } | {(0x44, 129)}
        for _ in range(len(expected)):
            b, selector = await self.query()
            expected.remove((b, selector))
            if b in (0x43, 0x44):
                data = bytes([7, selector, selector + 1, 0])
            elif b in (0x3C, 0x50):
                data = bytes([1, 0])
            else:
                data = bytes(1)
            self.ack()
            self.rx(block(b, data))
        result = await task
        assert not expected and self.tx.empty()
        ((path, settings),) = result.plant_configurations
        assert path == TEST_PATH and not settings.missing
        assert [
            (r.selector, r.hours)
            for r in settings.runtimes
            if r.register is RuntimeRegister.PUMP
        ] == [(0, 1), (129, 130)]
