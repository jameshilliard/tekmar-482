from __future__ import annotations

import asyncio
import unittest

from tekmar_482 import (
    CaptureGap,
    FailureReason,
    NativeAddress,
    NativeBlockBatch,
    NativeBlockQuery,
    NativeFrame,
    NativeMessage,
    NativePath,
    NativeSnapshot,
    NativeState,
    Packet,
    SnowmeltAddress,
    SnowmeltApplication,
    SnowmeltGroupResult,
    SnowmeltKey,
    SnowmeltOperatingState,
    SnowmeltRole,
    ThermostatMode,
    TxResult,
    TxStatus,
    WaterDetection,
    watch_native,
)
from tekmar_482 import (
    SnowmeltGroupStatus as GroupStatus,
)
from tekmar_482 import (
    SnowmeltOperation as Operation,
)
from tekmar_482.cli import build_parser
from tekmar_482.cli_native_watch import snow_fields, watch_all_from_args
from tekmar_482.native_status import NativeStatusBatch
from tekmar_482.raw import RawDispatcher
from tests.native_fixtures import TEST_PATH as SITE
from tests.native_fixtures import block, frame, identity

SENSOR = SnowmeltAddress(SnowmeltRole.SENSOR, 3)
ZONE = SnowmeltAddress(SnowmeltRole.ZONE, 3)
CONFIG = {
    0xD0: b"\x03\x03\x02",
    0xC0: b"\x01\x03",
    0xC1: b"\x01\x03",
    0xC2: b"\x01\x04",
    0xC3: b"\x01\x06",
    0xC4: b"\x01\x00",
    0x71: b"\x03\xf0\x05",
    0x72: b"\x03\x00\x00",
    0x78: b"\x03\x00\x00",
}


def snow(
    address: SnowmeltAddress,
    pairs: list[tuple[int, int]],
    path: NativePath = SITE,
    *,
    stamp: int = 1,
) -> NativeFrame:
    payload = bytes([address.request_byte + 8, len(pairs)])
    return frame(
        0x17E,
        payload + b"".join(bytes([k]) + v.to_bytes(2, "little") for k, v in pairs),
        path,
        stamp=stamp,
    )


def setup_frames(path: NativePath = SITE) -> list[NativeFrame]:
    return [
        identity(105801, path=path),
        *(block(b, data, path) for b, data in CONFIG.items()),
    ]


class SnowmeltStateTests(unittest.TestCase):
    def test_source_role_and_number_remain_independent(self) -> None:
        state = NativeState()
        other = NativePath(1, SITE.subnet, SITE.node)
        report = snow(
            ZONE,
            [(0x22, 0x600), (0x22, 0x601), (0x19, 4), (0x1B, 0x0403), (0x99, 0xCA35)],
        )
        state.observe(report)
        assert (
            not state.snowmelt_devices
            and state.snowmelt_channels(SITE)[0].last_frame is report
        )
        for item in setup_frames() + setup_frames(other):
            state.observe(item)
        state.observe(snow(SENSOR, [(0x22, 0x5F0), (0x19, 3)]))
        state.observe(snow(SnowmeltAddress(SnowmeltRole.ZONE, 4), [(0x22, 0x5FF)]))
        state.observe(snow(ZONE, [(0x22, 0x5F1)], other))
        device = state.snowmelt(SITE)
        assert device and device.primary
        zone = device.primary
        assert (
            device.setup.application is SnowmeltApplication.ELECTRIC
            and device.setup.primary == ZONE
        )
        assert device.setup.idle_setpoint_raw == 0 and device.setup.melt_group == 4
        assert len(device.channels) == 3 and len(state.snowmelt_devices) == 2
        assert zone.temperature_f(SnowmeltKey.SLAB_TEMPERATURE) == 68.7
        assert (
            zone.operating_state is SnowmeltOperatingState.MELTING
            and zone.water_detection is WaterDetection.OPEN
        )
        assert zone.raw(0x99) == 0xCA35 and zone.pump_on is None
        assert device.missing_keys == bytes.fromhex("23 29")
        assert not state.observe(snow(ZONE, [(0x22, 0x601)], stamp=100))
        refreshed = state.snowmelt(SITE)
        assert (
            refreshed
            and refreshed.primary
            and (latest := refreshed.primary.sample(0x22))
        )
        assert latest.timestamp_ms == 100 and zone.last_frame is report
        state.observe(snow(ZONE, [(0x22, 0xC000), (0x2A, 0)]))
        changed = state.snowmelt(SITE)
        assert changed and changed.primary and changed.primary.pump_on is False
        assert changed.primary.temperature_f(SnowmeltKey.SLAB_TEMPERATURE) is None
        assert changed.primary.raw(0x22) == 0xC000
        assert snow_fields(changed)["primary"] == {"role": "zone", "number": 3}
        alternate = state.snowmelt(other)
        assert alternate and alternate.primary and alternate.primary.raw(0x22) == 0x5F1

    def test_invalid_report_or_echo_does_not_create_or_change_state(self) -> None:
        state = NativeState()
        for data in (
            "0101220006",
            "6901220006",
            "ca01220006",
            "1b01220006",
            "1902220006",
        ):
            assert not state.observe(frame(0x17E, bytes.fromhex(data)))
        assert not state.paths
        for item in setup_frames():
            state.observe(item)
        for mid, data in (
            (0x17F, "110122"),
            (0x20F, "0411"),
            (0x21F, "0401"),
            (0x2DF, f"{SITE.subnet:02x}{SITE.node:02x}d00004"),
        ):
            assert not state.observe(frame(mid, bytes.fromhex(data)))
        device = state.snowmelt(SITE)
        assert device and device.primary is None and device.setup.primary == ZONE
        assert device.missing_keys == SnowmeltApplication.ELECTRIC.status_keys
        state.observe(frame(0x17E, bytes.fromhex("1901290000 220006")))
        device = state.snowmelt(SITE)
        assert device and device.primary and device.primary.raw(0x22) is None
        assert device.primary.raw(0x29) == 0

    def test_rebinding_and_reidentification_require_new_samples(self) -> None:
        state = NativeState()
        for item in setup_frames():
            state.observe(item)
        many = [snow(ZONE, [(0x22, 1234)]), snow(SENSOR, [(0x22, 1345)])]
        for item in many:
            state.observe(item)
        state.observe(block(0xC0, b"\x01\x04"))
        assert [c.address for c in state.snowmelt_channels(SITE)] == [SENSOR]
        state.observe(block(0xC0, CONFIG[0xC0]))
        device = state.snowmelt(SITE)
        assert device and device.primary is None
        state.observe(block(0xD0, b"\x01\x04"))
        device = state.snowmelt(SITE)
        assert device and device.setup.primary == SENSOR and not device.channels
        assert device.missing_keys == b"\x19\x22"
        state.observe(many[1])
        state.observe(block(0xC1, b"\x00"))
        device = state.snowmelt(SITE)
        assert device and device.setup.primary is None and device.missing_keys is None
        assert device.status_query() is None
        state.observe(block(0xD0, b"\x01\xaf"))
        state.observe(many[1])
        device = state.snowmelt(SITE)
        assert (
            device
            and device.setup.application_raw == 175
            and device.setup.application is None
        )
        assert device.primary is None
        assert state.observe(CaptureGap(2)) == (SITE,)
        state.observe(many[0])
        state.observe(identity(105801))
        assert not state.snowmelt_channels(SITE)
        state.observe(many[0])
        state.observe(identity(105102))
        assert not state.snowmelt_devices and not state.snowmelt_channels(SITE)

    def test_snapshot_keeps_partial_keys_and_unknown_words(self) -> None:
        result = TxResult(SITE.port, TxStatus.ACK)
        blocks = tuple(
            NativeBlockBatch(
                SITE.port,
                NativeBlockQuery(
                    SITE.address,
                    int.from_bytes(f.message.payload[:2], "little"),
                ),
                result,
                (f,),
            )
            for f in setup_frames()
        )
        (device,) = NativeSnapshot(blocks, ()).snowmelt_devices
        query = device.status_query()
        assert query
        answer = NativeStatusBatch(
            SITE.port,
            query,
            SITE.address,
            result,
            (snow(ZONE, [(0x22, 1230), (0x99, 4)]),),
            frozenset([0x22]),
            len(query.keys),
        )
        assert not answer.answered
        (device,) = NativeSnapshot(blocks, (answer,)).snowmelt_devices
        assert device.primary and device.primary.raw(0x22) == 1230
        assert device.primary.raw(0x99) == 4 and device.missing_keys == bytes.fromhex(
            "19 1b 23 29",
        )


class SnowmeltWatchTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tx: asyncio.Queue[bytes] = asyncio.Queue()
        self.stream = RawDispatcher(self.tx.put, 128)
        idle: asyncio.Queue[Packet] = asyncio.Queue()
        await self.enterAsyncContext(self.stream.receiving(idle.get))

    def rx(self, f: NativeFrame) -> None:
        self.stream.feed(
            Packet(
                1,
                bytes([0, f.port]) + f.timestamp_ms.to_bytes(4, "little") + f.body,
            ),
        )

    def ack(self, port: int = SITE.port) -> None:
        self.stream.feed(Packet(1, bytes([2, port, 0])))

    async def next_send(self) -> tuple[int, NativeMessage]:
        async with asyncio.timeout(1):
            wire = await self.tx.get()
        assert wire[0] == 1
        return wire[1], NativeMessage.from_bytes(wire[2:])

    async def boot(self) -> None:
        assert (await self.next_send())[1].message_id == 0x2D7
        self.ack()
        self.rx(identity(105801))

    def seed(self) -> None:
        for b, d in CONFIG.items():
            self.rx(block(b, d))

    def command(
        self,
        op: Operation,
        *,
        expected: int | None,
        preview: bool = False,
        enabled: bool = True,
        timeout: float = 1,
    ) -> asyncio.Task[SnowmeltGroupResult]:
        return asyncio.create_task(
            self.stream.set_snowmelt_group(
                SITE.port,
                SITE.address,
                op,
                enabled=enabled,
                expected_group=expected,
                preview=preview,
                timeout=timeout,
                source=NativeAddress(0, 0),
            ),
        )

    async def assignments(self, overrides: dict[int, bytes] | None = None) -> None:
        await self.boot()
        for b in (0xD0, 0xC0, 0xC2, 0xC3):
            port, msg = await self.next_send()
            assert (
                port == SITE.port
                and msg.message_id == 0x2D7
                and msg.payload[:2] == b.to_bytes(2, "little")
            )
            self.ack()
            self.rx(block(b, (overrides or {}).get(b, CONFIG[b])))

    async def test_control_preview_and_refuse_wrong_or_shared_group(self) -> None:
        task = self.command(Operation.STORM, expected=None, preview=True, enabled=False)
        await self.assignments()
        result = await task
        assert result.status is GroupStatus.PREVIEW and result.group == 6
        assert result.message.to_bytes() == bytes.fromhex("21f00002 0600")
        assert result.observed_state is None and self.tx.empty()
        with self.assertRaisesRegex(ValueError, "explicit expected group"):
            await self.command(Operation.MELT, expected=None)
        for expected, setup, error in (
            (7, {}, "differs from expected"),
            (4, {0xC3: b"\x01\x04"}, "ambiguous"),
            (4, {0xC2: b"\x00"}, "invalid"),
            (4, {0xC0: b"\x01\x00"}, "zone"),
        ):
            task = self.command(Operation.MELT, expected=expected)
            await self.assignments(setup)
            with self.assertRaisesRegex(ValueError, error):
                await task
            assert self.tx.empty()

    async def test_group_rejects_sensor_application_and_unrelated_identity(
        self,
    ) -> None:
        for number in (105102, 105801):
            task = self.command(Operation.MELT, expected=4)
            assert (await self.next_send())[1].payload[:2] == b"\0\0"
            self.ack()
            self.rx(identity(number))
            if number == 105801:
                assert (await self.next_send())[1].payload[:2] == b"\xd0\0"
                self.ack()
                self.rx(block(0xD0, b"\x01\x04"))
            with self.assertRaisesRegex(
                ValueError,
                "snowmelt control|control application",
            ):
                await task
            assert self.tx.empty()

    async def test_group_state_readback_uses_zone_not_ack_echo_or_other_sensor(
        self,
    ) -> None:
        for raw, expected, outcome in (
            (0x400, True, bytes([TxStatus.ACK])),
            (0x200, False, bytes([TxStatus.FAILED, FailureReason.MISSED_TIMING])),
            (0xA534, None, bytes([TxStatus.FAILED, FailureReason.BAD_END])),
        ):
            task = self.command(Operation.MELT, expected=4)
            await self.assignments()
            port, command = await self.next_send()
            assert port == SITE.port and command.to_bytes() == bytes.fromhex(
                "21f00002 0401",
            )
            self.rx(snow(ZONE, [(0x1B, 0x400)]))
            self.stream.feed(Packet(1, bytes([2, port]) + outcome))
            _, query = await self.next_send()
            assert query.to_bytes() == bytes.fromhex("17f00003 11011b")
            self.rx(snow(SENSOR, [(0x1B, 0x400)]))
            self.rx(snow(ZONE, [(0x1B, 0x400)], NativePath(SITE.port, SITE.subnet, 2)))
            self.rx(frame(0x20F, b"\x04\x11"))
            self.ack()
            self.rx(snow(ZONE, [(0x1B, raw)]))
            result = await task
            assert result.status is GroupStatus.STATE_REPORTED
            assert result.tx_result and result.tx_result.status == outcome[0]
            assert (
                result.matches_requested_state is expected
                and result.readback is not None
            )
            pair = result.readback.snowmelt_pair(0x1B)
            assert pair and pair.raw == raw and self.tx.empty()

    async def test_lost_group_completion_does_not_retry_or_read_occupied_port(
        self,
    ) -> None:
        task = self.command(Operation.MELT, expected=4, timeout=0.03)
        await self.assignments()
        assert (await self.next_send())[1].message_id == 0x21F
        result = await task
        assert result.status is GroupStatus.OUTCOME_UNKNOWN and self.tx.empty()
        self.ack()
        task = self.command(Operation.MELT, expected=4, timeout=0.03)
        await self.assignments()
        assert (await self.next_send())[1].message_id == 0x21F
        self.ack()
        assert (await self.next_send())[1].message_id == 0x17F
        self.ack()
        result = await task
        assert (
            result.status is GroupStatus.NO_STATE_REPORT
            and result.matches_requested_state is None
        )
        task = self.command(Operation.MELT, expected=4)
        await self.assignments()
        assert (await self.next_send())[1].message_id == 0x21F
        self.stream.feed(Packet(1, bytes([2, SITE.port, TxStatus.REJECTED, 2])))
        assert (await task).status is GroupStatus.LINK_FAILED and self.tx.empty()

    async def test_snow_report_retries_missing_identity_once_per_endpoint(self) -> None:
        task = asyncio.create_task(
            watch_native(self.stream, [SITE], duration=0.20, timeout=0.025),
        )
        assert (await self.next_send())[1].payload[:2] == b"\0\0"
        self.ack()
        await asyncio.sleep(0.04)
        sample = snow(ZONE, [(0x2F, 1)])
        self.rx(sample)
        assert (await self.next_send())[1].payload[:2] == b"\0\0"
        self.ack()
        await asyncio.sleep(0.04)
        for _ in range(3):
            self.rx(sample)
        result = await task
        assert (
            self.tx.empty() and result.reads == 2 and not result.state.snowmelt_devices
        )
        assert result.state.snowmelt_channels(SITE)[0].raw(0x2F) == 1

    async def test_secondary_role_can_prompt_but_not_supply_missing_keys(self) -> None:
        self.seed()
        task = asyncio.create_task(
            watch_native(
                self.stream,
                [SITE],
                schedules=False,
                duration=0.25,
                timeout=0.025,
            ),
        )
        await self.boot()
        _, initial = await self.next_send()
        assert (
            initial.payload
            == bytes([ZONE.request_byte, 5]) + SnowmeltApplication.ELECTRIC.status_keys
        )
        self.ack()
        await asyncio.sleep(0.04)
        sensor = snow(
            SENSOR,
            [(k, 3) for k in SnowmeltApplication.ELECTRIC.status_keys],
        )
        self.rx(sensor)
        assert (await self.next_send())[1] == initial
        self.ack()
        for _ in range(7):
            self.rx(sensor)  # A request already owns the slot.
        await asyncio.sleep(0.04)
        for _ in range(4):
            self.rx(sensor)
        self.rx(frame(0x17E, bytes.fromhex("1a02190300")))
        self.rx(snow(SENSOR, [(0x2F, 1)], NativePath(SITE.port, SITE.subnet, 3)))
        await asyncio.sleep(0.01)
        assert self.tx.empty()
        self.rx(sensor)
        assert (await self.next_send())[1] == initial
        self.ack()
        self.rx(snow(ZONE, [(k, 0) for k in SnowmeltApplication.ELECTRIC.status_keys]))
        for _ in range(9):
            self.rx(sensor)
        result = await task
        (device,) = result.state.snowmelt_devices
        assert result.reads == 4 and device.missing_keys == b"" and self.tx.empty()

    async def test_active_watch_reads_configuration_and_requests_only_missing_keys(
        self,
    ) -> None:
        task = asyncio.create_task(
            watch_native(self.stream, [SITE], schedules=False, duration=0.18),
        )
        await self.boot()
        self.rx(snow(ZONE, [(0x19, 0), (0x22, 0xC000)]))
        self.rx(snow(SENSOR, [(0x1B, 0x4FF), (0x29, 3)]))
        reads: list[int] = []
        while len(reads) != len(CONFIG):
            port, msg = await self.next_send()
            assert port == SITE.port
            self.ack()
            if msg.message_id == 0x2D7:
                b = int.from_bytes(msg.payload[:2], "little")
                reads.append(b)
                self.rx(block(b, CONFIG[b]))
            else:
                assert msg.message_id == 0x17F and msg.payload == bytes.fromhex(
                    "11031b2329",
                )
                self.rx(snow(ZONE, [(0x29, 0), (0x23, 1500), (0x1B, 0x100)]))
        result = await task
        (device,) = result.state.snowmelt_devices
        assert result.reads == len(CONFIG) + 2 and sorted(reads) == sorted(CONFIG)
        assert (
            device.primary
            and device.primary.operating_state is SnowmeltOperatingState.OFF
        )
        assert device.missing_keys == b"" and result.latest_statuses[0].answered
        assert self.tx.empty()

    async def test_group_change_probes_operating_state_on_one_port_without_setting_it(
        self,
    ) -> None:
        self.seed()
        self.rx(
            snow(
                ZONE,
                [
                    (k, 0x200 if k == 0x1B else 0)
                    for k in SnowmeltApplication.ELECTRIC.status_keys
                ],
            ),
        )
        seen: list[SnowmeltOperatingState | None] = []

        def changed(path: NativePath, state: NativeState) -> None:
            if (d := state.snowmelt(path)) and d.primary:
                seen.append(d.primary.operating_state)

        task = asyncio.create_task(
            watch_native(
                self.stream,
                [SITE],
                schedules=False,
                duration=0.18,
                on_change=changed,
            ),
        )
        await self.boot()
        other_port = NativePath(1, 0, 0)
        other = NativePath(SITE.port, 0, 0)
        self.rx(frame(0x21F, b"\x04\x01", other_port))
        self.rx(frame(0x21F, b"\x01\x01", other))
        self.rx(frame(0x20F, b"\x04\x01", other))
        _, msg = await self.next_send()
        assert msg.message_id == 0x17F and msg.payload == bytes.fromhex("11011b")
        assert seen.pop() is SnowmeltOperatingState.IDLE
        for flags in (0x11, 0x11, 0xF1):
            self.rx(frame(0x20F, bytes([4, flags]), other))
        self.ack()
        self.rx(snow(ZONE, [(0x1B, 0x401)]))
        assert (await self.next_send())[1] == msg
        self.ack()
        self.rx(snow(ZONE, [(0x1B, 0x401)]))
        self.rx(frame(0x20F, b"\x04\x11", other))
        result = await task
        assert seen[-1] is SnowmeltOperatingState.MELTING
        assert result.reads == 3 and self.tx.empty()

    async def test_inflight_status_does_not_cross_rebinding_or_block_another_port(
        self,
    ) -> None:
        stat = NativePath(2, 0x46, 5)
        self.seed()
        self.rx(block(3, bytes.fromhex("01 01"), stat))
        task = asyncio.create_task(
            watch_native(
                self.stream,
                [SITE, stat],
                schedules=False,
                thermostat_targets=False,
                duration=0.20,
                timeout=0.025,
            ),
        )
        starts = dict([await self.next_send(), await self.next_send()])
        for p in (SITE, stat):
            assert starts[p.port].message_id == 0x2D7
            self.ack(p.port)
        self.rx(identity(105801))
        self.rx(identity(path=stat))
        assert (await self.next_send())[1].payload[0] == ZONE.request_byte
        self.rx(snow(ZONE, [(0x22, 0x600)]))
        self.rx(block(0xD0, b"\x01\x04"))
        await asyncio.sleep(0.04)
        assert self.tx.empty()  # No terminal result yet, the old slot is still owned.
        self.ack()
        _, new = await self.next_send()
        assert new.payload == bytes([SENSOR.request_byte, 2, 0x19, 0x22])
        self.ack()
        self.rx(snow(SENSOR, [(0x22, 0x610), (0x19, 3)]))
        result = await task
        assert (
            len(result.latest_statuses) == 1
            and result.latest_statuses[0].query.payload == new.payload
        )
        (d,) = result.state.snowmelt_devices
        assert (
            d.setup.primary == SENSOR
            and d.primary
            and d.primary.water_detection is WaterDetection.WET
        )
        assert result.state.thermostats[0].setup.mode is ThermostatMode.HEAT

    async def test_passive_and_incomplete_cli(self) -> None:
        for f in setup_frames():
            self.rx(f)
        self.rx(snow(SENSOR, [(0x19, 3)]))
        passive = await watch_native(self.stream, source=None, duration=0.02)
        assert (
            self.tx.empty()
            and passive.reads == 0
            and len(passive.state.snowmelt_devices) == 1
        )
        args = build_parser().parse_args(
            [
                "watch-native",
                "--no-schedules",
                "--device",
                str(SITE.port),
                str(SITE.subnet),
                str(SITE.node),
                "--duration",
                "0.08",
                "--timeout",
                "0.03",
                "--tcp",
                "localhost:3001",
            ],
        )
        rows: list[object] = []
        self.seed()
        task = asyncio.create_task(watch_all_from_args(self.stream, args, rows.append))
        await self.boot()
        assert (await self.next_send())[1].message_id == 0x17F
        self.ack()
        self.rx(snow(ZONE, [(0x23, 0)]))
        assert await task == 1
        summary = rows[-1]
        assert isinstance(summary, dict) and summary["incomplete_snowmelt"][0][
            "missing_keys"
        ] == [0x19, 0x22, 0x1B, 0x29]
