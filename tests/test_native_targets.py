import asyncio
import unittest

from tekmar_482 import (
    CaptureGap,
    NativeAddress,
    NativeBlockBatch,
    NativeBlockQuery,
    NativeFrame,
    NativeMessage,
    NativePath,
    NativeSnapshot,
    NativeState,
    NativeWriteStatus,
    Packet,
    RoomLimit,
    TargetSlot,
    ThermostatTarget,
    ThermostatTargetChange,
    TxResult,
    TxStatus,
    watch_native,
)
from tekmar_482.cli import build_parser
from tekmar_482.cli_status import adjustment_fields
from tekmar_482.raw import RawDispatcher

from .native_fixtures import TEST_PATH, block, frame, identity

_HOST = NativeAddress(0, 0)
_MODERN = 105102  # 553
_LEGACY_FAN = 99203  # 544
_HEAT_ONLY = 102301  # 527
_T = ThermostatTarget
_S = TargetSlot


def _report(b: int, data: str, path: NativePath = TEST_PATH) -> NativeFrame:
    return block(b, bytes.fromhex(data), path)


def _update(target: ThermostatTarget, path: NativePath = TEST_PATH) -> NativeFrame:
    return frame(
        0x2DF,
        bytes([path.subnet, path.node, target, 0]) + bytes(5),
        NativePath(path.port, 0, 0),
    )


def _air(target: int, current: int = 0x600) -> NativeFrame:
    return frame(
        0x36F,
        bytes([1, 1, target]) + current.to_bytes(2, "little") + bytes(3),
    )


class AdjustmentTests(unittest.TestCase):
    def test_physical_slots_units_off_masks_and_limits(self) -> None:
        state = NativeState()
        state.observe(identity(_MODERN))
        for b, data in (
            (5, "15 2a 28 ff"),
            (6, "00"),
            (7, "21 00 48"),
            (9, "01 00"),
            (0xA3, "03 01 00"),
            (0xA4, "05 00 38"),
            (0x53, "15 3c 40 ff"),
        ):
            state.observe(_report(b, data))
        a = state.thermostats[0].adjustments
        assert a.humidity_setup and a.humidity_setup.enabled(_T.HUMIDIFY)
        assert (
            not a.humidity_setup.enabled(_T.DEHUMIDIFY)
            and a.table(_T.DEHUMIDIFY) is None
        )
        h, c, floor, fan, rh = (
            a.table(t) for t in (_T.HEAT, _T.COOL, _T.FLOOR, _T.FAN, _T.HUMIDIFY)
        )
        assert h and c and floor and fan and rh
        assert [h.value(s) for s in _S] == [21, None, 20, None, None]
        assert (
            h.raw(_S.AWAY) == 255
            and c.slots == (None,) * 5
            and 6 not in a.missing_blocks
        )
        assert (
            floor.maximum_raw == 72
            and floor.raw(_S.WAKE) == 0
            and floor.value(_S.WAKE) is None
        )
        assert rh.value(_S.WAKE) == 0 and fan.value(_S.WAKE) == 0
        (maximum,) = a.limits
        assert (
            maximum.limit is RoomLimit.MAXIMUM_ROOM_HEAT and maximum.raw(_S.WAKE) == 60
        )
        assert maximum.raw(_S.AWAY) == 255 and maximum.raw(_S.SLEEP) is None
        state.observe(identity(_LEGACY_FAN))
        state.observe(_report(9, "07 00 06 0b"))
        legacy = state.thermostats[0].adjustments.table(_T.FAN)
        assert legacy and [legacy.value(s) for s in _S] == [0, 60, None, None, None]
        assert not state.thermostats[0].adjustments.limits

    def test_operating_reports_never_become_stored_settings(self) -> None:
        state = NativeState()
        state.observe(identity(_MODERN))
        fan = frame(0x25F, bytes.fromhex("db 1206 1306"))
        slab = frame(0x26F, bytes.fromhex("09 2a 0006"))
        rh = frame(0x323, bytes.fromhex("01 00 34 12fe 33 40 29 3f"))
        for f in (fan, slab, rh):
            assert state.observe(f) == (TEST_PATH,)
            assert not state.observe(f)
        settings = state.thermostats[0].adjustments
        assert not settings.tables and settings.slab and not settings.slab.value.primary
        assert settings.fan and (settings.fan.value.fan1, settings.fan.value.fan2) == (
            3,
            2,
        )
        assert settings.humidity and settings.humidity.value.dewpoint_raw == 0xFE12
        assert (
            settings.humidity.value.relative_percent == 52
            and settings.humidity.frame is rh
        )
        assert {7, 9, 0xA3} <= set(settings.missing_blocks)
        assert not {0xA4, 0xA5}.intersection(settings.missing_blocks)
        for b, data in (
            (7, "21 28 48"),
            (9, "01 23"),
            (0xA4, "01 28"),
            (0xA5, "01 42"),
        ):
            state.observe(_report(b, data))
        state.observe(frame(0x323, bytes.fromhex("01 00 35 12fe 34 40 29 3f")))
        state.observe(frame(0x26F, bytes.fromhex("08 30 0406")))
        state.observe(frame(0x25F, bytes.fromhex("00 1506 1606")))
        assert all(state.block(TEST_PATH, b) for b in (7, 9, 0xA4, 0xA5))
        state.observe(frame(0x323, bytes.fromhex("01 00 35 12fe 34 40 2a 3f")))
        assert state.block(TEST_PATH, 0xA4) is None and state.block(TEST_PATH, 0xA5)
        state.observe(frame(0x26F, bytes.fromhex("01 2b 0406")))
        state.observe(frame(0x26F, bytes.fromhex("00 2c 0306")))
        assert state.block(TEST_PATH, 7) is None and state.block(TEST_PATH, 9)
        assert not state.observe(frame(0x323, bytes.fromhex("ff 00 00 0000 00")))

    def test_gate_transitions_and_updates_are_device_scoped(self) -> None:
        other = NativePath(2, TEST_PATH.subnet, TEST_PATH.node)
        state = NativeState()
        for p in (other, TEST_PATH):
            state.observe(identity(_MODERN, path=p))
            for b, data in (
                (8, "03 02 0c"),
                (0xA3, "03 01 00"),
                (0xA4, "1f 28 28 28 28 28"),
                (5, "1f 30 30 30 30 30"),
            ):
                state.observe(_report(b, data, p))
        assert all(9 not in d.adjustments.missing_blocks for d in state.thermostats)
        state.observe(_report(8, "03 01 0c"))
        state.observe(_report(0xA3, "03 00 01"))
        view = state.climate(TEST_PATH)
        assert view and {9, 0xA5} <= set(view.adjustments.missing_blocks)
        assert state.block(TEST_PATH, 0xA4) is None and state.block(other, 0xA4)
        assert state.observe(_update(_T.HEAT)) == (TEST_PATH,)
        assert state.block(TEST_PATH, 5) is None and state.block(other, 5)
        state.observe(CaptureGap(1))
        assert not state.thermostats

    def test_legacy_floor_role_changes_are_not_sensor_noise(self) -> None:
        state = NativeState()
        state.observe(identity(100103, 2))  # early 538 uses the sensor1 status word
        state.observe(_report(0x17, "07 00 1e01"))
        self.assertTrue(state.thermostats[0].adjustments.floor_configured)
        state.observe(_report(7, "3f 24 24 24 24 24 48"))
        state.observe(_report(0x17, "07 00 1f01"))
        assert state.block(TEST_PATH, 7)
        state.observe(_report(0x17, "07 00 0801"))
        assert state.thermostats[0].adjustments.floor_configured is False
        assert state.block(TEST_PATH, 7) is None

    def test_snapshot_and_cli_use_reported_slots_not_tx_results(self) -> None:
        frames = (identity(_MODERN), _report(5, "15 2a 00 ff"))
        snapshot = NativeSnapshot(
            tuple(
                NativeBlockBatch(
                    TEST_PATH.port,
                    NativeBlockQuery(TEST_PATH.address, b),
                    TxResult(TEST_PATH.port, TxStatus.ACK),
                    (f,),
                )
                for b, f in zip((0, 5), frames, strict=True)
            ),
            (),
        )
        settings = snapshot.thermostats[0].adjustments
        result = adjustment_fields(settings)["tables"]
        assert isinstance(result, dict)
        assert result["heat"]["block"] == 5 and result["heat"]["unit"] == "celsius"
        assert result["heat"]["slots"]["occupied"] == {"raw": 0, "value": None}
        assert result["heat"]["slots"]["sleep"] == {"raw": None, "value": None}
        parsed = build_parser().parse_args(
            [
                "set-thermostat-target",
                "3",
                "53",
                "7",
                "heat",
                "wake",
                "21.5",
                "--tcp",
                "localhost:3001",
            ],
        )
        assert not parsed.confirm and parsed.value == 21.5


class TargetSessionTests(unittest.IsolatedAsyncioTestCase):
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

    def ack(self) -> None:
        self.stream.feed(Packet(1, bytes([2, TEST_PATH.port, 0])))

    async def sent(self) -> NativeMessage:
        async with asyncio.timeout(1):
            raw = await self.tx.get()
        assert raw[:2] == bytes([1, TEST_PATH.port])
        return NativeMessage.from_bytes(raw[2:])

    async def query(self, b: int) -> None:
        assert await self.sent() == NativeBlockQuery(TEST_PATH.address, b).request(
            _HOST,
        )
        self.ack()

    async def test_target_change_retires_stale_read_and_ignores_temperature_drift(
        self,
    ) -> None:
        watch = asyncio.create_task(
            watch_native(self.stream, [TEST_PATH], schedules=False, duration=0.3),
        )
        await self.query(0)
        self.rx(identity(_HEAT_ONLY))
        await self.query(3)
        self.rx(_report(3, "01 01"))
        await self.query(5)
        self.rx(_update(_T.HEAT))
        self.rx(_update(_T.HEAT))
        self.rx(_report(5, "01 15"))  # old generation
        await self.query(5)
        self.rx(_report(5, "01 2a"))
        self.rx(_air(42))
        await self.query(3)
        self.rx(_report(3, "01 01"))
        self.rx(_air(42, 0x605))
        self.rx(_air(43, 0x605))
        await self.query(5)
        self.rx(_report(5, "01 2b"))
        await self.query(3)
        self.rx(_report(3, "01 01"))
        self.rx(_air(43, 0x608))
        done = await watch
        assert self.tx.empty() and done.reads == 7
        a = done.state.thermostats[0].adjustments
        h = a.table(_T.HEAT)
        assert h and h.value(_S.WAKE) == 21.5 and not a.missing_blocks
        assert len([b for b in done.latest_blocks if b.query.block_id == 5]) == 1

    async def test_preview_and_control_use_full_positional_bytes_and_readback(
        self,
    ) -> None:
        change = ThermostatTargetChange(_T.FAN, _S.OCCUPIED, 70)
        for preview in (True, False):
            edit = asyncio.create_task(
                self.stream.write_thermostat_target(
                    TEST_PATH.port,
                    TEST_PATH.address,
                    change,
                    source=_HOST,
                    preview=preview,
                ),
            )
            await self.query(0)
            self.rx(identity(_LEGACY_FAN))
            await self.query(8)
            self.rx(_report(8, "03 04 00"))
            await self.query(9)
            self.rx(_report(9, "05 05 fa 06 fe 0a"))
            expected = bytes.fromhex("05 fa 07 fe 0a")
            if not preview:
                sent = await self.sent()
                assert sent.message_id == 0x2DF and sent.payload[4:] == expected
                self.ack()
                await self.query(9)
                self.rx(_report(9, "04 07"))
            result = await edit
            assert result.status is (
                NativeWriteStatus.PREVIEW if preview else NativeWriteStatus.CONFIRMED
            )
            assert result.proposed.payload[4:] == expected and self.tx.empty()

    async def test_unknown_or_disabled_actor_and_absent_slot_reject_before_send(
        self,
    ) -> None:
        for change, reports, error in (
            (
                ThermostatTargetChange(_T.HUMIDIFY, _S.WAKE, 40),
                ((0xA3, "01 ff"),),
                "not available",
            ),
            (
                ThermostatTargetChange(_T.FAN, _S.WAKE, 80),
                ((8, "03 02 0c"),),
                "not available",
            ),
            (
                ThermostatTargetChange(_T.FLOOR, _S.WAKE, 21),
                ((13, "01 01"),),
                "not available",
            ),
            (
                ThermostatTargetChange(_T.FLOOR, _S.WAKE, 33),
                ((13, "02 02"), (7, "3f 28 28 28 28 28 40")),
                "maximum",
            ),
            (
                ThermostatTargetChange(_T.HEAT, _S.WAKE, 21),
                ((5, "1e fe 30 30 30 30"),),
                "does not report",
            ),
        ):
            task = asyncio.create_task(
                self.stream.write_thermostat_target(
                    TEST_PATH.port,
                    TEST_PATH.address,
                    change,
                    source=_HOST,
                    preview=False,
                ),
            )
            await self.query(0)
            self.rx(identity(_MODERN))
            for b, payload in reports:
                await self.query(b)
                self.rx(_report(b, payload))
            with self.assertRaisesRegex(ValueError, error):
                await task
            assert self.tx.empty()

    def test_settings_are_exactly_representable(self) -> None:
        for t, v, model in (
            (_T.FAN, 85, "544"),
            (_T.HEAT, 20.2, "553"),
            (_T.FLOOR, 0, "532"),
            (_T.HEAT, 127.5, "538"),
            (_T.HUMIDIFY, 19, "553"),
            (_T.DEHUMIDIFY, 81, "557"),
            (_T.FAN, float("nan"), "553"),
            (_T.COOL, float("inf"), "553"),
        ):
            with self.assertRaises(ValueError):
                ThermostatTargetChange(t, _S.AWAY, v).wire_value(model)
        assert ThermostatTargetChange(_T.FAN, _S.SLEEP, 0).wire_value("544") == 0
        assert ThermostatTargetChange(_T.HUMIDIFY, _S.AWAY, 0).wire_value("553") == 0
