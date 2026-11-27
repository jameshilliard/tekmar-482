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
    NativeSetpointControl,
    NativeSnapshot,
    NativeState,
    NativeWriteResult,
    Packet,
    SetpointApplication,
    SetpointSlot,
    SetpointTargetChange,
    StoredSetpointTarget,
    watch_native,
)
from tekmar_482.cli import build_parser
from tekmar_482.cli_native_watch import setpoint_fields, watch_all_from_args
from tekmar_482.native_write import NativeWriteStatus
from tekmar_482.raw import RawDispatcher

from .native_fixtures import TEST_PATH, block, frame, identity

_STATUS = bytes.fromhex("b3 dc 5800 4800 00c0 6c 35 ca e4")
_OTHER = NativePath(2, TEST_PATH.subnet, TEST_PATH.node)
_HOST = NativePath(TEST_PATH.port, 0, 0)
_161 = 101101
_162 = 101102
_APP = SetpointApplication
_SLOT = SetpointSlot


def _view(state: NativeState, path: NativePath = TEST_PATH) -> NativeSetpointControl:
    value = state.setpoint_control(path)
    assert value is not None
    return value


def _update(which: int, data: bytes) -> NativeFrame:
    return frame(
        0x2DF,
        bytes([TEST_PATH.subnet, TEST_PATH.node, which, 0]) + data,
        _HOST,
    )


class SetpointStateTests(unittest.TestCase):
    def test_live_units_require_identity_and_setup_at_sample_time(self) -> None:
        state = NativeState()
        assert not state.observe(frame(0x22F, _STATUS[:10]))
        for p, app in ((TEST_PATH, 2), (_OTHER, 4)):
            state.observe(frame(0x22F, _STATUS, p))
            raw = _view(state, p)
            assert raw.status and raw.status.demand and raw.status.cooling
            assert raw.status.temp2_raw == 0xC000 and raw.status.extension == b"\xe4"
            assert raw.readings is None and not state.climate(p)
            state.observe(block(0x26, bytes([0x41, app, 8]), p))
            state.observe(identity(_162, path=p))
            assert not _view(state, p).readings
            assert state.observe(frame(0x22F, _STATUS, p)) == (p,)
        dhw = _view(state)
        assert dhw.readings and (dhw.readings.target_f, dhw.readings.temperature_f) == (
            111.2,
            96.8,
        )
        assert dhw.assignment.remote_group is None
        floor = _view(state, _OTHER)
        assert floor.readings and floor.readings.temperature_f == -77.8
        assert floor.assignment.remote_group == 8
        assert not state.observe(frame(0x22F, _STATUS, stamp=25))
        assert _view(state).status_frame == frame(0x22F, _STATUS, stamp=25)
        assert state.observe(_update(0x26, bytes(8))) == (TEST_PATH,)
        assert not _view(state).readings and _view(state, _OTHER).readings
        state.observe(block(0x26, bytes([1, 4])))
        assert (
            _view(state).status is not None and _view(state).sample_application is None
        )
        assert not state.observe(_update(0x26, bytes(7)))
        state.observe(frame(0x22F, _STATUS))
        assert _view(state).sample_application is SetpointApplication.FLOOR
        state.observe(identity(100103))
        assert (
            state.setpoint_control(TEST_PATH) is None
            and len(state.setpoint_controls) == 1
        )
        state.observe(CaptureGap(1))
        assert not state.setpoint_controls

    def test_tables_are_selected_by_application_not_by_live_target_or_group(
        self,
    ) -> None:
        state = NativeState()
        state.observe(identity(_161))
        assert set(state.missing_setpoint_reads(TEST_PATH)) == {
            (0x26, None),
            (0x27, None),
            (0x32, None),
        }
        state.observe(block(0x26, bytes([1, 5])))
        assert (0x30, None) not in state.missing_setpoint_reads(TEST_PATH)
        state.observe(block(0x26, bytes([1, 4])))
        assert (0x2E, None) in state.missing_setpoint_reads(TEST_PATH)
        assert (0x2F, None) not in state.missing_setpoint_reads(TEST_PATH)
        state.observe(block(0x2E, bytes.fromhex("3f 0106 0000 0306")))
        state.observe(block(0x2F, bytes.fromhex("0d ca 3505")))
        state.observe(block(0x32, bytes.fromhex("03 34 12")))
        view = _view(state)
        assert (
            view.targets
            and view.targets.maximum_raw is None
            and view.targets.unoccupied_sleep_raw == 0
        )
        assert view.targets.wake_raw is None and view.targets.sleep_raw == 0x535
        assert view.manual_override_time_raw == 0x1234
        assert setpoint_fields(view)["status"] is None
        state.observe(_update(0x2F, bytes(4)))
        assert (0x2F, None) in state.missing_setpoint_reads(TEST_PATH)
        state.observe(block(0x26, bytes([1, 0])))
        state.observe(block(0x28, bytes.fromhex("03 0106")))
        state.observe(block(0x26, bytes([1, 3])))
        assert _view(state).targets is None and state.block(TEST_PATH, 0x28) is None
        assert state.block_revision(TEST_PATH, (0x28, None)) > 0
        node0 = NativePath(TEST_PATH.port, TEST_PATH.subnet, 0)
        state.observe(identity(_162, path=node0))
        assert not state.missing_setpoint_reads(node0)
        state.observe(frame(0x3E7, bytes([TEST_PATH.subnet, TEST_PATH.node]), node0))
        assert not state.setpoint_controls

    def test_configuration_snapshot_does_not_synthesize_status(self) -> None:
        batches = tuple(
            NativeBlockBatch(
                TEST_PATH.port,
                NativeBlockQuery(TEST_PATH.address, n),
                None,
                (f,),
            )
            for n, f in (
                (0, identity(_162)),
                (0x26, block(0x26, b"\x01\x02")),
                (0x2C, block(0x2C, bytes.fromhex("03 2006"))),
            )
        )
        (control,) = NativeSnapshot(batches, ()).setpoint_controls
        assert control.targets and control.targets.wake_occupied_raw == 0x620
        assert control.status is None and control.readings is None

    def test_stored_values_separate_off_temperature_and_unsupported_slots(self) -> None:
        state = NativeState()
        for report in (
            identity(_162),
            block(0x26, bytes.fromhex("01 01")),
            block(0x2A, bytes.fromhex("cf d804 b207 0000 6207")),
        ):
            state.observe(report)
        control = _view(state)
        off = control.target(_SLOT.WAKE_OCCUPIED)
        assert off and off.raw == 1240 and off.is_off and off.celsius is None
        assert control.target(_SLOT.AWAY) is None
        assert control.target(_SLOT.SLEEP) is None
        assert (adjustable := control.writable_target(_SLOT.WAKE_OCCUPIED)) == off
        assert adjustable is not None and adjustable.bounds_celsius is not None
        assert adjustable.supports_off and adjustable.bounds_celsius[1] == 40

        state.observe(block(0x26, bytes.fromhex("01 03")))
        assert _view(state).target(_SLOT.WAKE_OCCUPIED) is None
        state.observe(block(0x28, bytes.fromhex("0f 9204 ffff")))
        control = _view(state)
        zero = control.target(_SLOT.WAKE_OCCUPIED)
        invalid = control.target(_SLOT.UNOCCUPIED_SLEEP)
        assert zero and not zero.is_off and zero.celsius == 0
        assert invalid and invalid.raw == 0xFFFF and invalid.celsius is None
        assert control.writable_target(_SLOT.UNOCCUPIED_SLEEP) is None

    def test_reported_target_maximum_and_161_writability(self) -> None:
        state = NativeState()
        state.observe(identity(_161))
        state.observe(block(0x26, bytes.fromhex("01 04")))
        state.observe(block(0x2E, bytes.fromhex("cf fa05 4805 0000 d606")))
        control = _view(state)
        target = control.writable_target(_SLOT.WAKE_OCCUPIED)
        assert target and target.celsius == 20
        assert target.bounds_celsius is not None
        assert (
            target.bounds_celsius[1] == StoredSetpointTarget(_APP.FLOOR, 1750).celsius
        )
        assert control.target(_SLOT.UNOCCUPIED_SLEEP) is not None
        assert control.writable_target(_SLOT.UNOCCUPIED_SLEEP) is None

        dhw = StoredSetpointTarget(_APP.DHW, 0x0870, 0x0870)
        assert dhw.bounds_celsius is not None and dhw.bounds_celsius[1] == 55
        assert StoredSetpointTarget(_APP.DHW, 0x0870, 0x0430).bounds_celsius is None

    def test_compact_confirmation_preserves_the_need_to_refresh_sibling_words(
        self,
    ) -> None:
        state = NativeState()
        state.observe(identity(_162))
        state.observe(block(0x26, bytes.fromhex("01 01")))
        complete = block(0x2A, bytes.fromhex("cf d804 b207 0000 6207"))
        compact = block(0x2A, bytes.fromhex("03 fa05"))
        key = 0x2A, None
        state.observe(complete)
        assert key not in state.missing_setpoint_reads(TEST_PATH)
        state.observe(_update(0x2A, bytes(8)))
        revision = state.missing_setpoint_reads(TEST_PATH)[key]
        state.observe(compact)
        assert state.missing_setpoint_reads(TEST_PATH)[key] == revision
        assert _view(state).target(_SLOT.UNOCCUPIED_SLEEP) is None
        state.observe(complete)
        assert key not in state.missing_setpoint_reads(TEST_PATH)

        state.observe(compact)  # Same loss when observing someone else's confirmation.
        assert state.missing_setpoint_reads(TEST_PATH)[key] == revision + 1
        state.observe(compact)
        assert state.missing_setpoint_reads(TEST_PATH)[key] == revision + 1
        state.observe(block(0x26, bytes.fromhex("01 04")))
        assert key not in state.missing_setpoint_reads(TEST_PATH)

    def test_edit_range_and_off_follow_the_selected_application(self) -> None:
        assert SetpointTargetChange(_APP.DHW, _SLOT.AWAY, 55).wire_value() == 2160
        assert (
            SetpointTargetChange(_APP.HOT_TUB, _SLOT.WAKE_OCCUPIED, None).wire_value()
            == 1240
        )
        assert StoredSetpointTarget(_APP.HOT_TUB, 1890).celsius == 40
        for app, value in (
            (_APP.DHW, None),
            (_APP.HOT_TUB, 41),
            (_APP.HOT_TUB, 3.9),
            (_APP.FLOOR, True),
            (_APP.POOL, float("nan")),
            (_APP.SETPOINT_ON_OFF, -21),
        ):
            with self.subTest(app=app, value=value), self.assertRaises(ValueError):
                SetpointTargetChange(app, _SLOT.WAKE_OCCUPIED, value).wire_value()

    def test_cli_defaults_to_preview_with_off_separate_from_zero(self) -> None:
        parser = build_parser()
        for text, expected in (("off", None), ("0", 0.0)):
            args = parser.parse_args(
                [
                    "set-setpoint-target",
                    "1",
                    "0x31",
                    "7",
                    "hot_tub",
                    "sleep",
                    text,
                    "--tcp",
                    "localhost:3001",
                ],
            )
            assert (args.application, args.slot) == ("hot_tub", "sleep")
            assert args.value == expected and not args.confirm


class SetpointEditTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tx: asyncio.Queue[bytes] = asyncio.Queue()
        self.stream = RawDispatcher(self.tx.put, 64)
        uart: asyncio.Queue[Packet] = asyncio.Queue()
        await self.enterAsyncContext(self.stream.receiving(uart.get))
        self.source = NativeAddress(0x12, 2)

    async def sent(self) -> NativeMessage:
        async with asyncio.timeout(1):
            raw = await self.tx.get()
        assert raw[:2] == bytes([1, TEST_PATH.port])
        return NativeMessage.from_bytes(raw[2:])

    def ack(self) -> None:
        self.stream.feed(Packet(1, bytes([2, TEST_PATH.port, 0])))

    async def answer(self, report: NativeFrame) -> None:
        b = int.from_bytes(report.message.payload[:2], "little")
        query = NativeBlockQuery(TEST_PATH.address, b)
        self.assertEqual(await self.sent(), query.request(self.source))
        self.ack()
        self.stream.feed(Packet(1, bytes([0, report.port]) + bytes(4) + report.body))

    def edit(
        self,
        change: SetpointTargetChange,
        *,
        preview: bool,
    ) -> asyncio.Task[NativeWriteResult]:
        return asyncio.create_task(
            self.stream.write_setpoint_target(
                TEST_PATH.port,
                TEST_PATH.address,
                change,
                source=self.source,
                timeout=1,
                preview=preview,
            ),
        )

    async def test_short_floor_layout_with_ack_and_compact_readback(self) -> None:
        task = self.edit(
            SetpointTargetChange(_APP.FLOOR, _SLOT.WAKE_OCCUPIED, 20),
            preview=False,
        )
        await self.answer(identity(_161))
        await self.answer(block(0x26, bytes.fromhex("01 04")))
        await self.answer(block(0x2E, bytes.fromhex("3f 0006 0000 3006")))
        sent = await self.sent()
        assert sent.message_id == 0x2DF and sent.source == self.source
        assert sent.payload == bytes.fromhex("3507 2e00 fa05 0000 3006")
        self.ack()
        await self.answer(block(0x2E, bytes.fromhex("03 fa05")))
        result = await task
        assert result.status is NativeWriteStatus.CONFIRMED and self.tx.empty()
        assert (
            result.readback
            and result.readback.require_report().decoded["wake_occupied_raw"] == 1530
        )

    async def test_secondary_edit_does_not_fall_back_to_primary(self) -> None:
        for reported, preview in (("0f f005 0006", True), ("03 f005 ffff", False)):
            with self.subTest(reported=reported):
                task = self.edit(
                    SetpointTargetChange(_APP.DHW, _SLOT.SLEEP, 55),
                    preview=preview,
                )
                await self.answer(identity(_162))
                await self.answer(block(0x26, bytes.fromhex("01 02")))
                await self.answer(block(0x2C, bytes.fromhex("ff 2006 1006 0000 0009")))
                await self.answer(block(0x2D, bytes.fromhex(reported)))
                if preview:
                    result = await task
                    assert result.status is NativeWriteStatus.PREVIEW
                    assert result.proposed.payload == bytes.fromhex(
                        "3507 2d00 f005 7008",
                    )
                else:
                    with self.assertRaisesRegex(ValueError, "does not report"):
                        await task
                assert self.tx.empty()

    async def test_app_slot_and_maximum_must_allow_edit(self) -> None:
        for number, change, reports, error in (
            (
                _162,
                SetpointTargetChange(_APP.DHW, _SLOT.WAKE_OCCUPIED, 55),
                (block(0x26, bytes.fromhex("01 05")),),
                "confirm the selected application",
            ),
            (
                _161,
                SetpointTargetChange(_APP.HOT_TUB, _SLOT.AWAY, None),
                (),
                "occupied target",
            ),
            (
                _162,
                SetpointTargetChange(_APP.HOT_TUB, _SLOT.SLEEP, 40),
                (
                    block(0x26, bytes.fromhex("01 01")),
                    block(0x2A, bytes.fromhex("ff 0006 0000 0000 3007")),
                ),
                "reported maximum",
            ),
            (
                100103,
                SetpointTargetChange(_APP.DHW, _SLOT.WAKE_OCCUPIED, 55),
                (),
                "identified setpoint",
            ),
        ):
            with self.subTest(error=error):
                task = self.edit(change, preview=False)
                await self.answer(identity(number))
                for report in reports:
                    await self.answer(report)
                with self.assertRaisesRegex(ValueError, error):
                    await task
                assert self.tx.empty()


class SetpointWatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_passive_cli_reports_a_setpoint_sample_without_sending(self) -> None:
        writes: asyncio.Queue[bytes] = asyncio.Queue()
        stream = RawDispatcher(writes.put, 64)
        idle: asyncio.Queue[Packet] = asyncio.Queue()
        await self.enterAsyncContext(stream.receiving(idle.get))
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
        for f in (identity(_162), block(0x26, bytes([1, 2])), frame(0x22F, _STATUS)):
            stream.feed(Packet(1, bytes([0, f.port]) + bytes(4) + f.body))
        output: list[object] = []
        assert await watch_all_from_args(stream, args, output.append) == 0
        assert any(
            isinstance(item, dict)
            and "setpoint_control" in item
            and item["setpoint_control"]["live_readings"]
            == {"target_f": 111.2, "temperature_f": 96.8}
            for item in output
        )
        assert writes.empty()

    async def test_requests_configuration_not_live_status_and_retires_a_late_reply(
        self,
    ) -> None:
        tx: asyncio.Queue[bytes] = asyncio.Queue()
        stream = RawDispatcher(tx.put, 128)
        idle: asyncio.Queue[Packet] = asyncio.Queue()
        await self.enterAsyncContext(stream.receiving(idle.get))

        def receive(f: NativeFrame) -> None:
            stream.feed(
                Packet(
                    1,
                    bytes([0, f.port]) + f.timestamp_ms.to_bytes(4, "little") + f.body,
                ),
            )

        seen: list[int] = []
        async with asyncio.TaskGroup() as tasks:
            task = tasks.create_task(
                watch_native(
                    stream,
                    [TEST_PATH],
                    duration=0.4,
                    timeout=0.05,
                    schedules=False,
                    thermostat_targets=False,
                    plant_settings=False,
                ),
            )
            async with asyncio.timeout(1):
                while 0x2C not in seen:
                    outgoing = NativeMessage.from_bytes((await tx.get())[2:])
                    assert outgoing.message_id == 0x2D7
                    b = int.from_bytes(outgoing.payload[:2], "little")
                    seen.append(b)
                    if b == 0:
                        receive(identity(_162))
                    elif b == 0x26:
                        receive(block(b, bytes([1, 0])))
                    elif b == 0x28:
                        receive(block(0x26, bytes([1, 2])))
                        receive(block(b, bytes.fromhex("03 2106")))
                    elif b == 0x2C:
                        receive(block(b, bytes.fromhex("03 2206")))
                        receive(frame(0x22F, _STATUS))
                        receive(frame(0x22F, _STATUS, stamp=10))
                    else:
                        assert b in (0x27, 0x32)
                        receive(block(b, bytes(1)))
                    stream.feed(Packet(1, bytes([2, TEST_PATH.port, 0])))
        state = task.result().state
        assert tx.empty() and set(seen) == {0, 0x26, 0x27, 0x32, 0x28, 0x2C}
        assert len(seen) == len(set(seen)) and state.block(TEST_PATH, 0x28) is None
        view = _view(state)
        assert view.targets and view.targets.wake_occupied_raw == 0x622
        assert view.readings and view.readings.temperature_f == 96.8
        assert view.status_frame and view.status_frame.timestamp_ms == 10
