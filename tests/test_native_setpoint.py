import asyncio
import unittest

from tekmar_482 import (
    CaptureGap,
    NativeBlockBatch,
    NativeBlockQuery,
    NativeFrame,
    NativeMessage,
    NativePath,
    NativeSetpointControl,
    NativeSnapshot,
    NativeState,
    Packet,
    SetpointApplication,
    watch_native,
)
from tekmar_482.cli import build_parser
from tekmar_482.cli_native_watch import setpoint_fields, watch_all_from_args
from tekmar_482.raw import RawDispatcher

from .native_fixtures import TEST_PATH, block, frame, identity

_STATUS = bytes.fromhex("b3 dc 5800 4800 00c0 6c 35 ca e4")
_OTHER = NativePath(2, TEST_PATH.subnet, TEST_PATH.node)
_HOST = NativePath(TEST_PATH.port, 0, 0)
_161 = 101101
_162 = 101102


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
