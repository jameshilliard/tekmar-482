import asyncio
import unittest

from tekmar_482 import (
    ClockReport,
    GatewayAccess,
    GroupReport,
    LineReportKind,
    NativePath,
    NativeScene,
    NativeState,
    SceneReport,
)
from tekmar_482.cli import build_parser
from tekmar_482.cli_native_watch import watch_all_from_args
from tekmar_482.packet import Packet
from tekmar_482.raw import CaptureGap, RawDispatcher

from .native_fixtures import TEST_PATH, frame, identity

_MASTER = NativePath(0, TEST_PATH.subnet, 0)


class LineStateTests(unittest.TestCase):
    def test_last_observation_by_source_and_wire_group(self) -> None:
        state = NativeState()
        group = LineReportKind.SETPOINT_GROUP
        other = NativePath(1, TEST_PATH.subnet, TEST_PATH.node)
        assert not state.line_report(TEST_PATH, group, group=4)
        assert not state.paths
        for path, data in (
            (TEST_PATH, "04 01 ca"),
            (TEST_PATH, "09 10"),
            (other, "04 00"),
            (_MASTER, "04 11"),
        ):
            assert state.observe(frame(0x20F, bytes.fromhex(data), path)) == (path,)
        assert not state.observe(frame(0x21F, b"\x04\0"))
        assert not state.observe(frame(0x20F, b"\x04"))
        assert state.observe(frame(0x20F, bytes.fromhex("04 01 e4"), stamp=0xFFFFFFFF))
        assert not state.observe(frame(0x20F, bytes.fromhex("04 01 e4"), stamp=0))
        report = state.line_report(TEST_PATH, group, group=4)
        assert report and report.frame.timestamp_ms == 0
        assert report.value == GroupReport(4, 1) and report.payload.endswith(b"\xe4")
        assert [(r.path, r.value) for r in state.line_reports] == [
            (_MASTER, GroupReport(4, 0x11)),
            (other, GroupReport(4, 0)),
            (TEST_PATH, GroupReport(4, 1)),
            (TEST_PATH, GroupReport(9, 0x10)),
        ]
        assert not state.thermostats
        assert set(state.observe(CaptureGap(3))) == {TEST_PATH, other, _MASTER}
        assert not state.line_reports

    def test_queries_and_announcements_do_not_imply_settings(self) -> None:
        state = NativeState([_MASTER])
        for mid, data in ((0x200, b"\xf3"), (0x317, b""), (0x21F, b"\x01\x01")):
            assert not state.observe(frame(mid, data, _MASTER))
        assert not state.paths
        assert not state.observe(frame(0x200, b"\x02", TEST_PATH))
        assert state.observe(frame(0x200, b"\xf2", _MASTER)) == (_MASTER,)
        assert not state.observe(frame(0x200, b"\xf3", _MASTER))
        flags = state.line_report(_MASTER, LineReportKind.GATEWAY_ACCESS)
        assert flags and flags.value == GatewayAccess(0xF2)
        clock = bytes.fromhex("2d 63 ff ff ff 04 ca")
        state.observe(frame(0x31F, clock, _MASTER))
        value = state.line_report(_MASTER, LineReportKind.CLOCK)
        assert value and isinstance(value.value, ClockReport)
        assert value.value.wall_datetime is None and value.value.payload == clock
        timed = bytes.fromhex("05 b4 00")
        state.observe(frame(0x2FF, timed, _MASTER))
        assert not state.observe(frame(0x2FF, timed, _MASTER, stamp=99))
        scene = state.line_report(_MASTER, LineReportKind.SCENE)
        assert scene and isinstance(scene.value, SceneReport)
        assert scene.value.scene is NativeScene.TEMPORARY_OCCUPIED
        assert scene.frame.timestamp_ms == 99 and state.identity(_MASTER) is None
        assert not state.climate(_MASTER)

    def test_source_replacement_clears_reports_without_relabeling_them(self) -> None:
        state = NativeState()
        state.observe(identity())
        state.observe(frame(0x2EF, b"\x02"))
        state.observe(frame(0x2EF, b"\x01", _MASTER))
        state.observe(identity(number=101901))
        assert not state.line_reports_for(TEST_PATH)
        moved = NativePath(TEST_PATH.port, TEST_PATH.subnet, 3)
        state.observe(frame(0x30F, b"\x10"))
        state.observe(frame(0x010, b"\x07\x02\xff", moved))
        state.observe(
            frame(0x3E7, bytes([TEST_PATH.subnet, TEST_PATH.node | 32]), moved),
        )
        assert not state.line_reports_for(TEST_PATH)
        assert not state.line_reports_for(moved)
        assert [r.path for r in state.line_reports] == [_MASTER]


class LineWatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_passive_watch_emits_changes_and_loss_without_sending(self) -> None:
        tx: asyncio.Queue[bytes] = asyncio.Queue()
        idle: asyncio.Queue[Packet] = asyncio.Queue()
        stream = RawDispatcher(tx.put, 32)
        await self.enterAsyncContext(stream.receiving(idle.get))
        other = NativePath(TEST_PATH.port, TEST_PATH.subnet, 3)
        for f in (
            frame(0x200, b"\x03"),
            frame(0x20F, bytes.fromhex("08 01 ca")),
            frame(0x20F, bytes.fromhex("09 10")),
            frame(0x20F, bytes.fromhex("08 01 ca"), stamp=5),
            frame(0x2FF, bytes.fromhex("fe 35 00 e4"), _MASTER),
            frame(0x3E7, bytes([TEST_PATH.subnet, TEST_PATH.node]), other),
        ):
            stream.feed(
                Packet(
                    1,
                    bytes([0, f.port]) + f.timestamp_ms.to_bytes(4, "little") + f.body,
                ),
            )
        args = build_parser().parse_args(
            [
                "watch-native",
                "--passive",
                "--duration",
                "0.025",
                "--tcp",
                "localhost:3001",
            ],
        )
        events: list[object] = []
        assert await watch_all_from_args(stream, args, events.append) == 0
        reports = [
            e["line_report"]
            for e in events
            if isinstance(e, dict) and "line_report" in e
        ]
        assert [(r["kind"], r.get("group_id")) for r in reports] == [
            ("setpoint_group", 8),
            ("setpoint_group", 9),
            ("scene", None),
        ]
        assert reports[0]["extension_hex"] == "ca" and reports[0]["actual_enable"] == 0
        assert (
            reports[2]["body"].endswith("fe3500e4") and "scene_name" not in reports[2]
        )
        assert {
            "line_reports_unknown": {
                "port": TEST_PATH.port,
                "subnet": TEST_PATH.subnet,
                "node": TEST_PATH.node,
            },
        } in events
        assert tx.empty()
