import asyncio
import unittest
from typing import TYPE_CHECKING

from tekmar_482 import NativePath, NativeState, NetworkFault, SensorCircuit
from tekmar_482.cli import build_parser
from tekmar_482.cli_native_watch import watch_all_from_args
from tekmar_482.decoding import decode_network_error, decoded_to_dict
from tekmar_482.raw import CaptureGap, InvalidCapture, RawDispatcher

from .native_fixtures import TEST_PATH, extended_rx, frame, identity

if TYPE_CHECKING:
    from tekmar_482.packet import Packet

_MASTER = NativePath(0, TEST_PATH.subnet, 0)


class FaultStateTests(unittest.TestCase):
    def test_sources_repeats_extensions_and_explicit_clear(self) -> None:
        state = NativeState()
        sample = bytes.fromhex("8fe1 aa")
        assert state.fault_report(TEST_PATH) is None
        assert state.observe(frame(0x3D7, sample, stamp=0xFFFFFFFA)) == (TEST_PATH,)
        assert not state.observe(frame(0x3D7, sample, stamp=1))
        report = state.fault_report(TEST_PATH)
        assert report and report.extension == b"\xaa" and report.frame.timestamp_ms == 1
        assert report.fault.circuit is SensorCircuit.OPEN
        value = decode_network_error(0xE18F)
        assert value and value.fault == report.fault
        encoded = decoded_to_dict(value)
        assert isinstance(encoded, dict)
        assert (encoded["priority"], encoded["instance"]) == (3, 33)
        assert state.observe(frame(0x3D7, sample[:-1])) == (TEST_PATH,)
        assert state.observe(frame(0x3D7, bytes.fromhex("fd40"), _MASTER)) == (_MASTER,)
        assert not state.observe(frame(0x3D7, b"\0", _MASTER))
        assert not state.observe(InvalidCapture(0, 5, 2, 1, b"\x80"))
        assert state.observe(frame(0x3D7, bytes.fromhex("0080"))) == (TEST_PATH,)
        a, b = state.fault_reports
        assert a.path == _MASTER and a.fault == NetworkFault(0x40FD)
        assert b.path == TEST_PATH and b.fault.is_clear and b.fault.raw == 0x8000
        assert set(state.observe(CaptureGap(2))) == {_MASTER, TEST_PATH}
        assert not state.fault_reports

    def test_identity_replacement_and_address_move_forget_the_source(self) -> None:
        state = NativeState([TEST_PATH])
        state.observe(identity())
        sample = frame(0x3D7, bytes.fromhex("0988"))
        assert state.observe(sample) == (TEST_PATH,)
        state.observe(identity(number=101901))
        assert state.fault_report(TEST_PATH) is None
        state.observe(sample)
        moved = NativePath(TEST_PATH.port, TEST_PATH.subnet, 3)
        state.observe(
            frame(0x3E7, bytes([TEST_PATH.subnet, TEST_PATH.node | 32]), moved),
        )
        assert state.fault_report(TEST_PATH) is None
        assert not state.observe(frame(0x3D7, bytes(2), moved))
        assert not state.fault_reports


class FaultWatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_watch_distinguishes_cleared_from_lost(self) -> None:
        tx: asyncio.Queue[bytes] = asyncio.Queue()
        idle: asyncio.Queue[Packet] = asyncio.Queue()
        stream = RawDispatcher(tx.put, 32)
        await self.enterAsyncContext(stream.receiving(idle.get))
        new = NativePath(TEST_PATH.port, TEST_PATH.subnet, 3)
        for f in (
            frame(0x3D7, bytes.fromhex("0988")),
            frame(0x3D7, bytes.fromhex("0988"), stamp=5),
            frame(0x3D7, bytes.fromhex("8fe1"), _MASTER),
            frame(0x3D7, bytes.fromhex("0080"), _MASTER),
            frame(0x3E7, bytes([TEST_PATH.subnet, TEST_PATH.node]), new),
        ):
            stream.feed(
                extended_rx(f),
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
        await watch_all_from_args(stream, args, events.append)
        reports = [
            e["network_fault"]
            for e in events
            if isinstance(e, dict) and "network_fault" in e
        ]
        assert [(r["port"], r["raw"]) for r in reports] == [
            (3, 0x8809),
            (0, 0xE18F),
            (0, 0x8000),
        ]
        assert {
            "network_fault_unknown": {
                "port": 3,
                "subnet": TEST_PATH.subnet,
                "node": TEST_PATH.node,
            },
        } in events
        assert tx.empty()
