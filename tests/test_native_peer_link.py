import asyncio
import unittest

from tekmar_482 import (
    GatewayTraceGap,
    NativePath,
    NativeState,
    Packet,
    PeerBusErrors,
    PeerBusEvents,
    PeerLinkCounters,
)
from tekmar_482.cli import build_parser
from tekmar_482.cli_native_watch import watch_all_from_args
from tekmar_482.raw import RawDispatcher

from .native_fixtures import TEST_PATH, block, frame, identity

_OTHER = NativePath(0, TEST_PATH.subnet, TEST_PATH.node)


class PeerLinkStateTests(unittest.TestCase):
    def test_counts_keep_their_own_scope_and_mask(self) -> None:
        state = NativeState()
        assert state.peer_link_counters(TEST_PATH) is None
        errors = block(0x48, bytes.fromhex("85 09ff05"))
        events = block(0x49, bytes.fromhex("1f 09 00 07 03 04 ca35"))
        for f in (errors, events, block(0x62, bytes.fromhex("0f 34120080"))):
            assert state.observe(f) == (TEST_PATH,)
            assert not state.observe(f)
        state.observe(block(0x48, bytes.fromhex("ff 09 01 02 03 04 05 06 07"), _OTHER))
        state.observe(block(0x48, bytes.fromhex("01 01")))
        assert state.peer_links == (
            (
                _OTHER,
                PeerLinkCounters((PeerBusErrors(9, 1, 2, 3, 4, 5, 6, 7),), (), None),
            ),
            (
                TEST_PATH,
                PeerLinkCounters(
                    (
                        PeerBusErrors(1, None, None, None, None, None, None, None),
                        PeerBusErrors(9, None, 255, None, None, None, None, 5),
                    ),
                    (PeerBusEvents(9, 0, 7, 3, 4),),
                    0x80001234,
                ),
            ),
        )
        assert state.block(TEST_PATH, 0x49, 9) is events
        assert events.decoded["extension_hex"] == "ca35"
        assert not state.plant_configurations

    def test_partial_word_invalid_selector_and_new_address_assignment(self) -> None:
        state = NativeState()
        state.observe(identity())
        assert not state.observe(block(0x48, bytes.fromhex("04 20")))
        state.observe(block(0x62, bytes.fromhex("07 000000")))
        assert state.peer_link_counters(TEST_PATH) is None
        zero = block(0x62, bytes.fromhex("0f 00000000"))
        assert state.observe(zero) == (TEST_PATH,)
        assert state.peer_link_counters(TEST_PATH) == PeerLinkCounters((), (), 0)
        state.observe(identity(number=102002))
        assert not state.peer_links
        state.observe(zero)
        moved = NativePath(TEST_PATH.port, TEST_PATH.subnet, 3)
        state.observe(frame(0x3E7, bytes([TEST_PATH.subnet, TEST_PATH.node]), moved))
        assert not state.peer_links

    def test_gap_forgets_only_the_affected_observations(self) -> None:
        state = NativeState()
        for p in (TEST_PATH, _OTHER):
            state.observe(block(0x49, bytes.fromhex("03 00 01"), p))
        assert state.observe(GatewayTraceGap(1, 1 << TEST_PATH.port)) == (TEST_PATH,)
        assert [p for p, _ in state.peer_links] == [_OTHER]


class PeerLinkWatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_passive_watch_publishes_counts_without_sending(self) -> None:
        tx: asyncio.Queue[bytes] = asyncio.Queue()
        idle: asyncio.Queue[Packet] = asyncio.Queue()
        stream = RawDispatcher(tx.put, 32)
        await self.enterAsyncContext(stream.receiving(idle.get))
        match = block(0x49, bytes.fromhex("17 00 04 03 10"))
        for f in (match, match):
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
        await watch_all_from_args(stream, args, events.append)
        counters = [
            e["peer_link_counters"]
            for e in events
            if isinstance(e, dict) and "peer_link_counters" in e
        ]
        assert len(counters) == 1
        assert counters[0]["port"] == TEST_PATH.port
        assert counters[0]["events"] == (
            {
                "port_id": 0,
                "local_port_busy": 4,
                "remote_device_busy": 3,
                "arbitration_losses": None,
                "error_event_count": 16,
            },
        )
        assert tx.empty()
