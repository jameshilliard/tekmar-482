import asyncio
import unittest

from tekmar_482 import (
    GatewayTraceGap,
    LineReportKind,
    NativeFrame,
    NativePath,
    NativeState,
    OperatingPolicyBlock,
    Packet,
    SetbackThresholds,
    TargetSlot,
    WarmWeatherShutdown,
)
from tekmar_482.cli import build_parser
from tekmar_482.cli_native_watch import watch_all_from_args
from tekmar_482.raw import RawDispatcher

from .native_fixtures import TEST_PATH, block, extended_rx, frame, identity

_OTHER = NativePath(0, TEST_PATH.subnet, TEST_PATH.node)


def _report(b: int, data: str, path: NativePath = TEST_PATH) -> NativeFrame:
    return block(b, bytes.fromhex(data), path)


class NativeOperatingPolicyTests(unittest.TestCase):
    def test_configured_thresholds_are_not_live_shutdown_and_preserve_masks(
        self,
    ) -> None:
        state = NativeState()
        for report in (
            _report(0x0B, "13 02 4a aabb 39 cc"),
            _report(0x0C, "01 20"),
            _report(0xA9, "09 00ff"),
            _report(0xAA, "01 fe"),
            _report(0xAB, "01 ff"),
            _report(0xAC, "01 04"),
            _report(0xAD, "01 05"),
            _report(0xAE, "01 02"),
            _report(0xAF, "01 00"),
            _report(0xA6, "00"),
            _report(0xA7, "01 09"),
        ):
            state.observe(report)
        ((path, policy),) = state.operating_policies
        assert path == TEST_PATH
        assert policy.warm_shutdown == SetbackThresholds((74, None, None, 57, None))
        assert policy.cold_shutdown == SetbackThresholds((32, None, None, None, None))
        assert policy.balance_point is not None
        assert policy.balance_point.raw(TargetSlot.WAKE) == 0
        assert policy.balance_point.raw(TargetSlot.SLEEP) == 255
        assert policy.balance_point.raw(TargetSlot.OCCUPIED) is None
        assert policy.shutdown_source_raw == 2
        assert policy.balance_point_schedule_raw == 254
        assert policy.w1_warm_shutdown_raw == 255
        assert policy.w2_lockout_raw == 4
        assert policy.compressor_during_backup_raw == 5
        assert policy.baseload_raw == 2
        assert policy.unoccupied_humidity_raw == 0
        assert policy.heat_pump_cycles_raw is None
        assert policy.cool_purge_minutes == 9
        assert policy.humidity_differential_raw is None
        assert state.line_report(TEST_PATH, LineReportKind.WWSD) is None
        state.observe(frame(0x30F, b"\x02"))
        live = state.line_report(TEST_PATH, LineReportKind.WWSD)
        assert live is not None and live.value == WarmWeatherShutdown(2)
        assert state.operating_policy(TEST_PATH) == policy

    def test_mask_zero_source_only_and_peer_identity_lifetime(self) -> None:
        state = NativeState()
        state.observe(identity(105102))
        no_cycles = _report(0xA6, "00")
        state.observe(no_cycles)
        assert (
            state.block(TEST_PATH, OperatingPolicyBlock.HEAT_PUMP_CYCLES) is no_cycles
        )
        assert state.operating_policy(TEST_PATH) is None
        state.observe(_report(0x0B, "01 00"))
        policy = state.operating_policy(TEST_PATH)
        assert policy is not None and policy.shutdown_source_raw == 0
        assert policy.warm_shutdown is None
        state.observe(identity(102002))
        assert not state.operating_policies

    def test_update_invalidates_only_matching_block_not_other_ports_or_slots(
        self,
    ) -> None:
        state = NativeState()
        for path in (TEST_PATH, _OTHER):
            state.observe(_report(0x0B, "13 02 4a 39", path))
            state.observe(_report(0x0C, "01 20", path))
        sender = NativePath(TEST_PATH.port, 0, 0)
        update = bytes([TEST_PATH.subnet, TEST_PATH.node, 0x0B, 0, 1, 2, 3, 4, 5, 6])
        assert not state.observe(frame(0x2DF, update[:-1], sender))
        assert state.block(TEST_PATH, OperatingPolicyBlock.WARM_SHUTDOWN) is not None
        assert state.observe(frame(0x2DF, update, sender)) == (TEST_PATH,)
        policy = state.operating_policy(TEST_PATH)
        assert policy and policy.warm_shutdown is None and policy.cold_shutdown
        other = state.operating_policy(_OTHER)
        assert other is not None and other.warm_shutdown is not None
        assert state.observe(GatewayTraceGap(1, 1 << TEST_PATH.port)) == (TEST_PATH,)
        assert state.operating_policies == ((_OTHER, other),)


class NativeOperatingPolicyWatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_passive_cli_emits_changed_policy_and_address_move(self) -> None:
        tx: asyncio.Queue[bytes] = asyncio.Queue()
        idle: asyncio.Queue[Packet] = asyncio.Queue()
        stream = RawDispatcher(tx.put, 32)
        await self.enterAsyncContext(stream.receiving(idle.get))
        report = _report(0xA7, "01 09")
        moved = NativePath(TEST_PATH.port, TEST_PATH.subnet, TEST_PATH.node + 1)
        for observed in (
            report,
            report,
            frame(0x3E7, bytes([TEST_PATH.subnet, TEST_PATH.node]), moved),
        ):
            stream.feed(
                extended_rx(observed),
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
        policies = [
            e["operating_policy"]
            for e in events
            if isinstance(e, dict) and "operating_policy" in e
        ]
        assert len(policies) == 1
        assert policies[0]["cool_purge_minutes"] == 9
        assert {
            "operating_policy_removed": {
                "port": TEST_PATH.port,
                "subnet": TEST_PATH.subnet,
                "node": TEST_PATH.node,
            },
        } in events
        assert tx.empty()
