import asyncio
import json
import unittest

from tekmar_482 import (
    CaptureGap,
    GatewayTraceGap,
    LoopKind,
    NativeAddress,
    NativeFrame,
    NativeMessage,
    NativeState,
    Packet,
    WaterMonitor,
    read_water,
)
from tekmar_482.cli import build_parser
from tekmar_482.cli_status import water_fields, water_system_fields
from tekmar_482.raw import RawDispatcher

_LOCAL = NativeAddress(0x25, 31)
_OTHER = NativeAddress(0x36, 31)
_BASE = bytes.fromhex("bc07 00c0 9828 2211 fe")


def _frame(
    mid: int,
    data: bytes,
    *,
    port: int = 3,
    peer: NativeAddress = _LOCAL,
    stamp: int = 1234,
) -> NativeFrame:
    return NativeFrame(port, stamp, NativeMessage(mid, peer, data).to_bytes())


class MonitorTests(unittest.TestCase):
    def test_master_presence_without_slots_does_not_clear_previous_data(self) -> None:
        monitor = WaterMonitor()
        beacon = _frame(0x3FF, b"")
        assert monitor.observe(beacon)
        (bare,) = monitor.sources
        assert bare.address == _LOCAL and bare.assignment is None
        assert not bare.topology.announced and not bare.has_measurements
        assert not monitor.observe(beacon)
        assert monitor.observe(_frame(0x3FF, b"\x25"))
        assert monitor.observe(_frame(0x3CF, _BASE))
        (known,) = monitor.sources
        assert known.assignment and known.has_measurements
        assert not monitor.observe(beacon) and monitor.sources == (known,)
        assert monitor.observe(_frame(0x3FF, b"\0"))
        (unassigned,) = monitor.sources
        assert unassigned.topology.announced == (0,)
        assert unassigned.assignment is None and not unassigned.has_measurements

    def test_two_ports_repeat_both_statuses_but_own_different_loops(self) -> None:
        monitor = WaterMonitor()
        for port, local in ((1, _OTHER), (3, _LOCAL)):
            for sender in (_LOCAL, _OTHER):
                assert monitor.observe(_frame(0x3CF, _BASE, port=port, peer=sender))
            assert monitor.observe(_frame(0x3FF, b"\x25\x36", port=port, peer=local))
        remote, mix, boiler, repeat = monitor.sources
        assert remote.assignment is None and repeat.assignment is None
        assert mix.assignment is not None and mix.assignment.mix_number == 1
        assert (
            boiler.assignment is not None and boiler.assignment.kind is LoopKind.BOILER
        )
        assert all(s.has_measurements for s in monitor.sources)
        assert boiler.master is not None and boiler.master.value.supply_f == 113
        assert boiler.master.value.return_f is None
        assert boiler.plant is None and not boiler.tanks
        assert not monitor.observe(_frame(0x3CF, _BASE))

    def test_master_plant_dhw_and_tanks_keep_separate_fields_and_tails(self) -> None:
        monitor = WaterMonitor()
        for mid, body in (
            (0x3CF, _BASE + b"\xca"),
            (0x3CE, _BASE + b"\x12\x34"),
            (0x23F, bytes.fromhex("31 04 50 ff")),
            (0x173, b"\x04\x33\xf0\x00"),
            (0x173, b"\x01\xe0\x00\x33"),
        ):
            assert monitor.observe(_frame(mid, body))
        assert not monitor.observe(_frame(0x3CF, _BASE[:8]))
        (source,) = monitor.sources
        assert source.master is not None and source.master.extension == b"\xca"
        assert source.master.value.dhw_supply_raw is None
        assert source.plant is not None and source.plant.extension == b"\x12\x34"
        assert source.dhw is not None and source.dhw.value.demand
        assert source.dhw.value.tank_f is None and source.dhw.value.target_f == 104.0
        assert [(s.value.tank_id, s.value.tank_raw) for s in source.tanks] == [
            (1, 0),
            (4, 240),
        ]
        encoded = json.dumps(water_fields(source))
        assert '"extension_hex": "ca"' in encoded and '"assignment": null' in encoded
        assert monitor.observe(_frame(0x3CF, _BASE + b"\x41\x06"))
        current = monitor.sources[0].master
        assert current and current.value.dhw_supply_f == 75.1

    def test_sparse_setup_reassignment_and_explicit_unlink(self) -> None:
        monitor = WaterMonitor()
        assert not monitor.observe(_frame(0x2E7, bytes.fromhex("330000")))
        assert monitor.observe(_frame(0x3FF, b"\x25"))
        assert monitor.observe(_frame(0x3CF, _BASE))
        assert monitor.observe(_frame(0x2E7, bytes.fromhex("340030 25 82")))
        (source,) = monitor.sources
        assert source.assignment and source.assignment.mix_number == 2
        assert source.assignment.slot == 7 and source.master is None
        assert monitor.observe(_frame(0x3CF, _BASE))
        assert not monitor.observe(_frame(0x2E7, bytes.fromhex("340030 25")))
        assert not monitor.observe(_frame(0x2E7, bytes.fromhex("340030 25 13 22")))
        assert monitor.sources[0].master is not None
        for data, kind in (
            ("340030 25 00", LoopKind.UNLINKED),
            ("340030 25 22", LoopKind.UNKNOWN),
        ):
            assert monitor.observe(_frame(0x2E7, bytes.fromhex(data)))
            selected = monitor.sources[0].assignment
            assert selected and selected.kind is kind
        assert monitor.observe(_frame(0x2E7, bytes.fromhex("340010 25")))
        assert monitor.observe(_frame(0x2E7, bytes.fromhex("340020 05")))
        (partial,) = monitor.sources
        assert partial.assignment is None
        assert monitor.observe(_frame(0x2E7, bytes.fromhex("340000")))
        selected = monitor.sources[0].assignment
        assert selected and selected.kind is LoopKind.BOILER

    def test_node_lock_identity_moves_and_loss(self) -> None:
        monitor = WaterMonitor()
        first = NativeAddress(0x25, 0)
        locked = NativeAddress(0x25, 32)
        before = bytes.fromhex("000007 111111")
        after = bytes.fromhex("000007 112211")
        assert not monitor.observe(_frame(0x2E7, before, peer=first))
        assert monitor.observe(_frame(0x3CF, _BASE, peer=locked))
        (source,) = monitor.sources
        assert source.identity and source.identity.product_number == 0x111111
        assert source.address == first
        assert (
            monitor.observe(_frame(0x2E7, after, peer=locked)) and not monitor.sources
        )
        for peer in (first, _LOCAL, _OTHER):
            assert monitor.observe(_frame(0x3CF, _BASE, peer=peer))
        assert monitor.observe(_frame(0x3E7, bytes([first.subnet, first.node])))
        assert [s.address for s in monitor.sources] == [_OTHER]
        assert monitor.observe(CaptureGap(2)) and not monitor.sources
        assert not monitor.observe(CaptureGap(1)) and monitor.dropped == 3


class WaterSystemTests(unittest.TestCase):
    def test_shared_map_resolves_boiler_and_mix_on_their_claiming_ports(self) -> None:
        state = NativeState()
        slots = bytes([_LOCAL.subnet, _OTHER.subnet])
        for port, local in ((1, _OTHER), (3, _LOCAL)):
            for sender in (_LOCAL, _OTHER):
                state.observe(_frame(0x3CF, _BASE, port=port, peer=sender))
            claim = slots + (b"\0" if port == 1 else b"")
            state.observe(_frame(0x3FF, claim, port=port, peer=local))

        (system,) = state.water_systems(ports=(1, 3))
        boiler, mix = system.loops
        assert system.subnets == (_LOCAL.subnet, _OTHER.subnet)
        assert (
            boiler.source
            and boiler.source.port == 3
            and boiler.source.address == _LOCAL
        )
        assert mix.source and mix.source.port == 1 and mix.source.address == _OTHER
        assert len(state.water_sources) == 4
        (port_one,) = state.water_systems(ports=(1,))
        assert (
            port_one.loops[0].source is None and port_one.loops[1].source == mix.source
        )
        assert water_system_fields(system)["loops"] == [
            {
                "slot": 0,
                "subnet": _LOCAL.subnet,
                "advertised_kind": "boiler",
                "source": {"port": 3, "subnet": _LOCAL.subnet, "node": 31},
                "candidates": [{"port": 3, "subnet": _LOCAL.subnet, "node": 31}],
                "other_claims": [],
                "repeated_slot": False,
            },
            {
                "slot": 1,
                "subnet": _OTHER.subnet,
                "advertised_kind": "mix",
                "source": {"port": 1, "subnet": _OTHER.subnet, "node": 31},
                "candidates": [{"port": 1, "subnet": _OTHER.subnet, "node": 31}],
                "other_claims": [],
                "repeated_slot": False,
            },
        ]

        # Hearing another matching status is not a reason to change the route.
        state.observe(_frame(0x3CF, _BASE, port=1, stamp=1240))
        state.observe(GatewayTraceGap(2, 1 << 3))
        (after_drop,) = state.water_systems(ports=(1, 3))
        assert after_drop.loops[0].source is None
        assert after_drop.loops[1].source == mix.source
        assert any(s.address == _LOCAL and s.master for s in state.water_sources)

    def test_competing_maps_repeated_slots_and_partial_addresses_stay_ambiguous(
        self,
    ) -> None:
        state = NativeState()
        same_bus = NativeAddress(_LOCAL.subnet ^ 0x20, 31)
        state.observe(_frame(0x3FF, bytes([_LOCAL.subnet]), peer=same_bus))
        (wrong_subnet,) = state.water_systems(ports=(3,))
        assert wrong_subnet.loops[0].source is None
        state.observe(_frame(0x3FF, bytes([_LOCAL.subnet])))
        state.observe(_frame(0x3FF, bytes([_LOCAL.subnet]), port=1, peer=_LOCAL))
        (competing,) = state.water_systems(ports=(1, 3))
        assert len(competing.loops[0].candidates) == 2
        assert competing.loops[0].source is None

        state.observe(_frame(0x3FF, bytes([_LOCAL.subnet, _LOCAL.subnet]), port=1))
        single, repeated = state.water_systems(ports=(1, 3))
        assert single.loops[0].other_claims[0].port == 1
        assert single.loops[0].source is None
        assert all(
            loop.repeated_slot and loop.source is None for loop in repeated.loops
        )
        for ports in ((), (1, 6)):
            with self.subTest(ports=ports), self.assertRaises(ValueError):
                state.water_systems(ports=ports)

    def test_addressed_setup_invalidation_withholds_topology_route(self) -> None:
        state = NativeState()
        state.observe(_frame(0x3FF, bytes([_LOCAL.subnet])))
        state.observe(_frame(0x3CF, _BASE))
        assert state.water_systems(ports=(3,))[0].loops[0].source is not None

        update = bytes(
            [_LOCAL.subnet, _LOCAL.node, 0x33, 0, _LOCAL.subnet, 2, 0, 0, 0, 0, 0, 0],
        )
        state.observe(_frame(0x2DF, update, peer=NativeAddress(0, 0)))
        (pending,) = state.water_systems(ports=(3,))
        assert len(pending.loops[0].candidates) == 1 and pending.loops[0].source is None
        state.observe(_frame(0x2E7, bytes.fromhex("330003 25 02")))
        (confirmed,) = state.water_systems(ports=(3,))
        loop = confirmed.loops[0]
        assert loop.advertised.kind is LoopKind.BOILER
        assert (
            loop.source
            and loop.source.assignment
            and loop.source.assignment.kind is LoopKind.MIX
        )
        assert loop.source.master is None  # No old status relabelled as mixing.


class WaterReadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tx: asyncio.Queue[bytes] = asyncio.Queue()
        self.stream = RawDispatcher(self.tx.put, 16)
        idle: asyncio.Queue[Packet] = asyncio.Queue()
        await self.enterAsyncContext(self.stream.receiving(idle.get))

    def feed(self, mid: int, data: bytes, port: int = 3) -> None:
        self.stream.feed(
            Packet(1, bytes([0, port, 1, 0, 0, 0]) + _frame(mid, data).body),
        )

    async def test_query_completion_does_not_stop_capture(self) -> None:
        task = asyncio.create_task(
            read_water(self.stream, [1, 3, 3], duration=0.1, plant_settings=False),
        )
        async with asyncio.timeout(1):
            for port in (1, 3):
                out = await self.tx.get()
                assert out[:2] == bytes([0, port])
                assert NativeMessage.from_bytes(out[6:]).message_id == 0x3F7
                self.stream.feed(Packet(1, bytes([2, port, 0])))
                self.feed(0x3FF, b"" if port == 1 else b"\x25", port)
            for _ in range(2):
                request = await self.tx.get()
                port = request[1]
                message = NativeMessage.from_bytes(request[6:])
                assert message.message_id == 0x2D7 and message.payload[:2] == bytes(2)
                assert message.payload[2] == _LOCAL.subnet
                assert (
                    int.from_bytes(message.payload[3:7], "little")
                    == _LOCAL.request_mask()
                )
                self.stream.feed(Packet(1, bytes([2, port, 0])))
                product = 104106 if port == 3 else 104104  # 406 has 0033; 403 does not.
                self.feed(
                    0x2E7,
                    bytes.fromhex("000007") + product.to_bytes(3, "little"),
                    port,
                )
            setup = await self.tx.get()
            assert setup[1] == 3
            assert NativeMessage.from_bytes(setup[6:]).payload[:2] == b"\x33\x00"
            self.stream.feed(Packet(1, bytes([2, 3, 0])))
            self.feed(0x2E7, bytes.fromhex("330003 25 13"))
            await asyncio.sleep(0)
            self.feed(0x3CF, _BASE)
            self.feed(0x3CE, _BASE, 4)
            sample = await task
        assert all(q.answered for q in sample.master_queries)
        assert [(b.port, b.query.block_id) for b in sample.block_reads] == [
            (1, 0),
            (3, 0),
            (3, 0x33),
        ]
        assert [s.port for s in sample.sources] == [1, 3]
        assert [s.has_measurements for s in sample.sources] == [False, True]
        assert sample.sources[0].assignment is None
        assert (
            sample.sources[1].assignment
            and sample.sources[1].assignment.kind is LoopKind.TANK
        )
        systems = sample.water_systems(ports=(1, 3))
        assert len(systems) == 1 and systems[0].loops[0].source == sample.sources[1]
        assert self.tx.empty()

    async def test_master_node_zero_not_an_addressed_block_target(self) -> None:
        task = asyncio.create_task(read_water(self.stream, [3], duration=0.04))
        async with asyncio.timeout(1):
            await self.tx.get()
            self.stream.feed(Packet(1, bytes([2, 3, 0])))
            body = _frame(0x3FF, b"\x25", peer=NativeAddress(0x25, 0)).body
            self.stream.feed(Packet(1, bytes([0, 3, 1, 0, 0, 0]) + body))
            sample = await task
        assert sample.master_queries[0].answered and not sample.block_reads
        assert not sample.sources[0].has_measurements and self.tx.empty()

    async def test_passive_no_commands_and_unknown_assignment(self) -> None:
        self.feed(0x173, bytes([4, 80, 75, 0]))
        sample = await read_water(self.stream, [3], source=None, duration=0.01)
        assert not sample.master_queries and sample.sources[0].assignment is None
        assert self.tx.empty() and sample.dropped == 0
        args = build_parser().parse_args(
            [
                "read-water",
                "1",
                "3",
                "--passive",
                "--shared-network",
                "--tcp",
                "localhost:1234",
            ],
        )
        assert args.passive and args.shared_network and args.ports == [1, 3]

    async def test_missing_or_cancelled_query_releases_capture_reader(self) -> None:
        task = asyncio.create_task(read_water(self.stream, [3], duration=20))
        async with asyncio.timeout(1):
            await self.tx.get()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.stream.feed(Packet(1, bytes([2, 3, 0])))
            self.feed(0x3CF, _BASE)
            sample = await read_water(self.stream, [3], duration=0.02)
            assert not sample.master_queries and sample.unfinished_ports == (3,)
            assert sample.sources[0].has_measurements
            await self.tx.get()
            idle = asyncio.create_task(self.stream.wait_transmit_idle(3))
            await asyncio.sleep(0)
            assert not idle.done()
            self.stream.feed(Packet(1, bytes([2, 3, 0])))
            await idle

    async def test_invalid_inputs_do_not_send_or_consume(self) -> None:
        for ports, duration in (
            ([], 1.0),
            ([5], 1.0),
            ([1], float("nan")),
            ([1], float("inf")),
        ):
            with self.assertRaises(ValueError):
                await read_water(self.stream, ports, duration=duration)
        assert self.tx.empty()
