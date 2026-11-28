import asyncio
import unittest

from tekmar_482 import (
    NativeAddress,
    NativeDevice,
    NativeFrame,
    NativeIdentity,
    NativeInventory,
    NativeMessage,
    NativePath,
    NativeSnapshot,
    Packet,
    TargetSlot,
    TxStatus,
    read_native_devices,
    read_native_snapshot,
)
from tekmar_482._native_payloads import BLOCK_SELECTORS
from tekmar_482.raw import RawDispatcher


class NativeSnapshotTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.rx: asyncio.Queue[Packet] = asyncio.Queue()
        self.requests: list[tuple[int, int, int, int, int]] = []
        self.status_requests: list[NativeMessage] = []
        self.block_fields: dict[int, bytes] = {}
        self.identities: dict[int, tuple[int, int]] = {}
        self.availability: dict[int, int] = {}
        self.stalled: set[int] = set()
        self.skip_once: set[tuple[int, int]] = set()
        self.stream = RawDispatcher(self.write, 32)

    async def write(self, data: bytes) -> None:
        port = data[1]
        msg = NativeMessage.from_bytes(data[6:])
        assert data[0] == 0
        if msg.message_id == 0x17F:
            self.status_requests.append(msg)
            payload = bytes([msg.payload[0] + 8, len(msg.payload[2:])]) + b"".join(
                bytes([key, 5, 0]) for key in reversed(msg.payload[2:])
            )
            body = NativeMessage(0x17E, NativeAddress(0x16, 7), payload).to_bytes()
            self.stream.feed(Packet(1, bytes([2, port, TxStatus.ACK])))
            self.stream.feed(Packet(1, bytes([0, port, 0, 0, 0, 0]) + body))
            return
        assert msg.message_id == 0x2D7
        block = int.from_bytes(msg.payload[:2], "little")
        subnet = msg.payload[2]
        mask = int.from_bytes(msg.payload[3:7], "little")
        selector = int.from_bytes(msg.payload[7:], "little")
        self.requests.append((port, subnet, block, selector, mask))
        if port in self.stalled:
            return
        self.stream.feed(Packet(1, bytes([2, port, TxStatus.ACK])))
        for n, (product, j_number) in self.identities.items():
            if not mask & (1 << (n - 1)):
                continue
            if (n, block) in self.skip_once:
                self.skip_once.remove((n, block))
                continue
            if block == 0:
                fields = (
                    bytes([31])
                    + product.to_bytes(3, "little")
                    + j_number.to_bytes(2, "little")
                )
            elif block in self.block_fields:
                fields = self.block_fields[block]
            elif block == 0x50:
                fields = bytes([3, self.availability[n], 0])
            elif block in BLOCK_SELECTORS:
                fields = bytes([1, selector])
            else:
                fields = b"\0"
            source = NativeAddress(subnet, n | 32)
            body = NativeMessage(0x2E7, source, msg.payload[:2] + fields).to_bytes()
            self.stream.feed(Packet(1, bytes([0, port, 0, 0, 0, 0]) + body))

    async def test_group_plans_model_and_observed_instances_per_peer(self) -> None:
        self.identities = dict.fromkeys((2, 7), (104103, 1206))
        self.availability = {2: 0, 7: 9}
        inventory = NativeInventory()
        for node, selector in ((2, 5), (7, 3)):
            msg = NativeMessage(
                0x2E7,
                NativeAddress(0x24, node),
                bytes([0x44, 0, 1, selector]),
            )
            inventory.observe(NativeFrame(3, 0, msg.to_bytes()))
        async with self.stream.receiving(self.rx.get), asyncio.timeout(2):
            batches = await read_native_devices(self.stream, inventory.devices)
        assert self.requests[:2] == [(3, 0x24, 0, 0, 66), (3, 0x24, 0x50, 0, 66)]
        assert [(s, m) for _, _, b, s, m in self.requests if b == 0x44] == [
            (0, 66),
            (3, 64),
            (5, 2),
            (129, 66),
        ]
        assert [(s, m) for _, _, b, s, m in self.requests if b == 0x4D] == [
            (0, 64),
            (3, 64),
        ]
        assert all(not batch.missing_nodes for batch in batches)
        assert len(batches) == len(set(self.requests))

    async def test_counter_reads_use_model_or_observed_selectors_without_guessing(
        self,
    ) -> None:
        self.identities = {
            1: (102002, 1188),
            2: (105801, 1226),
            3: (102002, 1188),
            4: (105801, 1226),
        }
        devices = (
            NativeDevice(4, NativeAddress(0x21, 1)),
            NativeDevice(4, NativeAddress(0x21, 2)),
            NativeDevice(4, NativeAddress(0x20, 3)),
            NativeDevice(
                4,
                NativeAddress(0x21, 4),
                reported_blocks=frozenset({0x49}),
                reported_selectors=frozenset({(0x49, 7)}),
            ),
        )
        async with self.stream.receiving(self.rx.get), asyncio.timeout(2):
            await read_native_devices(self.stream, devices)
        assert [r for r in self.requests if r[2] in {0x48, 0x49, 0x62}] == [
            (4, 0x21, 0x48, 0, 1),
            (4, 0x21, 0x49, 0, 1),
            (4, 0x21, 0x49, 7, 8),
        ]

    async def test_553_snapshot_queries_verified_monitor_registers(self) -> None:
        self.identities = {2: (105102, 1222)}
        self.block_fields = {
            0x0A: bytes.fromhex("0f 9001 0400"),
            0x1B: bytes.fromhex("17 01000001"),
            0x99: bytes.fromhex("03 7856"),
            0xA8: bytes.fromhex("01 01"),
        }
        device = NativeDevice(3, NativeAddress(0x27, 2))
        async with self.stream.receiving(self.rx.get), asyncio.timeout(2):
            batches = await read_native_devices(self.stream, [device])
        requested = {b for _, _, b, _, _ in self.requests}
        assert {0x0A, 0x1B, *range(0x99, 0x9F), 0xA1, 0xA2, 0xA8} <= requested
        assert not {0x1C, 0x9F, 0xA0}.intersection(requested)
        (climate,) = NativeSnapshot(batches, ()).thermostats
        assert climate.adjustments.onboard_humidity_enabled is True
        relay = climate.equipment.relay_status
        assert relay and relay.value.heat_stage1 == 1 and relay.value.fan2 is None
        air_filter = climate.equipment.filter_usage
        assert air_filter and air_filter.value.interval_raw == 400
        assert [
            (r.block.value, r.hours)
            for r in climate.equipment.runtimes
            if r.hours is not None
        ] == [(0x99, 0x5678)]

    async def test_553_snapshot_queries_verified_operating_policy(self) -> None:
        self.identities = {2: (105102, 1222)}
        self.block_fields = {
            0x0B: bytes.fromhex("13 02 4a 39"),
            0x0C: bytes.fromhex("01 20"),
            0xA7: bytes.fromhex("01 09"),
            0xA9: bytes.fromhex("09 00ff"),
            0xAF: bytes.fromhex("01 00"),
        }
        path = NativePath(3, 0x27, 2)
        async with self.stream.receiving(self.rx.get), asyncio.timeout(2):
            batches = await read_native_devices(
                self.stream,
                [NativeDevice(path.port, path.address)],
            )
        requested = {b for _, _, b, _, _ in self.requests}
        assert {0x0B, 0x0C, 0xA7, *range(0xA9, 0xB0)} <= requested
        assert not {0xA6, 0xB0}.intersection(requested)
        ((reported_path, policy),) = NativeSnapshot(batches, ()).operating_policies
        assert reported_path == path
        assert policy.warm_shutdown and policy.warm_shutdown.raw(TargetSlot.SLEEP) == 57
        assert policy.cold_shutdown and policy.cold_shutdown.raw(TargetSlot.WAKE) == 32
        assert (
            policy.balance_point and policy.balance_point.raw(TargetSlot.SLEEP) == 255
        )
        assert policy.cool_purge_minutes == 9
        assert policy.unoccupied_humidity_raw == 0
        assert policy.humidity_differential_raw is None

    async def test_unknown_model_and_reused_address_do_not_inherit_candidates(
        self,
    ) -> None:
        self.identities = {1: (999, 12), 2: (101103, 1173)}
        dev = NativeDevice(3, NativeAddress(8, 1), reported_blocks=frozenset({0x77}))
        reused = NativeDevice(
            3,
            NativeAddress(8, 2),
            NativeIdentity(104103, 1206, None, None),
            frozenset({0x44}),
            frozenset({(0x44, 129)}),
        )
        async with self.stream.receiving(self.rx.get), asyncio.timeout(2):
            await read_native_devices(self.stream, [dev, reused])
        assert (3, 8, 0x77, 0, 1) in self.requests
        assert not any(b == 0x44 for _, _, b, _, _ in self.requests)
        assert [(s, m) for _, _, b, s, m in self.requests if b == 0x38] == [
            (i, 2) for i in range(4)
        ]

    async def test_missing_tx_stops_only_that_port(self) -> None:
        self.stalled = {1}
        self.identities = {2: (108402, 1)}
        devices = [
            NativeDevice(p, NativeAddress(s, 2)) for p, s in ((1, 9), (1, 10), (4, 10))
        ]
        async with self.stream.receiving(self.rx.get), asyncio.timeout(2):
            batches = await read_native_devices(self.stream, devices, timeout=0.03)
            self.stream.feed(Packet(1, bytes([2, 1, 0])))
        assert batches[0].port == 1 and batches[0].tx_result is None
        assert batches[0].missing_nodes == (2,)
        assert batches[1].port == 4 and not batches[1].missing_nodes
        assert len(self.requests) == 2

    async def test_late_peer_gets_one_targeted_retry(self) -> None:
        self.identities = dict.fromkeys((3, 8), (108402, 1))
        self.skip_once.add((8, 0))
        devices = [NativeDevice(4, NativeAddress(14, n)) for n in self.identities]
        async with self.stream.receiving(self.rx.get), asyncio.timeout(2):
            first, retry = await read_native_devices(
                self.stream,
                devices,
                timeout=0.025,
            )
        assert first.missing_nodes == (8,) and not retry.missing_nodes
        assert [(b, mask) for _, _, b, _, mask in self.requests] == [(0, 132), (0, 128)]

    async def test_snapshot_uses_confirmed_snowmelt_configuration_only_once(
        self,
    ) -> None:
        self.identities = {7: (105801, 1226)}
        self.block_fields = {
            0xD0: bytes([3, 3, 2]),
            0xC0: bytes([1, 8]),
            0xC1: bytes([1, 6]),
        }
        dev = NativeDevice(3, NativeAddress(0x16, 7))
        async with self.stream.receiving(self.rx.get), asyncio.timeout(2):
            snapshot = await read_native_snapshot(self.stream, [dev])
            assert len(snapshot.statuses) == 1 and snapshot.statuses[0].answered
            assert self.status_requests[0].payload == bytes.fromhex("390519221b2329")
            assert sum(q[2] == 0xD0 for q in self.requests) == 1
            assert sum(q[2] == 0xC0 for q in self.requests) == 1
            self.block_fields[0xC0] = b"\0"
            assert not (await read_native_snapshot(self.stream, [dev])).statuses
            assert len(self.status_requests) == 1
