import asyncio
import unittest

from tekmar_482 import (
    DiscoveredDevice,
    NativeAddress,
    NativeFrame,
    NativeInventory,
    NativeMessage,
    NativeTargets,
    Packet,
    ProtocolError,
    TxStatus,
    discover_native,
)
from tekmar_482.raw import RawDispatcher


def frame(port: int, node: int, block: bytes, *, mid: int = 0x2E7) -> NativeFrame:
    message = NativeMessage(mid, NativeAddress(0x15, node), block)
    return NativeFrame(port, 12, message.to_bytes())


def identity(number: int, j: int) -> bytes:
    return b"\0\0\x1f" + number.to_bytes(3, "little") + j.to_bytes(2, "little")


class NativeInventoryTests(unittest.TestCase):
    def test_identity_and_supported_blocks_are_path_specific(self) -> None:
        inventory = NativeInventory()
        # The two ambiguous legacy product codes are not serial numbers.
        data = identity(101901, 1159)
        first = inventory.observe(frame(3, 7, data))
        assert first and first.identity and first.identity.product
        assert first.identity.product.model == "346"
        inventory.observe(frame(2, 7, data))
        inventory.observe(frame(3, 39, bytes.fromhex("2300 01 02")))
        inventory.observe(frame(3, 39, bytes.fromhex("4400 07 01")))
        inventory.observe(frame(3, 39, bytes.fromhex("4700 00")))
        inventory.observe(frame(3, 39, bytes.fromhex("4500 81 112233")))
        current = inventory.observe(frame(3, 39, data[:2] + b"\x07" + data[3:6]))
        assert current and current.identity == first.identity
        assert current.address.node == 39
        assert current.reported_blocks == {0, 0x23}
        other = inventory.devices[0]
        assert other.port == 2 and other.identity == first.identity
        assert other.reported_blocks == {0}
        assert len(inventory.devices) == 2
        changed = inventory.observe(frame(3, 7, identity(101901, 1145)))
        assert changed and changed.identity and changed.identity.product
        assert changed.identity.product.model == "481"
        assert changed.reported_blocks == {0}
        assert inventory.observe(frame(3, 32, data)) is None

    def test_address_move_evicts_the_destination_and_tolerates_repeats(self) -> None:
        inventory = NativeInventory()
        previous = inventory.observe(frame(4, 3, identity(101103, 1173)))
        inventory.observe(frame(4, 5, identity(101901, 1159)))
        move = frame(4, 37, bytes.fromhex("1523"), mid=0x3E7)
        for _ in range(2):
            moved = inventory.observe(move)
            assert previous and moved and moved.identity == previous.identity
            assert inventory.devices == (moved,)
        unknown = inventory.observe(frame(4, 37, bytes.fromhex("1508"), mid=0x3E7))
        assert unknown and unknown.identity is None and not unknown.reported_blocks

    def test_legacy_thermostat_alias_needs_j_not_a_hardcoded_model(self) -> None:
        for number, version, model in (
            (10599181, 111803, "541"),
            (10599181, 111601, "542"),
            (10599181, None, None),
            (10599181, 120000, None),
            (3006346, 114101, "161"),
        ):
            device = DiscoveredDevice(4, type_code=number, version=version)
            assert device.model == model
            assert device.is_known_type is (model is not None)

    def test_multi_node_request_has_no_reserved_padding_bit(self) -> None:
        src = NativeAddress(0, 0)
        query = NativeMessage.request_block(
            0,
            source=src,
            target=NativeTargets(0x15),
            argument=0,
        )
        assert query.payload == bytes.fromhex("0000 15 ffffff7f 00000000")
        some = NativeTargets(0x12, (1 << 30) | 5)
        assert NativeMessage.request_block(
            2,
            source=src,
            target=some,
        ).payload == bytes.fromhex("02001205000040")
        for mask in (0, -1, 1 << 31):
            with self.assertRaises(ValueError):
                NativeTargets(0, mask)


class NativeScanTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.rx: asyncio.Queue[Packet] = asyncio.Queue()
        self.tx: asyncio.Queue[bytes] = asyncio.Queue()
        self.stream = RawDispatcher(self.tx.put, 8)

    def deliver(self, sample: NativeFrame) -> None:
        self.stream.feed(Packet(1, bytes((0, sample.port, 12, 0, 0, 0)) + sample.body))

    async def test_scan_is_once_per_subnet_and_stalled_ports_are_independent(
        self,
    ) -> None:
        sample = frame(3, 29, identity(105801, 1226))
        async with self.stream.receiving(self.rx.get), asyncio.timeout(2):
            scan = asyncio.create_task(
                discover_native(self.stream, duration=0.15, subnets=((4, 17),)),
            )
            self.deliver(sample)
            self.deliver(sample)
            self.deliver(frame(3, 0, b"", mid=0x3F7))
            requests = {await self.tx.get(), await self.tx.get()}
            assert {body[:2] + body[6:] for body in requests} == {
                bytes.fromhex("0104 000011 ffffff7f 00000000"),
                bytes.fromhex("0103 000015 ffffff7f 00000000"),
            }
            self.stream.feed(Packet(1, bytes((2, 3, 0))))
            result = await scan
            assert len(result.devices) == 1
            assert not result.host_records_dropped and self.tx.empty()
            successful, pending = result.probes
            assert (
                successful.port == 3
                and successful.result
                and successful.result.status is TxStatus.ACK
            )
            assert pending.port == 4 and pending.result is None
            # The scan ended, but caller cancellation does not release the wire slot.
            with self.assertRaisesRegex(ProtocolError, "already in progress"):
                await self.stream.send(4, sample.body)
            self.stream.feed(Packet(1, bytes((2, 4, 2, 1))))
            self.stream.check_drained()

    async def test_passive_scan_reports_gaps_but_never_sends(self) -> None:
        async with self.stream.receiving(self.rx.get), asyncio.timeout(2):
            for _ in range(10):
                self.deliver(frame(1, 1, b"", mid=0x3F7))
            result = await discover_native(self.stream, duration=0.03, source=None)
            assert self.tx.empty() and not result.probes
            assert result.host_records_dropped == 2
            assert len(result.devices) == 1 and result.devices[0].identity is None
            with self.assertRaises(ValueError):
                await discover_native(self.stream, source=None, subnets=((0, 0),))
            with self.assertRaises(ValueError):
                await discover_native(self.stream, subnets=((5, 0),))

    async def test_failed_stream_is_not_reported_as_silent(self) -> None:
        with self.assertRaisesRegex(ProtocolError, "link down"):
            async with self.stream.receiving(self.rx.get):
                self.stream.fail(ProtocolError("link down"))
                await discover_native(self.stream, duration=2)
