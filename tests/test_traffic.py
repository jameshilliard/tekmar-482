import struct
import unittest
from unittest.mock import AsyncMock

from tekmar_482 import FrameCounts, NativePath, ProtocolError, TrafficCounters


class TrafficTests(unittest.IsolatedAsyncioTestCase):
    async def test_port_and_stable_device_directory_preserve_u64(self) -> None:
        high = 2**64 - 1
        path = NativePath(4, 0x42, 7)
        key = bytes((4, 0x42, 7))
        counts = FrameCounts(high, 2**55, 5, 3)
        body = struct.pack("<4Q", high, 2**55, 5, 3)
        read = AsyncMock(
            side_effect=[
                b"\x04" + body + struct.pack("<3Q", 7, 0, 8),
                key + b"\x01" + body,
                struct.pack("<HH", 256, 1),
                bytes(2) + key + body,
            ],
        )
        counters = TrafficCounters(read)
        port = await counters.port(4)
        self.assertEqual(port.counts, counts)
        self.assertEqual(port.counts.tx_attempts, 2**55 + 8)
        dev = await counters.device(path)
        self.assertIsNotNone(dev)
        self.assertEqual([d async for d in counters.devices()], [dev])
        self.assertEqual(
            [c.args[0] for c in read.await_args_list],
            [b"\x04", key, b"", bytes(2)],
        )

    async def test_missing_or_inconsistent_addresses(self) -> None:
        path = NativePath(0, 0, 0)
        counters = TrafficCounters(
            AsyncMock(
                side_effect=[
                    bytes(36),
                    bytes(35),
                    bytes(3) + b"\x02" + bytes(32),
                    bytes(56),
                ],
            ),
        )
        self.assertIsNone(await counters.device(path))
        for _ in range(2):
            with self.assertRaises(ProtocolError):
                await counters.device(path)
        with self.assertRaises(ProtocolError):
            await counters.port(0)
