import unittest

from tekmar_482 import ProtocolError
from tekmar_482.packet import TYPE_TRPC, Packet


class PacketTest(unittest.TestCase):
    def test_packet_stores_type_and_data(self) -> None:
        packet = Packet(TYPE_TRPC, bytes([0x01, 0x3F, 0x01, 0x00, 0x00]))

        assert packet.type == TYPE_TRPC
        assert packet.data == bytes([0x01, 0x3F, 0x01, 0x00, 0x00])

    def test_packet_rejects_out_of_range_type(self) -> None:
        with self.assertRaisesRegex(ProtocolError, "packet type must fit"):
            Packet(0x100)
