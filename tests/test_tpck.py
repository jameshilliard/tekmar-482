import unittest

from tekmar_482.packet import TYPE_TRPC, Packet
from tekmar_482.tpck import EOF, ESC, SOF, TpckParser, serialize


class TpckTest(unittest.TestCase):
    def test_only_unescaped_eof_terminates_a_complete_frame(self) -> None:
        packet = Packet(TYPE_TRPC, bytes([EOF]))
        frame = serialize(packet)
        for invalid in (
            frame.replace(bytes([ESC, EOF]), bytes([EOF]), 1),
            frame[:-1] + bytes([ESC, EOF]),
            frame[:-1] + b"\x01" + frame[-1:],
        ):
            with self.subTest(invalid=invalid):
                assert TpckParser().feed_many(invalid + frame) == [packet]

    def test_tpck_round_trip_with_escaped_bytes(self) -> None:
        packet = Packet(TYPE_TRPC, bytes([SOF, EOF, ESC, 0x00]))

        frame = serialize(packet)

        assert frame.startswith(bytes([SOF]))
        assert bytes([ESC, SOF]) in frame
        assert bytes([ESC, EOF]) in frame
        assert bytes([ESC, ESC]) in frame
        assert TpckParser().feed_many(frame) == [packet]

    def test_tpck_parser_handles_split_frames(self) -> None:
        packet = Packet(TYPE_TRPC, b"\x01\x02\x03")
        frame = serialize(packet)
        parser = TpckParser()

        assert parser.feed_many(frame[:3]) == []
        assert parser.feed_many(frame[3:]) == [packet]

    def test_tpck_parser_ignores_bad_checksum_by_default(self) -> None:
        packet = Packet(TYPE_TRPC, b"\x01\x02\x03")
        frame = bytearray(serialize(packet))
        frame[-2] ^= 0xFF

        assert TpckParser().feed_many(frame) == []
