import unittest
from calendar import Day
from datetime import UTC, time

from tekmar_482 import (
    NativeAddress,
    NativeFrame,
    NativeMessage,
    ScheduleEvent,
    WeeklyEvent,
)


class NativeTests(unittest.TestCase):
    def test_snowmelt_status_request_keeps_order_and_is_not_a_group_update(
        self,
    ) -> None:
        source = NativeAddress(0, 0)
        request = NativeMessage.request_snowmelt_status(
            source=source,
            zone_type=2,
            zone_id=5,
            keys=bytes.fromhex("25262222"),
        )
        assert request.to_bytes() == bytes.fromhex("57f00006 2a04 25262222")
        assert request.decoded["keys_hex"] == "25262222"
        with self.assertRaises(ValueError):
            NativeMessage.request_snowmelt_status(
                source=source,
                zone_type=2,
                zone_id=32,
                keys=b"",
            )
        with self.assertRaises(ValueError):
            NativeMessage.request_snowmelt_status(
                source=source,
                zone_type=0,
                zone_id=0,
                keys=bytes(62),
            )

    def test_raw_group_update_is_not_an_addressed_block(self) -> None:
        source = NativeAddress(0, 0)
        update = NativeMessage.setpoint_group(12, enabled=True, source=source)
        assert update.to_bytes() == bytes.fromhex("61f00002 0c01")
        for invalid in (0, 13):
            with self.assertRaises(ValueError):
                NativeMessage.setpoint_group(invalid, enabled=False, source=source)

    def test_header_boundaries_and_unrecognized_payload(self) -> None:
        payload = bytes(range(63))
        body = bytes.fromhex("ffffffff") + payload
        message = NativeMessage.from_bytes(body)
        assert message.to_bytes() == body
        assert message.priority == 3
        assert message.source == NativeAddress(255, 63)
        for invalid in (b"", body[:3], body[:-1], body + b"\0"):
            with self.assertRaises(ValueError):
                NativeMessage.from_bytes(invalid)
        unknown = NativeMessage(0x123, NativeAddress(0, 0), b"\xca\x2f\x35")
        assert unknown.decoded == {}
        assert NativeMessage.from_bytes(unknown.to_bytes()) == unknown
        with self.assertRaises(ValueError):
            NativeMessage(0x400, unknown.source, b"")
        with self.assertRaises(ValueError):
            NativeMessage(0, unknown.source, bytes(64))

    def test_addressed_request_vectors(self) -> None:
        source = NativeAddress(0, 0)
        target = NativeAddress(0x12, 0x23)
        req = NativeMessage.request_block(5, source=source, target=target)
        assert req.to_bytes() == bytes.fromhex("6d70000705001204000000")
        assert NativeMessage.request_block(
            5,
            source=source,
            target=target,
            argument=0,
        ).to_bytes() == bytes.fromhex("6d70000b0500120400000000000000")
        assert NativeAddress(0, 31).request_mask() == 1 << 30
        for invalid_node in (0, 32):
            with self.assertRaises(ValueError):
                NativeAddress(0, invalid_node).request_mask()
        for subnet, node in [(-1, 3), (256, 0), (1, 64)]:
            with self.assertRaises(ValueError):
                NativeAddress(subnet, node)

    def test_single_week_edit_and_readback_vector(self) -> None:
        frame = NativeFrame(2, 4, bytes.fromhex("6e7128ca13007f00018f9024ff06"))
        week = WeeklyEvent.from_frame(frame)
        assert week.target == NativeAddress(0x12, 0x23)
        assert week.port == 2 and week.event is ScheduleEvent.SLEEP
        assert week.time_on(Day.MONDAY) == time(0)
        assert week.time_on(Day.WEDNESDAY) == time(23, 50)
        assert week.time_on(Day.THURSDAY) is None
        with self.assertRaises(ValueError):
            week.time_on(Day.SATURDAY)
        source = NativeAddress(0, 0)
        edit = week.with_time(Day.MONDAY, time(6, 10))
        assert edit.update(source).to_bytes() == bytes.fromhex(
            "6df0000b1223130025018f9024ff06",
        )
        assert edit.request(source).payload == bytes.fromhex("13001204000000")
        assert edit.with_time(Day.WEDNESDAY, None).days[2] == 144
        assert week.days[0] == 0
        for invalid in (
            time(12, 1),
            time(12, 0, 1),
            time(12, microsecond=1),
            time(12, tzinfo=UTC),
        ):
            with self.assertRaises(ValueError):
                week.with_time(Day.FRIDAY, invalid)

    def test_indexed_week_preserves_selector_and_matches_argument(self) -> None:
        frame = NativeFrame(1, 0, bytes.fromhex("6e7128cb3900ffab00010203040506"))
        week = WeeklyEvent.from_frame(frame)
        assert week.event is ScheduleEvent.UNOCCUPIED
        src = NativeAddress(0, 0)
        assert week.request(src).payload == bytes.fromhex("3900120400000003000000")
        assert week.with_time(Day.SUNDAY, None).update(src).payload == bytes.fromhex(
            "12233900ab00010203040590",
        )
        assert frame.decoded["schedule_id"] == 3
        assert frame.decoded["sunday"] == "01:00"

    def test_partial_or_extended_week_is_readable_but_not_editable(self) -> None:
        source = NativeAddress(0, 2)
        for method, raw in [
            (0x2E7, "10000103"),
            (0x2E7, "10007f0001020304050607"),
            (0x2E7, "11007f000102030405"),
            (0x2E7, "38007f00010203040506"),
            (0x2E7, "05001f0102030405"),
            (0x2DF, "10007f00010203040506"),
        ]:
            frame = NativeFrame(
                0,
                0,
                NativeMessage(method, source, bytes.fromhex(raw)).to_bytes(),
            )
            with self.assertRaises(ValueError):
                WeeklyEvent.from_frame(frame)
