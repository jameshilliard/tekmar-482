import unittest
from calendar import Day
from datetime import UTC, datetime, time

from tekmar_482 import (
    NativeAddress,
    NativeBlockQuery,
    NativeFrame,
    NativeMessage,
    ScheduleEvent,
    ScheduleProfile,
    SnowmeltBlock,
    SnowmeltSetpoint,
    StageCommand,
    StageSelection,
    WeeklyEvent,
)


class NativeTests(unittest.TestCase):
    def test_stage_controls_round_trip_without_implying_a_device_response(self) -> None:
        source = NativeAddress(0x12, 27)
        selection = StageSelection(0xAB, 1, 117, 0x808001)
        outputs = tuple(NativeAddress(0xAB, n) for n in (1, 16, 24))
        for kind in StageCommand:
            msg = NativeMessage.stage_command(
                kind,
                selection,
                source=source,
                priority=1,
            )
            assert msg.payload == bytes.fromhex("ab f5 01 80 80")
            assert msg.stage_selection == selection
            assert msg.destination_nodes == outputs
            assert NativeMessage.from_bytes(msg.to_bytes()) == msg
        raw = bytes.fromhex("79f126c6 ab f5 01 80 80 ca")
        extended = NativeMessage.from_bytes(raw)
        assert extended.message_id == StageCommand.START_PURGE
        assert extended.source == source and extended.destination_nodes == outputs
        assert extended.decoded["extension_hex"] == "ca"
        assert extended.to_bytes() == raw
        for n in range(5):
            short = NativeMessage(0x35F, source, extended.payload[:n])
            assert short.stage_selection is None and short.destination_nodes is None
        assert NativeMessage(0x39F, source, bytes(5)).destination_nodes == ()
        assert NativeMessage(0x367, source, bytes(5)).destination_nodes is None

    def test_block_destinations_exclude_padding_and_normalize_node_flags(self) -> None:
        source = NativeAddress(0, 0)
        for mid, data, nodes in (
            (0x2D7, "0000 ab 010000c0", (1, 31)),
            (0x2D7, "0000 ab 00000080", ()),
            (0x2DF, "ab e3 0400 01", (3,)),
        ):
            msg = NativeMessage(mid, source, bytes.fromhex(data))
            assert msg.destination_nodes == tuple(NativeAddress(0xAB, n) for n in nodes)
        for mid, data in (
            (0x2DF, "ab 20 0400"),
            (0x2D7, "0000 ab 010000"),
            (0x2DF, "ab 03 04"),
            (0x1F4, "ab 03 04"),
            (0x322, "ab 04000000"),
        ):
            assert (
                NativeMessage(mid, source, bytes.fromhex(data)).destination_nodes
                is None
            )

    def test_plant_report_matches_whole_pump_selector(self) -> None:
        master = NativeAddress(0x15, 31)
        query = NativeBlockQuery(master, 0x44, selector=129)
        assert query.request(NativeAddress(0, 0)).payload == bytes.fromhex(
            "4400 15 00000040 81000000",
        )
        for data, matches in (
            ("440007 813412", True),
            ("440007 013412", False),
            ("440006 813412", False),  # Unsupported selector, positional padding.
        ):
            msg = NativeMessage(0x2E7, master, bytes.fromhex(data))
            assert query.matches(msg) is matches
            assert NativeFrame(3, 1, msg.to_bytes()).decoded["pump_hours"] == 0x1234
        partial = NativeMessage(0x2E7, master, bytes.fromhex("440005 8134"))
        assert query.matches(partial)
        assert "pump_hours" not in partial.decoded
        assert partial.decoded["byte3_raw"] == 0x34

    def test_addressed_block_query_checks_report_and_selector(self) -> None:
        peer = NativeAddress(0x12, 3)
        locked = NativeAddress(0x12, 35)
        src = NativeAddress(0, 0)
        single = NativeBlockQuery(peer, SnowmeltBlock.ZONE_ID)
        indexed = NativeBlockQuery(peer, SnowmeltBlock.SNOW_ICE_SENSOR_TYPE, 0)
        self.assertEqual(
            single.request(src).payload,
            bytes.fromhex("c000120400000000000000"),
        )
        self.assertEqual(
            indexed.request(src).payload,
            bytes.fromhex("7a00120400000000000000"),
        )
        assert single.matches(NativeMessage(0x2E7, locked, bytes.fromhex("c00000")))
        assert indexed.matches(
            NativeMessage(0x2E7, locked, bytes.fromhex("7a00030004")),
        )
        for mid, source, data in (
            (0x2DF, locked, "7a00030004"),
            (0x2E7, NativeAddress(0x13, 3), "7a00030004"),
            (0x2E7, NativeAddress(0x12, 4), "7a00030004"),
            (0x2E7, locked, "7b00030004"),
            (0x2E7, locked, "7a00030104"),  # Different sensor.
            (0x2E7, locked, "7a00020000"),  # Unsupported ID, positional zero.
            (0x2E7, locked, "7a0011000400"),  # Ambiguous.
            (0x2E7, locked, "7a000300"),  # Truncated.
        ):
            assert not indexed.matches(NativeMessage(mid, source, bytes.fromhex(data)))
        week = NativeBlockQuery(peer, 0x38, 3)
        assert week.matches(NativeMessage(0x2E7, locked, bytes.fromhex("380001fb")))
        assert not week.matches(NativeMessage(0x2E7, locked, bytes.fromhex("380001fc")))
        for block, value in ((0x38, 8), (0xC5, None), (0xCC, 256), (0xC1, 0)):
            with self.assertRaises(ValueError):
                NativeBlockQuery(peer, block, value)
        with self.assertRaises(ValueError):
            NativeBlockQuery(src, 0)

    def test_snowmelt_edits_target_the_reported_node_and_only_one_word(self) -> None:
        peer, host = NativeAddress(0x12, 0x23), NativeAddress(0, 0)
        for block in (
            SnowmeltBlock.MELT_SETPOINT,
            SnowmeltBlock.IDLE_SETPOINT,
            SnowmeltBlock.STORM_SETPOINT,
        ):
            msg = NativeMessage(0x2E7, peer, bytes([block, 0, 3, 0x92, 4]))
            target = SnowmeltSetpoint.from_frame(NativeFrame(2, 7, msg.to_bytes()))
            assert target.port == 2 and target.target == peer and target.block is block
            assert not target.is_off and target.celsius == 0.0
            # 5 C = 41 F = 1260 degH; update data has no support mask.
            update = target.update_celsius(5, source=host)
            assert update.to_bytes() == bytes.fromhex(
                f"6df00006 1203 {block:02x}00 ec04",
            )
            disabled = SnowmeltSetpoint(2, peer, block, 0)
            assert disabled.celsius is None
            if block is SnowmeltBlock.MELT_SETPOINT:
                assert not disabled.is_off
                with self.assertRaises(ValueError):
                    target.update_celsius(None, source=host)
            else:
                assert disabled.is_off
                assert target.update_celsius(None, source=host).payload[-2:] == b"\0\0"
            for raw in (0x4000, 0x8000, 0xC000, 0xFFFF):
                unreadable = SnowmeltSetpoint(2, peer, block, raw)
                assert unreadable.celsius is None and not unreadable.is_off
            for celsius in (-65, (0x4000 - 1170) / 18):
                with self.assertRaises(ValueError):
                    target.update_celsius(celsius, source=host)
        for mid, data in (
            (0x2DF, "7200030000"),
            (0x2E7, "720001ff"),
            (0x2E7, "710003123456"),
            (0x2E7, "7400030000"),
        ):
            with self.assertRaises(ValueError):
                SnowmeltSetpoint.from_frame(
                    NativeFrame(
                        1,
                        0,
                        NativeMessage(mid, peer, bytes.fromhex(data)).to_bytes(),
                    ),
                )

    def test_native_clock_vectors_and_optional_dst_mode(self) -> None:
        source = NativeAddress(0, 0)
        assert NativeMessage.request_clock(source=source).to_bytes() == bytes.fromhex(
            "31700000",
        )
        published = datetime(2012, 6, 26, 10, 27, tzinfo=UTC)
        assert NativeMessage.clock_report(published, source=source).to_bytes() == (
            bytes.fromhex("71f00005 1b5205190c")
        )
        last = published.replace(year=2255)
        report = NativeMessage.clock_report(last, source=source, dst=True, dst_mode=0)
        # The caller supplied 10:27 daylight; the wire contains 09:27 standard.
        assert report.payload == bytes([27, 72 | last.isoweekday(), 0x15, 25, 255, 0])
        assert report.decoded["local_datetime"] == last.replace(tzinfo=None).isoformat(
            timespec="minutes",
        )
        assert report.decoded["year"] == 2255
        assert report.decoded["dst"] == 1
        assert report.decoded["dst_mode"] == 0
        assert "dst_mode" not in NativeMessage.clock_report(last, source=source).decoded
        with self.assertRaises(ValueError):
            NativeMessage.clock_report(published.replace(year=2256), source=source)
        with self.assertRaises(ValueError):
            NativeMessage.clock_report(last, source=source, dst_mode=256)

    def test_display_address_excludes_non_device_nodes(self) -> None:
        assert NativeAddress(0, 24).tha_address(0) == 1
        assert NativeAddress(0, 23).tha_address(0) == 2
        assert NativeAddress(0x17, 33).tha_address(4) == 4724
        for node in (0, 25, 31, 32, 63):
            assert NativeAddress(1, node).tha_address(1) is None
        with self.assertRaises(ValueError):
            NativeAddress(0, 1).tha_address(5)

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
        assert request.to_bytes() == bytes.fromhex("17f00006 2a04 25262222")
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
        assert message.name == "MasterPresent"
        assert message.priority == 3
        assert message.source == NativeAddress(255, 63)
        for invalid in (b"", body[:3], body[:-1], body + b"\0"):
            with self.assertRaises(ValueError):
                NativeMessage.from_bytes(invalid)
        unknown = NativeMessage(0x123, NativeAddress(0, 0), b"\xca\x2f\x35")
        assert unknown.decoded == {}
        assert unknown.name is None
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
            with self.assertRaises(ValueError):
                NativeMessage.update_block(
                    5,
                    b"",
                    source=source,
                    target=NativeAddress(0, invalid_node),
                )
        for subnet, node in [(-1, 3), (256, 0), (1, 64)]:
            with self.assertRaises(ValueError):
                NativeAddress(subnet, node)

    def test_block_update_clears_only_the_destination_lock_flag(self) -> None:
        origin = NativeAddress(9, 37)
        for node in range(1, 32):
            for flag in (0, 32):
                msg = NativeMessage.update_block(
                    0x72,
                    b"\0\0",
                    source=origin,
                    target=NativeAddress(0x12, node | flag),
                )
                assert NativeMessage.from_bytes(msg.to_bytes()).source == origin
                assert msg.payload == bytes((0x12, node, 0x72, 0, 0, 0))

    def test_single_week_edit_and_readback_vector(self) -> None:
        frame = NativeFrame(2, 4, bytes.fromhex("6e7128ca13007f00018f9024ff06"))
        with self.assertRaisesRegex(ValueError, "identify"):
            WeeklyEvent.from_frame(frame)
        profile = ScheduleProfile.from_identification({"product_number": 105102})
        week = WeeklyEvent.from_frame(frame, profile=profile)
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
            "6df0000b1203130025018f9024ff06",
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
            "12033900ab00010203040590",
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
