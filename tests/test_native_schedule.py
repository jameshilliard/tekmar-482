import asyncio
import json
import unittest
from calendar import Day
from datetime import time
from io import StringIO

from tekmar_482 import (
    DailyEvents,
    NativeAddress,
    NativeBlockBatch,
    NativeBlockQuery,
    NativeField,
    NativeFrame,
    NativeMessage,
    NativeSnapshot,
    NativeTargets,
    NativeWriteStatus,
    Packet,
    ScheduleBinding,
    ScheduleChange,
    ScheduleChannel,
    ScheduleCycle,
    ScheduleEvent,
    ScheduleProfile,
    ScheduleTarget,
    TxResult,
    TxStatus,
    WeeklyEvent,
    read_weekly_schedule,
)
from tekmar_482.cli import build_parser, run
from tekmar_482.raw import RawDispatcher
from tekmar_482.tpck import serialize

from .test_bridge import RAW_CAPABILITIES, listen, management

_HOST = NativeAddress(0, 0)
_PEER = NativeAddress(0x23, 5)
_TARGET = ScheduleTarget(_PEER)
_WEEK = bytes.fromhex("0306090c0f1215")


class NativeScheduleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.requests: list[tuple[int, int]] = []
        self.updates: list[NativeMessage] = []
        self.blocks: dict[tuple[int, int], bytes] = {}
        self.compact_result: bytes | None = None
        self.stream = RawDispatcher(self.receive_host, 64)
        idle: asyncio.Queue[Packet] = asyncio.Queue()
        await self.enterAsyncContext(self.stream.receiving(idle.get))

    def identity(self, product: int, z: int) -> None:
        self.blocks[0, 0] = b"\x27" + product.to_bytes(3, "little") + bytes([z])

    async def receive_host(self, send: bytes) -> None:
        assert send[:2] == b"\x01\x01"
        msg = NativeMessage.from_bytes(send[2:])
        self.stream.feed(Packet(1, bytes([2, 1, 0])))
        payload = msg.payload
        if msg.message_id == 0x2DF:
            self.updates.append(msg)
            assert payload[:2] == bytes([_PEER.subnet, _PEER.block_node])
            block = int.from_bytes(payload[2:4], "little")
            selector = payload[4] & 7 if block in range(0x38, 0x3C) else 0
            before = self.blocks[block, selector]
            self.blocks[block, selector] = (
                self.compact_result or before[:1] + payload[4:]
            )
            return
        assert msg.message_id == 0x2D7 and payload[2] == _PEER.subnet
        assert int.from_bytes(payload[3:7], "little") == _PEER.request_mask()
        query = (
            int.from_bytes(payload[:2], "little"),
            int.from_bytes(payload[7:], "little"),
        )
        self.requests.append(query)
        data = self.blocks.get(query)
        if data is not None:
            report = NativeMessage(0x2E7, _PEER, payload[:2] + data)
            self.stream.feed(Packet(1, bytes([0, 1, 4, 0, 0, 0]) + report.to_bytes()))

    def test_complete_row_round_trip_for_sunday_first_and_unknown_revision(
        self,
    ) -> None:
        msg = NativeMessage(0x2E7, _PEER, bytes.fromhex("11007f") + _WEEK)
        frame = NativeFrame(1, 1, msg.to_bytes())
        profile = ScheduleProfile.from_identification(
            {"product_number": 104401, "z_number": 2},
        )
        week = WeeklyEvent.from_frame(frame, profile=profile)
        assert week.days == _WEEK[1:] + _WEEK[:1]
        assert week.time_on(Day.SUNDAY) == time(0, 30)
        change = week.with_time(Day.MONDAY, None)
        assert change.update(_HOST).payload == bytes.fromhex("23051100 0390090c0f1215")
        with self.assertRaisesRegex(ValueError, "revision"):
            WeeklyEvent.from_frame(
                frame,
                profile=ScheduleProfile(profile.product, None),
            )

    async def test_sunday_first_two_event_calendar_and_sparse_unknown_days(
        self,
    ) -> None:
        self.identity(105103, 13)  # 552 N
        self.blocks[0xF, 0] = bytes.fromhex("0d 010101")  # shared zone, 5-2, two events
        self.blocks[0x10, 0] = b"\x7f" + _WEEK
        self.blocks[0x13, 0] = bytes.fromhex("43 90fe00")
        week = await read_weekly_schedule(
            self.stream,
            1,
            ScheduleTarget(_PEER, ScheduleChannel.COOL),
            source=_HOST,
        )
        assert self.requests == [(0, 0), (0xF, 0), (0x10, 0), (0x13, 0)]
        assert (
            week.config.binding is ScheduleBinding.ZONE
            and week.config.daily_events is DailyEvents.TWO
        )
        assert week.config.cycle is ScheduleCycle.FIVE_TWO and not week.table_complete
        wake, sleep = week.rows
        assert (
            wake.days == tuple(_WEEK[1:] + _WEEK[:1])
            and wake.display_event is ScheduleEvent.OCCUPIED
        )
        assert sleep.days == (254, None, None, None, None, 0, 144)
        assert sleep.display_event is ScheduleEvent.UNOCCUPIED and not sleep.complete

    async def test_group_edit_preserves_other_days_and_confirms_compact_readback(
        self,
    ) -> None:
        self.identity(104401, 1)  # 557 B
        self.blocks[0xF, 0] = bytes.fromhex("0d 010101")
        self.blocks[0x13, 0] = b"\x7f" + _WEEK
        self.compact_result = bytes.fromhex("41 9090")  # actual Sun, Sat
        result = await self.stream.write_schedule_time(
            1,
            _TARGET,
            ScheduleChange(ScheduleEvent.UNOCCUPIED, Day.SATURDAY, None),
            source=_HOST,
        )
        assert result.status is NativeWriteStatus.CONFIRMED
        assert self.requests == [(0, 0), (0xF, 0), (0x13, 0), (0x13, 0)]
        assert [m.payload for m in self.updates] == [
            bytes.fromhex("23051300 9006090c0f1290"),
        ]
        for event in (ScheduleEvent.WAKE, ScheduleEvent.SLEEP):
            with self.assertRaisesRegex(ValueError, "not active"):
                await self.stream.write_schedule_time(
                    1,
                    _TARGET,
                    ScheduleChange(event, Day.SUNDAY, time(6)),
                    source=_HOST,
                )
        assert len(self.updates) == 1

    async def test_member_unknown_or_disabled_does_not_read_or_write_owner_rows(
        self,
    ) -> None:
        self.identity(99202, 7)
        cool = ScheduleTarget(_PEER, ScheduleChannel.COOL)
        for setup, expected in (
            ("0f 01070102", ScheduleBinding.MEMBER_2),
            ("0d 010102", None),
            ("0f 010a0102", ScheduleBinding.UNOCCUPIED),
            ("0f 01000102", ScheduleBinding.NONE),
        ):
            self.blocks[0xF, 0] = bytes.fromhex(setup)
            self.requests.clear()
            week = await read_weekly_schedule(self.stream, 1, cool, source=_HOST)
            assert (
                week.config.binding is expected
                and not week.rows
                and not week.table_complete
            )
            with self.assertRaisesRegex(ValueError, "not an identified owner"):
                await self.stream.write_schedule_time(
                    1,
                    cool,
                    ScheduleChange(ScheduleEvent.WAKE, Day.FRIDAY, time(3)),
                    source=_HOST,
                )
            assert self.requests == [(0, 0), (0xF, 0)] * 2 and not self.updates

    async def test_setup_edit_protects_master_and_handles_fixed_sixth_byte(
        self,
    ) -> None:
        query = NativeBlockQuery(_PEER, 0xF)
        self.identity(99201, 10)
        self.blocks[0xF, 0] = bytes.fromhex("0f 010201010000")
        with self.assertRaisesRegex(ValueError, "mastership"):
            await self.stream.write_block(
                1,
                query,
                {NativeField.NUM_DAILY_EVENTS: 2},
                source=_HOST,
            )
        assert not self.updates
        self.identity(105102, 3)
        self.blocks[0xF, 0] = bytes.fromhex("1d 00ed020100")
        same = await self.stream.write_block(
            1,
            query,
            {NativeField.NUM_DAILY_EVENTS: 1},
            source=_HOST,
        )
        assert same.status is NativeWriteStatus.UNCHANGED and not self.updates
        new = await self.stream.write_block(
            1,
            query,
            {NativeField.HEATING_SCHEDULE_ID: ScheduleBinding.ZONE},
            source=_HOST,
        )
        assert new.status is NativeWriteStatus.CONFIRMED
        assert self.updates[0].payload.hex() == "23050f0001ed02010000"

    async def test_timer_slots_require_ownership_and_agree_on_headers(self) -> None:
        self.identity(101103, 0)
        target = ScheduleTarget(_PEER, system=3)
        wake, sleep = (0x38, 2), (0x3B, 2)
        self.blocks[wake] = (
            bytes.fromhex("ff 7a") + _WEEK
        )  # master, seven-day, 2 events
        self.blocks[sleep] = bytes.fromhex("ff 5a") + _WEEK
        with self.assertRaisesRegex(ValueError, "033 timer"):
            await read_weekly_schedule(self.stream, 1, _TARGET, source=_HOST)
        self.requests.clear()
        week = await read_weekly_schedule(self.stream, 1, target, source=_HOST)
        assert (
            self.requests == [(0, 0), wake, sleep]
            and week.config.binding is ScheduleBinding.MASTER_3
        )
        assert (
            week.rows[0].complete
            and not week.rows[1].matching_header
            and not week.table_complete
        )
        change = ScheduleChange(ScheduleEvent.UNOCCUPIED, Day.SUNDAY, time(7, 30))
        with self.assertRaisesRegex(ValueError, "header disagrees"):
            await self.stream.write_schedule_time(1, target, change, source=_HOST)
        self.blocks[sleep] = bytes.fromhex("ff 7a") + _WEEK
        self.requests.clear()
        preview = await self.stream.write_schedule_time(
            1,
            target,
            change,
            source=_HOST,
            preview=True,
        )
        assert preview.status is NativeWriteStatus.PREVIEW and not self.updates
        assert self.requests == [(0, 0), wake, sleep]
        assert preview.proposed.payload.hex() == "23053b007a0306090c0f122d"
        self.compact_result = bytes.fromhex(
            "81 ba2d",
        )  # Same time, now a four-event table.
        changed = await self.stream.write_schedule_time(1, target, change, source=_HOST)
        assert changed.status is NativeWriteStatus.DIFFERENT and changed.readback
        self.blocks[wake] = bytes.fromhex("ff 72") + _WEEK
        week = await read_weekly_schedule(self.stream, 1, target, source=_HOST)
        assert week.config.binding is ScheduleBinding.NONE and not week.rows

    async def test_missing_row_is_not_a_disabled_week(self) -> None:
        self.identity(105102, 3)
        self.blocks[0xF, 0] = bytes.fromhex("0d 020302")
        self.blocks[0x10, 0] = b"\x7f" + _WEEK
        self.blocks[0x12, 0] = b"\x7f" + bytes([144] * 7)
        self.blocks[0x13, 0] = b"\x7f" + _WEEK
        week = await read_weekly_schedule(
            self.stream,
            1,
            _TARGET,
            source=_HOST,
            timeout=0.05,
        )
        assert len(week.rows) == 4 and not week.table_complete
        assert week.rows[1].days == (None,) * 7 and week.rows[1].report is None
        assert week.rows[1].batch is not None and not week.rows[1].batch.reports
        assert week.rows[2].days == (144,) * 7 and week.rows[2].complete

    def test_directory_resolves_only_observed_unique_masters_in_selected_ports(
        self,
    ) -> None:
        def batch(
            port: int,
            block: int,
            entries: dict[int, bytes],
            selector: int | None = None,
        ) -> NativeBlockBatch:
            reports = tuple(
                NativeFrame(
                    port,
                    0,
                    NativeMessage(
                        0x2E7,
                        NativeAddress(3, node),
                        block.to_bytes(2, "little") + data,
                    ).to_bytes(),
                )
                for node, data in entries.items()
            )
            masks = sum(frame.message.source.request_mask() for frame in reports)
            return NativeBlockBatch(
                port,
                NativeBlockQuery(NativeTargets(3, masks), block, selector),
                TxResult(port, TxStatus.ACK),
                reports,
            )

        ident = b"\x07" + (99201).to_bytes(3, "little")
        setup = bytes.fromhex("0f 02020302")  # heat and cool both master 1
        reports = (
            batch(1, 0, {2: ident, 3: ident}),
            batch(1, 0xF, {34: setup, 3: bytes.fromhex("0f 06010302")}),
            batch(1, 0x10, {2: b"\x7f" + _WEEK}),
            batch(2, 0, {2: b"\x07" + (101103).to_bytes(3, "little")}),
            batch(2, 0x38, {2: b"\xff\x78" + _WEEK}, 0),
        )
        directory = NativeSnapshot(reports, ()).schedule_directory
        line = directory.system(1, ports=(1,))
        assert (
            len(line.masters) == 1
            and line.owner
            and line.owner.config.target.address.node == 34
        )
        assert not line.owner.table_complete and line.owner.rows[0].days == tuple(_WEEK)
        assert (
            len(line.members) == 1 and line.members[0].config.target.address.node == 3
        )
        both = directory.system(1, ports=(1, 2))
        assert len(both.masters) == 2 and both.owner is None
        assert directory.system(4, ports=(1, 2)).owner is None
        reconfigured = NativeSnapshot(
            (*reports, batch(1, 0xF, {2: bytes.fromhex("0f 01 02 01 01")})),
            (),
        ).schedule_directory
        owner = reconfigured.system(1, ports=(1,)).owner
        assert owner and not owner.rows and not owner.table_complete
        replaced = NativeSnapshot(
            (*reports, batch(1, 0, {2: ident})),
            (),
        ).schedule_directory
        assert replaced.system(1, ports=(1,)).owner is None

    async def test_cli_confirmed_group_edit_over_transparent_tcp(self) -> None:
        async def app(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            if (first := await reader.readexactly(1)) == b"?":
                assert await reader.readexactly(5) == b"\r\nJ\r\n"
                assert not await reader.read()
                return
            caps = management(1, 1)
            assert first + await reader.readexactly(len(caps) - 1) == caps
            writer.write(RAW_CAPABILITIES)
            for method, data in ((2, b"\x01"), (9, b"\0")):
                command = management(method, 0, data)
                assert await reader.readexactly(len(command)) == command
                writer.write(management(method, 3, data))
            identity = b"\x07" + (105102).to_bytes(3, "little")
            for block, data in (
                (0, identity),
                (0xF, bytes.fromhex("0d 010101")),
                (0x13, b"\x7f" + _WEEK),
                (-1, b""),
                (0x13, bytes.fromhex("60 3030")),
            ):
                if block == -1:
                    body = bytes.fromhex("6df0000b 23051300 0306090c0f3030")
                else:
                    body = NativeBlockQuery(_PEER, block).request(_HOST).to_bytes()
                command = serialize(Packet(1, b"\x01\x01" + body))
                assert await reader.readexactly(len(command)) == command
                reply = serialize(Packet(1, b"\x02\x01\0"))
                if block != -1:
                    msg = NativeMessage(
                        0x2E7,
                        _PEER,
                        block.to_bytes(2, "little") + data,
                    )
                    reply += serialize(Packet(1, b"\0\1" + bytes(4) + msg.to_bytes()))
                writer.write(reply)
            leave = management(2, 0, b"\0")
            assert await reader.readexactly(len(leave)) == leave
            writer.write(management(2, 3, b"\0"))

        async with listen(app) as endpoint, asyncio.timeout(5):
            out = StringIO()
            args = build_parser().parse_args(
                [
                    "write-schedule",
                    "1",
                    "0x23",
                    "5",
                    "unoccupied",
                    "sunday",
                    "08:00",
                    "--confirm",
                    "--json",
                    "--tcp",
                    f"{endpoint.address}:{endpoint.port}",
                ],
            )
            assert await run(args, stdout=out) == 0
            assert json.loads(out.getvalue())["status"] == "confirmed"
