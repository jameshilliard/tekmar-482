import asyncio
import unittest

from tekmar_482 import (
    CaptureGap,
    GatewayTraceGap,
    LocalScheduleOverride,
    NativeCalendar,
    NativeFrame,
    NativeMessage,
    NativePath,
    NativeState,
    Packet,
    ScheduleBinding,
    ScheduleEvent,
    watch_native,
)
from tekmar_482.cli import build_parser
from tekmar_482.cli_native_watch import watch_all_from_args
from tekmar_482.raw import CaptureRecord, RawDispatcher

from .native_fixtures import TEST_PATH, block, extended_rx, frame, identity

_WEEK = bytes(range(1, 8))
_TIMER = 101103
_THERMOSTAT = 105103
_OTHER = NativePath(2, TEST_PATH.subnet, TEST_PATH.node)
_MASTER = NativePath(TEST_PATH.port, TEST_PATH.subnet, 0)


def setup(
    binding: int,
    path: NativePath = TEST_PATH,
    *,
    events: int = 1,
) -> NativeFrame:
    return block(0xF, bytes([0x0D, binding, 3, events]), path)


def timer(
    block_id: int,
    slot: int,
    *,
    owner: bool = True,
    events: int = 1,
) -> NativeFrame:
    return block(
        block_id,
        bytes([255, slot | (int(owner) << 3) | 0x30 | (events << 6)]) + _WEEK,
    )


def role(
    slot: int,
    *,
    master: bool,
    previous: int | None = None,
    was_master: bool | None = None,
    events: int = 1,
    states: int = 0,
    edge: int = 1,
    path: NativePath = TEST_PATH,
) -> NativeFrame:
    prev = slot if previous is None else previous
    prior_master = master if was_master is None else was_master
    flags = (
        slot
        | (int(master) << 2)
        | (prev << 3)
        | (int(prior_master) << 5)
        | (events << 6)
    )
    return frame(0x2F7, bytes([flags, states, edge]), path)


def calendar(state: NativeState, path: NativePath = TEST_PATH) -> NativeCalendar:
    value = state.schedule(path)
    assert value is not None
    return value


class NativeCalendarTests(unittest.TestCase):
    def test_local_override_is_source_scoped_and_independent_of_calendar(self) -> None:
        state = NativeState()
        state.observe(identity(_THERMOSTAT))
        state.observe(setup(2))
        state.observe(role(0, master=True, states=0x09))
        previous = calendar(state)
        assert previous.local_override is None
        wake = block(0x37, b"\x01\x00")
        assert state.observe(wake) == (TEST_PATH,)
        automatic = block(0x37, b"\x01\x08", _OTHER)
        state.observe(automatic)
        current = calendar(state)
        assert current.local_override is not None
        assert current.local_override.mode is LocalScheduleOverride.WAKE
        assert current.local_override.frame is wake
        assert current.schedules == previous.schedules
        assert current.signals == previous.signals
        other = calendar(state, _OTHER).local_override
        assert other is not None and other.mode is LocalScheduleOverride.AUTO_2
        unknown = block(0x37, b"\x01\xff")
        state.observe(unknown)
        sample = calendar(state).local_override
        assert sample is not None and sample.raw == 255 and sample.mode is None
        state.observe(block(0x37, b"\x00\x05"))
        assert calendar(state).local_override is None
        assert state.observe(block(0x37, b"\x01")) == ()  # Missing supported byte.

        state.observe(wake)
        addressed = bytes([TEST_PATH.subnet, TEST_PATH.node, 0x37, 0, 7])
        sender = NativePath(TEST_PATH.port, 0, 0)
        assert state.observe(frame(0x2DF, addressed, sender)) == (TEST_PATH,)
        assert calendar(state).local_override is None
        assert calendar(state).schedules == previous.schedules
        assert calendar(state, _OTHER).local_override == other

    def test_shared_schedule_uses_only_the_preferred_paths_table(self) -> None:
        state = NativeState()
        scope = (_OTHER.port, TEST_PATH.port)
        state.observe(frame(0x3FF, b"", _MASTER))
        for path in (TEST_PATH, _OTHER):
            state.observe(identity(_THERMOSTAT, z=14, path=path))
        state.observe(setup(ScheduleBinding.MASTER_1, _OTHER))
        state.observe(block(0x10, b"\x7f" + _WEEK, _OTHER))
        forwarded = state.system_schedule(1, ports=scope)
        assert forwarded.owner is None and len(forwarded.unresolved_masters) == 1

        state.observe(setup(ScheduleBinding.MASTER_1))
        both = state.system_schedule(1, ports=scope)
        assert both.owner and not both.owner.rows
        assert both.owner.config.setup.port == TEST_PATH.port
        assert not both.unresolved_masters
        assert state.schedule_directory.system(1, ports=scope).owner is None

        days = bytes(range(12, 19))
        state.observe(block(0x10, b"\x7f" + days))
        owner = state.system_schedule(1, ports=scope).owner
        assert owner and owner.rows[0].days == tuple(days)
        assert owner.rows[0].report and owner.rows[0].report.port == TEST_PATH.port
        state.observe(GatewayTraceGap(1, 1 << TEST_PATH.port))
        lost = state.system_schedule(1, ports=scope)
        assert lost.owner is None and len(lost.unresolved_masters) == 1

    def test_shared_schedule_keeps_distinct_masters_and_resolves_members(self) -> None:
        state = NativeState()
        scope = (_OTHER.port, TEST_PATH.port)
        distinct = NativePath(_OTHER.port, TEST_PATH.subnet ^ 0x20, TEST_PATH.node)
        member = NativePath(TEST_PATH.port, TEST_PATH.subnet, TEST_PATH.node + 1)
        copied = NativePath(_OTHER.port, member.subnet, member.node)
        for path, binding in (
            (TEST_PATH, ScheduleBinding.MASTER_1),
            (distinct, ScheduleBinding.MASTER_1),
            (member, ScheduleBinding.MEMBER_1),
            (copied, ScheduleBinding.MEMBER_1),
        ):
            state.observe(identity(_THERMOSTAT, path=path))
            state.observe(setup(binding, path))
        state.observe(frame(0x3FF, b"", _MASTER))
        ambiguous = state.system_schedule(1, ports=scope)
        assert len(ambiguous.masters) == len(ambiguous.unresolved_masters) == 1
        assert ambiguous.owner is None
        (remote,) = ambiguous.members
        assert NativePath.from_frame(remote.config.setup) == member

        state.observe(frame(0x3FF, b"", NativePath(distinct.port, distinct.subnet, 0)))
        confirmed = state.system_schedule(1, ports=scope)
        assert len(confirmed.masters) == 2 and confirmed.owner is None
        assert not confirmed.unresolved_masters

    def test_phases_do_not_change_ownership_or_prove_a_setback(self) -> None:
        state = NativeState()
        state.observe(identity(_THERMOSTAT, 13))
        state.observe(setup(2))
        for b in (0x10, 0x13):
            state.observe(block(b, b"\x7f" + _WEEK))
        state.observe(role(0, master=True))
        hint = role(0, master=True, states=0x19, edge=0)
        assert state.observe(hint) == (TEST_PATH,)
        assert not state.observe(hint)
        view = calendar(state)
        assert not view.missing_blocks and view.schedules[0].table_complete
        assert view.schedules[0].rows[0].days == (2, 3, 4, 5, 6, 7, 1)
        assert (
            view.schedules[0].rows[0].batch is None and view.schedules[0].rows[0].report
        )
        signal = view.signals[0]
        assert (
            signal.frame is hint
            and signal.status.current_event is ScheduleEvent.UNOCCUPIED
        )
        assert signal.status.next_event is ScheduleEvent.OCCUPIED
        assert not state.observe(frame(0x2F7, b"\x02\x01"))
        # A demotion is not enough to distinguish None, a local table, or membership.
        for binding in (
            ScheduleBinding.NONE,
            ScheduleBinding.ZONE,
            ScheduleBinding.MEMBER_1,
        ):
            state.observe(role(0, master=False, was_master=True))
            absent = calendar(state)
            assert absent.missing_blocks == ((0xF, None),) and not absent.schedules
            assert (
                state.schedule_directory.system(1, ports=(TEST_PATH.port,)).owner
                is None
            )
            state.observe(block(0x10, b"\x7f" + _WEEK))  # preceding the new setup
            state.observe(setup(binding))
            current = calendar(state)
            assert current.schedules[0].config.binding is binding
            assert not current.schedules[0].rows
            assert bool(current.missing_blocks) == binding.owns_table
            state.observe(role(0, master=True, was_master=False))
            state.observe(setup(ScheduleBinding.MASTER_1))

    def test_setup_edits_partial_rows_and_physical_scope(self) -> None:
        state = NativeState([TEST_PATH, _OTHER])
        for path in (TEST_PATH, _OTHER):
            state.observe(identity(99201, 3, path))  # separate heating and cooling
            state.observe(block(15, bytes.fromhex("0f 02 06 03 01"), path))
            for b in (0x10, 0x13):
                state.observe(block(b, b"\x7f" + _WEEK, path))
        directory = state.schedule_directory
        assert len(directory.system(1, ports=(2, 3)).masters) == 2
        assert directory.system(1, ports=(2, 3)).owner is None
        sender = NativePath(TEST_PATH.port, 0, 0)
        dest = bytes([TEST_PATH.subnet, TEST_PATH.node | 0x20, 0x10, 0])
        assert state.observe(frame(0x2DF, dest + b"\0", sender)) == ()
        assert state.observe(frame(0x2DF, dest + _WEEK, sender)) == (TEST_PATH,)
        assert calendar(state).missing_blocks == ((0x10, None),)
        assert calendar(state, _OTHER).schedules[0].table_complete
        state.observe(block(0x10, bytes.fromhex("01 90")))
        week = calendar(state).schedules[0]
        assert not week.table_complete and week.rows[0].days == (144,) + (None,) * 6
        assert calendar(state).missing_blocks == ((0x10, None),)
        state.observe(block(15, bytes.fromhex("0f 02 06 03 02")))
        assert not calendar(state).schedules[0].rows
        assert len(calendar(state).missing_blocks) == 4
        assert not calendar(state).schedules[1].rows
        state.observe(identity(_TIMER))
        assert not calendar(state).schedules and not calendar(state).signals
        assert calendar(state).missing_blocks == tuple((0x38, s) for s in range(4))

    def test_timer_independent_slots_and_header_mismatch(self) -> None:
        state = NativeState()
        state.observe(identity(_TIMER))
        for s in range(4):
            state.observe(timer(0x38, s, owner=s != 3))
            state.observe(timer(0x3B, s))
        for s in (2, 0, 1):
            state.observe(role(s, master=True))
        state.observe(role(2, master=True, previous=1, was_master=True))
        view = calendar(state)
        assert view.missing_blocks == ((0x38, 1), (0x38, 2))
        assert [w.config.target.system for w in view.schedules] == [1, 4]
        assert view.schedules[0].table_complete and not view.schedules[1].rows
        assert [s.status.schedule_id for s in view.signals] == [0, 2]
        state.observe(timer(0x38, 2))
        state.observe(timer(0x3B, 2, events=2))
        bad = calendar(state).schedules[1].rows[-1]
        assert not bad.matching_header and bad.days == (None,) * 7 and bad.report
        state.observe(timer(0x3B, 2))
        # Even a duplicate-response bit with a new countdown is not another role change.
        state.observe(role(2, master=True, previous=1, states=0x10, edge=104))
        assert calendar(state).schedules[1].table_complete
        dest = bytes([TEST_PATH.subnet, TEST_PATH.node, 0x3B, 0, 0x7A])
        state.observe(frame(0x2DF, dest + _WEEK))
        assert calendar(state).missing_blocks == ((0x38, 1), (0x3B, 2))

    def test_unknown_node_zero_move_and_gap(self) -> None:
        state = NativeState()
        zero = NativePath(3, 44, 0)
        for f in (
            identity(_THERMOSTAT, path=zero),
            setup(2, zero),
            role(1, master=True, events=3, path=zero),
        ):
            state.observe(f)
        at_zero = calendar(state, zero)
        assert not at_zero.schedules and not at_zero.missing_blocks
        assert at_zero.signals[0].status.current_event is None
        state.observe(identity(0xF01234))
        state.observe(role(0, master=True))
        assert not calendar(state).missing_blocks and not calendar(state).schedules
        new = NativePath(TEST_PATH.port, 0x25, 5)
        state.observe(frame(0x3E7, bytes([TEST_PATH.subnet, TEST_PATH.node]), new))
        assert state.schedule(TEST_PATH) is None
        state.observe(CaptureGap(2))
        assert not state.paths and not state.schedule_directory.schedules


class LiveCalendarTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tx: asyncio.Queue[bytes] = asyncio.Queue()
        self.stream = RawDispatcher(self.tx.put, 128)
        idle: asyncio.Queue[Packet] = asyncio.Queue()
        await self.enterAsyncContext(self.stream.receiving(idle.get))

    def rx(self, f: NativeFrame) -> None:
        self.stream.feed(
            extended_rx(f),
        )

    async def query(self, b: int, selector: int = 0) -> None:
        async with asyncio.timeout(1):
            packet = await self.tx.get()
        assert packet[:2] == bytes([0, TEST_PATH.port])
        msg = NativeMessage.from_bytes(packet[6:])
        assert msg.message_id == 0x2D7 and msg.payload[:2] == b.to_bytes(2, "little")
        assert int.from_bytes(msg.payload[7:], "little") == selector

    def ack(self) -> None:
        self.stream.feed(Packet(1, bytes([2, TEST_PATH.port, 0])))

    async def test_timer_reads_retire_old_reply_and_queued_rows(self) -> None:
        seen: list[tuple[ScheduleBinding | None, ...]] = []
        records: list[NativeFrame] = []

        def changed(path: NativePath, state: NativeState) -> None:
            seen.append(
                tuple(w.config.binding for w in calendar(state, path).schedules),
            )

        def capture(record: CaptureRecord) -> None:
            if isinstance(record, NativeFrame):
                records.append(record)

        watch = asyncio.create_task(
            watch_native(
                self.stream,
                [TEST_PATH],
                duration=0.3,
                timeout=0.15,
                on_change=changed,
                on_record=capture,
            ),
        )
        await self.query(0)
        self.ack()
        self.rx(identity(_TIMER))
        for s in range(4):
            await self.query(0x38, s)
            self.ack()
            self.rx(timer(0x38, s, owner=s == 2, events=2))
        await self.query(0x39, 2)
        self.ack()
        demotion = role(2, master=False, was_master=True)
        self.rx(demotion)
        self.rx(role(2, master=False, was_master=True, edge=55))
        stale = timer(0x39, 2, events=2)
        self.rx(stale)
        await self.query(0x38, 2)
        self.ack()
        self.rx(timer(0x38, 2, owner=False))
        self.rx(demotion)
        done = await watch
        assert self.tx.empty() and done.reads == 7
        assert ScheduleBinding.MASTER_3 in {v for bindings in seen for v in bindings}
        assert stale in records and done.state.block(TEST_PATH, 0x39, 2) is None
        assert calendar(done.state).schedules[2].config.binding is ScheduleBinding.NONE
        assert not calendar(done.state).missing_blocks
        assert not any(b.query.block_id == 0x39 for b in done.latest_blocks)

    async def test_unanswered_setup_retries_on_first_role_not_countdown(self) -> None:
        watch = asyncio.create_task(
            watch_native(self.stream, [TEST_PATH], duration=0.25, timeout=0.035),
        )
        await self.query(0)
        self.ack()
        self.rx(identity(_TIMER))
        for s in range(4):
            await self.query(0x38, s)
            self.ack()
            if s != 0:
                self.rx(timer(0x38, s, owner=False))
        self.rx(role(0, master=False))
        await self.query(0x38, 0)
        self.ack()
        for countdown in range(5):
            self.rx(role(0, master=False, edge=countdown))
        result = await watch
        assert self.tx.empty() and result.reads == 6
        assert calendar(result.state).missing_blocks == ((0x38, 0),)

    async def test_member_becomes_local_owner_and_row_edits_need_reports(self) -> None:
        watch = asyncio.create_task(
            watch_native(
                self.stream,
                [TEST_PATH],
                duration=0.35,
                timeout=0.15,
                setpoint_settings=False,
            ),
        )
        await self.query(0)
        self.ack()
        self.rx(identity(101102))  # 162 setpoint control, not a thermostat.
        await self.query(0x0F)
        self.ack()
        self.rx(setup(ScheduleBinding.MEMBER_2))
        self.rx(role(0, master=False, previous=1, was_master=False))
        await self.query(0x0F)
        self.ack()
        self.rx(setup(ScheduleBinding.ZONE))
        for b in (0x10, 0x13):
            await self.query(b)
            self.ack()
            self.rx(block(b, b"\x7f" + _WEEK))
        dest = bytes([TEST_PATH.subnet, TEST_PATH.node, 0x13, 0])
        self.rx(frame(0x2DF, dest + bytes(7), NativePath(TEST_PATH.port, 0, 0)))
        self.rx(
            block(0x13, bytes.fromhex("01 90")),
        )  # Compact readback before the monitor can refresh.
        await self.query(0x13)
        self.ack()
        self.rx(block(0x13, b"\x7f" + bytes([144] * 7)))
        result = await watch
        assert self.tx.empty() and result.reads == 6
        view = calendar(result.state)
        assert (
            not view.incomplete
            and view.schedules[0].config.binding is ScheduleBinding.ZONE
        )
        assert view.schedules[0].rows[-1].days == (144,) * 7

    async def test_partial_weekday_reply_is_not_periodically_retried(self) -> None:
        watch = asyncio.create_task(
            watch_native(
                self.stream,
                [TEST_PATH],
                duration=0.3,
                timeout=0.12,
                setpoint_settings=False,
            ),
        )
        await self.query(0)
        self.ack()
        self.rx(identity(101102))
        await self.query(0x0F)
        self.ack()
        self.rx(setup(ScheduleBinding.ZONE))
        for b in (0x10, 0x13):
            await self.query(b)
            self.ack()
            self.rx(block(b, b"\x7f" + _WEEK))

        # An unobserved write left only a new Monday value in the capture.
        self.rx(block(0x13, bytes.fromhex("01 90")))
        await self.query(0x13)
        self.ack()
        self.rx(block(0x13, bytes.fromhex("03 90 02")))
        done = await watch
        assert self.tx.empty() and done.reads == 5
        view = calendar(done.state)
        assert view.missing_blocks == ((0x13, None),)
        assert view.schedules[0].rows[-1].days == (144, 2, None, None, None, None, None)

    async def test_capture_gap_discards_query_and_reidentifies_replacement(
        self,
    ) -> None:
        watch = asyncio.create_task(
            watch_native(self.stream, [TEST_PATH], duration=0.25, timeout=0.12),
        )
        await self.query(0)
        self.ack()
        self.rx(identity(_TIMER))
        await self.query(0x38, 0)
        self.ack()
        quiet = frame(0x3DF, b"")
        for _ in range(130):  # Overflow the simulated stream's 128-record buffer.
            self.rx(quiet)
        self.rx(timer(0x38, 0))
        await self.query(0)
        self.ack()
        self.rx(identity(0xF01234))
        result = await watch
        assert result.state.dropped == 3 and result.reads == 3 and self.tx.empty()
        assert (
            not calendar(result.state).schedules and not calendar(result.state).signals
        )
        assert result.state.block(TEST_PATH, 0x38, 0) is None

    async def test_passive_cli_has_phases_and_no_synthetic_tx(self) -> None:
        for f in (
            identity(_THERMOSTAT),
            setup(2),
            block(0x10, b"\x7f" + _WEEK),
            block(0x13, b"\x7f" + _WEEK),
            role(0, master=True, states=0x09),
            block(0x37, b"\x01\x06"),
        ):
            self.rx(f)
        args = build_parser().parse_args(
            [
                "watch-native",
                "--passive",
                "--duration",
                "0.02",
                "--tcp",
                "localhost:3001",
            ],
        )
        output: list[object] = []
        assert await watch_all_from_args(self.stream, args, output.append) == 0
        records = [
            r["native_calendar"]
            for r in output
            if isinstance(r, dict) and "native_calendar" in r
        ]
        assert records[-1]["signals"][0]["current_event"] == "unoccupied"
        assert records[-1]["local_override"] == {"mode": "away", "raw": 6}
        sched = records[-1]["schedules"][0]
        assert sched["table_complete"] and sched["rows"][0]["tx_status"] is None
        assert sched["rows"][0]["body"] and self.tx.empty()
