import asyncio
import unittest
from dataclasses import replace
from unittest.mock import patch

from tekmar_482 import (
    CaptureGap,
    IoDirectory,
    IoDirectoryRead,
    IoEndpoint,
    NativeAddress,
    NativeBlockBatch,
    NativeBlockQuery,
    NativeFrame,
    NativeIdentity,
    NativeInventory,
    NativeMessage,
    NativePath,
    NativeSnapshot,
    NativeState,
    NativeStatusBatch,
    NativeStatusQuery,
    Packet,
    TxResult,
    TxStatus,
    discover_native,
    read_io_directory,
    read_native_snapshot,
    watch_native,
)
from tekmar_482.cli import build_parser
from tekmar_482.cli_native_watch import io_fields, watch_all_from_args
from tekmar_482.native_status import StatusCollector
from tekmar_482.raw import CaptureRecord, RawDispatcher
from tests.native_fixtures import TEST_PATH as PATH
from tests.native_fixtures import block, frame, identity

END = IoEndpoint(0x43, 0xA5)
OTHER = IoEndpoint(0x43, 0x85)
LOCAL = IoEndpoint(PATH.subnet, PATH.node)
NEIGHBOR = NativePath(PATH.port, PATH.subnet, PATH.node + 1)
LABEL = bytes.fromhex("410042caff00")
ACK = TxResult(PATH.port, TxStatus.ACK)


def value(
    ch: int,
    raw: int,
    *,
    kind: int = 1,
    endpoint: IoEndpoint = END,
    path: NativePath = PATH,
    mid: int = 0x1F2,
    stamp: int = 1,
) -> NativeFrame:
    data = bytes([endpoint.subnet, endpoint.node, ch, kind]) + raw.to_bytes(2, "little")
    return frame(mid, data, path, stamp=stamp)


def discover(
    ch: int,
    count: int,
    *,
    raw: int = 1,
    kind: int = 1,
    endpoint: IoEndpoint = END,
    path: NativePath = PATH,
) -> NativeFrame:
    data = bytes([endpoint.subnet, endpoint.node, ch, count]) + LABEL + bytes([kind])
    return frame(0x1F5, data + raw.to_bytes(2, "little") + b"\xe4", path)


def batch(*frames: NativeFrame, tx: TxResult | None = ACK) -> NativeStatusBatch:
    c = StatusCollector(NativeStatusQuery.io(END.subnet, END.node), PATH.address)
    for f in frames:
        c.observe(f)
    return c.batch(PATH.port, tx)


def channel_reply(channel: int, *, kind: int = 1) -> NativeStatusBatch:
    c = StatusCollector(
        NativeStatusQuery.io(END.subnet, END.node, channel),
        PATH.address,
    )
    assert c.observe(value(channel, 1, mid=0x1F3, kind=kind))
    return c.batch(PATH.port, ACK)


def locked_identity() -> NativeFrame:
    report = identity(105102)
    msg = replace(report.message, source=NativeAddress(PATH.subnet, PATH.node | 32))
    return replace(report, body=msg.to_bytes())


class IoDirectoryTests(unittest.TestCase):
    def test_supplemental_reply_keeps_metadata_unknown_and_one_source(self) -> None:
        primary = batch(discover(7, 2))
        answer = channel_reply(250)
        directory = IoDirectoryRead(primary, (answer,)).directory
        assert directory.complete and directory.advertised_count == 2
        _, recovered = directory.channels
        assert recovered.sample.active is True
        assert recovered.sample.count is None and recovered.sample.label is None
        assert recovered.frame.message_id == 0x1F3
        snap = NativeSnapshot((), (primary,), (IoDirectoryRead(primary, (answer,)),))
        assert snap.io_directories == ((PATH, directory),)
        for rejected in (
            replace(answer, port=PATH.port + 1),
            replace(answer, reports=(value(250, 1, mid=0x1F3, path=NEIGHBOR),)),
            replace(answer, reports=(value(250, 1, mid=0x1F3, endpoint=OTHER),)),
            replace(
                answer,
                reports=(value(250, 1, mid=0x1F3, stamp=4),),
                tx_result=None,
            ),
            replace(answer, reports=(value(250, 1),)),
            channel_reply(7),
        ):
            with self.subTest(rejected=rejected):
                assert not IoDirectoryRead(primary, (rejected,)).directory.complete

    def test_supplement_does_not_prune_or_replace_newer_channel_types(self) -> None:
        state = NativeState()
        original = batch(discover(7, 2), discover(8, 2))
        state.reconcile_io(original, before=None)
        before = state.io_directory(PATH, END)
        assert before and before.complete
        primary, supplemental = batch(discover(7, 2)), (channel_reply(250),)
        state.reconcile_io(primary, before=before, supplemental=(channel_reply(8),))
        updated = state.io_directory(PATH, END)
        assert (
            updated and updated.complete and updated.channels[-1].sample.label == LABEL
        )
        before = updated
        state.reconcile_io(primary, before=before, supplemental=supplemental)
        partial = state.io_directory(PATH, END)
        assert partial and not partial.complete
        assert [c.sample.channel for c in partial.channels] == [7, 8, 250]
        state.reconcile_io(batch(discover(7, 2), discover(250, 2)), before=partial)
        before = state.io_directory(PATH, END)
        assert before and before.complete
        state.observe(value(250, 55, kind=8, stamp=20))
        state.reconcile_io(primary, before=before, supplemental=supplemental)
        latest = state.io_directory(PATH, END)
        assert latest and not latest.complete
        assert latest.channels[-1].sample.value == 55
        assert latest.channels[-1].sample.label is None

    def test_wire_services_and_independent_payload_and_header_coordinates(self) -> None:
        answer = batch(
            discover(7, 2),
            value(2, 1),
            value(2, 1, mid=0x1F3),
            discover(2, 2, endpoint=OTHER),
            discover(2, 2, path=NEIGHBOR),
            discover(7, 2, raw=9),
            discover(250, 2, kind=0xFE, raw=0xCA35),
        )
        directory = IoDirectory.from_batch(answer)
        assert answer.answered and directory.complete and directory.endpoint == END
        a, b = directory.channels
        assert (a.sample.channel, a.sample.value, a.sample.label) == (7, 9, LABEL)
        assert a.sample.active is None and a.frame.decoded["extension_hex"] == "e4"
        assert (b.sample.kind, b.sample.type_id, b.sample.value) == (None, 0xFE, 0xCA35)
        state = NativeSnapshot((), (answer,)).observed_state()
        assert state.io_directories == ((PATH, directory),)
        for count, second in ((0, 0), (3, 2), (4, 4)):
            d = IoDirectory.from_batch(batch(discover(7, count), discover(8, second)))
            assert not d.complete and len(d.channels) == 2
        assert not IoDirectory.from_batch(batch(discover(1, 1), tx=None)).complete
        assert not IoDirectory.from_batch(batch()).channels

    def test_partial_refresh_never_prunes_and_newer_telemetry_wins(self) -> None:
        state = NativeState()
        for f in (discover(2, 3), discover(4, 3), discover(6, 3)):
            state.observe(f)
        first = state.io_directory(PATH, END)
        assert first and not first.complete
        partial = batch(discover(6, 2))
        state.reconcile_io(partial, before=first)
        prior = state.io_directory(PATH, END)
        assert prior and [c.sample.channel for c in prior.channels] == [2, 4, 6]
        # A fresh sample even with exactly the same value is evidence. Do not
        # delete it just because a completed directory didn't list it.
        state.observe(value(2, 1, stamp=77))
        state.observe(value(6, 0, stamp=78))
        complete = batch(discover(6, 1))
        state.reconcile_io(complete, before=prior)
        now = state.io_directory(PATH, END)
        assert now and not now.complete
        assert [(c.sample.channel, c.frame.timestamp_ms) for c in now.channels] == [
            (2, 77),
            (6, 78),
        ]
        assert now.channels[1].sample.active is False
        state.reconcile_io(complete, before=now)
        final = state.io_directory(PATH, END)
        assert final and final.complete and len(final.channels) == 1
        assert final.channels[0].sample.label == LABEL
        assert not state.observe(value(6, 1, stamp=79))
        assert state.observe(value(6, 1, kind=9)) == (PATH,)
        retyped = state.io_directory(PATH, END)
        assert (
            retyped
            and not retyped.complete
            and retyped.channels[0].sample.label is None
        )

    def test_removal_filters_terminal_names_and_endpoint_discovery(self) -> None:
        state = NativeState([PATH])
        inv = NativeInventory()
        for f in (
            identity(105102),
            value(2, 1),
            value(2, 3, endpoint=OTHER),
            value(4, 1, path=NEIGHBOR),
            frame(0x1F5, b"short"),
            frame(0x1F4, bytes([0x99, 0xF3, 1])),
        ):
            state.observe(f)
            inv.observe(f)
        assert state.paths == (PATH,) and len(state.io_for(PATH)) == 2
        assert inv.devices[0].io_endpoints == {END, OTHER}
        assert inv.devices[1].io_endpoints == {END}
        d = state.io_directory(PATH, END)
        assert d and "board_terminal" not in str(io_fields(d, "553"))
        local = IoEndpoint(PATH.subnet, PATH.node)
        state.observe(discover(1, 1, endpoint=local))
        d = state.io_directory(PATH, local)
        assert d and "'board_terminal': 'Acc'" in str(io_fields(d, "553"))
        moved = frame(0x3E7, bytes([PATH.subnet, PATH.node]), NEIGHBOR)
        moved_device = inv.observe(moved)
        assert moved_device and not moved_device.io_endpoints
        assert state.observe(moved) == (PATH,) and not state.io_directories
        for f in (identity(105102), value(2, 1), identity(105801)):
            state.observe(f)
            device = inv.observe(f)
        assert not state.io_directories and device and not device.io_endpoints
        state.observe(value(3, 0))
        assert state.observe(CaptureGap(4)) == (PATH,) and not state.io_directories

    def test_same_channel_numbers_on_two_ports_and_two_payload_nodes(self) -> None:
        state = NativeState()
        second = NativePath(1, PATH.subnet, PATH.node)
        for path, endpoint, raw in ((PATH, END, 3), (PATH, OTHER, 5), (second, END, 7)):
            state.observe(value(2, raw, kind=8, path=path, endpoint=endpoint))
        assert {
            (p, d.endpoint): d.channels[0].sample.value for p, d in state.io_directories
        } == {(PATH, END): 3, (PATH, OTHER): 5, (second, END): 7}

    def test_repeated_move_to_node_zero_keeps_new_route_evidence(self) -> None:
        master = NativePath(PATH.port, PATH.subnet, 0)
        move = frame(0x3E7, bytes([PATH.subnet, PATH.node]), master)
        inv = NativeInventory()
        inv.observe(value(1, 0))
        inv.observe(value(1, 0, path=master))
        inv.observe(move)
        assert not inv.devices
        inv.observe(value(1, 1, path=master, endpoint=OTHER))
        inv.observe(move)
        (only,) = inv.devices
        assert only.address == master.address and only.io_endpoints == {OTHER}


class IoExchangeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tx: asyncio.Queue[bytes] = asyncio.Queue()
        self.stream = RawDispatcher(self.tx.put, 128)
        wire: asyncio.Queue[Packet] = asyncio.Queue()
        await self.enterAsyncContext(self.stream.receiving(wire.get))

    async def send(self, port: int = PATH.port) -> NativeMessage:
        async with asyncio.timeout(2):
            data = await self.tx.get()
        assert data[:2] == bytes([1, port])
        return NativeMessage.from_bytes(data[2:])

    def rx(self, f: NativeFrame) -> None:
        self.stream.feed(
            Packet(
                1,
                bytes([0, f.port]) + f.timestamp_ms.to_bytes(4, "little") + f.body,
            ),
        )

    def ack(self, port: int = PATH.port) -> None:
        self.stream.feed(Packet(1, bytes([2, port, TxStatus.ACK])))

    async def _reply_with_553_configuration(self) -> None:
        for b in (0, 3, 4, 0x97):
            msg = await self.send()
            assert msg.message_id == 0x2D7
            assert msg.payload[:2] == b.to_bytes(2, "little")
            self.ack()
            self.rx(identity(105102) if not b else block(b, b"\0"))

    async def test_watch_repairs_partial_directory_without_replaying_old_value(
        self,
    ) -> None:
        local = IoEndpoint(PATH.subnet, PATH.node)
        task = asyncio.create_task(
            watch_native(
                self.stream,
                [PATH],
                duration=0.35,
                timeout=0.08,
                schedules=False,
                thermostat_targets=False,
                thermostat_sensors=False,
                plant_settings=False,
                setpoint_settings=False,
            ),
        )
        await self._reply_with_553_configuration()
        request = await self.send()
        assert request.message_id == 0x1F6 and request.payload == bytes(
            (local.subnet, local.node),
        )
        self.ack()
        for ch in range(5):
            self.rx(discover(ch, 6, endpoint=local))
        request = await self.send()
        assert request.message_id == 0x1F4 and request.payload[-1] == 5
        self.ack()
        self.rx(value(5, 0, endpoint=local, mid=0x1F3))
        self.rx(value(5, 1, endpoint=local, stamp=2))
        result = await task
        view = result.state.io_directory(PATH, local)
        assert view and view.complete and view.channels[-1].sample.active is True
        assert len(result.latest_statuses) == 2 and result.reads == 6

    async def test_initial_io_discovery_precedes_bulk_configuration(self) -> None:
        task = asyncio.create_task(
            watch_native(
                self.stream,
                [PATH],
                duration=0.25,
                timeout=0.08,
            ),
        )
        await self._reply_with_553_configuration()
        discovery = await self.send()
        assert (discovery.message_id, discovery.payload) == (
            0x1F6,
            bytes((LOCAL.subnet, LOCAL.node)),
        )
        self.ack()
        for ch in range(6):
            self.rx(discover(ch, 6, endpoint=LOCAL))

        configuration = await self.send()
        assert configuration.message_id == 0x2D7
        assert int.from_bytes(configuration.payload[:2], "little") not in (
            0,
            3,
            4,
            0x97,
        )
        result = await task
        directory = result.state.io_directory(PATH, LOCAL)
        assert directory and directory.complete

    async def test_watch_does_not_repair_a_moved_reporter(self) -> None:
        local = IoEndpoint(PATH.subnet, PATH.node)
        task = asyncio.create_task(
            watch_native(
                self.stream,
                [PATH],
                duration=0.35,
                timeout=0.08,
                schedules=False,
                thermostat_targets=False,
                thermostat_sensors=False,
                plant_settings=False,
                setpoint_settings=False,
            ),
        )
        await self._reply_with_553_configuration()
        assert (await self.send()).message_id == 0x1F6
        self.ack()
        for ch in range(5):
            self.rx(discover(ch, 6, endpoint=local))
        self.rx(frame(0x3E7, bytes([PATH.subnet, PATH.node]), NEIGHBOR))
        # A watch pinned to the old path may check its identity again, but
        # must not send the queued recovery to the former occupant's route.
        assert (await self.send()).message_id == 0x2D7
        self.ack()
        result = await task
        assert result.state.io_directory(PATH, local) is None
        assert not result.latest_statuses and self.tx.empty()

    async def test_automatic_watch_prefers_the_ports_own_subnet_over_a_mirror(
        self,
    ) -> None:
        direct = replace(PATH, port=2)
        task = asyncio.create_task(
            watch_native(
                self.stream,
                ports=(PATH.port, direct.port),
                duration=0.25,
                timeout=0.08,
                schedules=False,
                thermostat_targets=False,
                thermostat_sensors=False,
                plant_settings=False,
                setpoint_settings=False,
            ),
        )
        self.rx(frame(0x3FF, b"", NativePath(PATH.port, PATH.subnet + 8, 0)))
        for path in (PATH, direct):
            for b in (3, 4, 0x97):
                self.rx(block(b, b"\0", path))
            self.rx(identity(105102, path=path))
        # Both copies are already known when the actual local subnet appears.
        self.rx(frame(0x3FF, b"", NativePath(direct.port, direct.subnet, 0)))
        request = await self.send(direct.port)
        assert (request.message_id, request.payload) == (
            0x1F6,
            bytes([PATH.subnet, PATH.node]),
        )
        self.ack(direct.port)
        for channel in range(6):
            self.rx(discover(channel, 6, endpoint=LOCAL, path=direct))
        result = await task
        ((path, directory),) = result.state.io_directories
        assert path == direct and directory.complete
        assert result.reads == 1 and self.tx.empty()

    async def test_silent_local_io_probe_does_not_create_channels_or_repeat(
        self,
    ) -> None:
        task = asyncio.create_task(
            watch_native(
                self.stream,
                [PATH],
                duration=0.25,
                timeout=0.04,
                schedules=False,
                thermostat_targets=False,
                thermostat_sensors=False,
                plant_settings=False,
                setpoint_settings=False,
            ),
        )
        await self._reply_with_553_configuration()
        assert (await self.send()).message_id == 0x1F6
        self.ack()
        # A valid frame on the port from another source is not an answer.
        self.rx(discover(0, 1, path=NEIGHBOR, endpoint=LOCAL))
        await asyncio.sleep(0.08)
        self.rx(identity(105102))
        result = await task
        assert not result.state.io_directories
        assert result.reads == 5 and self.tx.empty()
        (only,) = result.latest_statuses
        assert not only.answered

    async def test_known_endpoint_overrides_the_model_probe_and_requires_its_peer(
        self,
    ) -> None:
        task = asyncio.create_task(
            watch_native(
                self.stream,
                [PATH],
                duration=0.25,
                timeout=0.08,
                schedules=False,
                thermostat_targets=False,
                thermostat_sensors=False,
                plant_settings=False,
                setpoint_settings=False,
                io_endpoints={PATH: (END, END)},
            ),
        )
        assert (await self.send()).message_id == 0x2D7
        self.ack()
        for b in (3, 4, 0x97):
            self.rx(block(b, b"\0"))
        self.rx(identity(105102))
        request = await self.send()
        assert (request.message_id, request.payload) == (0x1F6, b"\x43\xa5")
        self.ack()
        self.rx(discover(9, 1, path=NEIGHBOR))
        self.rx(discover(7, 1))
        self.rx(value(7, 0, stamp=2))
        result = await task
        ((path, directory),) = result.state.io_directories
        assert path == PATH and directory.complete and directory.endpoint == END
        (channel,) = directory.channels
        assert channel.sample.channel == 7 and channel.sample.active is False
        assert result.reads == 2 and self.tx.empty()

    async def test_address_move_drops_a_known_endpoint_before_it_can_be_read(
        self,
    ) -> None:
        task = asyncio.create_task(
            watch_native(
                self.stream,
                [PATH],
                duration=0.25,
                timeout=0.04,
                io_endpoints={PATH: (END,)},
            ),
        )
        assert (await self.send()).message_id == 0x2D7
        self.ack()
        self.rx(frame(0x3E7, bytes([PATH.subnet, PATH.node]), NEIGHBOR))
        assert (await self.send()).message_id == 0x2D7
        self.ack()
        self.rx(identity(0x123456))
        result = await task
        assert not result.latest_statuses
        assert not result.state.io_directories and self.tx.empty()

    async def test_watch_rejects_io_endpoints_without_active_selected_paths(
        self,
    ) -> None:
        for paths, source, enabled in (
            (None, NativeAddress(0, 0), True),
            ([NEIGHBOR], NativeAddress(0, 0), True),
            ([PATH], None, True),
            ([PATH], NativeAddress(0, 0), False),
        ):
            with (
                self.subTest(paths=paths, source=source, enabled=enabled),
                self.assertRaisesRegex(
                    ValueError,
                    "active watch of their device paths",
                ),
            ):
                await watch_native(
                    self.stream,
                    paths,
                    source=source,
                    io_discovery=enabled,
                    io_endpoints={PATH: (END,)},
                )
        assert self.tx.empty()

    async def test_cli_reads_node_zero_endpoint_and_lists_silent_endpoint(self) -> None:
        master = NativePath(PATH.port, 0x81, 0)
        args = build_parser().parse_args(
            [
                "watch-native",
                "--io-endpoint",
                str(master.port),
                "0x81",
                "0",
                "0x43",
                "0xa5",
                "--io-endpoint",
                str(master.port),
                "0x81",
                "0",
                "0x43",
                "0x85",
                "--duration",
                "0.25",
                "--timeout",
                "0.08",
                "--tcp",
                "localhost:1234",
            ],
        )
        output: list[object] = []
        task = asyncio.create_task(
            watch_all_from_args(self.stream, args, output.append),
        )
        first = await self.send()
        assert (first.message_id, first.payload) == (0x1F6, b"\x43\x85")
        self.ack()
        self.rx(discover(11, 1, path=master, endpoint=OTHER))
        second = await self.send()
        assert (second.message_id, second.payload) == (0x1F6, b"\x43\xa5")
        self.ack()
        assert await task == 1 and self.tx.empty()
        summary = output[-1]
        assert isinstance(summary, dict)
        assert summary["completed_reads"] == 2 and not summary["missing_identities"]
        assert summary["incomplete_io"] == [
            {
                "port": master.port,
                "subnet": master.subnet,
                "node": 0,
                "endpoint": {"subnet": END.subnet, "node": END.node},
            },
        ]

    async def test_read_directory_supplements_a_missing_known_sparse_channel(
        self,
    ) -> None:
        previous = IoDirectory.from_batch(batch(discover(7, 2), discover(250, 2)))
        task = asyncio.create_task(
            read_io_directory(
                self.stream,
                PATH.port,
                END,
                peer=PATH.address,
                before=previous,
                timeout=0.08,
            ),
        )
        msg = await self.send()
        assert (msg.message_id, msg.payload) == (0x1F6, b"\x43\xa5")
        self.ack()
        self.rx(discover(7, 2))
        msg = await self.send()
        assert (msg.message_id, msg.payload) == (0x1F4, b"\x43\xa5\xfa")
        self.ack()
        self.rx(value(250, 1, mid=0x1F3))
        read = await task
        assert read.directory.complete and not read.discovery.answered
        assert len(read.supplemental) == 1 and self.tx.empty()

    async def test_no_repair_when_count_or_model_does_not_establish_candidates(
        self,
    ) -> None:
        model = NativeIdentity(105102, 1222, 1, 1)
        local = IoEndpoint(PATH.subnet, PATH.node)
        for endpoint, ch, count in ((END, 3, 6), (local, 3, 5), (local, 7, 6)):
            with self.subTest(endpoint=endpoint, channel=ch, count=count):
                task = asyncio.create_task(
                    read_io_directory(
                        self.stream,
                        PATH.port,
                        endpoint,
                        peer=PATH.address,
                        identity=model,
                        timeout=0.03,
                    ),
                )
                assert (await self.send()).message_id == 0x1F6
                self.ack()
                self.rx(discover(ch, count, endpoint=endpoint))
                read = await task
                assert not read.directory.complete and not read.supplemental
                assert self.tx.empty()

    async def test_discovery_waits_for_capture_order_and_does_not_replay_old_values(
        self,
    ) -> None:
        paused, release = asyncio.Event(), asyncio.Event()

        class DelayedCapture(RawDispatcher):
            async def read(self) -> CaptureRecord:
                record = await super().read()
                if isinstance(record, NativeFrame) and record.message_id == 0x1F5:
                    paused.set()
                    await release.wait()
                return record

        self.stream = DelayedCapture(self.tx.put, 32)
        wire: asyncio.Queue[Packet] = asyncio.Queue()
        await self.enterAsyncContext(self.stream.receiving(wire.get))
        master = NativePath(PATH.port, PATH.subnet, 0)
        task = asyncio.create_task(watch_native(self.stream, [master], duration=0.16))
        self.rx(value(7, 0, path=master))
        assert (await self.send()).message_id == 0x1F6
        self.rx(value(7, 1, path=master))
        self.ack()
        self.rx(discover(7, 1, path=master))
        async with asyncio.timeout(1):
            await paused.wait()
        self.rx(value(7, 0, path=master))
        release.set()
        result = await task
        d = result.state.io_directory(master, END)
        assert d and d.complete and d.channels[0].sample.active is False
        (last,) = result.latest_statuses[0].reports
        assert last.timestamp_ms == d.channels[0].frame.timestamp_ms
        assert last.capture_index < d.channels[0].frame.capture_index
        assert result.reads == 1 and self.tx.empty()

    async def test_snapshot_uses_observed_endpoints_even_for_unknown_model(
        self,
    ) -> None:
        inv = NativeInventory()
        inv.observe(value(7, 8))
        inv.observe(value(7, 8, endpoint=OTHER))
        task = asyncio.create_task(read_native_snapshot(self.stream, iter(inv.devices)))
        assert (await self.send()).message_id == 0x2D7
        self.ack()
        self.rx(identity(0x123456))
        for endpoint in sorted([END, OTHER]):
            msg = await self.send()
            assert msg.message_id == 0x1F6 and msg.payload == bytes(
                [endpoint.subnet, endpoint.node],
            )
            self.ack()
            self.rx(discover(7, 1, endpoint=endpoint))
        snapshot = await task
        assert [d.endpoint for _, d in snapshot.io_directories] == sorted([END, OTHER])
        assert all(s.answered for s in snapshot.statuses) and self.tx.empty()
        # A timed-out, still-owned port must not start even a known directory.
        blocked = asyncio.create_task(
            read_native_snapshot(self.stream, inv.devices, timeout=0.02),
        )
        assert (await self.send()).message_id == 0x2D7
        assert not (await blocked).statuses and self.tx.empty()
        self.ack()

    async def test_snapshot_repairs_with_identity_from_matching_block_read(
        self,
    ) -> None:
        local = IoEndpoint(PATH.subnet, PATH.node)
        inv = NativeInventory()
        inv.observe(identity(105102))
        info = NativeBlockBatch(
            PATH.port,
            NativeBlockQuery(PATH.address, 0),
            ACK,
            (identity(105102),),
        )
        statuses: list[NativeStatusBatch] = []
        # Supply the identity pass; run both status exchanges on the wire.
        with patch(
            "tekmar_482.native_snapshot.read_native_devices",
            return_value=(info,),
        ):
            task = asyncio.create_task(
                read_native_snapshot(
                    self.stream,
                    inv.devices,
                    timeout=0.08,
                    on_status=statuses.append,
                ),
            )
            request = await self.send()
            assert (request.message_id, request.payload) == (
                0x1F6,
                bytes((local.subnet, local.node)),
            )
            self.ack()
            for ch in range(5):
                self.rx(discover(ch, 6, endpoint=local))
            request = await self.send()
            assert request.message_id == 0x1F4 and request.payload[-1] == 5
            self.ack()
            self.rx(value(5, 0, endpoint=local, kind=0, mid=0x1F3))
            result = await task
        (directory_read,) = result.io_reads
        assert result.statuses == (directory_read.discovery,)
        assert statuses == [directory_read.discovery, *directory_read.supplemental]
        assert result.io_directories == ((PATH, directory_read.directory),)
        assert directory_read.directory.complete

    async def test_snapshot_does_not_guess_from_a_seed_locked_node_or_other_model(
        self,
    ) -> None:
        inv = NativeInventory()
        inv.observe(identity(105102))
        for report in (None, locked_identity(), identity(105801)):
            with self.subTest(report=report):
                info = NativeBlockBatch(
                    PATH.port,
                    NativeBlockQuery(PATH.address, 0),
                    ACK,
                    (report,) if report is not None else (),
                )
                with patch(
                    "tekmar_482.native_snapshot.read_native_devices",
                    return_value=(info,),
                ):
                    snapshot = await read_native_snapshot(self.stream, inv.devices)
                assert not snapshot.statuses and self.tx.empty()

    async def test_active_watch_queries_once_then_rediscovery_only_on_new_evidence(
        self,
    ) -> None:
        ready = asyncio.Event()

        def changed(path: NativePath, state: NativeState) -> None:
            view = state.io_directory(path, END)
            if view and view.complete:
                ready.set()

        task = asyncio.create_task(
            watch_native(
                self.stream,
                [PATH],
                duration=0.4,
                timeout=0.03,
                on_change=changed,
            ),
        )
        assert (await self.send()).message_id == 0x2D7
        self.ack()
        self.rx(identity(0x123456))
        self.rx(value(7, 0))
        assert (await self.send()).payload == b"\x43\xa5"
        self.ack()
        self.rx(discover(7, 1))
        async with asyncio.timeout(1):
            await ready.wait()
        self.rx(value(7, 0))
        self.rx(value(7, 1))
        self.rx(value(9, 42, kind=8))
        assert (await self.send()).message_id == 0x1F6
        self.ack()
        self.rx(discover(9, 2, kind=8))
        answer = await task
        directory = answer.state.io_directory(PATH, END)
        assert directory and not directory.complete and len(directory.channels) == 2
        assert answer.reads == 3 and self.tx.empty()
        (only,) = answer.latest_statuses
        assert not only.complete and only.covered == {9}

    async def test_node_zero_io_uses_the_payload_address_not_a_block_bitmap(
        self,
    ) -> None:
        master = NativePath(PATH.port, 0x81, 0)
        inv = NativeInventory()
        inv.observe(value(15, 5, path=master))
        (device,) = inv.devices
        assert device.address.node == 0 and device.io_endpoints == {END}
        task = asyncio.create_task(read_native_snapshot(self.stream, inv.devices))
        assert (await self.send()).message_id == 0x1F6
        self.ack()
        self.rx(discover(15, 1, path=master))
        snapshot = await task
        assert not snapshot.blocks and snapshot.statuses[0].answered
        assert snapshot.io_directories[0][0] == master
        watch = asyncio.create_task(watch_native(self.stream, [master], duration=0.12))
        assert (await self.send()).message_id == 0x1F6
        self.ack()
        self.rx(discover(15, 1, path=master))
        result = await watch
        assert result.reads == 1 and result.latest_statuses[0].answered
        assert not result.latest_blocks and self.tx.empty()
        args = build_parser().parse_args(
            [
                "read-device",
                "3",
                "0x81",
                "0",
                "--io-endpoint",
                "0x43",
                "0xa5",
                "--tcp",
                "localhost:1234",
            ],
        )
        assert args.io_endpoint == [[END.subnet, END.node]]

    async def test_capture_gap_forgets_routes_whose_move_could_have_been_missed(
        self,
    ) -> None:
        heard = asyncio.Event()
        task = asyncio.create_task(
            discover_native(
                self.stream,
                source=None,
                duration=0.12,
                on_record=lambda _r: heard.set(),
            ),
        )
        self.rx(value(7, 1))
        async with asyncio.timeout(1):
            await heard.wait()
        for _ in range(135):
            self.rx(frame(0x3F7, b"", NEIGHBOR))
        scan = await task
        assert scan.host_records_dropped and len(scan.devices) == 1
        assert (
            scan.devices[0].address == NEIGHBOR.address
            and not scan.devices[0].io_endpoints
        )
        assert self.tx.empty()

    async def test_passive_cli_reports_io_and_never_sends(self) -> None:
        args = build_parser().parse_args(
            [
                "watch-native",
                "--tcp",
                "localhost:1234",
                "--passive",
                "--duration",
                "0.04",
            ],
        )
        output: list[object] = []
        self.rx(discover(9, 1, raw=37, kind=6))
        assert await watch_all_from_args(self.stream, args, output.append) == 0
        assert "native_io" in str(output) and "'percent': 37" in str(output)
        assert "'complete': False" in str(output) and self.tx.empty()
