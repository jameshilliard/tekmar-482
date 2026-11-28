from __future__ import annotations

import asyncio
import unittest
from typing import TYPE_CHECKING

from tekmar_482 import (
    ConnectionClosedError,
    GatewayEndpoint,
    NativeMonitor,
    NativePath,
    Packet,
    ProtocolError,
)
from tekmar_482.tpck import serialize

from .native_fixtures import TEST_PATH, frame, identity

if TYPE_CHECKING:
    from tekmar_482 import NativeFrame

_CAP = "0101f04b54"
_SUPPORT = "0401f04b54030005bf2e"  # Only the targeted reads used by these scripts.
_SCAN_SUPPORT = "0401f04b54030005bf3e"
_TRACE = "010bf04b54"
_COUNTS = "00" * 40
_ID = "03354000000000000000"
_LOOP = bytes.fromhex("bc07 00c0 9828 2211 fe")


class Gateway:
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.support = _SUPPORT

    async def expect(self, request: str) -> None:
        data = serialize(Packet(6, bytes.fromhex(request)))
        assert await self.reader.readexactly(len(data)) == data

    def send(self, message: str, *, packet_type: int = 6) -> None:
        self.writer.write(serialize(Packet(packet_type, bytes.fromhex(message))))

    async def exchange(self, request: str, reply: str) -> None:
        await self.expect(request)
        self.send(reply)

    async def setup(self, mask: str, *, active: bool = True) -> None:
        await self.exchange(_CAP, self.support)  # Application identification.
        await self.exchange(_CAP, self.support)  # Normal peer capture features.
        await self.exchange(_TRACE, "040bf04b5400" + _COUNTS)
        await self.exchange("000bf04b54" + mask, "030bf04b54" + mask)
        if active:
            await self.exchange(_CAP, self.support)  # Ordered active reads.

    def report(self, packet: NativeFrame) -> None:
        self.send(
            f"00{packet.port:02x}0100000001" + packet.body.hex(),
            packet_type=0xD0,
        )

    async def request(self, key: str) -> None:
        await self.exchange(_CAP, self.support)
        await self.expect("010ff04b54" + key)

    async def identify(self, key: str, product: int, path: NativePath) -> None:
        await self.request(key)
        self.send("040ff04b54" + key)
        packet = identity(product, path=path)
        self.report(packet)
        self.send(f"020ff04b54{path.port:02x}000001000000" + packet.body.hex())

    async def request_master(self, port: int) -> None:
        key = f"{port:02x}000000f703"
        await self.exchange(_CAP, self.support)
        await self.exchange("0110f04b54" + key, "0410f04b54" + key)

    async def discover(self, port: int, subnet: int) -> None:
        key = f"{port:02x}{subnet:02x}"
        await self.exchange(_CAP, self.support)
        await self.exchange("0113f04b54" + key, "0413f04b54" + key)
        self.send(f"0213f04b54{port:02x}00")

    def complete_master(self, packet: NativeFrame, *, traced: bool = True) -> None:
        if traced:
            self.report(packet)
        self.send(f"0210f04b54{packet.port:02x}0001000000" + packet.body.hex())
        self.send(f"0210f04b54{packet.port:02x}0100")

    def expire_master(self, port: int, *, tx_outcome: str = "00") -> None:
        self.send(f"0210f04b54{port:02x}02{tx_outcome}")

    async def start(self, product: int) -> None:
        await self.setup("08")
        self.report(frame(0x3FF, b""))
        await self.identify(_ID, product, TEST_PATH)

    async def restore(self) -> None:
        await self.exchange("000bf04b5400", "030bf04b5400")
        await self.exchange(_TRACE, "040bf04b5400" + _COUNTS)

    async def close(self) -> None:
        self.writer.close()
        await self.writer.wait_closed()


async def wait_for_product(
    monitor: NativeMonitor,
    product: int,
    path: NativePath = TEST_PATH,
) -> None:
    revision = monitor.revision
    while (reported := monitor.state.identity(path)) is None or (
        reported.product_number != product
    ):
        revision = await monitor.wait_for_change(revision)


async def wait_for_master(monitor: NativeMonitor, port: int) -> None:
    revision = monitor.revision
    while not monitor.state.has_master_announcement(port):
        revision = await monitor.wait_for_change(revision)


async def wait_for_port_check(monitor: NativeMonitor, port: int) -> None:
    revision = monitor.revision
    while not monitor.state.port_subnets_checked(port):
        revision = await monitor.wait_for_change(revision)


class NativeMonitorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.accepted: asyncio.Queue[Gateway] = asyncio.Queue()

        def accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            gateway = Gateway(reader, writer)
            self.addAsyncCleanup(gateway.close)
            self.accepted.put_nowait(gateway)

        server = await self.enterAsyncContext(
            await asyncio.start_server(accept, "127.0.0.1", 0),
        )
        self.endpoint = GatewayEndpoint.tcp(
            "127.0.0.1",
            server.sockets[0].getsockname()[1],
        )

    async def test_first_peer_checks_subnets_without_blocking_other_ports(self) -> None:
        quiet = TEST_PATH
        known = NativePath(2, 0x52, 5)
        monitor = NativeMonitor(self.endpoint, ports=[quiet.port, known.port])
        async with asyncio.TaskGroup() as tasks, asyncio.timeout(2):
            running = tasks.create_task(monitor.run())
            gateway = await self.accepted.get()
            await gateway.setup("0c")
            gateway.report(identity(200003, path=quiet))
            await gateway.request_master(quiet.port)
            await wait_for_product(monitor, 200003, quiet)
            assert not monitor.state.port_subnets_checked(quiet.port)

            gateway.report(frame(0x3FF, b"", NativePath(known.port, known.subnet, 0)))
            gateway.report(identity(200004, path=known))
            await wait_for_product(monitor, 200004, known)
            assert monitor.state.port_subnets_checked(known.port)
            assert not monitor.state.port_subnets_checked(quiet.port)

            gateway.expire_master(quiet.port)
            await wait_for_port_check(monitor, quiet.port)
            assert not monitor.state.master_subnets(quiet.port)
            gateway.report(identity(200003, path=quiet))
            with self.assertRaises(TimeoutError):
                async with asyncio.timeout(0.04):
                    await gateway.reader.read(1)
            running.cancel()
            await gateway.restore()

        assert not monitor.state.port_subnets_checked(quiet.port)

    async def test_unsent_query_does_not_check_port(self) -> None:
        path = TEST_PATH
        monitor = NativeMonitor(self.endpoint, ports=[path.port])
        async with asyncio.TaskGroup() as tasks, asyncio.timeout(2):
            running = tasks.create_task(monitor.run())
            gateway = await self.accepted.get()
            await gateway.setup("08")
            gateway.report(identity(200003, path=path))
            await gateway.request_master(path.port)
            gateway.expire_master(path.port, tx_outcome="ff")
            with self.assertRaises(TimeoutError):
                async with asyncio.timeout(0.04):
                    await gateway.reader.read(1)
            assert not monitor.state.port_subnets_checked(path.port)

            gateway.report(frame(0x3FF, b"", NativePath(path.port, path.subnet, 0)))
            await wait_for_port_check(monitor, path.port)
            assert monitor.state.master_subnets(path.port) == (path.subnet,)
            running.cancel()
            await gateway.restore()

    async def test_passive_monitor_does_not_query_an_unclaimed_port(self) -> None:
        path = TEST_PATH
        monitor = NativeMonitor(self.endpoint, ports=[path.port], passive=True)
        async with asyncio.TaskGroup() as tasks, asyncio.timeout(2):
            running = tasks.create_task(monitor.run())
            gateway = await self.accepted.get()
            await gateway.setup("08", active=False)
            gateway.report(identity(200003, path=path))
            await wait_for_product(monitor, 200003, path)
            assert not monitor.state.port_subnets_checked(path.port)
            with self.assertRaises(TimeoutError):
                async with asyncio.timeout(0.04):
                    await gateway.reader.read(1)
            running.cancel()
            await gateway.restore()

    async def test_gap_supersedes_inflight_port_check_and_rearms_only_that_port(
        self,
    ) -> None:
        path = TEST_PATH
        other = NativePath(2, 0x55, 0)
        monitor = NativeMonitor(self.endpoint, ports=[path.port, other.port])
        async with asyncio.TaskGroup() as tasks, asyncio.timeout(2):
            running = tasks.create_task(monitor.run())
            gateway = await self.accepted.get()
            await gateway.setup("0c")
            gateway.report(frame(0x3FF, b"", other))
            await wait_for_port_check(monitor, other.port)
            gateway.report(identity(200003, path=path))
            await gateway.request_master(path.port)
            gateway.send("05080100000000000000", packet_type=0xD0)
            gateway.report(identity(200004, path=path))
            gateway.expire_master(path.port)
            await gateway.request_master(path.port)
            await wait_for_product(monitor, 200004, path)
            assert not monitor.state.port_subnets_checked(path.port)
            assert monitor.state.port_subnets_checked(other.port)
            gateway.expire_master(path.port, tx_outcome="01")  # Completed without ACK.
            await wait_for_port_check(monitor, path.port)
            running.cancel()
            await gateway.restore()

    async def test_subnet_scan_identifies_quiet_nodes_and_rearms_only_after_loss(
        self,
    ) -> None:
        master = NativePath(3, 0x53, 0)
        quiet = NativePath(master.port, master.subnet, 5)
        claim = frame(0x3FF, b"", master)
        monitor = NativeMonitor(self.endpoint, ports=[2, 3])
        async with asyncio.TaskGroup() as tasks, asyncio.timeout(2):
            running = tasks.create_task(monitor.run())
            gateway = await self.accepted.get()
            gateway.support = _SCAN_SUPPORT
            await gateway.setup("0c")
            gateway.report(claim)
            await gateway.discover(master.port, master.subnet)
            assert monitor.state.identity(quiet) is None
            gateway.report(identity(200003, path=quiet))
            await wait_for_product(monitor, 200003, quiet)
            gateway.send("05040100000000000000", packet_type=0xD0)  # Only port 2.
            gateway.report(claim)
            with self.assertRaises(TimeoutError):
                async with asyncio.timeout(0.04):
                    await gateway.reader.read(1)

            gateway.send("05080100000000000000", packet_type=0xD0)
            gateway.report(claim)
            await gateway.discover(master.port, master.subnet)
            assert monitor.state.identity(quiet) is None
            running.cancel()
            await gateway.restore()

    async def test_competing_subnet_headers_do_not_trigger_a_queued_scan(self) -> None:
        master = NativePath(3, 0x53, 0)
        other = NativePath(3, 0x62, 0)
        monitor = NativeMonitor(self.endpoint, ports=[3])
        async with asyncio.TaskGroup() as tasks, asyncio.timeout(2):
            running = tasks.create_task(monitor.run())
            gateway = await self.accepted.get()
            gateway.support = _SCAN_SUPPORT
            await gateway.setup("08")
            gateway.report(frame(0x3CF, _LOOP, master))
            await gateway.request_master(3)
            claim = frame(0x3FF, bytes([master.subnet]), master)
            gateway.report(claim)
            gateway.report(frame(0x3FF, b"", other))
            gateway.complete_master(claim, traced=False)
            with self.assertRaises(TimeoutError):
                async with asyncio.timeout(0.04):
                    await gateway.reader.read(1)
            assert monitor.state.master_subnets(3) == (0x53, 0x62)
            running.cancel()
            await gateway.restore()

    async def test_reconnect_clears_old_observations_and_reuses_new_connection(
        self,
    ) -> None:
        monitor = NativeMonitor(self.endpoint, ports=[3], reconnect_interval=0.05)
        async with asyncio.TaskGroup() as tasks, asyncio.timeout(2):
            running = tasks.create_task(monitor.run())
            first = await self.accepted.get()
            await first.start(200003)
            await wait_for_product(monitor, 200003)
            previous = monitor.state
            manager = monitor.management
            assert manager is not None
            self.assertIsNone(monitor.last_error)
            with self.assertRaisesRegex(RuntimeError, "already running"):
                await monitor.run()
            operation = tasks.create_task(manager.capabilities())
            await first.exchange(_CAP, _SUPPORT)
            assert await operation is not None and self.accepted.empty()

            revision = monitor.revision
            await first.close()
            await monitor.wait_for_change(revision)
            self.assertIsNone(monitor.management)
            self.assertIsInstance(monitor.last_error, ConnectionClosedError)
            self.assertFalse(monitor.state.paths)
            self.assertFalse(previous.paths)

            second = await self.accepted.get()
            await second.start(200004)
            await wait_for_product(monitor, 200004)
            self.assertIsNotNone(monitor.management)
            self.assertIsNot(monitor.management, manager)
            self.assertIsNone(monitor.last_error)
            self.assertFalse(previous.paths)
            with self.assertRaises(ConnectionClosedError):
                await manager.capabilities()
            running.cancel()
            await second.restore()
            with self.assertRaises(asyncio.CancelledError):
                await running
            assert await second.reader.read() == b""
            assert not monitor.state.paths and monitor.management is None

    async def test_quiet_capture_checks_the_gateway_without_polling_peers(self) -> None:
        monitor = NativeMonitor(
            self.endpoint,
            ports=[TEST_PATH.port],
            passive=True,
            idle_check_interval=0.05,
        )
        async with asyncio.TaskGroup() as tasks, asyncio.timeout(2):
            running = tasks.create_task(monitor.run())
            gateway = await self.accepted.get()
            await gateway.setup("08", active=False)
            gateway.report(identity(200003))
            await wait_for_product(monitor, 200003)
            previous = monitor.state

            for _ in range(2):
                await gateway.exchange(_CAP, _SUPPORT)
                await gateway.exchange(_TRACE, "040bf04b5408" + _COUNTS)

            assert monitor.state is previous and monitor.state.paths
            assert self.accepted.empty()
            running.cancel()
            await gateway.restore()

    async def test_gateway_restart_with_open_stream_reopens_capture(self) -> None:
        monitor = NativeMonitor(
            self.endpoint,
            ports=[TEST_PATH.port],
            passive=True,
            idle_check_interval=0.05,
            reconnect_interval=0.05,
        )
        async with asyncio.TaskGroup() as tasks, asyncio.timeout(2):
            running = tasks.create_task(monitor.run())
            first = await self.accepted.get()
            await first.setup("08", active=False)
            first.report(identity(200003))
            await wait_for_product(monitor, 200003)
            previous = monitor.state
            manager = monitor.management
            assert manager is not None

            # The gateway restarted but neither a UART nor a TCP bridge needs
            # to close its stream. The new firmware instance has no trace mask.
            revision = monitor.revision
            await first.exchange(_CAP, _SUPPORT)
            await first.exchange(_TRACE, "040bf04b5400" + _COUNTS)
            # The restarted gateway does not own the previous trace mask.
            assert await first.reader.read(1) == b""
            await monitor.wait_for_change(revision)
            assert monitor.management is None and not monitor.state.paths
            assert not previous.paths
            self.assertIsInstance(monitor.last_error, ConnectionClosedError)

            second = await self.accepted.get()
            await second.setup("08", active=False)
            second.report(identity(200004))
            await wait_for_product(monitor, 200004)
            assert monitor.management is not manager and monitor.last_error is None
            with self.assertRaises(ConnectionClosedError):
                await manager.capabilities()
            running.cancel()
            await second.restore()

    async def test_unanswered_idle_check_reopens_an_open_stream(self) -> None:
        monitor = NativeMonitor(
            self.endpoint,
            ports=[TEST_PATH.port],
            passive=True,
            idle_check_interval=0.05,
            reconnect_interval=0.05,
            timeout=0.15,
        )
        async with asyncio.TaskGroup() as tasks, asyncio.timeout(2):
            running = tasks.create_task(monitor.run())
            first = await self.accepted.get()
            await first.setup("08", active=False)
            first.report(identity(200003))
            await wait_for_product(monitor, 200003)

            revision = monitor.revision
            await first.expect(_CAP)
            # No reply and no EOF, as with a powered-off gateway behind serial.
            # No restore request or extra request timeout on the dead link.
            assert await first.reader.read(1) == b""
            await monitor.wait_for_change(revision)
            assert monitor.management is None and not monitor.state.paths
            self.assertIsInstance(monitor.last_error, TimeoutError)

            second = await self.accepted.get()
            await second.setup("08", active=False)
            second.report(identity(200004))
            await wait_for_product(monitor, 200004)
            assert monitor.last_error is None
            running.cancel()
            await second.restore()

    async def test_independent_plant_probes_require_trace_and_recover_after_loss(
        self,
    ) -> None:
        boiler = NativePath(3, 0x53, 0)
        mix = NativePath(2, 0x62, 0)
        slots = bytes([boiler.subnet, mix.subnet])
        monitor = NativeMonitor(self.endpoint, ports=[2, 3])
        async with asyncio.TaskGroup() as tasks, asyncio.timeout(2):
            running = tasks.create_task(monitor.run())
            gateway = await self.accepted.get()
            await gateway.setup("0c")
            gateway.report(frame(0x3CF, _LOOP, boiler))
            await gateway.request_master(3)
            gateway.report(frame(0x3CF, _LOOP, mix))
            await gateway.request_master(2)
            gateway.complete_master(frame(0x3FF, slots, mix))
            await wait_for_master(monitor, 2)
            # A report in an operation result is not a trace observation.
            gateway.complete_master(frame(0x3FF, slots, boiler), traced=False)
            gateway.report(frame(0x3CE, _LOOP, boiler))
            with self.assertRaises(TimeoutError):
                async with asyncio.timeout(0.04):
                    await gateway.reader.read(1)
            assert not monitor.state.has_master_announcement(3)
            assert not monitor.state.port_subnets_checked(3)

            gateway.send("05080100000000000000", packet_type=0xD0)
            gateway.report(frame(0x3CE, _LOOP, boiler))
            await gateway.request_master(3)
            gateway.complete_master(frame(0x3FF, slots, boiler))
            await wait_for_master(monitor, 3)
            (system,) = monitor.state.water_systems(ports=(2, 3))
            assert [s.source.port for s in system.loops if s.source] == [3, 2]
            assert monitor.state.dropped == 1
            running.cancel()
            await gateway.restore()

    async def test_plant_probe_restarts_from_new_evidence_after_reconnect(self) -> None:
        path = NativePath(3, 0x53, 0)
        monitor = NativeMonitor(self.endpoint, ports=[3], reconnect_interval=0.01)
        loop = frame(0x3CF, _LOOP, path)
        claim = frame(0x3FF, bytes([path.subnet]), path)
        async with asyncio.TaskGroup() as tasks, asyncio.timeout(2):
            running = tasks.create_task(monitor.run())
            for first in (True, False):
                gateway = await self.accepted.get()
                await gateway.setup("08")
                assert not monitor.state.has_master_announcement(3)
                gateway.report(loop)
                await gateway.request_master(3)
                gateway.complete_master(claim)
                await wait_for_master(monitor, 3)
                if first:
                    await gateway.close()
                else:
                    running.cancel()
                    await gateway.restore()

    async def test_rejected_capture_features_are_not_retried(self) -> None:
        monitor = NativeMonitor(self.endpoint, reconnect_interval=0.01)
        async with asyncio.timeout(1), asyncio.TaskGroup() as tasks:

            async def fail() -> None:
                with self.assertRaisesRegex(
                    ProtocolError,
                    "requires capable custom firmware",
                ):
                    await monitor.run()

            tasks.create_task(fail())
            peer = await self.accepted.get()
            await peer.exchange(_CAP, _SUPPORT)
            await peer.exchange(_CAP, "0401f04b540300050100")
            assert await peer.reader.read() == b""
        assert isinstance(monitor.last_error, ProtocolError)
        assert monitor.management is None and self.accepted.empty()

    async def test_busy_port_does_not_reset_the_monitor_or_wait_for_unrelated_work(
        self,
    ) -> None:
        other = NativePath(2, TEST_PATH.subnet, TEST_PATH.node)
        monitor = NativeMonitor(self.endpoint, ports=[2, 3], reconnect_interval=0.01)
        async with asyncio.TaskGroup() as tasks, asyncio.timeout(2):
            running = tasks.create_task(monitor.run())
            gateway = await self.accepted.get()
            await gateway.setup("0c")
            gateway.report(frame(0x3FF, b""))
            await gateway.request(_ID)
            gateway.send("0213f04b54 0300")  # Earlier completion on this port.
            gateway.send("0406f04b54 0ff04b54 01")
            gateway.send("020ff04b54 03040001000000" + identity().body.hex())
            gateway.send("020ff04b54 030000")  # Truncated final report.
            gateway.send("0212f04b54 0200")  # Other port, not a release.
            gateway.report(frame(0x3FF, b"", other))
            await gateway.identify("02" + _ID[2:], 200004, other)
            await wait_for_product(monitor, 200004, other)
            previous = monitor.state
            manager = monitor.management
            assert manager is not None and previous.identity(TEST_PATH) is None
            gateway.send("020ff04b54 030100")  # Actual terminal, after Busy.
            await gateway.identify(_ID, 200003, TEST_PATH)
            await wait_for_product(monitor, 200003)
            assert monitor.state is previous and monitor.management is manager
            assert monitor.last_error is None and self.accepted.empty()
            running.cancel()
            await gateway.restore()

    async def test_busy_and_terminal_in_one_burst_do_not_retry_a_moved_address(
        self,
    ) -> None:
        moved = NativePath(3, TEST_PATH.subnet, TEST_PATH.node + 1)
        monitor = NativeMonitor(self.endpoint, ports=[3])
        async with asyncio.TaskGroup() as tasks, asyncio.timeout(2):
            running = tasks.create_task(monitor.run())
            gateway = await self.accepted.get()
            await gateway.setup("08")
            gateway.report(frame(0x3FF, b""))
            await gateway.request(_ID)
            gateway.send("0406f04b54 0ff04b54 01")
            gateway.report(frame(0x3E7, bytes.fromhex("3507"), moved))
            gateway.send("0210f04b54 0302ff")  # A different method owned the slot.
            await gateway.request_master(moved.port)
            gateway.expire_master(moved.port)
            await gateway.identify("03358000000000000000", 200003, moved)
            await wait_for_product(monitor, 200003, moved)
            assert monitor.state.paths == (moved,) and self.accepted.empty()
            running.cancel()
            await gateway.restore()

    async def test_admitted_read_timeout_keeps_capture_alive_and_is_not_replayed(
        self,
    ) -> None:
        other = NativePath(2, TEST_PATH.subnet, TEST_PATH.node)
        next_peer = NativePath(3, TEST_PATH.subnet, TEST_PATH.node + 1)
        monitor = NativeMonitor(self.endpoint, ports=[2, 3], timeout=0.15)
        async with asyncio.TaskGroup() as tasks, asyncio.timeout(2):
            running = tasks.create_task(monitor.run())
            gateway = await self.accepted.get()
            await gateway.setup("0c")
            gateway.report(frame(0x3FF, b""))
            await gateway.request(_ID)
            gateway.send("040ff04b54" + _ID)  # Already admitted, unlike Busy.
            with self.assertRaises(TimeoutError):
                async with asyncio.timeout(0.2):
                    await gateway.reader.read(1)
            gateway.report(frame(0x3FF, b"", next_peer))
            gateway.send("0213f04b54 0300")  # Not the admitted method's terminal.
            gateway.report(frame(0x3FF, b"", other))
            await gateway.identify("02" + _ID[2:], 200004, other)
            await wait_for_product(monitor, 200004, other)
            gateway.send("020ff04b54 030100")
            await gateway.identify("03358000000000000000", 200003, next_peer)
            await wait_for_product(monitor, 200003, next_peer)
            assert monitor.state.identity(TEST_PATH) is None
            assert monitor.last_error is None and self.accepted.empty()
            running.cancel()
            await gateway.restore()

    async def test_silent_gateway_startup_respects_timeout_and_can_be_cancelled(
        self,
    ) -> None:
        monitor = NativeMonitor(self.endpoint, timeout=0.04)
        async with asyncio.timeout(1), asyncio.TaskGroup() as tasks:
            running = tasks.create_task(monitor.run())
            peer = await self.accepted.get()
            await peer.expect(_CAP)
            await monitor.wait_for_change(0)
            assert isinstance(monitor.last_error, TimeoutError)
            assert not monitor.state.paths and monitor.management is None
            assert await peer.reader.read() == b""
            running.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await running
            assert self.accepted.empty()
