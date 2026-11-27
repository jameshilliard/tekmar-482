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
_SUPPORT = "0401f04b54020005bf3e"
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

    async def expect(self, request: str) -> None:
        data = serialize(Packet(6, bytes.fromhex(request)))
        assert await self.reader.readexactly(len(data)) == data

    def send(self, message: str, *, packet_type: int = 6) -> None:
        self.writer.write(serialize(Packet(packet_type, bytes.fromhex(message))))

    async def exchange(self, request: str, reply: str) -> None:
        await self.expect(request)
        self.send(reply)

    async def setup(self, mask: str) -> None:
        await self.exchange(_CAP, _SUPPORT)  # Application identification.
        await self.exchange(_CAP, _SUPPORT)  # Normal peer capture features.
        await self.exchange(_TRACE, "040bf04b5400" + _COUNTS)
        await self.exchange("000bf04b54" + mask, "030bf04b54" + mask)
        await self.exchange(_CAP, _SUPPORT)  # Ordered active reads.

    def report(self, packet: NativeFrame) -> None:
        self.send(
            f"00{packet.port:02x}0100000001" + packet.body.hex(),
            packet_type=0xD0,
        )

    async def request(self, key: str) -> None:
        await self.exchange(_CAP, _SUPPORT)
        await self.expect("010ff04b54" + key)

    async def identify(self, key: str, product: int, path: NativePath) -> None:
        await self.request(key)
        self.send("040ff04b54" + key)
        packet = identity(product, path=path)
        self.report(packet)
        self.send(f"020ff04b54{path.port:02x}000001000000" + packet.body.hex())

    async def request_master(self, port: int) -> None:
        key = f"{port:02x}000000f703"
        await self.exchange(_CAP, _SUPPORT)
        await self.exchange("0110f04b54" + key, "0410f04b54" + key)

    def complete_master(self, packet: NativeFrame, *, traced: bool = True) -> None:
        if traced:
            self.report(packet)
        self.send(f"0210f04b54{packet.port:02x}0001000000" + packet.body.hex())
        self.send(f"0210f04b54{packet.port:02x}0100")

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
            await peer.exchange(_CAP, "0401f04b540200050100")
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
