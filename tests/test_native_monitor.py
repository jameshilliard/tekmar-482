import asyncio
import unittest

from tekmar_482 import (
    ConnectionClosedError,
    GatewayEndpoint,
    NativeMonitor,
    Packet,
    ProtocolError,
)
from tekmar_482.tpck import serialize

from .native_fixtures import TEST_PATH, frame, identity

_CAP = "0101f04b54"
_SUPPORT = "0401f04b54020005bf3e"
_TRACE = "010bf04b54"
_COUNTS = "00" * 40
_ID = "03354000000000000000"


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

    async def start(self, product: int) -> None:
        await self.exchange(_CAP, _SUPPORT)  # Application identification.
        await self.exchange(_CAP, _SUPPORT)  # Normal peer capture features.
        await self.exchange(_TRACE, "040bf04b5400" + _COUNTS)
        await self.exchange("000bf04b5408", "030bf04b5408")
        await self.exchange(_CAP, _SUPPORT)  # Ordered active reads.
        self.send(
            "00030100000001" + frame(0x3FF, b"").body.hex(),
            packet_type=0xD0,
        )
        await self.exchange(_CAP, _SUPPORT)
        await self.exchange("010ff04b54" + _ID, "040ff04b54" + _ID)
        body = identity(product).body.hex()
        self.send("00030200000001" + body, packet_type=0xD0)
        self.send("020ff04b5403000002000000" + body)

    async def restore(self) -> None:
        await self.exchange("000bf04b5400", "030bf04b5400")
        await self.exchange(_TRACE, "040bf04b5400" + _COUNTS)

    async def close(self) -> None:
        self.writer.close()
        await self.writer.wait_closed()


async def wait_for_product(monitor: NativeMonitor, product: int) -> None:
    revision = monitor.revision
    while (reported := monitor.state.identity(TEST_PATH)) is None or (
        reported.product_number != product
    ):
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
