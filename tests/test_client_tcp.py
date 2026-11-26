from __future__ import annotations

import asyncio
import errno
import unittest
from unittest.mock import Mock, patch

from tekmar_482 import ConnectionClosedError, NullTransport, Tekmar482Client
from tekmar_482.tpck import TpckParser, serialize
from tekmar_482.transport import _TpckProtocol
from tekmar_482.trpc import TrpcMethod, TrpcPacket, TrpcService


class ClientTcpTest(unittest.IsolatedAsyncioTestCase):
    async def test_cancelled_raw_write_does_not_stop_coalesced_writer(self) -> None:
        protocol = _TpckProtocol(closed_message="TCP transport closed")
        wire = Mock(spec=asyncio.Transport)
        wire.is_closing.return_value = False
        writes: asyncio.Queue[bytes] = asyncio.Queue()
        wire.write.side_effect = writes.put_nowait
        protocol.connection_made(wire)
        protocol.pause_writing()
        transport = NullTransport()
        async with Tekmar482Client(transport) as client, asyncio.timeout(2):
            with patch.object(transport, "write_packet", protocol.write_packet):
                packet = TrpcPacket.create(
                    method=TrpcMethod.PROTOCOL_VERSION,
                ).to_packet()
                raw = asyncio.create_task(client.write_packet(packet))
                assert await writes.get() == serialize(packet)
                first = asyncio.create_task(
                    client.set_latest_heat_setpoint_celsius(1201, 21),
                )
                first_frame = await writes.get()
                raw.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await raw
                protocol.resume_writing()
                first_result = await first
                assert first_result is not None
                assert serialize(first_result.to_packet()) == first_frame
                next_result = await client.set_latest_heat_setpoint_celsius(1201, 22)
                assert next_result is not None
                assert serialize(next_result.to_packet()) == await writes.get()
                assert wire.write.call_count == 3

    async def test_connection_loss_wakes_all_paused_writers(self) -> None:
        protocol = _TpckProtocol(closed_message="TCP transport closed")
        wire = Mock(spec=asyncio.Transport)
        wire.is_closing.return_value = False
        protocol.connection_made(wire)
        protocol.pause_writing()
        packet = TrpcPacket.create(method=TrpcMethod.PROTOCOL_VERSION).to_packet()
        async with asyncio.timeout(2):
            pending = [
                asyncio.create_task(protocol.write_packet(packet)) for _ in range(2)
            ]
            await asyncio.sleep(0)
            assert wire.write.call_count == 2
            protocol.connection_lost(None)
            for task in pending:
                with self.assertRaises(ConnectionClosedError):
                    await task

    async def test_queued_writes_do_not_reach_replacement_tcp_connection(self) -> None:
        accepted: asyncio.Queue[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = (
            asyncio.Queue()
        )

        def accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            accepted.put_nowait((reader, writer))

        async with (
            await asyncio.start_server(accept, "127.0.0.1", 0) as server,
            asyncio.timeout(2),
        ):
            port = server.sockets[0].getsockname()[1]
            async with Tekmar482Client.tcp("127.0.0.1", port) as client:
                first, first_writer = await accepted.get()
                busy = asyncio.create_task(
                    client.request_response(TrpcMethod.PROTOCOL_VERSION, timeout=None),
                )
                await first.read(256)
                writes = [
                    asyncio.create_task(
                        client.update(
                            TrpcMethod.HEAT_SETPOINT,
                            {"address": 1201, "setback": 7, "setpoint": n},
                        ),
                    )
                    for n in range(40, 65)
                ]
                await asyncio.sleep(0)
                await client.close()
                first_writer.close()
                await first_writer.wait_closed()
                await client.open()
                second, second_writer = await accepted.get()
                try:
                    for task in [busy, *writes]:
                        with self.assertRaises(ConnectionClosedError):
                            await task
                    fresh = await client.request_protocol_version()
                    await client.close()
                    assert await second.read() == serialize(fresh.to_packet())
                finally:
                    second_writer.close()
                    await second_writer.wait_closed()

    async def test_socket_timeout_is_connection_loss_not_a_read_deadline(self) -> None:
        protocol = _TpckProtocol(closed_message="TCP transport closed")
        transport = Mock(spec=asyncio.Transport)
        transport.is_closing.return_value = False
        protocol.connection_made(transport)
        assert await protocol.read_packet(timeout=0.002) is None
        lost = TimeoutError(errno.ETIMEDOUT, "socket timed out")
        protocol.connection_lost(lost)
        for timeout in (None, 1):
            with (
                self.subTest(timeout=timeout),
                self.assertRaises(ConnectionClosedError) as caught,
            ):
                await protocol.read_packet(timeout=timeout)
            assert caught.exception.__cause__ is lost

    async def test_client_raw_tcp_transport_round_trip(self) -> None:
        received: list[TrpcPacket] = []

        async def handle(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            parser = TpckParser()
            while not received:
                chunk = await reader.read(256)
                assert chunk
                packets = parser.feed_many(chunk)
                if packets:
                    received.append(TrpcPacket.from_packet(packets[0]))

            response = TrpcPacket.create(
                service=TrpcService.REPORT,
                method=TrpcMethod.FIRMWARE_REVISION,
                fields={"revision": 0x1234},
            )
            writer.write(serialize(response.to_packet()))
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        try:
            socket = server.sockets[0]
            host, port = socket.getsockname()[:2]

            async with Tekmar482Client.tcp(str(host), int(port)) as client:
                await client.request_firmware_revision()
                response = await client.read_message(timeout=1)

            assert received[0].service == TrpcService.REQUEST
            assert received[0].method == TrpcMethod.FIRMWARE_REVISION
            assert response is not None
            assert response.service == TrpcService.REPORT
            assert response.method == TrpcMethod.FIRMWARE_REVISION
            assert response.body == {"revision": 0x1234}
        finally:
            server.close()
            await server.wait_closed()

    async def test_client_raw_tcp_transport_handles_split_escaped_frame(self) -> None:
        async def handle(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            del reader
            response = TrpcPacket.create(
                service=TrpcService.REPORT,
                method=TrpcMethod.OUTDOOR_TEMP,
                fields={"temp": 0x35},
            )
            for value in serialize(response.to_packet()):
                writer.write(bytes((value,)))
                await writer.drain()
                await asyncio.sleep(0)
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        try:
            socket = server.sockets[0]
            host, port = socket.getsockname()[:2]

            async with Tekmar482Client.tcp(str(host), int(port)) as client:
                response = await client.read_message(timeout=1)

            assert response is not None
            assert response.service == TrpcService.REPORT
            assert response.method == TrpcMethod.OUTDOOR_TEMP
            assert response.body == {"temp": 0x35}
        finally:
            server.close()
            await server.wait_closed()
