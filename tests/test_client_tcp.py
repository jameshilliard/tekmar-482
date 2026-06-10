from __future__ import annotations

import asyncio
import unittest

from tekmar_482 import Tekmar482Client
from tekmar_482.tpck import TpckParser, serialize
from tekmar_482.trpc import TrpcMethod, TrpcPacket, TrpcService


class ClientTcpTest(unittest.IsolatedAsyncioTestCase):
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
