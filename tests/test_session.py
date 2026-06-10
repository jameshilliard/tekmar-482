import asyncio
import unittest

from tekmar_482 import (
    ConnectionClosedError,
    NullTransport,
    Packet,
    ResponseMatch,
    Tekmar482Session,
    TrpcCommand,
    TrpcMethod,
    TrpcMethodId,
    TrpcPacket,
    TrpcService,
)

_EXPECTED_CANCELED_UPDATE = "expected canceled coalesced update"
_READER_FAILED = "reader failed"


def response(method: TrpcMethodId, **fields: int) -> TrpcPacket:
    return TrpcPacket.create(
        service=TrpcService.RESPONSE_REQUEST,
        method=method,
        fields=fields,
    )


def update_response(method: TrpcMethodId, **fields: int) -> TrpcPacket:
    return TrpcPacket.create(
        service=TrpcService.RESPONSE_UPDATE,
        method=method,
        fields=fields,
    )


def report(method: TrpcMethodId, **fields: int) -> TrpcPacket:
    return TrpcPacket.create(service=TrpcService.REPORT, method=method, fields=fields)


class SessionTest(unittest.IsolatedAsyncioTestCase):
    async def test_session_ignores_response_already_in_backlog(self) -> None:
        transport = NullTransport()
        transport.queue_packet(
            response(TrpcMethod.PROTOCOL_VERSION, version=3).to_packet(),
        )
        session = Tekmar482Session(transport)

        async with session:
            for _ in range(10):
                await asyncio.sleep(0)
                if session.message_backlog_count:
                    break

            assert session.message_backlog_count == 1
            message = await session.request_response(
                TrpcCommand.request(TrpcMethod.PROTOCOL_VERSION),
                timeout=0.01,
            )
            stale = await session.read_message(timeout=0.01)

        assert message is None
        assert stale is not None
        assert stale.body == {"version": 3}
        assert [
            TrpcPacket.from_packet(packet).method for packet in transport.written
        ] == [TrpcMethod.PROTOCOL_VERSION]

    async def test_session_consumes_matched_response(self) -> None:
        transport = NullTransport()
        session = Tekmar482Session(transport)

        async with session:
            task = asyncio.create_task(
                session.request_response(
                    TrpcCommand.request(TrpcMethod.DEVICE_TYPE, {"address": 1201}),
                    response_match=ResponseMatch.create(
                        TrpcMethod.DEVICE_TYPE,
                        services=(TrpcService.RESPONSE_REQUEST,),
                        address=1201,
                    ),
                    timeout=1,
                ),
            )
            await asyncio.sleep(0)

            transport.queue_packet(
                response(TrpcMethod.DEVICE_TYPE, address=1201, type=107201).to_packet(),
            )

            message = await task
            assert message is not None
            assert message.body == {"address": 1201, "type": 107201}
            assert await session.read_message(timeout=0.01) is None

    async def test_request_response_ignores_update_response_backlog(self) -> None:
        transport = NullTransport()
        transport.queue_packet(
            update_response(TrpcMethod.REPORTING_STATE, state=0).to_packet(),
        )
        transport.queue_packet(
            response(TrpcMethod.REPORTING_STATE, state=1).to_packet(),
        )
        session = Tekmar482Session(transport)

        async with session:
            message = await session.request_response(
                TrpcCommand.request(TrpcMethod.REPORTING_STATE),
                timeout=1,
            )
            stale = await session.read_message(timeout=0.01)

        assert message is not None
        assert message.service == TrpcService.RESPONSE_REQUEST
        assert message.body == {"state": 1}
        assert stale is not None
        assert stale.service == TrpcService.RESPONSE_UPDATE
        assert stale.body == {"state": 0}

    async def test_update_response_ignores_request_response_backlog(self) -> None:
        transport = NullTransport()
        transport.queue_packet(
            response(TrpcMethod.REPORTING_STATE, state=0).to_packet(),
        )
        transport.queue_packet(
            update_response(TrpcMethod.REPORTING_STATE, state=1).to_packet(),
        )
        session = Tekmar482Session(transport)

        async with session:
            message = await session.update_response(
                TrpcCommand.update(TrpcMethod.REPORTING_STATE, {"state": 1}),
                timeout=1,
            )
            stale = await session.read_message(timeout=0.01)

        assert message is not None
        assert message.service == TrpcService.RESPONSE_UPDATE
        assert message.body == {"state": 1}
        assert stale is not None
        assert stale.service == TrpcService.RESPONSE_REQUEST
        assert stale.body == {"state": 0}

    async def test_session_publishes_unmatched_reports(self) -> None:
        transport = NullTransport()
        session = Tekmar482Session(transport)

        async with session, session.reports() as reports:
            transport.queue_packet(
                report(
                    TrpcMethod.CURRENT_TEMPERATURE,
                    address=1201,
                    temp=1550,
                ).to_packet(),
            )

            message = await asyncio.wait_for(anext(reports), timeout=1)

        assert message.method == TrpcMethod.CURRENT_TEMPERATURE
        assert message.body == {"address": 1201, "temp": 1550}

    async def test_reports_raise_reader_error(self) -> None:
        session = Tekmar482Session(FailingReadTransport())

        async with session, session.reports() as reports:
            with self.assertRaisesRegex(ConnectionClosedError, "reader failed"):
                await asyncio.wait_for(anext(reports), timeout=1)

    async def test_coalesced_update_replaces_unsent_command(self) -> None:
        transport = BlockingWriteTransport()
        session = Tekmar482Session(transport)

        async with session:
            first = asyncio.create_task(
                session.coalesced_update(
                    TrpcCommand.update(
                        TrpcMethod.HEAT_SETPOINT,
                        {"address": 1201, "setback": 7, "setpoint": 40},
                    ),
                    coalesce_key=(TrpcMethod.HEAT_SETPOINT, 1201, 7),
                ),
            )
            first_packet = await asyncio.wait_for(transport.next_write(), timeout=1)

            second = asyncio.create_task(
                session.coalesced_update(
                    TrpcCommand.update(
                        TrpcMethod.HEAT_SETPOINT,
                        {"address": 1201, "setback": 7, "setpoint": 41},
                    ),
                    coalesce_key=(TrpcMethod.HEAT_SETPOINT, 1201, 7),
                ),
            )
            await asyncio.sleep(0)
            third = asyncio.create_task(
                session.coalesced_update(
                    TrpcCommand.update(
                        TrpcMethod.HEAT_SETPOINT,
                        {"address": 1201, "setback": 7, "setpoint": 42},
                    ),
                    coalesce_key=(TrpcMethod.HEAT_SETPOINT, 1201, 7),
                ),
            )

            assert await asyncio.wait_for(second, timeout=1) is None

            transport.release_write()
            first_result = await asyncio.wait_for(first, timeout=1)
            third_packet = await asyncio.wait_for(transport.next_write(), timeout=1)
            transport.release_write()
            third_result = await asyncio.wait_for(third, timeout=1)

        assert first_result is not None
        assert third_result is not None
        assert TrpcPacket.from_packet(first_packet).body["setpoint"] == 40
        assert TrpcPacket.from_packet(third_packet).body["setpoint"] == 42
        assert [
            TrpcPacket.from_packet(packet).body["setpoint"]
            for packet in transport.written
        ] == [40, 42]

    async def test_coalesced_update_response_publishes_matched_update_response(
        self,
    ) -> None:
        transport = NullTransport()
        session = Tekmar482Session(transport)

        async with session, session.reports() as reports:
            task = asyncio.create_task(
                session.coalesced_update_response(
                    TrpcCommand.update(
                        TrpcMethod.HEAT_SETPOINT,
                        {"address": 1201, "setback": 7, "setpoint": 42},
                    ),
                    coalesce_key=(TrpcMethod.HEAT_SETPOINT, 1201, 7),
                    response_match=ResponseMatch.create(
                        TrpcMethod.HEAT_SETPOINT,
                        services=(TrpcService.RESPONSE_UPDATE,),
                        address=1201,
                        fields={"setback": 7},
                    ),
                    timeout=1,
                ),
            )
            await asyncio.sleep(0)
            transport.queue_packet(
                update_response(
                    TrpcMethod.HEAT_SETPOINT,
                    address=1201,
                    setback=7,
                    setpoint=42,
                ).to_packet(),
            )

            result = await asyncio.wait_for(task, timeout=1)
            published = await asyncio.wait_for(anext(reports), timeout=1)

        assert result is not None
        assert result.service == TrpcService.RESPONSE_UPDATE
        assert result.method == TrpcMethod.HEAT_SETPOINT
        assert published.body == {"address": 1201, "setback": 7, "setpoint": 42}

    async def test_coalesced_update_response_can_match_report_when_requested(
        self,
    ) -> None:
        transport = NullTransport()
        session = Tekmar482Session(transport)

        async with session, session.reports() as reports:
            task = asyncio.create_task(
                session.coalesced_update_response(
                    TrpcCommand.update(
                        TrpcMethod.HEAT_SETPOINT,
                        {"address": 1201, "setback": 7, "setpoint": 42},
                    ),
                    coalesce_key=(TrpcMethod.HEAT_SETPOINT, 1201, 7),
                    response_match=ResponseMatch.create(
                        TrpcMethod.HEAT_SETPOINT,
                        services=(TrpcService.REPORT,),
                        address=1201,
                        fields={"setback": 7},
                    ),
                    timeout=1,
                ),
            )
            await asyncio.sleep(0)
            transport.queue_packet(
                report(
                    TrpcMethod.HEAT_SETPOINT,
                    address=1201,
                    setback=7,
                    setpoint=42,
                ).to_packet(),
            )

            result = await asyncio.wait_for(task, timeout=1)
            published = await asyncio.wait_for(anext(reports), timeout=1)

        assert result is not None
        assert result.service == TrpcService.REPORT
        assert published.body == {"address": 1201, "setback": 7, "setpoint": 42}

    async def test_canceled_coalesced_update_is_removed_before_write(self) -> None:
        transport = NullTransport()
        session = Tekmar482Session(transport)

        async with session:
            pending = asyncio.create_task(
                session.coalesced_update_response(
                    TrpcCommand.update(
                        TrpcMethod.HEAT_SETPOINT,
                        {"address": 1201, "setback": 7, "setpoint": 42},
                    ),
                    coalesce_key=(TrpcMethod.HEAT_SETPOINT, 1201, 7),
                    timeout=1,
                ),
            )
            await asyncio.sleep(0)

            assert session.queued_update_count == 1

            pending.cancel()
            try:
                await pending
            except asyncio.CancelledError:
                pass
            else:
                raise AssertionError(_EXPECTED_CANCELED_UPDATE)

            assert session.queued_update_count == 0

        assert transport.written == []


class BlockingWriteTransport(NullTransport):
    def __init__(self) -> None:
        super().__init__()
        self._write_started: asyncio.Queue[Packet] = asyncio.Queue()
        self._write_release: asyncio.Queue[None] = asyncio.Queue()

    async def write_packet(self, packet: Packet) -> None:
        self.written.append(packet)
        await self._write_started.put(packet)
        await self._write_release.get()

    async def next_write(self) -> Packet:
        return await self._write_started.get()

    def release_write(self) -> None:
        self._write_release.put_nowait(None)


class FailingReadTransport(NullTransport):
    async def read_packet(self, timeout: float | None = None) -> Packet | None:
        del timeout
        raise ConnectionClosedError(_READER_FAILED)
