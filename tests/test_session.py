import asyncio
import unittest

from tekmar_482 import (
    ConnectionClosedError,
    NullTransport,
    Packet,
    ResponseMatch,
    Tekmar482Client,
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
    async def test_outdoor_source_is_not_an_echo_of_the_host_temperature(self) -> None:
        transport = BlockingWriteTransport()
        method = TrpcMethod.OUTDOOR_TEMP
        async with Tekmar482Client(transport) as client, asyncio.timeout(2):
            for invalid in (-1, 0xBEA, 0xFFFF):
                with self.assertRaises(ValueError):
                    await client.set_outdoor_temperature(invalid)
            assert not transport.written
            query = asyncio.create_task(client.get_outdoor_temperature_celsius())
            assert (await transport.next_write()).data == bytes.fromhex(
                "01170100000000",
            )
            old = asyncio.create_task(client.set_outdoor_temperature(0xBE9))
            await asyncio.sleep(0)
            new = asyncio.create_task(client.set_outdoor_temperature_celsius(0))
            assert await old is None
            transport.release_write()
            transport.queue_packet(response(method, temp=0xFFFF).to_packet())
            assert await query is None
            assert (await transport.next_write()).data == bytes.fromhex(
                "00170100009204",
            )
            transport.release_write()
            actual = update_response(method, temp=1250)
            transport.queue_packet(actual.to_packet())
            assert await new == actual
            query = asyncio.create_task(client.get_outdoor_temperature_celsius())
            await transport.next_write()
            transport.release_write()
            transport.queue_packet(response(method, temp=1170).to_packet())
            assert await query == 0
            with self.assertRaises(TimeoutError):
                await client.get_outdoor_temperature_celsius(timeout=0.01)

    async def test_version_marker_does_not_retire_custom_bus_read(self) -> None:
        transport = BlockingWriteTransport()
        method = TrpcMethod.MODE_SETTING
        query = TrpcCommand.request(method, {"address": 1})
        marker = TrpcCommand.request(TrpcMethod.PROTOCOL_VERSION)
        async with (
            Tekmar482Session(
                transport,
                readback_methods=frozenset({method}),
            ) as session,
            asyncio.timeout(2),
        ):
            transport.release_write()
            assert await session.request_response(query, timeout=0.01) is None
            assert await transport.next_write() == query.to_message().to_packet()
            retry = asyncio.create_task(session.request_response(query))
            sync = asyncio.create_task(session.request_response(marker))
            assert await transport.next_write() == marker.to_message().to_packet()
            transport.release_write()
            version = response(marker.method, version=3)
            transport.queue_packet(version.to_packet())
            assert await sync == version
            assert await session.request_response(query, timeout=0.01) is None
            assert len(transport.written) == 2
            transport.queue_packet(response(method, address=1, mode=0).to_packet())
            assert await transport.next_write() == query.to_message().to_packet()
            transport.release_write()
            fresh = response(method, address=1, mode=1)
            transport.queue_packet(fresh.to_packet())
            assert await retry == fresh

    async def test_raw_short_request_keeps_wire_bytes_and_owns_one_reply(self) -> None:
        transport = BlockingWriteTransport()
        query = TrpcCommand.request(TrpcMethod.PROTOCOL_VERSION)
        packet = query.to_message().to_packet()
        short = Packet(packet.type, packet.data[:-2])  # Header-only gateway request.
        async with Tekmar482Session(transport) as session, asyncio.timeout(2):
            transport.release_write()
            await session.write_packet(short)
            assert await transport.next_write() == short
            assert await session.request_response(query, timeout=0.01) is None
            assert transport.written == [short]
            old = response(query.method, version=2)
            transport.queue_packet(old.to_packet())
            assert await session.read_message() == old
            retry = asyncio.create_task(session.request_response(query))
            assert await transport.next_write() == packet
            transport.release_write()
            fresh = response(query.method, version=3)
            transport.queue_packet(fresh.to_packet())
            assert await retry == fresh

    async def test_retry_consumes_only_its_own_read_reply(self) -> None:
        for method, target, value in (
            (TrpcMethod.CURRENT_TEMPERATURE, {"address": 1}, "temp"),
            (TrpcMethod.SETBACK_STATE, {"address": 1}, "setback"),
            (TrpcMethod.MODE_SETTING, {"address": 1}, "mode"),
            (TrpcMethod.SETPOINT_GROUP_ENABLE, {"group_id": 1}, "enable"),
            (TrpcMethod.NETWORK_ERROR, {}, "error"),
        ):
            with self.subTest(method=method):
                transport = BlockingWriteTransport()
                query = TrpcCommand.request(method, target)
                old = response(method, **target, **{value: 1})
                fresh = response(method, **target, **{value: 0})
                async with (
                    Tekmar482Session(transport) as session,
                    session.reports() as reports,
                    asyncio.timeout(2),
                ):
                    transport.release_write()
                    assert await session.request_response(query, timeout=0.01) is None
                    assert (
                        await transport.next_write() == query.to_message().to_packet()
                    )
                    retry = asyncio.create_task(session.request_response(query))
                    # An independent target can still use the wire.
                    other = (
                        TrpcCommand.request(method, dict.fromkeys(target, 2))
                        if target
                        else TrpcCommand.request(TrpcMethod.REPORTING_STATE)
                    )
                    unrelated = asyncio.create_task(session.request_response(other))
                    assert (
                        await transport.next_write() == other.to_message().to_packet()
                    )
                    transport.release_write()
                    answer = response(other.method, **other.fields)
                    transport.queue_packet(answer.to_packet())
                    assert await unrelated == TrpcPacket.from_packet(answer.to_packet())
                    pushed = report(method, **old.body)
                    transport.queue_packet(pushed.to_packet())
                    assert await reports.next_event() == (pushed, None)
                    assert len(transport.written) == 2
                    transport.queue_packet(old.to_packet())
                    assert (
                        await transport.next_write() == query.to_message().to_packet()
                    )
                    transport.release_write()
                    transport.queue_packet(fresh.to_packet())
                    assert await retry == fresh
                    assert not list(reports.drain())
                    assert session.message_backlog_count == 1  # Only the pushed report.

    async def test_moved_devices_no_wait_echo_is_drained_not_published(self) -> None:
        transport = BlockingWriteTransport()
        method = TrpcMethod.HEAT_SETPOINT
        cmd = TrpcCommand.update(
            method,
            {"address": 1201, "setback": 6, "setpoint": 40},
        )
        ack = update_response(method, **cmd.fields)
        async with (
            Tekmar482Session(transport) as session,
            session.reports() as reports,
            asyncio.timeout(1),
        ):
            transport.release_write()
            await session.send(cmd)
            assert await transport.next_write() == cmd.to_message().to_packet()
            session.discard_device_updates({1201})
            transport.queue_packet(ack.to_packet())
            barrier = report(TrpcMethod.OUTDOOR_TEMP, temp=1550)
            transport.queue_packet(barrier.to_packet())
            assert await reports.next_event() == (barrier, None)
            # It was consumed, so the replacement can confirm its own identical write.
            pending = asyncio.create_task(session.update_response(cmd))
            assert await transport.next_write() == cmd.to_message().to_packet()
            transport.release_write()
            transport.queue_packet(ack.to_packet())
            assert await pending == ack
            assert await reports.next_event() == (ack, cmd)

    async def test_short_mode_ack_cannot_confirm_off(self) -> None:
        transport = BlockingWriteTransport()
        command = TrpcCommand.update(
            TrpcMethod.MODE_SETTING,
            {"address": 1201, "mode": 0},
        )
        ack = update_response(TrpcMethod.MODE_SETTING, **command.fields)
        frame = ack.to_packet()
        async with Tekmar482Session(transport) as session, asyncio.timeout(2):
            pending = asyncio.create_task(session.update_response(command))
            await transport.next_write()
            transport.release_write()
            transport.queue_packet(Packet(frame.type, frame.data[:-1]))
            barrier = report(TrpcMethod.OUTDOOR_TEMP, temp=1500)
            transport.queue_packet(barrier.to_packet())
            assert await session.read_message() == barrier
            assert not pending.done()
            transport.queue_packet(frame)
            assert await pending == ack

    async def test_overlapping_unconfirmed_echoes_never_infer_active_slot(self) -> None:
        transport = NullTransport()
        method = TrpcMethod.HEAT_SETPOINT
        async with Tekmar482Session(transport) as session, session.reports() as reports:
            for slot in (7, 6):
                await session.send(
                    TrpcCommand.update(
                        method,
                        {"address": 1201, "setpoint": 44, "setback": slot},
                    ),
                )
            echo = update_response(method, address=1201, setpoint=44, setback=6)
            for _ in range(2):
                transport.queue_packet(echo.to_packet())
                assert await reports.next_event() == (echo, None)

    async def test_no_wait_readbacks_do_not_guess_which_value_was_sent(self) -> None:
        transport = NullTransport()
        method = TrpcMethod.MODE_SETTING
        session = Tekmar482Session(transport, readback_methods=frozenset({method}))
        async with session, session.reports() as reports, asyncio.timeout(2):
            for mode in (1, 0):
                await session.send(
                    TrpcCommand.update(method, {"address": 1, "mode": mode}),
                )
            echo = update_response(method, address=1, mode=0)
            for _ in range(2):
                transport.queue_packet(echo.to_packet())
                assert await reports.next_event() == (echo, None)

    async def test_late_mode_echo_does_not_confirm_a_second_off(self) -> None:
        method = TrpcMethod.MODE_SETTING
        off = TrpcCommand.update(method, {"address": 1201, "mode": 0})
        ack = update_response(method, **off.fields)
        for wait in (True, False):
            with self.subTest(wait=wait):
                transport = BlockingWriteTransport()
                async with Tekmar482Session(transport) as session, asyncio.timeout(2):
                    transport.release_write()
                    if wait:
                        assert await session.update_response(off, timeout=0.01) is None
                    else:
                        await session.send(off)
                    assert await transport.next_write() == off.to_message().to_packet()
                    repeated = asyncio.create_task(
                        session.coalesced_update_response(off, coalesce_key="mode"),
                    )
                    # A targeted read is still allowed and cannot drain an update echo.
                    query = TrpcCommand.request(method, {"address": 1201})
                    read = asyncio.create_task(session.request_response(query))
                    assert (
                        await transport.next_write() == query.to_message().to_packet()
                    )
                    transport.release_write()
                    actual = response(method, address=1201, mode=0)
                    transport.queue_packet(actual.to_packet())
                    assert await read == actual
                    assert not repeated.done()
                    transport.queue_packet(ack.to_packet())
                    assert await transport.next_write() == off.to_message().to_packet()
                    assert not repeated.done()
                    transport.release_write()
                    transport.queue_packet(ack.to_packet())
                    assert await repeated == ack
                    assert len(transport.written) == 3

    async def test_named_write_order_depends_on_observed_setback_enable(self) -> None:
        method = TrpcMethod.HEAT_SETPOINT
        away = TrpcCommand.update(
            method,
            {"address": 1201, "setback": 6, "setpoint": 34},
        )
        occupied = TrpcCommand.update(
            method,
            {"address": 1201, "setback": 4, "setpoint": 44},
        )
        for initial, enabled in ((None, None), (0, 0), (1, 1), (1, 0), (0, 1)):
            with self.subTest(initial=initial, enabled=enabled):
                transport = BlockingWriteTransport()
                async with Tekmar482Session(transport) as session, asyncio.timeout(2):
                    if initial is not None:
                        setting = response(TrpcMethod.SETBACK_ENABLE, enable=initial)
                        transport.queue_packet(setting.to_packet())
                        assert await session.read_message() == setting
                    transport.release_write()
                    assert await session.update_response(away, timeout=0.01) is None
                    assert await transport.next_write() == away.to_message().to_packet()
                    # Hold the wire while both commands register in the old setting.
                    barrier = TrpcCommand.request(TrpcMethod.PROTOCOL_VERSION)
                    gate = asyncio.create_task(session.send(barrier))
                    assert (
                        await transport.next_write() == barrier.to_message().to_packet()
                    )
                    older = asyncio.create_task(
                        session.coalesced_update_response(away, coalesce_key="away"),
                    )
                    newer = asyncio.create_task(
                        session.coalesced_update_response(
                            occupied,
                            coalesce_key="occupied",
                        ),
                    )
                    await asyncio.sleep(0)
                    if initial != enabled and enabled is not None:
                        setting = report(TrpcMethod.SETBACK_ENABLE, enable=enabled)
                        transport.queue_packet(setting.to_packet())
                        assert await session.read_message() == setting
                    transport.release_write()
                    await gate
                    late = update_response(method, **away.fields)
                    if enabled != 1:
                        read = asyncio.create_task(session.send(barrier))
                        assert (
                            await transport.next_write()
                            == barrier.to_message().to_packet()
                        )
                        transport.release_write()
                        await read
                        transport.queue_packet(late.to_packet())
                    writes = (
                        ((occupied, newer), (away, older))
                        if enabled == 1
                        else ((away, older), (occupied, newer))
                    )
                    for command, waiter in writes:
                        if enabled == 1 and command is away:
                            transport.queue_packet(late.to_packet())
                        assert (
                            await transport.next_write()
                            == command.to_message().to_packet()
                        )
                        transport.release_write()
                        ack = update_response(method, **command.fields)
                        transport.queue_packet(ack.to_packet())
                        assert await waiter == ack

    async def test_late_setpoint_ack_cannot_confirm_current_write(self) -> None:
        method = TrpcMethod.HEAT_SETPOINT
        fields = {"address": 2101, "setback": 6, "setpoint": 34}
        away = TrpcCommand.update(method, fields)
        current = TrpcCommand.update(method, {**fields, "setback": 7})
        distinct = TrpcCommand.update(
            method,
            {**fields, "address": 2102, "setpoint": 44},
        )
        for coalesced_first in (True, False):
            with self.subTest(coalesced_first=coalesced_first):
                transport = BlockingWriteTransport()
                async with Tekmar482Session(transport) as session, asyncio.timeout(2):
                    transport.release_write()
                    if coalesced_first:
                        with self.assertRaises(TimeoutError):
                            await session.coalesced_update_response(
                                away,
                                coalesce_key="away",
                                timeout=0.01,
                            )
                    else:
                        assert await session.update_response(away, timeout=0.01) is None
                    assert await transport.next_write() == away.to_message().to_packet()
                    waiting = asyncio.create_task(
                        session.update_response(current)
                        if coalesced_first
                        else session.coalesced_update_response(
                            current,
                            coalesce_key="current",
                        ),
                    )
                    transport.queue_packet(response(method, **fields).to_packet())
                    other = asyncio.create_task(
                        session.coalesced_update_response(
                            distinct,
                            coalesce_key="other",
                        ),
                    )
                    assert (
                        await transport.next_write()
                        == distinct.to_message().to_packet()
                    )
                    transport.release_write()
                    distinct_ack = update_response(method, **distinct.fields)
                    transport.queue_packet(distinct_ack.to_packet())
                    assert await other == distinct_ack
                    assert not waiting.done()
                    transport.queue_packet(
                        update_response(method, **fields).to_packet(),
                    )
                    assert (
                        await transport.next_write() == current.to_message().to_packet()
                    )
                    assert not waiting.done()
                    transport.release_write()
                    genuine = update_response(method, **{**fields, "setback": 4})
                    transport.queue_packet(genuine.to_packet())
                    assert await waiting == genuine
                    assert len(transport.written) == 3

    async def test_unconfirmed_echoes_do_not_confirm_later_write(self) -> None:
        method = TrpcMethod.HEAT_SETPOINT
        fields = {"address": 2101, "setback": 6, "setpoint": 34}
        away = TrpcCommand.update(method, fields)
        current = TrpcCommand.update(method, {**fields, "setback": 7})
        ack = update_response(method, **fields)
        transport = BlockingWriteTransport()
        async with (
            Tekmar482Session(transport) as session,
            session.reports() as reports,
            asyncio.timeout(2),
        ):
            transport.release_write()
            await session.send(away)
            transport.release_write()
            await session.coalesced_update(away, coalesce_key="away")
            assert await transport.next_write() == away.to_message().to_packet()
            assert await transport.next_write() == away.to_message().to_packet()
            waiting = asyncio.create_task(session.update_response(current))
            transport.queue_packet(ack.to_packet())
            assert await reports.next_event() == (ack, away)
            await asyncio.sleep(0)
            assert len(transport.written) == 2
            transport.queue_packet(ack.to_packet())
            assert await reports.next_event() == (ack, away)
            assert await transport.next_write() == current.to_message().to_packet()
            assert not waiting.done()
            transport.release_write()
            genuine = update_response(method, **{**fields, "setback": 4})
            transport.queue_packet(genuine.to_packet())
            assert await waiting == genuine
            assert await reports.next_event() == (genuine, current)

    async def test_conflicting_unsent_writes_keep_submission_order(self) -> None:
        method = TrpcMethod.HEAT_SETPOINT
        fields = {"address": 2101, "setback": 6, "setpoint": 34}
        old = TrpcCommand.update(method, fields)
        current = TrpcCommand.update(method, {**fields, "setback": 7})
        later = TrpcCommand.update(method, {**fields, "setpoint": 44})
        transport = BlockingWriteTransport()
        async with Tekmar482Session(transport) as session, asyncio.timeout(2):
            transport.release_write()
            assert await session.update_response(old, timeout=0.01) is None
            assert await transport.next_write() == old.to_message().to_packet()
            first = asyncio.create_task(
                session.coalesced_update_response(current, coalesce_key="current"),
            )
            second = asyncio.create_task(
                session.coalesced_update_response(later, coalesce_key="away"),
            )
            # An unrelated request is a wire barrier: neither write passed the guard.
            barrier = TrpcCommand.request(TrpcMethod.PROTOCOL_VERSION)
            read = asyncio.create_task(session.send(barrier))
            assert await transport.next_write() == barrier.to_message().to_packet()
            transport.release_write()
            await read
            ack = update_response(method, **fields)
            transport.queue_packet(ack.to_packet())
            assert await transport.next_write() == current.to_message().to_packet()
            transport.release_write()
            transport.queue_packet(ack.to_packet())
            assert await first == ack
            assert await transport.next_write() == later.to_message().to_packet()
            transport.release_write()
            last = update_response(method, **later.fields)
            transport.queue_packet(last.to_packet())
            assert await second == last

    async def test_ambiguous_queued_write_can_time_out_without_transmission(
        self,
    ) -> None:
        fields = {"address": 2101, "setback": 6, "setpoint": 34}
        method = TrpcMethod.HEAT_SETPOINT
        transport = NullTransport()
        async with Tekmar482Session(transport) as session, asyncio.timeout(1):
            assert (
                await session.update_response(
                    TrpcCommand.update(method, fields),
                    timeout=0.01,
                )
                is None
            )
            with self.assertRaises(TimeoutError):
                await session.coalesced_update_response(
                    TrpcCommand.update(method, {**fields, "setback": 7}),
                    coalesce_key="current",
                    timeout=0.01,
                )
            assert len(transport.written) == 1

    async def test_late_setback_reply_cannot_resolve_current(self) -> None:
        method = TrpcMethod.HEAT_SETPOINT
        away = TrpcCommand.request(method, {"address": 1201, "setback": 6})
        current = TrpcCommand.request(method, {"address": 1201, "setback": 7})
        wake = TrpcCommand.request(method, {"address": 1201, "setback": 0})
        for cancel in (False, True):
            with self.subTest(cancel=cancel):
                transport = BlockingWriteTransport()
                async with (
                    Tekmar482Session(transport) as session,
                    session.reports() as reports,
                    asyncio.timeout(2),
                ):
                    first = asyncio.create_task(
                        session.request_response(
                            away,
                            timeout=None if cancel else 0.02,
                        ),
                    )
                    assert await transport.next_write() == away.to_message().to_packet()
                    transport.release_write()
                    if cancel:
                        first.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await first
                    else:
                        assert await first is None
                    retry = asyncio.create_task(
                        session.request_response(current, publish_match=True),
                    )
                    other = asyncio.create_task(session.request_response(wake))
                    assert await transport.next_write() == wake.to_message().to_packet()
                    transport.release_write()
                    genuine = response(method, address=1201, setback=0, setpoint=44)
                    transport.queue_packet(genuine.to_packet())
                    assert await other == genuine
                    assert not retry.done()
                    transport.queue_packet(
                        response(
                            method,
                            address=1201,
                            setback=6,
                            setpoint=34,
                        ).to_packet(),
                    )
                    assert (
                        await transport.next_write() == current.to_message().to_packet()
                    )
                    transport.release_write()
                    assert list(reports.drain_events()) == []
                    transport.queue_packet(genuine.to_packet())
                    assert await retry == genuine
                    assert await reports.next_event() == (genuine, current)

    async def test_missing_current_blocks_named_read_until_reopen(self) -> None:
        method = TrpcMethod.HEAT_SETPOINT
        current = TrpcCommand.request(method, {"address": 1201, "setback": 7})
        away = TrpcCommand.request(method, {"address": 1201, "setback": 6})
        transport = BlockingWriteTransport()
        session = Tekmar482Session(transport)
        async with session, asyncio.timeout(2):
            transport.release_write()
            assert await session.request_response(current, timeout=0.01) is None
            assert await transport.next_write() == current.to_message().to_packet()
            assert await session.request_response(away, timeout=0.01) is None
            assert len(transport.written) == 1
            pending = asyncio.create_task(session.request_response(away, timeout=None))
            await asyncio.sleep(0)
            await session.close()
            await session.open()
            with self.assertRaises(ConnectionClosedError):
                await pending
            fresh = asyncio.create_task(session.request_response(away))
            assert await transport.next_write() == away.to_message().to_packet()
            transport.release_write()
            msg = response(method, address=1201, setback=6, setpoint=34)
            transport.queue_packet(msg.to_packet())
            assert await fresh == msg

    async def test_waiting_commands_fail_across_reopen(self) -> None:
        transport = NullTransport()
        session = Tekmar482Session(transport)
        read = TrpcCommand.request(TrpcMethod.PROTOCOL_VERSION)
        write = TrpcCommand.update(
            TrpcMethod.HEAT_SETPOINT,
            {"address": 1201, "setback": 7, "setpoint": 42},
        )
        async with session, asyncio.timeout(1):
            busy = asyncio.create_task(session.request_response(read, timeout=None))
            await asyncio.sleep(0)
            queued = [
                asyncio.create_task(session.send(write)),
                asyncio.create_task(session.update_response(write, timeout=None)),
                asyncio.create_task(session.request_inventory(0, timeout=None)),
                asyncio.create_task(
                    session.coalesced_update(write, coalesce_key="heat"),
                ),
            ]
            await asyncio.sleep(0)
            assert transport.written == [read.to_message().to_packet()]
            await session.close()
            await session.open()
            for task in [busy, *queued]:
                with self.assertRaises(ConnectionClosedError):
                    await task
            assert transport.written == [read.to_message().to_packet()]
            await session.send(write)
            assert transport.written[-1] == write.to_message().to_packet()

    async def test_invalid_setpoint_cannot_replace_a_queued_write(self) -> None:
        transport = BlockingWriteTransport()
        async with Tekmar482Client(transport) as client, asyncio.timeout(1):
            read = asyncio.create_task(
                client.request_response(TrpcMethod.PROTOCOL_VERSION),
            )
            await transport.next_write()
            transport.release_write()
            valid = asyncio.create_task(
                client.set_latest_heat_setpoint_celsius(2101, 21),
            )
            await asyncio.sleep(0)
            for target in (-1.0, -0.1, 127.1, 127.5, float("nan"), float("inf")):
                with self.subTest(target=target), self.assertRaises(ValueError):
                    await client.set_latest_heat_setpoint_celsius(2101, target)
            with self.assertRaisesRegex(ValueError, "setback"):
                await client.set_latest_heat_setpoint_celsius(2101, 22, setback=8)
            assert client.session.queued_update_count == 1
            transport.queue_packet(
                response(TrpcMethod.PROTOCOL_VERSION, version=3).to_packet(),
            )
            await read
            await transport.next_write()
            transport.release_write()
            assert await valid is not None
            assert [
                TrpcPacket.from_packet(p).get("setpoint") for p in transport.written
            ] == [None, 42]

    async def test_confirmed_write_matches_resolved_slot(self) -> None:
        transport = BlockingWriteTransport()
        async with Tekmar482Client(transport) as client, asyncio.timeout(1):
            for slot, expected_slot in ((7, 4), (5, 5)):
                with self.subTest(slot=slot):
                    task = asyncio.create_task(
                        client.set_latest_heat_setpoint(
                            2101,
                            42,
                            setback=slot,
                            confirm=True,
                        ),
                    )
                    await transport.next_write()
                    transport.release_write()
                    for address, reply_slot, value in (
                        (2102, expected_slot, 42),
                        (2101, expected_slot, 41),
                        (2101, 255 if slot == 7 else 4, 42),
                        (2101, expected_slot, 42),
                    ):
                        transport.queue_packet(
                            update_response(
                                TrpcMethod.HEAT_SETPOINT,
                                address=address,
                                setback=reply_slot,
                                setpoint=value,
                            ).to_packet(),
                        )
                    reply = await task
                    assert reply is not None
                    assert reply.body == {
                        "address": 2101,
                        "setback": expected_slot,
                        "setpoint": 42,
                    }

    async def test_latest_write_stays_replaceable_behind_request(self) -> None:
        for cancel in (False, True):
            with self.subTest(cancel=cancel):
                transport = BlockingWriteTransport()
                async with Tekmar482Client(transport) as client, asyncio.timeout(1):
                    read = asyncio.create_task(
                        client.request_response(TrpcMethod.PROTOCOL_VERSION),
                    )
                    await transport.next_write()
                    transport.release_write()
                    old = asyncio.create_task(
                        client.set_latest_heat_setpoint(2101, 40, confirm=True),
                    )
                    await asyncio.sleep(0)
                    # Give the writer a turn while the read owns the transaction lock.
                    await asyncio.sleep(0)
                    if cancel:
                        old.cancel()
                    new = asyncio.create_task(client.set_latest_heat_setpoint(2101, 42))
                    if cancel:
                        with self.assertRaises(asyncio.CancelledError):
                            await old
                    else:
                        assert await old is None
                    transport.queue_packet(
                        response(TrpcMethod.PROTOCOL_VERSION, version=3).to_packet(),
                    )
                    await read
                    await transport.next_write()
                    transport.release_write()
                    assert await new is not None
                    assert [
                        TrpcPacket.from_packet(p).get("setpoint")
                        for p in transport.written
                    ] == [None, 42]

    async def test_unacknowledged_confirmed_write_raises_timeout(self) -> None:
        async with Tekmar482Client(NullTransport()) as client:
            with self.assertRaises(TimeoutError):
                await client.set_latest_heat_setpoint(
                    2101,
                    42,
                    confirm=True,
                    timeout=0.01,
                )

    async def test_stalled_drain_releases_wire_without_retry(self) -> None:
        for confirm in (True, False):
            with self.subTest(confirm=confirm):
                transport = BlockingWriteTransport()
                async with Tekmar482Client(transport) as client, asyncio.timeout(1):
                    write = asyncio.create_task(
                        client.set_latest_heat_setpoint(
                            2101,
                            42,
                            confirm=confirm,
                            timeout=0.05,
                        ),
                    )
                    sent = TrpcPacket.from_packet(await transport.next_write())
                    if confirm:
                        async with client.reports() as reports:
                            ack = update_response(TrpcMethod.HEAT_SETPOINT, **sent.body)
                            transport.queue_packet(ack.to_packet())
                            assert await anext(reports) == ack

                    read = asyncio.create_task(
                        client.request_response(TrpcMethod.PROTOCOL_VERSION),
                    )
                    with self.assertRaises(TimeoutError):
                        await write
                    assert (
                        TrpcPacket.from_packet(await transport.next_write()).method
                        == TrpcMethod.PROTOCOL_VERSION
                    )
                    reply = response(TrpcMethod.PROTOCOL_VERSION, version=3)
                    transport.queue_packet(reply.to_packet())
                    transport.release_write()
                    assert await read == reply
                    assert client.session.pending_request_count == 0
                    assert len(transport.written) == 2

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
            assert await asyncio.wait_for(reports.next_event(), timeout=1) is None

    async def test_subscription_eof_wakes_all_readers_and_persists(self) -> None:
        async with Tekmar482Session(NullTransport()) as session, asyncio.timeout(1):
            reports = session.reports(max_queue_size=1)
            readers = [asyncio.create_task(reports.next_event()) for _ in range(2)]
            await asyncio.sleep(0)
            reports.close()
            assert await asyncio.gather(*readers) == [None, None]
            assert list(reports.drain()) == []
            for _ in range(2):
                with self.assertRaises(StopAsyncIteration):
                    await anext(reports)

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
