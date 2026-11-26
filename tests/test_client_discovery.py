import asyncio
import unittest
from datetime import UTC, datetime
from unittest.mock import patch

from tekmar_482 import (
    ConnectionClosedError,
    DeviceAttributeBit,
    DeviceAttributes,
    DeviceMode,
    DeviceValue,
    DiscoveredDevice,
    DiscoveryResult,
    GatewayInfo,
    InventoryDrainTimeoutError,
    NullTransport,
    Packet,
    ProtocolError,
    Tekmar482Client,
    ThaSetback,
    TraceReplayTransport,
    TraceStep,
    TrpcMethod,
    TrpcMethodId,
    TrpcPacket,
    TrpcService,
    celsius_to_dege,
    method_from_name,
)

_EXPECTED_VALUE_ERROR = "expected ValueError"
_CURRENT_FIRST = (
    ThaSetback.CURRENT,
    *(s for s in ThaSetback if s is not ThaSetback.CURRENT),
)


def response(method: str | TrpcMethodId, **fields: int) -> TrpcPacket:
    return TrpcPacket.create(
        service=TrpcService.RESPONSE_REQUEST,
        method=_method(method),
        fields=fields,
    )


def update_response(method: str | TrpcMethodId, **fields: int) -> TrpcPacket:
    return TrpcPacket.create(
        service=TrpcService.RESPONSE_UPDATE,
        method=_method(method),
        fields=fields,
    )


def report(method: str | TrpcMethodId, **fields: int) -> TrpcPacket:
    return TrpcPacket.create(
        service=TrpcService.REPORT,
        method=_method(method),
        fields=fields,
    )


def request_step(
    method: str | TrpcMethodId,
    *,
    response: TrpcPacket | None = None,
    received: tuple[TrpcPacket, ...] = (),
    **fields: int,
) -> TraceStep:
    return TraceStep(
        sent=TrpcPacket.create(
            service=TrpcService.REQUEST,
            method=_method(method),
            fields=fields,
        ),
        received=received if response is None else (response,),
    )


def update_step(method: str | TrpcMethodId, **fields: int) -> TraceStep:
    return TraceStep(
        sent=TrpcPacket.create(
            service=TrpcService.UPDATE,
            method=_method(method),
            fields=fields,
        ),
        received=(update_response(method, **fields),),
    )


def _method(method: str | TrpcMethodId) -> TrpcMethodId:
    return method_from_name(method) if isinstance(method, str) else method


def _heat_setpoint_device(
    address: int = 1001,
    *,
    events: int | None = None,
) -> DiscoveredDevice:
    return DiscoveredDevice(
        address=address,
        attributes=DeviceAttributes(DeviceAttributeBit.HEAT_SETPOINT),
        setback_events=events,
    )


def _runtime_seed_steps(
    *,
    address: int = 1001,
    setback: ThaSetback = ThaSetback.CURRENT,
) -> tuple[TraceStep, ...]:
    return (
        request_step(
            "CurrentTemperature",
            address=address,
            response=response("CurrentTemperature", address=address, temp=1550),
        ),
        request_step(
            "ActiveDemand",
            address=address,
            response=response("ActiveDemand", address=address, demand=0),
        ),
        request_step(
            "SetbackState",
            address=address,
            response=response("SetbackState", address=address, setback=setback),
        ),
        request_step(
            "ModeSetting",
            address=address,
            response=response("ModeSetting", address=address, mode=0),
        ),
    )


class ClientDiscoveryTest(unittest.IsolatedAsyncioTestCase):
    async def test_late_generic_reply_cannot_complete_another_address_read(
        self,
    ) -> None:
        method = TrpcMethod.CURRENT_TEMPERATURE
        late = response(method, address=1201, temp=1500)
        expected = response(method, address=2101, temp=1600)
        transport = TraceReplayTransport(
            [
                request_step(method, address=1201),
                request_step(method, address=2101, received=(late, expected)),
            ],
        )
        async with Tekmar482Client(transport) as client:
            assert (
                await client.request_response(
                    method,
                    fields={"address": 1201},
                    timeout=0.01,
                )
                is None
            )
            assert (
                await client.request_response(method, fields={"address": 2101})
                == expected
            )
            assert await client.read_message(timeout=0.01) is None
        transport.assert_complete()

    async def test_gateway_poll_cannot_continue_on_a_reopened_connection(self) -> None:
        method = TrpcMethod.REPORTING_STATE
        transport = TraceReplayTransport(
            [
                request_step(method, response=response(method, state=0)),
            ],
        )
        async with Tekmar482Client(transport) as client, asyncio.timeout(1):
            scan = asyncio.create_task(
                client.poll_gateway(
                    gateway_info=GatewayInfo(),
                    include_setpoint_groups=False,
                    write_delay=0.03,
                ),
            )
            assert await client.read_packet(timeout=1) is not None
            await client.close()
            await client.open()
            with self.assertRaises(ConnectionClosedError):
                await scan
        transport.assert_complete()

    async def test_discovery_never_restores_reporting_on_a_replacement_connection(
        self,
    ) -> None:
        transport = TraceReplayTransport(
            [update_step(TrpcMethod.REPORTING_STATE, state=0)],
        )
        async with Tekmar482Client(transport) as client, asyncio.timeout(1):
            scan = asyncio.create_task(
                client.discover(manage_reporting=True, write_delay=0.03),
            )
            assert await client.read_packet(timeout=1) is not None
            await client.close()
            await client.open()
            with self.assertRaises(ConnectionClosedError) as raised:
                await scan
            assert any(
                "restore reporting" in note for note in raised.exception.__notes__
            )
            assert len(transport.written) == 1
        transport.assert_complete()

    async def test_humidity_queries_follow_individual_attributes(self) -> None:
        measurement = TrpcMethod.RELATIVE_HUMIDITY
        minimum = TrpcMethod.HUMIDITY_SET_MIN
        maximum = TrpcMethod.HUMIDITY_SET_MAX
        families = {
            measurement: DeviceValue.RELATIVE_HUMIDITY,
            minimum: DeviceValue.HUMIDITY_SETPOINT_MIN,
            maximum: DeviceValue.HUMIDITY_SETPOINT_MAX,
        }
        for attrs, model, version, expected in (
            (0x10, None, 2, {measurement}),
            (0x20, None, 3, {maximum}),
            (0x40, None, 3, {minimum}),
            (0x70, None, 3, set(families)),
            (0x19, 105102, 3, {measurement}),
            (0, 105102, 3, set()),
            (None, 105102, 3, {measurement}),
            (0x70, None, 1, set()),
        ):
            device = DiscoveredDevice(
                2101,
                type_code=model,
                attributes=DeviceAttributes(attrs),
            )
            client = Tekmar482Client(NullTransport())
            await self.enterAsyncContext(client)
            with (
                self.subTest(attrs=attrs, model=model, version=version),
                patch.object(
                    client.session,
                    "request_response",
                    return_value=None,
                ) as request,
            ):
                (snapshot,) = await client.poll_devices(
                    [device],
                    protocol_version=version,
                )
                assert {
                    c.args[0].method for c in request.await_args_list
                } & families.keys() == expected
                assert snapshot.runtime.available_values & set(families.values()) == {
                    families[m] for m in expected
                }

    async def test_datetime_requires_a_successful_write_echo(self) -> None:
        when = datetime(2026, 6, 9, 21, 46, tzinfo=UTC)
        step = update_step(
            "DateTime",
            year=2026,
            month=6,
            day=9,
            weekday=2,
            hour=21,
            minute=46,
        )
        rejected = update_response(
            "DateTime",
            year=65535,
            month=255,
            day=255,
            weekday=255,
            hour=255,
            minute=255,
        )
        stale = update_response("DateTime", **(step.sent.body | {"minute": 45}))
        transport = TraceReplayTransport(
            [
                TraceStep(step.sent, (stale, *step.received)),
                TraceStep(step.sent, (rejected,)),
            ],
        )
        async with Tekmar482Client(transport) as client, asyncio.timeout(1):
            assert await client.set_datetime(when) == step.received[0]
            with self.assertRaisesRegex(ProtocolError, "rejected DateTime"):
                await client.set_datetime(when, timeout=None)
        transport.assert_complete()

    async def test_discovery_deadline_includes_transaction_queue(self) -> None:
        transport = BlockingWriteTransport()
        async with Tekmar482Client(transport) as client, asyncio.timeout(1):
            pending = asyncio.create_task(
                client.request_response(TrpcMethod.PROTOCOL_VERSION, timeout=None),
            )
            await transport.next_write()
            transport.release_write()
            with self.assertRaisesRegex(TimeoutError, "gateway metadata"):
                await client.discover(timeout=0.02)
            with self.assertRaisesRegex(TimeoutError, "disabling reporting"):
                await client.discover(timeout=0.02, manage_reporting=True)
            assert len(transport.written) == 1
            transport.queue_packet(response("ProtocolVersion", version=3).to_packet())
            assert await pending is not None

    async def test_managed_discovery_restores_after_unacknowledged_disable(
        self,
    ) -> None:
        disable = update_step("ReportingState", state=0)
        enable = update_step("ReportingState", state=1)
        for restore_replies in (enable.received, ()):
            transport = TraceReplayTransport(
                [TraceStep(disable.sent, ()), TraceStep(enable.sent, restore_replies)],
            )
            async with Tekmar482Client(transport) as client, asyncio.timeout(1):
                with self.assertRaisesRegex(
                    TimeoutError,
                    "disabling reporting",
                ) as caught:
                    await client.discover(timeout=0.05, manage_reporting=True)
            assert bool(getattr(caught.exception, "__notes__", ())) == (
                not restore_replies
            )
            transport.assert_complete()

    async def test_managed_discovery_requires_restore_ack(self) -> None:
        transport = TraceReplayTransport(
            [
                update_step("ReportingState", state=0),
                request_step(
                    "FirmwareRevision",
                    response=response("FirmwareRevision", revision=154),
                ),
                request_step(
                    "ProtocolVersion",
                    response=response("ProtocolVersion", version=3),
                ),
                request_step(
                    "DeviceInventory",
                    address=0,
                    response=response("DeviceInventory", address=0),
                ),
                TraceStep(
                    update_step("ReportingState", state=1).sent,
                    (update_response("ReportingState", state=0),),
                ),
            ],
        )
        async with Tekmar482Client(transport) as client, asyncio.timeout(1):
            with self.assertRaisesRegex(TimeoutError, "restoring reporting"):
                await client.discover(timeout=0.05, manage_reporting=True)
        transport.assert_complete()

    async def test_cancelled_discovery_bounds_restore_waiting_for_wire(self) -> None:
        transport = BlockingWriteTransport()
        async with Tekmar482Client(transport) as client, asyncio.timeout(1):
            scan = asyncio.create_task(
                client.discover(timeout=0.1, manage_reporting=True),
            )
            disable = TrpcPacket.from_packet(await transport.next_write())
            assert disable == update_step("ReportingState", state=0).sent
            queued = asyncio.create_task(
                client.request_response(TrpcMethod.PROTOCOL_VERSION, timeout=None),
            )
            await asyncio.sleep(0)
            scan.cancel()
            assert (
                TrpcPacket.from_packet(await transport.next_write()).method
                == TrpcMethod.PROTOCOL_VERSION
            )
            with self.assertRaises(asyncio.CancelledError) as caught:
                await scan
            assert any(
                "Could not restore reporting" in note
                for note in caught.exception.__notes__
            )
            transport.queue_packet(response("ProtocolVersion", version=3).to_packet())
            transport.release_write()
            assert await queued is not None
            assert len(transport.written) == 2

    async def test_active_and_explicit_slot_writes_keep_arrival_order(self) -> None:
        for first, other in (
            (ThaSetback.CURRENT, ThaSetback.OCC_2),
            (ThaSetback.OCC_2, ThaSetback.CURRENT),
        ):
            transport = BlockingWriteTransport()
            async with Tekmar482Client(transport) as client, asyncio.timeout(1):
                blocker = asyncio.create_task(
                    client.request_response(TrpcMethod.PROTOCOL_VERSION),
                )
                await transport.next_write()
                tasks = [
                    asyncio.create_task(
                        client.set_latest_heat_setpoint_celsius(
                            1201,
                            temp,
                            setback=slot,
                            confirm=False,
                        ),
                    )
                    for slot, temp in ((first, 20), (other, 21), (first, 22))
                ]
                assert await tasks[0] is None
                transport.queue_packet(
                    response("ProtocolVersion", version=3).to_packet(),
                )
                transport.release_write()
                await blocker
                for expected, temp in ((other, 21), (first, 22)):
                    message = TrpcPacket.from_packet(await transport.next_write())
                    assert message.get("setback") == expected
                    assert message.get("setpoint") == celsius_to_dege(temp)
                    transport.release_write()
                await asyncio.gather(*tasks)

    async def test_explicit_setback_configuration_requires_matching_ack(self) -> None:
        transport = TraceReplayTransport(
            [
                request_step(
                    "FirmwareRevision",
                    response=response("FirmwareRevision", revision=154),
                ),
                request_step(
                    "ProtocolVersion",
                    response=response("ProtocolVersion", version=3),
                ),
                TraceStep(
                    update_step("SetbackEnable", enable=0).sent,
                    (update_response("SetbackEnable", enable=1),),
                ),
            ],
        )
        async with Tekmar482Client(transport) as client:
            with self.assertRaisesRegex(TimeoutError, "SetbackEnable=0"):
                await client.discover(setback_enable=False, timeout=0.05)
        transport.assert_complete()

    async def test_553_setpoints_follow_reported_attributes(self) -> None:
        address = 2101
        for raw, heat, cool, fan in (
            (0x19, True, False, True),
            (0x02, False, True, False),
            (0x03, True, True, False),
            (0x08, False, False, True),
            (0, False, False, False),
            (None, True, True, True),
        ):
            with self.subTest(raw=raw):
                transport = TraceReplayTransport(
                    [
                        request_step(
                            "DeviceType",
                            address=address,
                            response=response(
                                "DeviceType",
                                address=address,
                                type=105102,
                            ),
                        ),
                        request_step(
                            "DeviceVersion",
                            address=address,
                            response=response(
                                "DeviceVersion",
                                address=address,
                                j_number=1,
                            ),
                        ),
                        request_step(
                            "DeviceAttributes",
                            address=address,
                            response=response(
                                "DeviceAttributes",
                                address=address,
                                attributes=raw,
                            )
                            if raw is not None
                            else None,
                        ),
                        request_step(
                            "SetbackEvents",
                            address=address,
                            response=response(
                                "SetbackEvents",
                                address=address,
                                events=0,
                            ),
                        ),
                    ],
                )
                async with Tekmar482Client(transport) as client:
                    device = await client.get_device_info(address, timeout=0.1)
                assert device.attributes.raw == raw
                assert device.supports_heat_setpoint is heat
                assert device.supports_cool_setpoint is cool
                assert device.supports_fan_percent is fan

    def test_device_attributes_expose_firmware_capability_bits(self) -> None:
        attributes = DeviceAttributes(
            DeviceAttributeBit.SLAB_SETPOINT_ALT
            | DeviceAttributeBit.HUMIDITY_SET_MIN
            | DeviceAttributeBit.SETPOINT_DEVICE,
        )
        device = DiscoveredDevice(address=1201, attributes=attributes)

        assert attributes.slab_setpoint is True
        assert attributes.humidity is True
        assert attributes.setpoint_device is True
        assert device.kind == "setpoint"
        assert device.supports_slab_setpoint is True
        assert device.supports_setpoint_device is True
        assert device.supports_mode_setting is False

    async def test_request_response_filters_by_address(self) -> None:
        transport = NullTransport()
        transport.queue_packet(
            response("DeviceType", address=1002, type=101101).to_packet(),
        )
        transport.queue_packet(
            response("DeviceType", address=1001, type=100101).to_packet(),
        )
        client = Tekmar482Client(transport)

        await client.open()
        message = await client.request_response(
            TrpcMethod.DEVICE_TYPE,
            match_address=1001,
            fields={"address": 1001},
        )

        assert message is not None
        assert message.body == {"address": 1001, "type": 100101}
        assert (
            transport.written[0]
            == TrpcPacket.create(
                service=TrpcService.REQUEST,
                method=TrpcMethod.DEVICE_TYPE,
                fields={"address": 1001},
            ).to_packet()
        )

    async def test_set_setpoint_group_accepts_report_acknowledgement(self) -> None:
        transport = TraceReplayTransport(
            [
                TraceStep(
                    sent=TrpcPacket.create(
                        service=TrpcService.UPDATE,
                        method=TrpcMethod.SETPOINT_GROUP_ENABLE,
                        fields={"group_id": 1, "enable": 0},
                    ),
                    received=(report("SetpointGroupEnable", group_id=1, enable=0),),
                ),
            ],
        )

        async with Tekmar482Client(transport) as client:
            message = await client.set_setpoint_group(1, enabled=False)

        assert message is not None
        assert message.service == TrpcService.REPORT
        assert message.body == {"group_id": 1, "enable": 0}

    async def test_group_switch_requires_requested_state(self) -> None:
        method = TrpcMethod.SETPOINT_GROUP_ENABLE
        command = update_step(method, group_id=1, enable=1).sent
        wrong = report(method, group_id=1, enable=0)
        other = report(method, group_id=2, enable=1)
        right = report(method, group_id=1, enable=1)
        transport = TraceReplayTransport(
            [
                TraceStep(
                    command,
                    (wrong, other, update_response(method, group_id=1, enable=1)),
                ),
                TraceStep(command, (wrong, right)),
            ],
        )
        async with Tekmar482Client(transport) as client:
            for group in (0, 13):
                with self.assertRaises(ValueError):
                    await client.set_setpoint_group(group, enabled=True)
            with self.assertRaisesRegex(TimeoutError, "group 1"):
                await client.set_setpoint_group(1, enabled=True, timeout=0.02)
            assert await client.set_setpoint_group(1, enabled=True) == right
        transport.assert_complete()

    async def test_get_group_reads_the_cache_not_a_report_or_a_neighbor(self) -> None:
        method = TrpcMethod.SETPOINT_GROUP_ENABLE
        request = request_step(method, group_id=12).sent
        wrong = response(method, group_id=1, enable=1)
        unsolicited = report(method, group_id=12, enable=1)
        transport = TraceReplayTransport(
            [
                TraceStep(
                    request,
                    (wrong, unsolicited, response(method, group_id=12, enable=value)),
                )
                for value in (0, 1, 255)
            ],
        )
        async with Tekmar482Client(transport) as client:
            for expected in (False, True, None):
                assert await client.get_setpoint_group(12) is expected
            with self.assertRaises(ValueError):
                await client.get_setpoint_group(13)
        transport.assert_complete()

    async def test_get_attributes_matches_exact_read_not_push(self) -> None:
        method = TrpcMethod.DEVICE_ATTRIBUTES
        transport = TraceReplayTransport(
            [
                TraceStep(
                    request_step(method, address=1).sent,
                    (
                        report(method, address=1, attributes=4),
                        response(method, address=2, attributes=8),
                        response(method, address=1, attributes=0x103),
                    ),
                ),
            ],
        )
        async with Tekmar482Client(transport) as client:
            attrs = await client.get_device_attributes(1)
            assert attrs.raw == 0x103
            assert attrs.slab_setpoint and attrs.zone_cooling
        transport.assert_complete()

    async def test_late_attribute_reply_cannot_restore_removed_capabilities(
        self,
    ) -> None:
        method = TrpcMethod.DEVICE_ATTRIBUTES
        old = response(method, address=1, attributes=11)
        transport = TraceReplayTransport(
            [
                request_step(method, address=1),
                request_step(
                    method,
                    address=1,
                    response=response(method, address=1, attributes=1),
                ),
            ],
        )
        async with Tekmar482Client(transport) as client:
            with self.assertRaises(TimeoutError):
                await client.get_device_attributes(1, timeout=0.01)
            with self.assertRaises(TimeoutError):
                await client.get_device_attributes(1, timeout=0.01)
            assert len(transport.written) == 1
            transport.queue_message(old)
            assert (await client.get_device_attributes(1)).raw == 1
        transport.assert_complete()

    async def test_request_response_serializes_transactions(self) -> None:
        transport = BlockingTransport()
        client = Tekmar482Client(transport)
        await client.open()

        first = asyncio.create_task(
            client.request_response(
                TrpcMethod.DEVICE_TYPE,
                fields={"address": 1001},
                timeout=1,
            ),
        )
        await asyncio.sleep(0)
        second = asyncio.create_task(
            client.request_response(
                TrpcMethod.DEVICE_VERSION,
                fields={"address": 1001},
                timeout=1,
            ),
        )
        await asyncio.sleep(0)

        assert [TrpcPacket.from_packet(item).method for item in transport.written] == [
            TrpcMethod.DEVICE_TYPE,
        ]

        transport.queue_packet(
            response("DeviceType", address=1001, type=107201).to_packet(),
        )
        first_response = await first
        await asyncio.sleep(0)

        assert first_response is not None
        assert first_response.method == TrpcMethod.DEVICE_TYPE
        assert [TrpcPacket.from_packet(item).method for item in transport.written] == [
            TrpcMethod.DEVICE_TYPE,
            TrpcMethod.DEVICE_VERSION,
        ]

        transport.queue_packet(
            response("DeviceVersion", address=1001, j_number=123402).to_packet(),
        )
        second_response = await second

        assert second_response is not None
        assert second_response.method == TrpcMethod.DEVICE_VERSION

    async def test_discover_builds_gateway_and_device_models(self) -> None:
        transport = TraceReplayTransport(
            [
                request_step(
                    "FirmwareRevision",
                    response=response("FirmwareRevision", revision=154),
                ),
                request_step(
                    "ProtocolVersion",
                    response=response("ProtocolVersion", version=3),
                ),
                request_step(
                    "DeviceInventory",
                    address=0,
                    received=(
                        response("DeviceInventory", address=1001),
                        response("DeviceInventory", address=0),
                    ),
                ),
                request_step(
                    "DeviceType",
                    address=1001,
                    response=response("DeviceType", address=1001, type=100101),
                ),
                request_step(
                    "DeviceVersion",
                    address=1001,
                    response=response(
                        "DeviceVersion",
                        address=1001,
                        j_number=202405,
                    ),
                ),
                request_step(
                    "DeviceAttributes",
                    address=1001,
                    response=response(
                        "DeviceAttributes",
                        address=1001,
                        attributes=0b1011,
                    ),
                ),
                request_step(
                    "SetbackEvents",
                    address=1001,
                    response=response("SetbackEvents", address=1001, events=7),
                ),
            ],
        )

        client = Tekmar482Client(transport)
        await client.open()

        result = await client.discover(write_delay=0)

        assert result.gateway.firmware_revision == 154
        assert result.gateway.protocol_version == 3
        assert len(result.devices) == 1

        device = result.devices[0]
        assert device.address == 1001
        assert device.model == "540"
        assert device.kind == "thermostat"
        assert device.version == 202405
        assert device.setback_events is None
        assert device.attributes.zone_heating is True
        assert device.attributes.zone_cooling is True
        assert device.attributes.slab_setpoint is False
        assert device.attributes.fan_percent is True

    async def test_discover_normalizes_not_available_device_type(self) -> None:
        transport = TraceReplayTransport(
            [
                request_step(
                    "FirmwareRevision",
                    response=response("FirmwareRevision", revision=154),
                ),
                request_step(
                    "ProtocolVersion",
                    response=response("ProtocolVersion", version=3),
                ),
                request_step(
                    "DeviceInventory",
                    address=0,
                    received=(
                        response("DeviceInventory", address=201),
                        response("DeviceInventory", address=0),
                    ),
                ),
                request_step(
                    "DeviceType",
                    address=201,
                    response=response("DeviceType", address=201, type=0xFFFFFFFF),
                ),
                request_step(
                    "DeviceVersion",
                    address=201,
                    response=response(
                        "DeviceVersion",
                        address=201,
                        j_number=0xFFFFFFFF,
                    ),
                ),
                request_step(
                    "DeviceAttributes",
                    address=201,
                    response=response(
                        "DeviceAttributes",
                        address=201,
                        attributes=0b0001,
                    ),
                ),
                request_step(
                    "SetbackEvents",
                    address=201,
                    response=response("SetbackEvents", address=201, events=0),
                ),
            ],
        )

        client = Tekmar482Client(transport)
        await client.open()

        result = await client.discover(write_delay=0)

        device = result.devices[0]
        assert device.type_code is None
        assert device.version is None
        assert device.is_known_type is False
        assert device.kind == "thermostat"
        assert device.address_parts == (0, 2, 1)

    async def test_targeted_inventory_has_no_terminator(self) -> None:
        method = TrpcMethod.DEVICE_INVENTORY
        transport = TraceReplayTransport(
            [
                request_step(
                    method,
                    address=1201,
                    response=response(method, address=1201),
                ),
                request_step(
                    method,
                    address=0,
                    received=(
                        response(method, address=1202),
                        response(method, address=0),
                    ),
                ),
            ],
        )
        async with Tekmar482Client(transport) as client, asyncio.timeout(1):
            assert await client.get_inventory_addresses(start_address=1201) == (1201,)
            assert await client.get_inventory_addresses() == (1202,)
        transport.assert_complete()

    async def test_inventory_stops_on_not_available_address(self) -> None:
        transport = TraceReplayTransport(
            [
                request_step(
                    "DeviceInventory",
                    address=1,
                    response=response("DeviceInventory", address=0xFFFF),
                ),
            ],
        )
        client = Tekmar482Client(transport)
        await client.open()

        addresses = await client.get_inventory_addresses(start_address=1)

        assert addresses == ()
        assert (
            transport.written[0]
            == TrpcPacket.create(
                service=TrpcService.REQUEST,
                method=TrpcMethod.DEVICE_INVENTORY,
                fields={"address": 1},
            ).to_packet()
        )

    async def test_incomplete_inventory_does_not_complete_the_next_scan(self) -> None:
        method = TrpcMethod.DEVICE_INVENTORY
        transport = TraceReplayTransport(
            [
                request_step(
                    method,
                    address=0,
                    response=response(method, address=1201),
                ),
                request_step(
                    method,
                    address=0,
                    received=(
                        response(method, address=2101),
                        response(method, address=0),
                    ),
                ),
            ],
        )
        async with Tekmar482Client(transport) as client:
            with self.assertRaises(TimeoutError):
                await client.get_inventory_addresses(timeout=0.02)
            retry = asyncio.create_task(client.get_inventory_addresses(timeout=1))
            await asyncio.sleep(0)
            assert len(transport.written) == 1
            transport.queue_message(response(method, address=1202))
            transport.queue_message(response(method, address=0))
            assert await retry == (2101,)
        transport.assert_complete()

    async def test_cancelled_inventory_recovers_on_reconnect(self) -> None:
        method = TrpcMethod.DEVICE_INVENTORY
        version = TrpcMethod.PROTOCOL_VERSION
        transport = TraceReplayTransport(
            [
                request_step(
                    method,
                    address=0,
                    response=response(method, address=1201),
                ),
                request_step(version, response=response(version, version=3)),
                request_step(method, address=0, response=response(method, address=0)),
            ],
        )
        client = Tekmar482Client(transport)
        async with client, asyncio.timeout(1):
            walk = asyncio.create_task(client.get_inventory_addresses(timeout=None))
            assert await client.read_packet(timeout=1) is not None
            walk.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await walk
            with self.assertRaises(InventoryDrainTimeoutError):
                await client.get_inventory_addresses(timeout=0.02)
            assert await client.request_response(version) is not None
        async with client:
            assert await client.get_inventory_addresses(timeout=1) == ()
        transport.assert_complete()

    async def test_inventory_ignores_update_responses(self) -> None:
        transport = TraceReplayTransport(
            [
                request_step(
                    "DeviceInventory",
                    address=0,
                    received=(
                        update_response(TrpcMethod.DEVICE_INVENTORY, address=999),
                        response("DeviceInventory", address=1001),
                        response("DeviceInventory", address=0),
                    ),
                ),
            ],
        )
        client = Tekmar482Client(transport)
        await client.open()

        addresses = await client.get_inventory_addresses()
        stale = await client.read_message(timeout=0.01)

        assert addresses == (1001,)
        assert stale is not None
        assert stale.service == TrpcService.RESPONSE_UPDATE
        assert stale.body == {"address": 999}

    async def test_dump_available_info_builds_snapshot(self) -> None:
        transport = TraceReplayTransport(
            [
                request_step(
                    "FirmwareRevision",
                    response=response("FirmwareRevision", revision=154),
                ),
                request_step(
                    "ProtocolVersion",
                    response=response("ProtocolVersion", version=3),
                ),
                request_step(
                    "DeviceInventory",
                    address=0,
                    received=(
                        response("DeviceInventory", address=1001),
                        response("DeviceInventory", address=0),
                    ),
                ),
                request_step(
                    "DeviceType",
                    address=1001,
                    response=response("DeviceType", address=1001, type=100101),
                ),
                request_step(
                    "DeviceVersion",
                    address=1001,
                    response=response(
                        "DeviceVersion",
                        address=1001,
                        j_number=202405,
                    ),
                ),
                request_step(
                    "DeviceAttributes",
                    address=1001,
                    response=response(
                        "DeviceAttributes",
                        address=1001,
                        attributes=0b1011,
                    ),
                ),
                request_step(
                    "SetbackEvents",
                    address=1001,
                    response=response("SetbackEvents", address=1001, events=2),
                ),
                request_step(
                    "ReportingState",
                    response=response("ReportingState", state=1),
                ),
                request_step(
                    "SetbackEnable",
                    response=response("SetbackEnable", enable=0),
                ),
                request_step(
                    "OutdoorTemp",
                    response=response("OutdoorTemp", temp=1550),
                ),
                request_step(
                    "NetworkError",
                    response=response("NetworkError", error=0),
                ),
                *(
                    request_step(
                        "SetpointGroupEnable",
                        group_id=group,
                        response=response(
                            "SetpointGroupEnable",
                            group_id=group,
                            enable=int(group == 1),
                        ),
                    )
                    for group in range(1, 13)
                ),
                request_step(
                    "CurrentTemperature",
                    address=1001,
                    response=response(
                        "CurrentTemperature",
                        address=1001,
                        temp=1550,
                    ),
                ),
                request_step(
                    "ActiveDemand",
                    address=1001,
                    response=response("ActiveDemand", address=1001, demand=1),
                ),
                request_step(
                    "SetbackState",
                    address=1001,
                    response=response("SetbackState", address=1001, setback=7),
                ),
                request_step(
                    "ModeSetting",
                    address=1001,
                    response=response("ModeSetting", address=1001, mode=2),
                ),
                request_step(
                    "HeatSetpoint",
                    address=1001,
                    setback=7,
                    response=response(
                        "HeatSetpoint",
                        address=1001,
                        setback=7,
                        setpoint=42,
                    ),
                ),
                request_step(
                    "CoolSetpoint",
                    address=1001,
                    setback=7,
                    response=response(
                        "CoolSetpoint",
                        address=1001,
                        setback=7,
                        setpoint=48,
                    ),
                ),
                request_step(
                    "FanPercent",
                    address=1001,
                    setback=7,
                    response=response(
                        "FanPercent",
                        address=1001,
                        setback=7,
                        percent=55,
                    ),
                ),
                *(
                    request_step(
                        method,
                        address=1001,
                        setback=slot,
                        response=response(method, address=1001, setback=slot, **fields),
                    )
                    for method, fields in (
                        ("HeatSetpoint", {"setpoint": 42}),
                        ("CoolSetpoint", {"setpoint": 48}),
                        ("FanPercent", {"percent": 55}),
                    )
                    for slot in _CURRENT_FIRST[1:]
                ),
            ],
        )

        client = Tekmar482Client(transport)
        await client.open()

        snapshot = await client.dump_available_info(write_delay=0)
        transport.assert_complete()

        assert snapshot.gateway.info.firmware_revision == 154
        assert snapshot.gateway.outdoor_temp == 1550
        assert snapshot.gateway.network_error == 0
        assert snapshot.gateway.reporting_enabled is True
        assert snapshot.gateway.setback_enabled is False
        assert snapshot.gateway.setpoint_groups is not None
        assert snapshot.gateway.setpoint_groups[1] is True
        assert snapshot.gateway.setpoint_groups[2] is False

        device = snapshot.devices[0]
        assert device.info.address == 1001
        assert device.info.setback_events == 4
        assert device.values["current_temperature"] == 1550
        assert device.values["active_demand"] == 1
        assert device.values["setback_state"] == 7
        assert device.values["mode_setting"] == 2
        assert device.runtime.heat_setpoints.current == 42
        assert device.runtime.cool_setpoints.current == 48
        assert device.runtime.fan_percent.current == 55
        assert device.decoded_values["heat_setpoints"] is not None

    async def test_poll_runtime_uses_existing_discovery_without_inventory_walk(
        self,
    ) -> None:
        transport = TraceReplayTransport(
            [
                request_step(
                    "ReportingState",
                    response=response("ReportingState", state=1),
                ),
                request_step(
                    "SetbackEnable",
                    response=response("SetbackEnable", enable=0),
                ),
                request_step(
                    "OutdoorTemp",
                    response=response("OutdoorTemp", temp=1550),
                ),
                request_step(
                    "NetworkError",
                    response=response("NetworkError", error=0),
                ),
                request_step(
                    "CurrentTemperature",
                    address=1001,
                    response=response(
                        "CurrentTemperature",
                        address=1001,
                        temp=1550,
                    ),
                ),
                request_step(
                    "ActiveDemand",
                    address=1001,
                    response=response("ActiveDemand", address=1001, demand=0),
                ),
                request_step(
                    "SetbackState",
                    address=1001,
                    response=response("SetbackState", address=1001, setback=4),
                ),
                request_step(
                    "ModeSetting",
                    address=1001,
                    response=response("ModeSetting", address=1001, mode=1),
                ),
                request_step(
                    "HeatSetpoint",
                    address=1001,
                    setback=ThaSetback.CURRENT,
                    response=response(
                        "HeatSetpoint",
                        address=1001,
                        setback=4,
                        setpoint=42,
                    ),
                ),
            ],
        )

        client = Tekmar482Client(transport)
        await client.open()
        discovery = DiscoveryResult(
            gateway=GatewayInfo(firmware_revision=154, protocol_version=3),
            devices=(
                DiscoveredDevice(
                    address=1001,
                    type_code=107201,
                    attributes=DeviceAttributes(0b0001),
                ),
            ),
        )

        snapshot = await client.poll_runtime(
            discovery,
            include_setpoint_groups=False,
            include_setbacks=False,
            write_delay=0,
        )

        written_methods = [
            TrpcPacket.from_packet(packet).method for packet in transport.written
        ]
        assert TrpcMethod.DEVICE_INVENTORY not in written_methods
        assert TrpcMethod.DEVICE_TYPE not in written_methods
        assert snapshot.gateway.outdoor_temp == 1550
        assert snapshot.devices[0].values["current_temperature"] == 1550

    async def test_poll_devices_uses_attribute_capabilities_for_unknown_models(
        self,
    ) -> None:
        transport = TraceReplayTransport(
            [
                request_step(
                    "CurrentTemperature",
                    address=1201,
                    response=response(
                        "CurrentTemperature",
                        address=1201,
                        temp=1550,
                    ),
                ),
                request_step(
                    "ActiveDemand",
                    address=1201,
                    response=response("ActiveDemand", address=1201, demand=0),
                ),
                request_step(
                    "SetbackState",
                    address=1201,
                    response=response("SetbackState", address=1201, setback=7),
                ),
                request_step(
                    "CurrentFloorTemperature",
                    address=1201,
                    response=response(
                        "CurrentFloorTemperature",
                        address=1201,
                        temp=0xFFFF,
                    ),
                ),
                request_step(
                    "SlabSetpoint",
                    address=1201,
                    setback=7,
                    response=response(
                        "SlabSetpoint",
                        address=1201,
                        setback=7,
                        setpoint=40,
                    ),
                ),
                request_step(
                    "SetpointDevice",
                    address=1201,
                    setback=7,
                    response=response(
                        "SetpointDevice",
                        address=1201,
                        setback=7,
                        temp=400,
                    ),
                ),
            ],
        )

        client = Tekmar482Client(transport)
        await client.open()
        device = DiscoveredDevice(
            address=1201,
            attributes=DeviceAttributes(
                DeviceAttributeBit.SLAB_SETPOINT_ALT
                | DeviceAttributeBit.SETPOINT_DEVICE,
            ),
        )

        snapshots = await client.poll_devices(
            (device,),
            protocol_version=3,
            include_setbacks=False,
            write_delay=0,
        )

        written_methods = [
            TrpcPacket.from_packet(packet).method for packet in transport.written
        ]
        assert TrpcMethod.MODE_SETTING not in written_methods
        assert TrpcMethod.SLAB_SETPOINT in written_methods
        assert TrpcMethod.SETPOINT_DEVICE in written_methods
        assert snapshots[0].values["slab_setpoints"] == {"current": 40}
        assert snapshots[0].values["setpoint_targets"] == {"current": 400}

    async def test_dump_setpoints_match_requested_setback(self) -> None:
        transport = TraceReplayTransport(
            [
                *_runtime_seed_steps(),
                *(
                    request_step(
                        "HeatSetpoint",
                        address=1001,
                        setback=setback,
                        response=(
                            response(
                                "HeatSetpoint",
                                address=1001,
                                setback=6,
                                setpoint=33,
                            )
                            if setback is ThaSetback.AWAY
                            else response(
                                "HeatSetpoint",
                                address=1001,
                                setback=0,
                                setpoint=44,
                            )
                            if setback in (ThaSetback.WAKE_4, ThaSetback.CURRENT)
                            else None
                        ),
                    )
                    for setback in _CURRENT_FIRST
                ),
            ],
        )

        client = Tekmar482Client(transport)
        await client.open()

        snapshots = await client.poll_devices(
            (_heat_setpoint_device(),),
            protocol_version=2,
            include_setbacks=True,
            timeout=0.01,
            write_delay=0,
        )
        values = snapshots[0].runtime.heat_setpoints

        assert values.wake_4 == 44
        assert values.away == 33

    async def test_dump_setpoints_requests_all_setbacks(self) -> None:
        transport = TraceReplayTransport(
            [
                *_runtime_seed_steps(),
                *(
                    request_step(
                        "HeatSetpoint",
                        address=1001,
                        setback=setback,
                        response=response(
                            "HeatSetpoint",
                            address=1001,
                            setback=setback,
                            setpoint=40 + setback,
                        ),
                    )
                    for setback in _CURRENT_FIRST
                ),
            ],
        )
        client = Tekmar482Client(transport)
        await client.open()

        snapshots = await client.poll_devices(
            (_heat_setpoint_device(),),
            protocol_version=2,
            include_setbacks=True,
            timeout=0.01,
        )
        values = snapshots[0].runtime.heat_setpoints

        assert [
            TrpcPacket.from_packet(packet).body["setback"]
            for packet in transport.written
            if TrpcPacket.from_packet(packet).method is TrpcMethod.HEAT_SETPOINT
        ] == list(_CURRENT_FIRST)
        assert values.as_dict() == {
            "current": 47,
            "wake_4": 44,
            "occ_4": 44,
            "occ_2": 44,
            "unocc_4": 45,
            "sleep_4": 45,
            "unocc_2": 45,
            "away": 46,
        }

    async def test_dump_current_setpoint_accepts_reported_active_setback(self) -> None:
        transport = TraceReplayTransport(
            [
                *_runtime_seed_steps(setback=ThaSetback.OCC_2),
                request_step(
                    "HeatSetpoint",
                    address=1001,
                    setback=ThaSetback.CURRENT,
                    response=response(
                        "HeatSetpoint",
                        address=1001,
                        setback=ThaSetback.OCC_2,
                        setpoint=42,
                    ),
                ),
            ],
        )
        client = Tekmar482Client(transport)
        await client.open()

        snapshots = await client.poll_devices(
            (_heat_setpoint_device(),),
            protocol_version=2,
            include_setbacks=False,
            timeout=0.01,
        )
        values = snapshots[0].runtime.heat_setpoints

        assert values.current == 42

    async def test_dump_all_setpoints_uses_current_observed_setback(self) -> None:
        method = TrpcMethod.HEAT_SETPOINT
        transport = TraceReplayTransport(
            [
                *_runtime_seed_steps(),
                *(
                    request_step(
                        method,
                        address=1001,
                        setback=slot,
                        response=response(
                            method,
                            address=1001,
                            setback=ThaSetback.OCC_2
                            if slot is ThaSetback.CURRENT
                            else slot,
                            setpoint=52
                            if slot is ThaSetback.CURRENT
                            else 40 + int(slot),
                        ),
                    )
                    for slot in _CURRENT_FIRST
                ),
            ],
        )
        async with Tekmar482Client(transport) as client:
            snapshots = await client.poll_devices(
                (_heat_setpoint_device(),),
                protocol_version=2,
                include_setbacks=True,
                timeout=0.01,
            )
        transport.assert_complete()
        values = snapshots[0].runtime.heat_setpoints

        assert values.occ_2 == 44
        assert values.current == 44

    async def test_runtime_slots_follow_advertised_schedule(self) -> None:
        method = TrpcMethod.HEAT_SETPOINT
        for events, slots in ((0, (7,)), (2, (7, 4, 5, 6)), (4, (7, 0, 1, 2, 3, 6))):
            with self.subTest(events=events):
                transport = TraceReplayTransport(
                    [
                        *_runtime_seed_steps(),
                        *(
                            request_step(
                                method,
                                address=1001,
                                setback=slot,
                                response=response(
                                    method,
                                    address=1001,
                                    setback=slot,
                                    setpoint=42,
                                )
                                if slot == 7
                                else None,
                            )
                            for slot in slots
                        ),
                    ],
                )
                async with Tekmar482Client(transport) as client:
                    (snapshot,) = await client.poll_devices(
                        (_heat_setpoint_device(events=events),),
                        protocol_version=2,
                        include_setbacks=True,
                        timeout=0.01,
                    )
                transport.assert_complete()
                assert snapshot.runtime.heat_setpoints.current == 42

    async def test_unknown_reply_slot_does_not_overwrite_current(self) -> None:
        method = TrpcMethod.HEAT_SETPOINT
        transport = TraceReplayTransport(
            [
                *_runtime_seed_steps(setback=ThaSetback.OCC_2),
                request_step(
                    method,
                    address=1001,
                    setback=ThaSetback.CURRENT,
                    received=(
                        report(
                            method,
                            address=1001,
                            setback=ThaSetback.OCC_2,
                            setpoint=48,
                        ),
                        response(method, address=1001, setback=255, setpoint=60),
                    ),
                ),
            ],
        )
        async with Tekmar482Client(transport) as client:
            (snapshot,) = await client.poll_devices(
                (_heat_setpoint_device(),),
                protocol_version=2,
                timeout=0.01,
            )
        assert snapshot.runtime.heat_setpoints.current == 48
        assert snapshot.runtime.heat_setpoints.occ_2 == 48
        transport.assert_complete()

    async def test_unknown_slot_does_not_consume_a_pending_current_request(
        self,
    ) -> None:
        method = TrpcMethod.HEAT_SETPOINT
        transport = TraceReplayTransport(
            [
                *_runtime_seed_steps(),
                request_step(
                    method,
                    address=1001,
                    setback=ThaSetback.CURRENT,
                    received=(
                        response(method, address=1001, setback=255, setpoint=60),
                        response(
                            method,
                            address=1001,
                            setback=ThaSetback.WAKE_4,
                            setpoint=52,
                        ),
                    ),
                ),
            ],
        )
        async with Tekmar482Client(transport) as client:
            (snapshot,) = await client.poll_devices(
                (_heat_setpoint_device(),),
                protocol_version=2,
                timeout=0.01,
            )
        values = snapshot.runtime.heat_setpoints
        assert values.current == values.wake_4 == 52
        assert values.current_observed_setback is ThaSetback.WAKE_4
        transport.assert_complete()

    async def test_celsius_setters_encode_tna_setpoints(self) -> None:
        transport = TraceReplayTransport(
            [
                update_step(
                    TrpcMethod.HEAT_SETPOINT,
                    address=1001,
                    setback=7,
                    setpoint=42,
                ),
                update_step(
                    TrpcMethod.COOL_SETPOINT,
                    address=1001,
                    setback=7,
                    setpoint=48,
                ),
                update_step(
                    TrpcMethod.SLAB_SETPOINT,
                    address=1001,
                    setback=7,
                    setpoint=40,
                ),
                update_step(
                    TrpcMethod.FAN_PERCENT,
                    address=1001,
                    setback=7,
                    percent=55,
                ),
                update_step(
                    TrpcMethod.SETPOINT_DEVICE,
                    address=1001,
                    setback=7,
                    temp=1548,
                ),
                TraceStep(
                    sent=TrpcPacket.create(
                        service=TrpcService.UPDATE,
                        method=TrpcMethod.MODE_SETTING,
                        fields={"address": 1001, "mode": 1},
                    ),
                ),
            ],
        )
        client = Tekmar482Client(transport)
        await client.open()

        await client.set_heat_setpoint_celsius(1001, 21.0)
        await client.set_cool_setpoint_celsius(1001, 24.0)
        await client.set_slab_setpoint_celsius(1001, 20.0)
        await client.set_fan_percent(1001, 55)
        await client.set_setpoint_device_celsius(1001, 21.0)
        await client.set_mode(1001, 1)

        messages = [TrpcPacket.from_packet(packet) for packet in transport.written]
        assert [(item.method, item.body) for item in messages] == [
            (
                TrpcMethod.HEAT_SETPOINT,
                {"address": 1001, "setback": 7, "setpoint": 42},
            ),
            (
                TrpcMethod.COOL_SETPOINT,
                {"address": 1001, "setback": 7, "setpoint": 48},
            ),
            (
                TrpcMethod.SLAB_SETPOINT,
                {"address": 1001, "setback": 7, "setpoint": 40},
            ),
            (
                TrpcMethod.FAN_PERCENT,
                {"address": 1001, "setback": 7, "percent": 55},
            ),
            (
                TrpcMethod.SETPOINT_DEVICE,
                {"address": 1001, "setback": 7, "temp": 1548},
            ),
            (
                TrpcMethod.MODE_SETTING,
                {"address": 1001, "mode": 1},
            ),
        ]

    async def test_set_fan_percent_validates_range(self) -> None:
        client = Tekmar482Client(NullTransport())

        try:
            await client.set_fan_percent(1001, 101)
        except ValueError as err:
            assert "percent must be between 0 and 100" in str(err)
        else:
            raise AssertionError(_EXPECTED_VALUE_ERROR)

    async def test_set_mode_replaces_unsent_mode_for_same_address(self) -> None:
        transport = BlockingWriteTransport()
        client = Tekmar482Client(transport)

        async with client:
            first = asyncio.create_task(client.set_mode(1001, DeviceMode.HEAT))
            first_packet = await asyncio.wait_for(transport.next_write(), timeout=1)

            second = asyncio.create_task(client.set_mode(1001, DeviceMode.COOL))
            await asyncio.sleep(0)
            third = asyncio.create_task(client.set_mode(1001, DeviceMode.AUTO))

            assert await asyncio.wait_for(second, timeout=1) is None

            transport.release_write()
            first_result = await asyncio.wait_for(first, timeout=1)
            third_packet = await asyncio.wait_for(transport.next_write(), timeout=1)
            transport.release_write()
            third_result = await asyncio.wait_for(third, timeout=1)

        assert first_result is not None
        assert third_result is not None
        assert TrpcPacket.from_packet(first_packet).body == {
            "address": 1001,
            "mode": DeviceMode.HEAT,
        }
        assert TrpcPacket.from_packet(third_packet).body == {
            "address": 1001,
            "mode": DeviceMode.AUTO,
        }
        assert [
            TrpcPacket.from_packet(packet).body["mode"] for packet in transport.written
        ] == [
            DeviceMode.HEAT,
            DeviceMode.AUTO,
        ]


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


class BlockingTransport:
    def __init__(self) -> None:
        self.written: list[Packet] = []
        self._packets: asyncio.Queue[Packet] = asyncio.Queue()
        self._is_open = False

    @property
    def is_open(self) -> bool:
        return self._is_open

    async def open(self) -> None:
        self._is_open = True

    async def close(self) -> None:
        self._is_open = False

    async def read_packet(self, timeout: float | None = None) -> Packet | None:
        try:
            if timeout is None:
                return await self._packets.get()
            return await asyncio.wait_for(self._packets.get(), timeout)
        except TimeoutError:
            return None

    async def write_packet(self, packet: Packet) -> None:
        self.written.append(packet)

    def queue_packet(self, packet: Packet) -> None:
        self._packets.put_nowait(packet)
