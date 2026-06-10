import asyncio
import unittest

from tekmar_482 import (
    AddressSetbackFields,
    DeviceAttributeBit,
    DeviceAttributes,
    DeviceMode,
    DiscoveredDevice,
    DiscoveryResult,
    GatewayInfo,
    NullTransport,
    Packet,
    Tekmar482Client,
    ThaSetback,
    TraceReplayTransport,
    TraceStep,
    TrpcMethod,
    TrpcMethodId,
    TrpcPacket,
    TrpcService,
    method_from_name,
)

_EXPECTED_VALUE_ERROR = "expected ValueError"


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


def _heat_setpoint_device(address: int = 1001) -> DiscoveredDevice:
    return DiscoveredDevice(
        address=address,
        attributes=DeviceAttributes(DeviceAttributeBit.HEAT_SETPOINT),
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


class CurrentObservedSetbackClient(Tekmar482Client):
    async def request_response(
        self,
        method: TrpcMethodId,
        *,
        response_method: TrpcMethodId | None = None,
        response_services: object | None = None,
        match_address: int | None = None,
        match_fields: object | None = None,
        timeout: float | None = 5,
        fields: object | None = None,
    ) -> TrpcPacket | None:
        del response_method, response_services, match_address, match_fields, timeout
        if method == TrpcMethod.CURRENT_TEMPERATURE:
            return response(method, address=1001, temp=1550)
        if method == TrpcMethod.ACTIVE_DEMAND:
            return response(method, address=1001, demand=0)
        if method == TrpcMethod.SETBACK_STATE:
            return response(method, address=1001, setback=ThaSetback.CURRENT)
        if method != TrpcMethod.HEAT_SETPOINT or not isinstance(
            fields,
            AddressSetbackFields,
        ):
            return None

        setback = ThaSetback(fields.setback)
        if setback is ThaSetback.OCC_2:
            return None
        if setback is ThaSetback.CURRENT:
            return response(
                method,
                address=fields.address,
                setback=ThaSetback.OCC_2,
                setpoint=52,
            )
        return response(
            method,
            address=fields.address,
            setback=setback,
            setpoint=40 + int(setback),
        )


class ClientDiscoveryTest(unittest.IsolatedAsyncioTestCase):
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
        assert device.setback_events == 7
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
                    response=response("SetbackEvents", address=1001, events=7),
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
                request_step(
                    "DateTime",
                    response=response(
                        "DateTime",
                        year=2026,
                        month=6,
                        day=9,
                        weekday=2,
                        hour=21,
                        minute=46,
                    ),
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
                    "CurrentFloorTemperature",
                    address=1001,
                    response=response(
                        "CurrentFloorTemperature",
                        address=1001,
                        temp=0xFFFF,
                    ),
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
            ],
        )

        client = Tekmar482Client(transport)
        await client.open()

        snapshot = await client.dump_available_info(
            include_setbacks=False,
            write_delay=0,
        )

        assert snapshot.gateway.info.firmware_revision == 154
        assert snapshot.gateway.outdoor_temp == 1550
        assert snapshot.gateway.network_error == 0
        assert snapshot.gateway.date_time is not None
        assert snapshot.gateway.date_time.as_dict() == {
            "year": 2026,
            "month": 6,
            "day": 9,
            "weekday": 2,
            "hour": 21,
            "minute": 46,
        }
        assert snapshot.gateway.reporting_enabled is True
        assert snapshot.gateway.setback_enabled is False
        assert snapshot.gateway.setpoint_groups is not None
        assert snapshot.gateway.setpoint_groups[1] is True
        assert snapshot.gateway.setpoint_groups[2] is False

        device = snapshot.devices[0]
        assert device.info.address == 1001
        assert device.values["current_temperature"] == 1550
        assert device.values["active_demand"] == 1
        assert device.values["setback_state"] == 7
        assert device.values["mode_setting"] == 2
        assert device.values["heat_setpoints"] == {"current": 42}
        assert device.values["cool_setpoints"] == {"current": 48}
        assert device.values["fan_percent"] == {"current": 55}
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
                    "CurrentFloorTemperature",
                    address=1001,
                    response=response(
                        "CurrentFloorTemperature",
                        address=1001,
                        temp=0xFFFF,
                    ),
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
                            if setback is ThaSetback.WAKE_4
                            else None
                        ),
                    )
                    for setback in ThaSetback
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
                    for setback in ThaSetback
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
        ] == [int(setback) for setback in ThaSetback]
        assert values.as_dict() == {
            setback.name.lower(): 40 + int(setback) for setback in ThaSetback
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
        client = CurrentObservedSetbackClient(NullTransport())

        snapshots = await client.poll_devices(
            (_heat_setpoint_device(),),
            protocol_version=2,
            include_setbacks=True,
            timeout=0.01,
        )
        values = snapshots[0].runtime.heat_setpoints

        assert values.current == 52
        assert values.occ_2 == 52

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
