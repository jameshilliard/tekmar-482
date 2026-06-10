import asyncio
import unittest

from tekmar_482 import (
    AvailableInfo,
    DiscoveryResult,
    NullTransport,
    ProtocolError,
    Tekmar482Client,
    Tekmar482Controller,
    TraceReplayTransport,
    TraceStep,
    TrpcMethod,
    TrpcMethodId,
    TrpcPacket,
    TrpcService,
)

_DISCOVERY_FAILED = "discovery failed"


def request_step(
    method: TrpcMethodId,
    *,
    response: TrpcPacket | None = None,
    received: tuple[TrpcPacket, ...] = (),
    **fields: int,
) -> TraceStep:
    return TraceStep(
        sent=TrpcPacket.create(
            service=TrpcService.REQUEST,
            method=method,
            fields=fields,
        ),
        received=received if response is None else (response,),
    )


def update_step(method: TrpcMethodId, **fields: int) -> TraceStep:
    return TraceStep(
        sent=TrpcPacket.create(
            service=TrpcService.UPDATE,
            method=method,
            fields=fields,
        ),
        received=(update_response(method, **fields),),
    )


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


def discovery_steps() -> tuple[TraceStep, ...]:
    return (
        request_step(
            TrpcMethod.FIRMWARE_REVISION,
            response=response(TrpcMethod.FIRMWARE_REVISION, revision=154),
        ),
        request_step(
            TrpcMethod.PROTOCOL_VERSION,
            response=response(TrpcMethod.PROTOCOL_VERSION, version=3),
        ),
        request_step(
            TrpcMethod.DEVICE_INVENTORY,
            address=0,
            received=(
                response(TrpcMethod.DEVICE_INVENTORY, address=1201),
                response(TrpcMethod.DEVICE_INVENTORY, address=0),
            ),
        ),
        request_step(
            TrpcMethod.DEVICE_TYPE,
            address=1201,
            response=response(TrpcMethod.DEVICE_TYPE, address=1201, type=107201),
        ),
        request_step(
            TrpcMethod.DEVICE_VERSION,
            address=1201,
            response=response(
                TrpcMethod.DEVICE_VERSION,
                address=1201,
                j_number=202405,
            ),
        ),
        request_step(
            TrpcMethod.DEVICE_ATTRIBUTES,
            address=1201,
            response=response(
                TrpcMethod.DEVICE_ATTRIBUTES,
                address=1201,
                attributes=1,
            ),
        ),
        request_step(
            TrpcMethod.SETBACK_EVENTS,
            address=1201,
            response=response(TrpcMethod.SETBACK_EVENTS, address=1201, events=7),
        ),
    )


def runtime_steps(
    *,
    current_temperature: int = 1550,
    reporting_state: int = 0,
) -> tuple[TraceStep, ...]:
    return (
        request_step(
            TrpcMethod.REPORTING_STATE,
            response=response(TrpcMethod.REPORTING_STATE, state=reporting_state),
        ),
        request_step(
            TrpcMethod.SETBACK_ENABLE,
            response=response(TrpcMethod.SETBACK_ENABLE, enable=0),
        ),
        request_step(
            TrpcMethod.OUTDOOR_TEMP,
            response=response(TrpcMethod.OUTDOOR_TEMP, temp=1550),
        ),
        request_step(
            TrpcMethod.NETWORK_ERROR,
            response=response(TrpcMethod.NETWORK_ERROR, error=0),
        ),
        request_step(
            TrpcMethod.DATE_TIME,
            response=response(
                TrpcMethod.DATE_TIME,
                year=2026,
                month=6,
                day=9,
                weekday=2,
                hour=21,
                minute=46,
            ),
        ),
        request_step(
            TrpcMethod.CURRENT_TEMPERATURE,
            address=1201,
            response=response(
                TrpcMethod.CURRENT_TEMPERATURE,
                address=1201,
                temp=current_temperature,
            ),
        ),
        request_step(
            TrpcMethod.ACTIVE_DEMAND,
            address=1201,
            response=response(TrpcMethod.ACTIVE_DEMAND, address=1201, demand=0),
        ),
        request_step(
            TrpcMethod.SETBACK_STATE,
            address=1201,
            response=response(TrpcMethod.SETBACK_STATE, address=1201, setback=7),
        ),
        request_step(
            TrpcMethod.CURRENT_FLOOR_TEMPERATURE,
            address=1201,
            response=response(
                TrpcMethod.CURRENT_FLOOR_TEMPERATURE,
                address=1201,
                temp=0xFFFF,
            ),
        ),
        request_step(
            TrpcMethod.MODE_SETTING,
            address=1201,
            response=response(TrpcMethod.MODE_SETTING, address=1201, mode=1),
        ),
        request_step(
            TrpcMethod.HEAT_SETPOINT,
            address=1201,
            setback=7,
            response=response(
                TrpcMethod.HEAT_SETPOINT,
                address=1201,
                setback=7,
                setpoint=42,
            ),
        ),
    )


def start_steps() -> tuple[TraceStep, ...]:
    return (
        update_step(TrpcMethod.REPORTING_STATE, state=0),
        *discovery_steps(),
        *runtime_steps(),
        update_step(TrpcMethod.REPORTING_STATE, state=1),
    )


def reporting_update_states(transport: TraceReplayTransport) -> list[int]:
    return [
        message.require("state")
        for packet in transport.written
        if (message := TrpcPacket.from_packet(packet)).service == TrpcService.UPDATE
        and message.method == TrpcMethod.REPORTING_STATE
    ]


class FailingStartClient(Tekmar482Client):
    def __init__(self, *, close_error: BaseException | None = None) -> None:
        super().__init__(NullTransport())
        self._is_open = False
        self.reporting_states: list[bool] = []
        self.closed = False
        self.close_error = close_error

    @property
    def is_open(self) -> bool:
        return self._is_open

    async def open(self) -> None:
        self._is_open = True

    async def close(self) -> None:
        self.closed = True
        self._is_open = False
        if self.close_error is not None:
            raise self.close_error

    async def set_reporting_state(
        self,
        *,
        enabled: bool,
        timeout: float | None = 5,
    ) -> TrpcPacket | None:
        del timeout
        self.reporting_states.append(enabled)
        return None

    async def discover(
        self,
        *,
        timeout: float | None = 30,
        manage_reporting: bool = False,
        setback_enable: bool | None = None,
        write_delay: float = 0.1,
    ) -> DiscoveryResult:
        del timeout, manage_reporting, setback_enable, write_delay
        raise OSError(_DISCOVERY_FAILED)


class ControllerTest(unittest.IsolatedAsyncioTestCase):
    async def test_controller_starts_and_applies_report_updates(self) -> None:
        transport = TraceReplayTransport(
            [
                *start_steps(),
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        controller = Tekmar482Controller(
            Tekmar482Client(transport),
            discovery_timeout=1,
            poll_timeout=1,
            reconnect_interval=0.01,
        )
        update_received = asyncio.Event()

        def listener(data: AvailableInfo) -> None:
            runtime = data.device_runtime(1201)
            if runtime is not None and runtime.current_temperature == 0x6400:
                update_received.set()

        controller.add_listener(listener)
        data = await controller.start()
        assert data.gateway.reporting_enabled is True
        transport.queue_message(
            report(TrpcMethod.CURRENT_TEMPERATURE, address=1201, temp=0x6400),
        )

        await asyncio.wait_for(update_received.wait(), timeout=1)
        handle = controller.device(1201)
        assert handle.info.model == "532"
        assert handle.runtime is not None
        assert handle.runtime.current_temperature == 0x6400
        with self.assertRaisesRegex(ProtocolError, "does not support CoolSetpoint"):
            await handle.set_cool_setpoint_celsius(24.0)

        await controller.stop()

        assert controller.state is not None
        runtime = controller.state.runtime(1201)
        assert runtime is not None
        assert runtime.current_temperature == 0x6400
        transport.assert_complete()

    async def test_controller_start_restores_reporting_after_failure(self) -> None:
        client = FailingStartClient()
        controller = Tekmar482Controller(client)

        with self.assertRaisesRegex(OSError, "discovery failed"):
            await controller.start()

        assert client.reporting_states == [False, True]
        assert client.closed is True
        assert client.is_open is False

    async def test_controller_start_preserves_failure_when_close_fails(self) -> None:
        client = FailingStartClient(close_error=OSError("close failed"))
        controller = Tekmar482Controller(client)

        with self.assertRaisesRegex(OSError, "discovery failed"):
            await controller.start()

        assert client.reporting_states == [False, True]
        assert client.closed is True

    async def test_controller_rediscover_leaves_reporting_enabled_by_default(
        self,
    ) -> None:
        transport = TraceReplayTransport(
            [
                *start_steps(),
                *discovery_steps(),
                *runtime_steps(current_temperature=1600, reporting_state=1),
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        controller = Tekmar482Controller(
            Tekmar482Client(transport),
            discovery_timeout=1,
            poll_timeout=1,
            reconnect_interval=0.01,
        )

        await controller.start()
        discovery = await controller.rediscover()
        await controller.stop()

        assert [device.address for device in discovery.devices] == [1201]
        assert controller.state is not None
        assert controller.state.data.gateway.reporting_enabled is True
        runtime = controller.state.runtime(1201)
        assert runtime is not None
        assert runtime.current_temperature == 1600
        assert reporting_update_states(transport) == [0, 1, 0]
        transport.assert_complete()

    async def test_controller_rediscover_can_pause_reporting(self) -> None:
        transport = TraceReplayTransport(
            [
                *start_steps(),
                update_step(TrpcMethod.REPORTING_STATE, state=0),
                *discovery_steps(),
                *runtime_steps(current_temperature=1600),
                update_step(TrpcMethod.REPORTING_STATE, state=1),
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        controller = Tekmar482Controller(
            Tekmar482Client(transport),
            discovery_timeout=1,
            poll_timeout=1,
            reconnect_interval=0.01,
        )

        await controller.start()
        await controller.rediscover(pause_reporting=True)
        await controller.stop()

        assert controller.state is not None
        assert controller.state.data.gateway.reporting_enabled is True
        runtime = controller.state.runtime(1201)
        assert runtime is not None
        assert runtime.current_temperature == 1600
        assert reporting_update_states(transport) == [0, 1, 0, 1, 0]
        transport.assert_complete()
