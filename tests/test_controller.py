import asyncio
import unittest
from dataclasses import replace
from unittest.mock import AsyncMock, patch

from tekmar_482 import (
    AvailableInfo,
    ConnectionClosedError,
    DeviceMode,
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
_MODE_REFRESH_BURST_TEST_WINDOW = 0.2


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


def gateway_runtime_steps(
    *,
    reporting_state: int = 0,
    setback_enable: int = 0,
) -> tuple[TraceStep, ...]:
    return (
        request_step(
            TrpcMethod.REPORTING_STATE,
            response=response(TrpcMethod.REPORTING_STATE, state=reporting_state),
        ),
        request_step(
            TrpcMethod.SETBACK_ENABLE,
            response=response(TrpcMethod.SETBACK_ENABLE, enable=setback_enable),
        ),
        request_step(
            TrpcMethod.OUTDOOR_TEMP,
            response=response(TrpcMethod.OUTDOOR_TEMP, temp=1550),
        ),
        request_step(
            TrpcMethod.NETWORK_ERROR,
            response=response(TrpcMethod.NETWORK_ERROR, error=0),
        ),
    )


def runtime_steps(
    *,
    current_temperature: int = 1550,
    reporting_state: int = 0,
    setback_enable: int = 0,
    groups: tuple[TraceStep, ...] = (),
) -> tuple[TraceStep, ...]:
    return (
        *gateway_runtime_steps(
            reporting_state=reporting_state,
            setback_enable=setback_enable,
        ),
        *groups,
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


def start_steps(*, enabled_group: int | None = None) -> tuple[TraceStep, ...]:
    method = TrpcMethod.SETPOINT_GROUP_ENABLE
    groups = (
        tuple(
            request_step(
                method,
                group_id=i,
                response=response(method, group_id=i, enable=int(i == enabled_group)),
            )
            for i in range(1, 13)
        )
        if enabled_group is not None
        else ()
    )
    return (
        update_step(TrpcMethod.REPORTING_STATE, state=0),
        *discovery_steps(),
        *runtime_steps(groups=groups),
        update_step(TrpcMethod.REPORTING_STATE, state=1),
    )


def start_steps_with_setback_enabled() -> tuple[TraceStep, ...]:
    return (
        update_step(TrpcMethod.REPORTING_STATE, state=0),
        request_step(
            TrpcMethod.FIRMWARE_REVISION,
            response=response(TrpcMethod.FIRMWARE_REVISION, revision=154),
        ),
        request_step(
            TrpcMethod.PROTOCOL_VERSION,
            response=response(TrpcMethod.PROTOCOL_VERSION, version=3),
        ),
        update_step(TrpcMethod.SETBACK_ENABLE, enable=1),
        *discovery_steps()[2:],
        *runtime_steps(setback_enable=1),
        update_step(TrpcMethod.REPORTING_STATE, state=1),
    )


def reporting_update_states(transport: TraceReplayTransport) -> list[int]:
    return [
        message.require("state")
        for packet in transport.written
        if (message := TrpcPacket.from_packet(packet)).service == TrpcService.UPDATE
        and message.method == TrpcMethod.REPORTING_STATE
    ]


def written_request_count(
    transport: TraceReplayTransport,
    method: TrpcMethod,
) -> int:
    return sum(
        1
        for packet in transport.written
        if (message := TrpcPacket.from_packet(packet)).service == TrpcService.REQUEST
        and message.method == method
    )


def written_update_bodies(
    transport: TraceReplayTransport,
    method: TrpcMethod,
) -> list[dict[str, int]]:
    bodies: list[dict[str, int]] = []
    for packet in transport.written:
        message = TrpcPacket.from_packet(packet)
        if message.service == TrpcService.UPDATE and message.method == method:
            bodies.append(message.body)
    return bodies


async def wait_for_written_message(
    transport: TraceReplayTransport,
    *,
    service: TrpcService,
    method: TrpcMethod,
    body: dict[str, int] | None = None,
) -> TrpcPacket:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 1
    while loop.time() < deadline:
        for packet in transport.written:
            message = TrpcPacket.from_packet(packet)
            if message.service != service or message.method != method:
                continue
            if body is not None and any(
                message.get(key) != value for key, value in body.items()
            ):
                continue
            return message
        await asyncio.sleep(0.01)
    msg = f"timed out waiting for {service.name}:{method.name} write"
    raise AssertionError(msg)


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
        return update_response(TrpcMethod.REPORTING_STATE, state=int(enabled))

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
    async def test_address_change_does_not_retain_an_identical_models_samples(
        self,
    ) -> None:
        temp = TrpcMethod.CURRENT_TEMPERATURE
        for old, new in ((1201, 1202), (1202, 1201)):
            with self.subTest(old=old, new=new):
                scan = list(discovery_steps())
                # Neither the old snapshot nor a report before identification is valid.
                scan[0] = replace(
                    scan[0],
                    received=(report(temp, address=1201, temp=1700), *scan[0].received),
                )
                transport = TraceReplayTransport(
                    [
                        *start_steps(),
                        *scan,
                        *(
                            replace(s, received=()) if s.sent.method is temp else s
                            for s in runtime_steps(reporting_state=1)
                        ),
                        update_step(TrpcMethod.REPORTING_STATE, state=0),
                    ],
                )
                controller = Tekmar482Controller(
                    Tekmar482Client(transport),
                    poll_timeout=0.02,
                )
                seen = asyncio.Event()

                def notify(
                    _message: TrpcPacket,
                    *,
                    event: asyncio.Event = seen,
                ) -> None:
                    event.set()

                controller.add_topology_listener(notify)
                async with controller, asyncio.timeout(2):
                    original_revision = controller.device_revision(1201)
                    before = controller.require_ready_state().device_snapshots[1201]
                    assert before.runtime.current_temperature == 1550
                    transport.queue_message(
                        report(
                            TrpcMethod.TAKING_ADDRESS,
                            old_address=old,
                            new_address=new,
                        ),
                    )
                    await seen.wait()
                    with self.assertRaises(ConnectionClosedError):
                        controller.device_revision(1201)
                    assert (
                        controller.require_ready_state()
                        .device_snapshots[1201]
                        .runtime.current_temperature
                        is None
                    )
                    await controller.rediscover()
                    assert controller.device_revision(1201) != original_revision
                    after = controller.require_ready_state().device_snapshots[1201]
                    assert after.info == before.info
                    assert after.runtime.current_temperature is None
                    assert after.runtime.mode_setting == 1
                transport.assert_complete()

    async def test_private_group_read_updates_state_and_stops_expiry(self) -> None:
        method = TrpcMethod.SETPOINT_GROUP_ENABLE
        off = response(method, group_id=2, enable=0)
        transport = TraceReplayTransport(
            [
                *start_steps(enabled_group=2),
                request_step(method, group_id=2, response=off),
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        client = Tekmar482Client(transport)
        controller = Tekmar482Controller(client, include_setpoint_groups=True)
        cleared = asyncio.Event()

        def observe(data: AvailableInfo) -> None:
            if (
                data.gateway.setpoint_groups
                and data.gateway.setpoint_groups[2] is False
            ):
                cleared.set()

        controller.add_listener(observe)
        with patch("tekmar_482.controller._GROUP_CACHE_SECONDS", 0.1):
            async with controller, client.reports() as public, asyncio.timeout(2):
                assert (
                    await client.request_response(method, fields={"group_id": 2}) == off
                )
                await cleared.wait()
                await asyncio.sleep(0.15)
                assert list(public.drain()) == []
                assert written_request_count(transport, method) == 13
        transport.assert_complete()

    async def test_address_change_blocks_queued_and_new_writes_until_fresh_scan(
        self,
    ) -> None:
        barrier = TrpcMethod.PROTOCOL_VERSION
        moved = report(TrpcMethod.TAKING_ADDRESS, old_address=1201, new_address=1202)
        mode = update_step(TrpcMethod.MODE_SETTING, address=1201, mode=0)
        scan = [*discovery_steps(), *runtime_steps(reporting_state=1)]
        transport = TraceReplayTransport(
            [
                *start_steps(),
                request_step(barrier),
                *scan[:-1],
                replace(scan[-1], received=(*scan[-1].received, moved)),
                *scan,
                mode,
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        client = Tekmar482Client(transport)
        controller = Tekmar482Controller(client)
        seen = asyncio.Event()
        controller.add_topology_listener(lambda _: seen.set())
        async with controller, asyncio.timeout(2):
            transport.written.clear()
            busy = asyncio.create_task(client.request_response(barrier))
            await wait_for_written_message(
                transport,
                service=TrpcService.REQUEST,
                method=barrier,
            )
            write = asyncio.create_task(
                controller.device(1201).set_mode(DeviceMode.OFF),
            )
            await asyncio.sleep(0)
            assert client.session.queued_update_count == 1
            transport.queue_message(moved)
            await seen.wait()
            with self.assertRaises(ConnectionClosedError):
                await write
            for addr in (1201, 1202):
                with self.assertRaises(ConnectionClosedError):
                    await controller.device(addr).set_mode(DeviceMode.OFF)
            assert len(transport.written) == 1
            transport.queue_message(response(barrier, version=3))
            await busy
            await controller.rediscover()
            # A second change during the scan still needs its own inventory.
            with self.assertRaises(ConnectionClosedError):
                await controller.device(1201).set_mode(DeviceMode.OFF)
            await controller.rediscover()
            await controller.device(1201).set_mode(DeviceMode.OFF)
        transport.assert_complete()

    async def test_enabled_group_is_rechecked_until_off_or_unknown(self) -> None:
        method = TrpcMethod.SETPOINT_GROUP_ENABLE
        for off in (True, False):
            with (
                self.subTest(off=off),
                patch("tekmar_482.controller._GROUP_CACHE_SECONDS", 0.04),
            ):
                transport = TraceReplayTransport(
                    [
                        *start_steps(enabled_group=2),
                        request_step(
                            method,
                            group_id=2,
                            response=response(method, group_id=2, enable=1),
                        ),
                        request_step(
                            method,
                            group_id=2,
                            response=response(method, group_id=2, enable=0)
                            if off
                            else None,
                        ),
                        update_step(TrpcMethod.REPORTING_STATE, state=0),
                    ],
                )
                controller = Tekmar482Controller(
                    Tekmar482Client(transport),
                    include_setpoint_groups=True,
                    poll_timeout=0.02,
                )
                cleared = asyncio.Event()

                def observe(
                    data: AvailableInfo,
                    *,
                    expected: bool | None = False if off else None,
                    event: asyncio.Event = cleared,
                ) -> None:
                    flags = data.gateway.setpoint_groups
                    if flags is not None and flags[2] is expected:
                        event.set()

                controller.add_listener(observe)
                async with asyncio.timeout(2), controller:
                    await cleared.wait()
                    await asyncio.sleep(0.08)
                    assert written_request_count(transport, method) == 14
                    assert not written_update_bodies(transport, method)
                transport.assert_complete()

    async def test_unchanged_group_report_postpones_readback(self) -> None:
        method = TrpcMethod.SETPOINT_GROUP_ENABLE
        transport = TraceReplayTransport(
            [
                *start_steps(enabled_group=2),
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        controller = Tekmar482Controller(
            Tekmar482Client(transport),
            include_setpoint_groups=True,
        )
        with patch("tekmar_482.controller._GROUP_CACHE_SECONDS", 0.1):
            async with controller:
                # An identical report refreshes the lease even without a state change.
                with patch("tekmar_482.controller._GROUP_CACHE_SECONDS", 60):
                    transport.queue_message(report(method, group_id=2, enable=1))
                    await asyncio.sleep(0.15)
                    assert written_request_count(transport, method) == 12
                transport.queue_message(report(method, group_id=2, enable=1))
                transport.queue_message(report(method, group_id=2, enable=0))
                await asyncio.sleep(0.15)
                assert written_request_count(transport, method) == 12
        transport.assert_complete()

    async def test_device_writes_and_samples_do_not_cross_rediscovery(self) -> None:
        mode = TrpcMethod.MODE_SETTING
        temperature = TrpcMethod.CURRENT_TEMPERATURE
        before = update_step(mode, address=1201, mode=0)
        inventory = list(discovery_steps())
        inventory[0] = replace(
            inventory[0],
            received=(
                report(temperature, address=1201, temp=1600),
                *inventory[0].received,
            ),
        )
        inventory = [
            replace(
                s,
                received=(
                    response(TrpcMethod.DEVICE_TYPE, address=1201, type=105102),
                    report(TrpcMethod.ACTIVE_DEMAND, address=1201, demand=1),
                ),
            )
            if s.sent.method == TrpcMethod.DEVICE_TYPE
            else s
            for s in inventory
        ]
        poll = [
            replace(s, received=())
            if s.sent.method in {temperature, TrpcMethod.ACTIVE_DEMAND}
            else s
            for s in runtime_steps(reporting_state=1)
        ]
        transport = TraceReplayTransport(
            [
                *start_steps(),
                TraceStep(before.sent),
                *inventory,
                TraceStep(poll[0].sent),
                *poll[1:],
                before,
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        controller = Tekmar482Controller(
            Tekmar482Client(transport),
            poll_timeout=0.1,
            mode_refresh_burst_window=5,
        )
        async with controller, asyncio.timeout(2):
            transport.written.clear()
            device = controller.device(1201)
            earlier = asyncio.create_task(device.set_mode(DeviceMode.OFF))
            await wait_for_written_message(
                transport,
                service=TrpcService.UPDATE,
                method=mode,
                body={"address": 1201, "mode": 0},
            )
            abandoned = asyncio.create_task(controller.rediscover())
            await asyncio.sleep(0)
            abandoned.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await abandoned
            controller.require_ready_state()
            scan = asyncio.create_task(controller.rediscover())
            await asyncio.sleep(0)
            with self.assertRaises(ConnectionClosedError):
                await device.set_heat_setpoint_celsius(22)
            assert len(transport.written) == 1
            transport.queue_message(before.received[0])
            await earlier
            await wait_for_written_message(
                transport,
                service=TrpcService.REQUEST,
                method=TrpcMethod.REPORTING_STATE,
                body={"state": 0},
            )
            with self.assertRaises(ConnectionClosedError):
                await device.set_mode(DeviceMode.OFF)
            transport.queue_message(poll[0].received[0])
            fresh = await scan
            assert fresh.devices[0].type_code == 105102
            runtime = device.runtime
            assert runtime is not None and runtime.current_temperature is None
            assert runtime.active_demand == 1
            await device.set_mode(DeviceMode.OFF)
        transport.assert_complete()

    async def test_failed_paused_scan_reconnects_after_missing_restore(self) -> None:
        reporting = TrpcMethod.REPORTING_STATE
        off = update_step(reporting, state=0)
        transport = TraceReplayTransport(
            [
                *start_steps(),
                off,
                request_step(TrpcMethod.FIRMWARE_REVISION),
                TraceStep(update_step(reporting, state=1).sent),
                *start_steps(),
                off,
            ],
        )
        client = Tekmar482Client(transport)
        controller = Tekmar482Controller(client, reconnect_interval=0)
        recovered = asyncio.Event()
        async with controller, asyncio.timeout(2):
            generation = client.session.connection_generation

            def record(_: AvailableInfo) -> None:
                if client.session.connection_generation != generation:
                    recovered.set()

            controller.add_listener(record)
            controller.discovery_timeout = 0.03
            with (
                patch("tekmar_482.controller._REPORTING_CLEANUP_TIMEOUT", 0.02),
                self.assertRaisesRegex(TimeoutError, "gateway metadata"),
            ):
                await controller.rediscover(pause_reporting=True)
            with self.assertRaises(ConnectionClosedError):
                controller.require_ready_state()
            await recovered.wait()
            assert (
                controller.require_ready_state().data.gateway.reporting_enabled is True
            )
        transport.assert_complete()

    async def test_mode_timeout_retries_only_the_targeted_read(self) -> None:
        mode = TrpcMethod.MODE_SETTING
        start = [
            replace(s, received=(response(mode, address=1201, mode=0),))
            if s.sent.method == mode
            else s
            for s in start_steps()
        ]
        transport = TraceReplayTransport(
            [
                *start,
                request_step(mode, address=1201),
                request_step(
                    mode,
                    address=1201,
                    response=response(mode, address=1201, mode=1),
                ),
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        controller = Tekmar482Controller(
            Tekmar482Client(transport),
            mode_refresh_burst_window=0,
            poll_timeout=0.02,
        )
        seen: asyncio.Queue[AvailableInfo] = asyncio.Queue()
        async with controller, asyncio.timeout(2):
            controller.add_listener(seen.put_nowait)
            transport.queue_message(
                report(TrpcMethod.ACTIVE_DEMAND, address=1201, demand=1),
            )
            while True:
                runtime = (await seen.get()).device_runtime(1201)
                if runtime is not None and runtime.mode_setting == DeviceMode.HEAT:
                    break
        transport.assert_complete()

    async def test_writes_wait_for_confirmed_inventory_on_each_connection(self) -> None:
        reporting = TrpcMethod.REPORTING_STATE
        inventory = TrpcMethod.DEVICE_INVENTORY
        group = TrpcMethod.SETPOINT_GROUP_ENABLE
        on = update_step(reporting, state=1)
        off = update_step(reporting, state=0)
        transport = TraceReplayTransport(
            [
                *start_steps(),
                off,
                *discovery_steps()[:2],
                request_step(
                    inventory,
                    address=0,
                    response=response(inventory, address=0),
                ),
                *gateway_runtime_steps(),
                TraceStep(on.sent),
                TraceStep(
                    update_step(group, group_id=1, enable=1).sent,
                    (report(group, group_id=1, enable=1),),
                ),
                off,
            ],
        )
        controller = Tekmar482Controller(
            Tekmar482Client(transport),
            reconnect_interval=0,
        )
        published: asyncio.Queue[AvailableInfo] = asyncio.Queue()
        controller.add_listener(published.put_nowait)
        handle = controller.device(1201)
        async with controller, asyncio.timeout(2):
            cached = await published.get()
            transport.written.clear()
            await transport.close()
            await wait_for_written_message(
                transport,
                service=TrpcService.UPDATE,
                method=reporting,
                body={"state": 1},
            )
            assert controller.data is cached and handle.runtime is not None
            before = len(transport.written)
            with self.assertRaises(ConnectionClosedError):
                await controller.start()
            with self.assertRaises(ConnectionClosedError):
                await handle.set_mode(DeviceMode.OFF)
            with self.assertRaises(ConnectionClosedError):
                await handle.set_heat_setpoint_celsius(22)
            with self.assertRaises(ConnectionClosedError):
                await controller.set_setpoint_group(1, enabled=True)
            assert len(transport.written) == before
            transport.queue_message(on.received[0])
            fresh = await published.get()
            assert await controller.start() is fresh
            assert fresh.devices == ()
            with self.assertRaisesRegex(ProtocolError, "unknown tekmar device"):
                await handle.set_mode(DeviceMode.OFF)
            await controller.set_setpoint_group(1, enabled=True)
        with self.assertRaises(ConnectionClosedError):
            await controller.set_setpoint_group(1, enabled=True)
        transport.assert_complete()

    async def test_stuck_inventory_retry_reconnects_and_publishes_fresh_state(
        self,
    ) -> None:
        inventory = TrpcMethod.DEVICE_INVENTORY
        transport = TraceReplayTransport(
            [
                *start_steps(),
                *discovery_steps()[:2],
                request_step(inventory, address=0),
                *start_steps(),
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        client = Tekmar482Client(transport)
        controller = Tekmar482Controller(
            client,
            discovery_timeout=0.03,
            reconnect_interval=0,
        )
        async with controller, asyncio.timeout(2):
            original = controller.discovery
            generation = client.session.connection_generation
            recovered = asyncio.Event()
            controller.add_listener(lambda _: recovered.set())
            with self.assertRaisesRegex(TimeoutError, "complete device inventory"):
                await controller.rediscover()
            assert controller.discovery is original
            with self.assertRaisesRegex(TimeoutError, "previous inventory"):
                await controller.rediscover()
            await recovered.wait()
            assert client.session.connection_generation != generation
            assert controller.discovery is not original
            assert controller.last_error is None
        transport.assert_complete()

    async def test_uncertain_mode_write_rechecks_once_after_trigger(self) -> None:
        mode = TrpcMethod.MODE_SETTING
        trigger = report(TrpcMethod.ACTIVE_DEMAND, address=1201, demand=1)
        transport = TraceReplayTransport(
            [
                *start_steps(),
                replace(
                    update_step(mode, address=1201, mode=DeviceMode.OFF),
                    received=(trigger, trigger),
                ),
                request_step(
                    mode,
                    address=1201,
                    response=response(mode, address=1201, mode=DeviceMode.OFF),
                ),
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        controller = Tekmar482Controller(
            Tekmar482Client(transport),
            mode_refresh_burst_window=0,
        )
        async with controller, asyncio.timeout(1):
            changed = asyncio.Event()

            def listener(data: AvailableInfo) -> None:
                runtime = data.device_runtime(1201)
                if runtime is not None and runtime.mode_setting == DeviceMode.OFF:
                    changed.set()

            controller.add_listener(listener)
            with self.assertRaises(TimeoutError):
                await controller.device(1201).set_mode(DeviceMode.OFF, timeout=0.02)
            await changed.wait()
            assert written_request_count(transport, mode) == 2
        transport.assert_complete()

    async def test_stop_closes_transport_with_transaction_blocked(self) -> None:
        address = 2102
        transport = TraceReplayTransport(
            [*start_steps(), request_step(TrpcMethod.DEVICE_VERSION, address=address)],
        )
        client = Tekmar482Client(transport)
        controller = Tekmar482Controller(client)
        async with asyncio.timeout(1):
            await controller.start()
            outstanding = asyncio.create_task(
                client.request_response(
                    TrpcMethod.DEVICE_VERSION,
                    fields={"address": address},
                    timeout=None,
                ),
            )
            await wait_for_written_message(
                transport,
                service=TrpcService.REQUEST,
                method=TrpcMethod.DEVICE_VERSION,
                body={"address": address},
            )
            with patch("tekmar_482.controller._REPORTING_CLEANUP_TIMEOUT", 0.02):
                await controller.stop()
            with self.assertRaises(ConnectionClosedError):
                await outstanding
        assert not transport.is_open
        assert not controller.is_running
        transport.assert_complete()

    async def test_stop_queued_during_overlapping_starts(self) -> None:
        first, *rest = start_steps()
        transport = TraceReplayTransport(
            [
                TraceStep(first.sent),
                *rest,
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        controller = Tekmar482Controller(Tekmar482Client(transport))
        async with asyncio.timeout(2):
            first_start = asyncio.create_task(controller.start())
            await wait_for_written_message(
                transport,
                service=TrpcService.UPDATE,
                method=TrpcMethod.REPORTING_STATE,
            )
            second_start = asyncio.create_task(controller.start())
            stop = asyncio.create_task(controller.stop())
            await asyncio.sleep(0)
            assert not stop.done()
            transport.queue_message(first.received[0])
            assert await first_start is await second_start
            await stop
        assert not controller.is_running
        assert not transport.is_open
        assert written_request_count(transport, TrpcMethod.DEVICE_INVENTORY) == 1
        transport.assert_complete()

    async def test_missing_device_attributes_do_not_replace_topology(self) -> None:
        transport = TraceReplayTransport(
            [
                *start_steps(),
                *discovery_steps()[:5],
                request_step(TrpcMethod.DEVICE_ATTRIBUTES, address=1201),
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        controller = Tekmar482Controller(Tekmar482Client(transport))
        async with controller:
            original = controller.discovery
            controller.discovery_timeout = 0.05
            with self.assertRaisesRegex(TimeoutError, "metadata for device 1201"):
                await controller.rediscover()
            assert controller.discovery is original
            assert written_request_count(transport, TrpcMethod.SETBACK_EVENTS) == 1
        transport.assert_complete()

    async def test_newer_report_wins_over_confirmed_write(self) -> None:
        method = TrpcMethod.HEAT_SETPOINT
        sent = update_step(method, address=1201, setback=7, setpoint=44).sent
        ack = update_response(method, address=1201, setback=4, setpoint=44)
        later = report(method, address=1201, setback=4, setpoint=46)
        transport = TraceReplayTransport(
            [
                *start_steps(),
                TraceStep(sent, (ack, later)),
                update_step(method, address=1201, setback=6, setpoint=36),
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        controller = Tekmar482Controller(
            Tekmar482Client(transport),
            mode_refresh_burst_window=5,
        )
        seen: list[int | None] = []
        received = asyncio.Event()
        away_updated = asyncio.Event()

        def record(data: AvailableInfo) -> None:
            runtime = data.device_runtime(1201)
            assert runtime is not None
            seen.append(runtime.heat_setpoints.current)
            if seen[-1] == 46:
                received.set()
            if runtime.heat_setpoints.away == 36:
                away_updated.set()

        controller.add_listener(record)
        async with controller, asyncio.timeout(1):
            device = controller.device(1201)
            assert await device.set_heat_setpoint_celsius(22) == ack
            await received.wait()
            assert seen == [42, 44, 46]
            await device.set_heat_setpoint_celsius(18, setback=6, confirm=False)
            await away_updated.wait()
            # Startup read SetbackEnable=0: the later Away write fills all slots.
            assert seen[-1] == 36
        transport.assert_complete()

    async def test_broken_listener_does_not_interrupt_push(self) -> None:
        transport = TraceReplayTransport(
            [
                *start_steps(),
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        controller = Tekmar482Controller(Tekmar482Client(transport))
        seen = asyncio.Event()

        def broken(_: AvailableInfo) -> None:
            raise ValueError(_DISCOVERY_FAILED)

        def healthy(data: AvailableInfo) -> None:
            if data.gateway.outdoor_temp == 1400:
                seen.set()

        controller.add_listener(broken)
        controller.add_listener(healthy)
        with self.assertLogs("tekmar_482.controller", level="ERROR") as captured:
            async with controller, asyncio.timeout(1):
                transport.queue_message(report(TrpcMethod.OUTDOOR_TEMP, temp=1400))
                await seen.wait()
                assert controller.is_running
        assert len(captured.records) == 2
        transport.assert_complete()

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
            if runtime is not None and runtime.heat_setpoints.current == 50:
                update_received.set()

        controller.add_listener(listener)
        data = await controller.start()
        assert data.gateway.reporting_enabled is True
        transport.queue_message(
            report(TrpcMethod.HEAT_SETPOINT, address=1201, setback=7, setpoint=50),
        )

        await asyncio.wait_for(update_received.wait(), timeout=1)
        handle = controller.device(1201)
        assert handle.info.model == "532"
        assert handle.runtime is not None
        assert handle.runtime.heat_setpoints.current == 50
        with self.assertRaisesRegex(ProtocolError, "does not support CoolSetpoint"):
            await handle.set_cool_setpoint_celsius(24.0)

        await controller.stop()

        assert controller.state is not None
        runtime = controller.state.runtime(1201)
        assert runtime is not None
        assert runtime.heat_setpoints.current == 50
        transport.assert_complete()

    async def test_controller_can_enable_setback_support_on_start(self) -> None:
        transport = TraceReplayTransport(
            [
                *start_steps_with_setback_enabled(),
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        controller = Tekmar482Controller(
            Tekmar482Client(transport),
            discovery_timeout=1,
            poll_timeout=1,
            reconnect_interval=0.01,
            setback_enable=True,
        )

        data = await controller.start()
        await controller.stop()

        assert data.gateway.setback_enabled is True
        assert written_update_bodies(transport, TrpcMethod.SETBACK_ENABLE) == [
            {"enable": 1},
        ]
        transport.assert_complete()

    async def test_controller_refreshes_mode_after_mode_adjacent_report(self) -> None:
        transport = TraceReplayTransport(
            [
                *start_steps(),
                request_step(
                    TrpcMethod.MODE_SETTING,
                    address=1201,
                    response=response(
                        TrpcMethod.MODE_SETTING,
                        address=1201,
                        mode=DeviceMode.OFF,
                    ),
                ),
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
            if runtime is not None and runtime.mode_setting == DeviceMode.OFF:
                update_received.set()

        controller.add_listener(listener)
        await controller.start()
        transport.queue_message(
            report(TrpcMethod.ACTIVE_DEMAND, address=1201, demand=1),
        )

        await asyncio.wait_for(update_received.wait(), timeout=1)
        await controller.stop()

        assert controller.state is not None
        runtime = controller.state.runtime(1201)
        assert runtime is not None
        assert runtime.mode_setting == DeviceMode.OFF
        assert written_request_count(transport, TrpcMethod.MODE_SETTING) == 2
        transport.assert_complete()

    async def test_controller_refreshes_mode_after_setpoint_report(self) -> None:
        transport = TraceReplayTransport(
            [
                *start_steps(),
                request_step(
                    TrpcMethod.MODE_SETTING,
                    address=1201,
                    response=response(
                        TrpcMethod.MODE_SETTING,
                        address=1201,
                        mode=DeviceMode.OFF,
                    ),
                ),
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
            if runtime is not None and runtime.mode_setting == DeviceMode.OFF:
                update_received.set()

        controller.add_listener(listener)
        await controller.start()
        transport.queue_message(
            report(TrpcMethod.HEAT_SETPOINT, address=1201, setback=7, setpoint=42),
        )

        await asyncio.wait_for(update_received.wait(), timeout=1)
        await controller.stop()

        assert written_request_count(transport, TrpcMethod.MODE_SETTING) == 2
        transport.assert_complete()

    async def test_controller_coalesces_mode_refresh_report_burst(self) -> None:
        transport = TraceReplayTransport(
            [
                *start_steps(),
                request_step(
                    TrpcMethod.MODE_SETTING,
                    address=1201,
                    response=response(
                        TrpcMethod.MODE_SETTING,
                        address=1201,
                        mode=DeviceMode.OFF,
                    ),
                ),
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        controller = Tekmar482Controller(
            Tekmar482Client(transport),
            discovery_timeout=1,
            poll_timeout=1,
            reconnect_interval=0.01,
            mode_refresh_burst_window=_MODE_REFRESH_BURST_TEST_WINDOW,
        )
        update_received = asyncio.Event()

        def listener(data: AvailableInfo) -> None:
            runtime = data.device_runtime(1201)
            if runtime is not None and runtime.mode_setting == DeviceMode.OFF:
                update_received.set()

        controller.add_listener(listener)
        await controller.start()
        transport.queue_message(
            report(TrpcMethod.CURRENT_TEMPERATURE, address=1201, temp=1551),
        )
        await asyncio.sleep(_MODE_REFRESH_BURST_TEST_WINDOW * 0.65)
        assert written_request_count(transport, TrpcMethod.MODE_SETTING) == 1

        transport.queue_message(
            report(TrpcMethod.ACTIVE_DEMAND, address=1201, demand=1),
        )
        # Past the first report's deadline, but still inside the second's.
        await asyncio.sleep(_MODE_REFRESH_BURST_TEST_WINDOW * 0.65)
        assert written_request_count(transport, TrpcMethod.MODE_SETTING) == 1

        await asyncio.wait_for(update_received.wait(), timeout=1)
        await controller.stop()

        assert written_request_count(transport, TrpcMethod.MODE_SETTING) == 2
        transport.assert_complete()

    async def test_mode_report_during_read_schedules_one_follow_up(self) -> None:
        mode = TrpcMethod.MODE_SETTING
        transport = TraceReplayTransport(
            [
                *start_steps(),
                request_step(mode, address=1201),
                request_step(
                    mode,
                    address=1201,
                    response=response(mode, address=1201, mode=DeviceMode.OFF),
                ),
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        controller = Tekmar482Controller(
            Tekmar482Client(transport),
            mode_refresh_burst_window=0,
        )
        mode_changed = asyncio.Event()

        def record_mode(data: AvailableInfo) -> None:
            runtime = data.device_runtime(1201)
            if runtime is not None and runtime.mode_setting == DeviceMode.OFF:
                mode_changed.set()

        controller.add_listener(record_mode)
        async with controller, asyncio.timeout(1):
            transport.written.clear()
            trigger = report(TrpcMethod.ACTIVE_DEMAND, address=1201, demand=0)
            controller.handle_message(trigger)
            await wait_for_written_message(
                transport,
                service=TrpcService.REQUEST,
                method=mode,
            )
            controller.handle_message(trigger)
            controller.handle_message(trigger)
            transport.queue_message(response(mode, address=1201, mode=DeviceMode.HEAT))
            await mode_changed.wait()
            assert written_request_count(transport, mode) == 2
        transport.assert_complete()

    async def test_reconnect_publishes_fresh_seed_without_a_report(self) -> None:
        transport = TraceReplayTransport(
            [
                *start_steps_with_setback_enabled(),
                update_step(TrpcMethod.REPORTING_STATE, state=0),
                *discovery_steps()[:2],
                update_step(TrpcMethod.SETBACK_ENABLE, enable=1),
                *discovery_steps()[2:],
                *runtime_steps(current_temperature=1600, setback_enable=1),
                update_step(TrpcMethod.REPORTING_STATE, state=1),
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        controller = Tekmar482Controller(
            Tekmar482Client(transport),
            reconnect_interval=0,
            setback_enable=True,
        )
        seen: asyncio.Queue[int | None] = asyncio.Queue()
        errors: list[BaseException] = []

        def record(data: AvailableInfo) -> None:
            runtime = data.device_runtime(1201)
            assert runtime is not None
            seen.put_nowait(runtime.current_temperature)

        controller.add_listener(record)
        controller.add_error_listener(errors.append)
        async with controller, asyncio.timeout(1):
            assert await seen.get() == 1550
            transport.queue_message(
                report(TrpcMethod.CURRENT_TEMPERATURE, address=1201, temp=1551),
            )
            assert await seen.get() == 1551
            await transport.close()
            assert await seen.get() == 1600
            assert controller.last_error is None
            assert written_request_count(transport, TrpcMethod.DEVICE_INVENTORY) == 2
            assert (
                controller.data is not None
                and controller.data.gateway.reporting_enabled is True
                and controller.data.gateway.setback_enabled is True
            )
        assert len(errors) == 1
        assert seen.empty()
        transport.assert_complete()

    async def test_reconnect_retries_wrong_setback_before_publishing_topology(
        self,
    ) -> None:
        # The device left while disconnected; inventory now ends immediately.
        inventory = TrpcMethod.DEVICE_INVENTORY
        empty_discovery = (
            update_step(TrpcMethod.REPORTING_STATE, state=0),
            *discovery_steps()[:2],
            update_step(TrpcMethod.SETBACK_ENABLE, enable=1),
            request_step(inventory, address=0, response=response(inventory, address=0)),
        )
        transport = TraceReplayTransport(
            [
                *start_steps_with_setback_enabled(),
                *empty_discovery,
                *gateway_runtime_steps(setback_enable=0),
                update_step(TrpcMethod.REPORTING_STATE, state=1),
                *empty_discovery,
                *gateway_runtime_steps(setback_enable=1),
                update_step(TrpcMethod.REPORTING_STATE, state=1),
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        controller = Tekmar482Controller(
            Tekmar482Client(transport),
            reconnect_interval=0,
            setback_enable=True,
        )
        published: asyncio.Queue[AvailableInfo] = asyncio.Queue()
        errors: list[BaseException] = []
        controller.add_listener(published.put_nowait)
        controller.add_error_listener(errors.append)
        async with controller, asyncio.timeout(2):
            initial = await published.get()
            assert initial.device_runtime(1201) is not None
            await transport.close()
            recovered = await published.get()
            assert recovered.devices == ()
            assert recovered.gateway.setback_enabled is True
            assert recovered.gateway.reporting_enabled is True
            assert controller.discovery is not None and not controller.discovery.devices
            assert controller.last_error is None
            assert len(errors) == 2
            assert isinstance(errors[1], ProtocolError)
            assert published.empty()
        transport.assert_complete()

    async def test_controller_skips_mode_refresh_during_mode_update(self) -> None:
        transport = TraceReplayTransport(
            [
                *start_steps(),
                TraceStep(
                    sent=TrpcPacket.create(
                        service=TrpcService.UPDATE,
                        method=TrpcMethod.MODE_SETTING,
                        fields={"address": 1201, "mode": DeviceMode.OFF},
                    ),
                ),
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
            if runtime is not None and runtime.active_demand == 1:
                update_received.set()

        controller.add_listener(listener)
        await controller.start()
        handle = controller.device(1201)
        set_mode = asyncio.create_task(
            handle.set_mode(DeviceMode.OFF, timeout=1),
            name="test set mode",
        )
        await wait_for_written_message(
            transport,
            service=TrpcService.UPDATE,
            method=TrpcMethod.MODE_SETTING,
            body={"address": 1201, "mode": DeviceMode.OFF},
        )
        transport.queue_message(
            report(TrpcMethod.ACTIVE_DEMAND, address=1201, demand=1),
        )
        await asyncio.wait_for(update_received.wait(), timeout=1)
        transport.queue_message(
            update_response(
                TrpcMethod.MODE_SETTING,
                address=1201,
                mode=DeviceMode.OFF,
            ),
        )

        result = await asyncio.wait_for(set_mode, timeout=1)
        await controller.stop()

        assert result is not None
        assert written_request_count(transport, TrpcMethod.MODE_SETTING) == 1
        transport.assert_complete()

    async def test_controller_start_restores_reporting_after_failure(self) -> None:
        client = FailingStartClient()
        controller = Tekmar482Controller(client)

        with self.assertRaisesRegex(OSError, "discovery failed"):
            await controller.start()

        assert client.reporting_states == [False, True]
        assert client.closed is True
        assert client.is_open is False

    async def test_start_requires_reporting_ack_and_can_retry(self) -> None:
        for missing in (False, True):
            with self.subTest(missing=missing):
                transport = TraceReplayTransport(
                    [
                        *(start_steps()[1:-1] if missing else ()),
                        *start_steps(),
                        update_step(TrpcMethod.REPORTING_STATE, state=0),
                    ],
                )
                client = Tekmar482Client(transport)
                controller = Tekmar482Controller(client)
                seen: list[AvailableInfo] = []
                controller.add_listener(seen.append)
                ack = update_response(TrpcMethod.REPORTING_STATE, state=1)
                responses = (
                    [update_response(TrpcMethod.REPORTING_STATE, state=0)]
                    if missing
                    else []
                )
                reporting = AsyncMock(side_effect=[*responses, None, ack])
                with (
                    patch.object(client, "set_reporting_state", reporting),
                    self.assertRaisesRegex(
                        TimeoutError,
                        f"ReportingState={int(missing)}",
                    ),
                ):
                    await controller.start()
                self.assertFalse(controller.is_running)
                assert not client.is_open
                assert controller.data is None
                assert not seen
                assert reporting.await_args is not None
                assert reporting.await_args.kwargs == {"enabled": True}
                async with controller:
                    assert controller.is_running
                    assert seen[-1].gateway.reporting_enabled is True
                transport.assert_complete()

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

    async def test_rediscover_orders_reports_and_reads(self) -> None:
        poll = list(runtime_steps(current_temperature=1560, reporting_state=1))
        poll[0] = TraceStep(
            sent=poll[0].sent,
            received=(
                report(TrpcMethod.OUTDOOR_TEMP, temp=1400),
                report(TrpcMethod.HEAT_SETPOINT, address=1201, setback=7, setpoint=51),
                *poll[0].received,
            ),
        )
        last = poll[-1]
        poll[-1] = TraceStep(
            sent=last.sent,
            received=(report(TrpcMethod.CURRENT_TEMPERATURE, address=1201, temp=1600),),
        )
        transport = TraceReplayTransport(
            [
                *start_steps(),
                *discovery_steps(),
                *poll,
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        controller = Tekmar482Controller(
            Tekmar482Client(transport),
            mode_refresh_burst_window=5,
        )
        temperatures: list[int | None] = []

        def record(data: AvailableInfo) -> None:
            runtime = data.device_runtime(1201)
            assert runtime is not None
            if runtime.current_temperature == 1600 and 1600 not in temperatures:
                # Hold the final poll reply until the newer report was published.
                transport.queue_message(
                    response(
                        TrpcMethod.HEAT_SETPOINT,
                        address=1201,
                        setback=0,
                        setpoint=42,
                    ),
                )
            temperatures.append(runtime.current_temperature)

        controller.add_listener(record)
        async with controller:
            controller.handle_message(
                report(TrpcMethod.SETBACK_STATE, address=1201, setback=6),
            )
            await controller.rediscover()
            assert controller.data is not None
            assert controller.data.gateway.outdoor_temp == 1550
            runtime = controller.data.device_runtime(1201)
            assert runtime is not None
            assert runtime.heat_setpoints.away == 51
            assert runtime.heat_setpoints.wake_4 == runtime.heat_setpoints.current == 42
            assert temperatures[-1] == 1600
            assert 1560 not in temperatures
        transport.assert_complete()

    async def test_start_rediscovery_standalone_and_live_reports_agree(self) -> None:
        burst = (
            report(TrpcMethod.SETBACK_STATE, address=1201, setback=4),
            report(TrpcMethod.HEAT_SETPOINT, address=1201, setback=7, setpoint=46),
            report(TrpcMethod.SETBACK_STATE, address=1201, setback=0),
            report(TrpcMethod.HEAT_SETPOINT, address=1201, setback=7, setpoint=52),
            report(TrpcMethod.CURRENT_TEMPERATURE, address=1201, temp=0xFFFF),
            report(TrpcMethod.OUTDOOR_TEMP, temp=1500),
        )
        poll = runtime_steps(reporting_state=1)
        last = poll[-1]
        interleaved = (*poll[:-1], replace(last, received=(*last.received, *burst)))
        off, *_, on = start_steps()
        transport = TraceReplayTransport(
            [
                *start_steps(),
                *discovery_steps(),
                *interleaved,
                off,
                off,
                *discovery_steps(),
                *interleaved,
                on,
                off,
                *interleaved,
            ],
        )
        client = Tekmar482Client(transport)
        controller = Tekmar482Controller(client, mode_refresh_burst_window=5)
        async with controller:
            for message in burst:
                controller.handle_message(message)
            expected = controller.data
            assert expected is not None
            runtime = expected.device_runtime(1201)
            assert runtime is not None
            assert runtime.heat_setpoints.occ_2 == 52
            assert runtime.heat_setpoints.wake_4 == runtime.heat_setpoints.current == 52
            assert runtime.current_temperature == 0xFFFF
            assert runtime.decoded.current_temperature is None
            assert expected.gateway.outdoor_temp == 1500
            discovery = await controller.rediscover()
            assert controller.data == expected
        async with controller:
            assert controller.data == expected
        async with client:
            snapshot = await client.poll_runtime(
                discovery,
                include_setpoint_groups=False,
            )
            assert snapshot == expected
        transport.assert_complete()

    async def test_setback_report_is_not_configuration_readback(self) -> None:
        transport = TraceReplayTransport(
            [
                replace(step, received=(report(TrpcMethod.SETBACK_ENABLE, enable=1),))
                if step.sent.service is TrpcService.REQUEST
                and step.sent.method is TrpcMethod.SETBACK_ENABLE
                else step
                for step in start_steps_with_setback_enabled()
            ],
        )
        controller = Tekmar482Controller(
            Tekmar482Client(transport),
            setback_enable=True,
            poll_timeout=0.01,
        )
        with self.assertRaisesRegex(ProtocolError, "read back None"):
            await controller.start()
        assert controller.data is None
        transport.assert_complete()

    async def test_runtime_refresh_publishes_only_changed_data(self) -> None:
        poll = runtime_steps(current_temperature=1600, reporting_state=1)
        transport = TraceReplayTransport(
            [
                *start_steps(),
                *poll,
                *poll,
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        controller = Tekmar482Controller(Tekmar482Client(transport))
        async with controller:
            seen: list[AvailableInfo] = []
            controller.add_listener(seen.append)
            refreshed = await controller.refresh_runtime()
            assert seen == [refreshed]
            runtime = refreshed.device_runtime(1201)
            assert runtime is not None
            assert runtime.current_temperature == 1600
            assert await controller.refresh_runtime() == refreshed
            assert seen == [refreshed]
        transport.assert_complete()

    async def test_reporting_loss_invalidates_state_and_reseeds(self) -> None:
        method = TrpcMethod.REPORTING_STATE
        for live in (False, True):
            with self.subTest(live=live):
                transport = TraceReplayTransport(
                    [
                        *start_steps(),
                        *(() if live else gateway_runtime_steps(reporting_state=0)[:1]),
                        *start_steps(),
                        update_step(method, state=0),
                    ],
                )
                client = Tekmar482Client(transport)
                controller = Tekmar482Controller(client, reconnect_interval=0)
                seen: asyncio.Queue[AvailableInfo] = asyncio.Queue()
                errors: asyncio.Queue[BaseException] = asyncio.Queue()
                controller.add_listener(seen.put_nowait)
                controller.add_error_listener(errors.put_nowait)
                async with controller, asyncio.timeout(2):
                    initial = await seen.get()
                    generation = client.session.connection_generation
                    if live:
                        transport.queue_message(report(method, state=0))
                    else:
                        with self.assertRaisesRegex(ProtocolError, "disabled"):
                            await controller.refresh_runtime()
                    assert "disabled reporting" in str(await errors.get())
                    assert controller.data is initial
                    with self.assertRaises(ConnectionClosedError):
                        controller.require_ready_state()
                    restored = await seen.get()
                    assert restored is not initial
                    assert controller.require_ready_state().data is restored
                    assert restored.gateway.reporting_enabled is True
                    assert client.session.connection_generation != generation
                transport.assert_complete()

    async def test_rediscover_retains_missing_read_but_not_explicit_na(self) -> None:
        missing = {
            TrpcMethod.CURRENT_TEMPERATURE,
            TrpcMethod.HEAT_SETPOINT,
            TrpcMethod.OUTDOOR_TEMP,
        }
        poll = [
            replace(s, received=()) if s.sent.method in missing else s
            for s in runtime_steps(reporting_state=1)
        ]
        transport = TraceReplayTransport(
            [
                *start_steps(),
                *discovery_steps(),
                *poll,
                *discovery_steps(),
                *runtime_steps(reporting_state=1, current_temperature=0xFFFF),
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        controller = Tekmar482Controller(Tekmar482Client(transport), poll_timeout=0.03)
        async with controller:
            await controller.rediscover()
            runtime = controller.device(1201).runtime
            assert runtime is not None
            assert runtime.current_temperature == 1550
            assert runtime.current_heat_setpoint == 42
            assert controller.data is not None
            assert controller.data.gateway.outdoor_temp == 1550
            # Release the unanswered CURRENT read before retrying that selector.
            transport.queue_message(
                response(
                    TrpcMethod.HEAT_SETPOINT,
                    address=1201,
                    setback=7,
                    setpoint=36,
                ),
            )
            await controller.rediscover()
            runtime = controller.device(1201).runtime
            assert runtime is not None
            assert runtime.current_temperature == 0xFFFF
            assert runtime.decoded.current_temperature is None
        transport.assert_complete()

    async def test_missing_read_does_not_survive_device_or_connection_replacement(
        self,
    ) -> None:
        missing = [
            replace(s, received=())
            if s.sent.method == TrpcMethod.CURRENT_TEMPERATURE
            else s
            for s in runtime_steps(reporting_state=1)
        ]
        inventory = [
            replace(
                s,
                received=(response(TrpcMethod.DEVICE_TYPE, address=1201, type=105102),),
            )
            if s.sent.method == TrpcMethod.DEVICE_TYPE
            else s
            for s in discovery_steps()
        ]
        off = update_step(TrpcMethod.REPORTING_STATE, state=0)
        transport = TraceReplayTransport(
            [
                *start_steps(),
                *inventory,
                *missing,
                off,
                off,
                *inventory,
                *missing,
                update_step(TrpcMethod.REPORTING_STATE, state=1),
                off,
            ],
        )
        controller = Tekmar482Controller(Tekmar482Client(transport), poll_timeout=0.03)
        async with controller:
            await controller.rediscover()
            runtime = controller.device(1201).runtime
            assert runtime is not None
            assert runtime.current_temperature is None
            controller.handle_message(
                response(TrpcMethod.CURRENT_TEMPERATURE, address=1201, temp=1600),
            )
        async with controller:
            runtime = controller.device(1201).runtime
            assert runtime is not None
            assert runtime.current_temperature is None
        transport.assert_complete()

    async def test_incomplete_inventory_preserves_live_topology(self) -> None:
        transport = TraceReplayTransport(
            [
                *start_steps(),
                *discovery_steps()[:2],
                request_step(
                    TrpcMethod.DEVICE_INVENTORY,
                    address=0,
                    response=response(
                        TrpcMethod.DEVICE_INVENTORY,
                        address=2101,
                    ),
                ),
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        controller = Tekmar482Controller(Tekmar482Client(transport))
        async with controller:
            initial = controller.discovery
            controller.discovery_timeout = 0.05
            with self.assertRaisesRegex(TimeoutError, "complete device inventory"):
                await controller.rediscover()
            assert controller.discovery is initial
            assert controller.device(1201).runtime is not None
            assert controller.device(2101).runtime is None
            with self.assertRaises(ConnectionClosedError):
                await controller.device(1201).set_mode(DeviceMode.OFF)
        transport.assert_complete()

    async def test_controller_rediscover_reapplies_setback_enable(self) -> None:
        transport = TraceReplayTransport(
            [
                *start_steps_with_setback_enabled(),
                *discovery_steps()[:2],
                update_step(TrpcMethod.SETBACK_ENABLE, enable=1),
                *discovery_steps()[2:],
                *runtime_steps(reporting_state=1, setback_enable=1),
                update_step(TrpcMethod.REPORTING_STATE, state=0),
            ],
        )
        controller = Tekmar482Controller(
            Tekmar482Client(transport),
            discovery_timeout=1,
            poll_timeout=1,
            reconnect_interval=0.01,
            setback_enable=True,
        )

        await controller.start()
        await controller.rediscover()
        await controller.stop()

        assert written_update_bodies(transport, TrpcMethod.SETBACK_ENABLE) == [
            {"enable": 1},
            {"enable": 1},
        ]
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
