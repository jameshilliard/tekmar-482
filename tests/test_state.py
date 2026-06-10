import unittest

from tekmar_482 import (
    AvailableInfo,
    DecodedSetbackValues,
    DeviceAttributeBit,
    DeviceAttributes,
    DeviceRuntime,
    DeviceSnapshot,
    DeviceValue,
    DiscoveredDevice,
    DiscoveryResult,
    GatewayDateTime,
    GatewayInfo,
    GatewayRuntime,
    GatewaySnapshot,
    SetpointValues,
    Tekmar482State,
    ThaSetback,
    ThermostatRuntime,
    TrpcMethod,
    TrpcPacket,
    TrpcService,
    apply_message,
    is_topology_message,
)


def _state() -> Tekmar482State:
    device = DiscoveredDevice(
        address=1201,
        type_code=107201,
        attributes=DeviceAttributes(1),
    )
    discovery = DiscoveryResult(
        gateway=GatewayInfo(firmware_revision=154, protocol_version=3),
        devices=(device,),
    )
    data = AvailableInfo(
        gateway=GatewaySnapshot(info=discovery.gateway),
        devices=(
            DeviceSnapshot(
                DeviceRuntime.create(
                    info=device,
                    available_values=frozenset(
                        {
                            DeviceValue.CURRENT_TEMPERATURE,
                            DeviceValue.HEAT_SETPOINTS,
                        },
                    ),
                    current_temperature=None,
                    heat_setpoints=SetpointValues.from_mapping(
                        {ThaSetback.CURRENT: None},
                    ),
                ),
            ),
        ),
    )
    return Tekmar482State(discovery, data)


class StateTest(unittest.TestCase):
    def test_state_applies_gateway_and_device_reports(self) -> None:
        state = _state()

        assert state.apply_message(
            TrpcPacket.create(
                service=TrpcService.REPORT,
                method=TrpcMethod.REPORTING_STATE,
                fields={"state": 1},
            ),
        )
        assert state.apply_message(
            TrpcPacket.create(
                service=TrpcService.REPORT,
                method=TrpcMethod.CURRENT_TEMPERATURE,
                fields={"address": 1201, "temp": 0x6400},
            ),
        )
        assert state.apply_message(
            TrpcPacket.create(
                service=TrpcService.REPORT,
                method=TrpcMethod.HEAT_SETPOINT,
                fields={
                    "address": 1201,
                    "setback": ThaSetback.CURRENT,
                    "setpoint": 68,
                },
            ),
        )

        assert state.data.gateway.reporting_enabled is True
        snapshot = state.device_snapshots[1201]
        assert snapshot.values["current_temperature"] == 0x6400
        assert snapshot.values["heat_setpoints"] == {"current": 68}

        runtime = state.runtime(1201)
        assert isinstance(runtime, ThermostatRuntime)
        assert runtime.current_temperature == 0x6400
        assert runtime.heat_setpoints.current == 68
        assert state.data.runtime.device_map[1201] == runtime

    def test_apply_message_ignores_unknown_address(self) -> None:
        state = _state()

        result = apply_message(
            state.discovery,
            state.data,
            TrpcPacket.create(
                service=TrpcService.REPORT,
                method=TrpcMethod.CURRENT_TEMPERATURE,
                fields={"address": 9999, "temp": 0x6400},
            ),
        )

        assert result is None

    def test_apply_message_returns_false_for_unchanged_snapshot(self) -> None:
        state = _state()
        message = TrpcPacket.create(
            service=TrpcService.REPORT,
            method=TrpcMethod.CURRENT_TEMPERATURE,
            fields={"address": 1201, "temp": 0x6400},
        )

        assert state.apply_message(message)
        assert not state.apply_message(message)

    def test_topology_message_detection(self) -> None:
        assert is_topology_message(
            TrpcPacket.create(
                service=TrpcService.REPORT,
                method=TrpcMethod.TAKING_ADDRESS,
                fields={"old_address": 0, "new_address": 1201},
            ),
        )

    def test_generic_runtime_view_for_unknown_kind(self) -> None:
        device = DiscoveredDevice(address=1201)
        snapshot = DeviceSnapshot.from_raw_values(
            device,
            {
                "setpoint_target": 1550,
                "fan_percent": {"current": 55},
            },
        )

        runtime = snapshot.runtime

        assert isinstance(runtime, DeviceRuntime)
        assert not isinstance(runtime, ThermostatRuntime)
        assert runtime.setpoint_target == 1550
        assert runtime.fan_percent.current == 55

    def test_setpoint_device_reports_are_setback_indexed(self) -> None:
        device = DiscoveredDevice(
            address=1201,
            attributes=DeviceAttributes(DeviceAttributeBit.SETPOINT_DEVICE),
        )
        discovery = DiscoveryResult(
            gateway=GatewayInfo(firmware_revision=154, protocol_version=3),
            devices=(device,),
        )
        state = Tekmar482State(
            discovery,
            AvailableInfo(
                gateway=GatewaySnapshot(info=discovery.gateway),
                devices=(
                    DeviceSnapshot(
                        DeviceRuntime.create(
                            info=device,
                            available_values=frozenset(
                                {
                                    DeviceValue.SETBACK_STATE,
                                    DeviceValue.SETPOINT_TARGETS,
                                },
                            ),
                            setback_state=int(ThaSetback.OCC_2),
                            setpoint_targets=SetpointValues.from_mapping(
                                {ThaSetback.CURRENT: 1500},
                                active_setback=ThaSetback.OCC_2,
                            ),
                        ),
                    ),
                ),
            ),
        )

        state.apply_message(
            TrpcPacket.create(
                service=TrpcService.RESPONSE_UPDATE,
                method=TrpcMethod.SETPOINT_DEVICE,
                fields={"address": 1201, "setback": ThaSetback.WAKE_4, "temp": 1400},
            ),
        )
        runtime = state.runtime(1201)
        assert runtime is not None
        assert runtime.setpoint_target == 1500
        assert runtime.setpoint_targets.wake_4 == 1400

        state.apply_message(
            TrpcPacket.create(
                service=TrpcService.RESPONSE_UPDATE,
                method=TrpcMethod.SETPOINT_DEVICE,
                fields={"address": 1201, "setback": ThaSetback.OCC_2, "temp": 1550},
            ),
        )

        runtime = state.runtime(1201)
        assert runtime is not None
        assert runtime.setpoint_target == 1550
        assert runtime.setpoint_targets.occ_2 == 1550

    def test_setback_state_refreshes_current_setpoint_values(self) -> None:
        state = _state()
        snapshot = state.device_snapshots[1201]
        state.data = AvailableInfo(
            gateway=state.data.gateway,
            devices=(
                DeviceSnapshot(
                    DeviceRuntime.create(
                        info=snapshot.info,
                        available_values=frozenset(
                            {
                                DeviceValue.SETBACK_STATE,
                                DeviceValue.HEAT_SETPOINTS,
                                DeviceValue.SETPOINT_TARGETS,
                            },
                        ),
                        setback_state=int(ThaSetback.WAKE_4),
                        heat_setpoints=SetpointValues.from_mapping(
                            {
                                ThaSetback.CURRENT: 40,
                                ThaSetback.WAKE_4: 40,
                                ThaSetback.OCC_2: 50,
                            },
                            active_setback=ThaSetback.WAKE_4,
                        ),
                        setpoint_targets=SetpointValues.from_mapping(
                            {
                                ThaSetback.CURRENT: 1400,
                                ThaSetback.WAKE_4: 1400,
                                ThaSetback.OCC_2: 1550,
                            },
                            active_setback=ThaSetback.WAKE_4,
                        ),
                    ),
                ),
            ),
        )

        state.apply_message(
            TrpcPacket.create(
                service=TrpcService.REPORT,
                method=TrpcMethod.SETBACK_STATE,
                fields={"address": 1201, "setback": ThaSetback.OCC_2},
            ),
        )

        runtime = state.runtime(1201)
        assert runtime is not None
        assert runtime.heat_setpoints.current == 50
        assert runtime.setpoint_target == 1550

        state.apply_message(
            TrpcPacket.create(
                service=TrpcService.REPORT,
                method=TrpcMethod.SETBACK_STATE,
                fields={"address": 1201, "setback": ThaSetback.UNOCC_2},
            ),
        )

        runtime = state.runtime(1201)
        assert runtime is not None
        assert runtime.heat_setpoints.current is None
        assert runtime.setpoint_target is None

    def test_current_setpoint_report_updates_active_slot(self) -> None:
        state = _state()
        snapshot = state.device_snapshots[1201]
        state.data = AvailableInfo(
            gateway=state.data.gateway,
            devices=(
                DeviceSnapshot(
                    DeviceRuntime.create(
                        info=snapshot.info,
                        available_values=frozenset(
                            {
                                DeviceValue.SETBACK_STATE,
                                DeviceValue.HEAT_SETPOINTS,
                            },
                        ),
                        setback_state=int(ThaSetback.OCC_2),
                        heat_setpoints=SetpointValues.from_mapping(
                            {ThaSetback.OCC_2: 50},
                            active_setback=ThaSetback.OCC_2,
                        ),
                    ),
                ),
            ),
        )

        state.apply_message(
            TrpcPacket.create(
                service=TrpcService.REPORT,
                method=TrpcMethod.HEAT_SETPOINT,
                fields={
                    "address": 1201,
                    "setback": ThaSetback.CURRENT,
                    "setpoint": 55,
                },
            ),
        )

        runtime = state.runtime(1201)
        assert runtime is not None
        assert runtime.heat_setpoints.current == 55
        assert runtime.heat_setpoints.occ_2 == 55
        assert state.device_snapshots[1201].values["heat_setpoints"] == {
            "current": 55,
            "occ_2": 55,
        }

    def test_runtime_models_freeze_mutable_mapping_inputs(self) -> None:
        raw_setpoints = {ThaSetback.OCC_2: 50}
        values = SetpointValues(raw_setpoints, active_setback=ThaSetback.OCC_2)
        raw_setpoints[ThaSetback.OCC_2] = 99

        raw_groups = {1: True}
        raw_date_time = {
            "year": 2026,
            "month": 6,
            "day": 9,
            "weekday": 2,
            "hour": 21,
            "minute": 46,
        }
        date_time = GatewayDateTime.from_mapping(raw_date_time)
        gateway = GatewaySnapshot(
            info=GatewayInfo(),
            date_time=date_time,
            setpoint_groups=raw_groups,
        )
        runtime = GatewayRuntime(
            metadata=GatewayInfo(),
            date_time=date_time,
            setpoint_groups=raw_groups,
        )
        raw_groups[1] = False
        raw_date_time["minute"] = 59

        decoded = DecodedSetbackValues(
            {ThaSetback.OCC_2: "occupied"},
            active_setback=ThaSetback.OCC_2,
        )

        assert values.current == 50
        assert gateway.setpoint_groups is not None
        assert gateway.setpoint_groups[1] is True
        assert runtime.setpoint_groups[1] is True
        assert gateway.date_time == GatewayDateTime(
            year=2026,
            month=6,
            day=9,
            weekday=2,
            hour=21,
            minute=46,
        )
        assert runtime.date_time == gateway.date_time
        assert decoded.current == "occupied"

        with self.assertRaises(TypeError):
            values.values[ThaSetback.OCC_2] = 99  # type: ignore[index]
        with self.assertRaises(TypeError):
            gateway.setpoint_groups[1] = False  # type: ignore[index]
        with self.assertRaises(TypeError):
            runtime.setpoint_groups[1] = False  # type: ignore[index]
        with self.assertRaises(TypeError):
            decoded.values[ThaSetback.OCC_2] = "changed"  # type: ignore[index]

    def test_current_setpoint_uses_observed_current_when_active_is_unavailable(
        self,
    ) -> None:
        values = SetpointValues.from_mapping(
            {
                ThaSetback.OCC_2: None,
                ThaSetback.CURRENT: 52,
            },
            active_setback=ThaSetback.OCC_2,
            current_observed_setback=ThaSetback.OCC_2,
        )
        decoded = values.decoded(
            lambda value: value if isinstance(value, int) else None,
        )

        assert values.current == 52
        assert decoded.current == 52
