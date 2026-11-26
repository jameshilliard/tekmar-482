import unittest

from tekmar_482 import (
    SETBACK_2_EVENT_SETBACKS,
    SETBACK_4_EVENT_SETBACKS,
    AvailableInfo,
    DecodedSetbackValues,
    DeviceAttributeBit,
    DeviceAttributes,
    DeviceRuntime,
    DeviceSnapshot,
    DeviceValue,
    DiscoveredDevice,
    DiscoveryResult,
    GatewayInfo,
    GatewayRuntime,
    GatewaySnapshot,
    SetpointValues,
    Tekmar482State,
    ThaSetback,
    ThermostatRuntime,
    TrpcCommand,
    TrpcMethod,
    TrpcPacket,
    TrpcService,
    apply_message,
    decode_dege,
    is_topology_message,
    schedule_setbacks_for_events,
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
    def test_fan_percent_step_is_model_specific_and_raw_values_survive(self) -> None:
        for code, expected in [
            (99201, 70),
            (99202, 70),
            (99203, 70),
            (107201, 7),
            (None, 7),
        ]:
            device = DiscoveredDevice(1, type_code=code)
            snapshot = DeviceSnapshot.from_raw_values(
                device,
                {"fan_percent": {"current": 7}},
            )
            assert snapshot.runtime.current_fan_percent == expected
            assert snapshot.decoded.fan_percent.current == expected
            assert snapshot.values["fan_percent"] == {"current": 7}
            assert device.encode_fan_percent(expected) == 7
            assert device.decode_fan_percent(255) is None
        with self.assertRaises(ValueError):
            DiscoveredDevice(1, type_code=99201).encode_fan_percent(73)

    def test_reported_setpoint_capability_takes_precedence_over_model(self) -> None:
        for attrs, expected in ((None, True), (0, False), (128, True)):
            device = DiscoveredDevice(
                1201,
                type_code=101101,
                attributes=DeviceAttributes(attrs),
            )
            assert device.supports_setpoint_device is expected

    def test_current_reply_selects_one_period_for_the_entire_device(self) -> None:
        state = _state()
        occupied, away = ThaSetback.OCC_2, ThaSetback.AWAY
        for slot, value in ((occupied, 50), (away, 56)):
            state.apply_message(
                TrpcPacket.create(
                    method=TrpcMethod.COOL_SETPOINT,
                    service=TrpcService.REPORT,
                    fields={"address": 1201, "setback": slot, "setpoint": value},
                ),
            )
        state.apply_message(
            TrpcPacket.create(
                method=TrpcMethod.SETBACK_STATE,
                service=TrpcService.REPORT,
                fields={"address": 1201, "setback": occupied},
            ),
        )
        command = TrpcCommand.request(
            TrpcMethod.HEAT_SETPOINT,
            {"address": 1201, "setback": ThaSetback.CURRENT},
        )
        reply = TrpcPacket.create(
            method=command.method,
            service=TrpcService.RESPONSE_REQUEST,
            fields={"address": 1201, "setback": away, "setpoint": 40},
        )
        state.apply_message_result(reply)
        assert state.device_snapshots[1201].runtime.cool_setpoints.current == 50
        state.apply_message_result(reply, command)
        runtime = state.device_snapshots[1201].runtime
        assert runtime.setback_state == away
        assert runtime.heat_setpoints.current == 40
        assert runtime.cool_setpoints.current == 56
        assert runtime.fan_percent.active_setback is away

    def test_unsupported_floor_report_cannot_supply_a_temperature(self) -> None:
        state = _state()
        assert not state.apply_message(
            TrpcPacket.create(
                method=TrpcMethod.CURRENT_FLOOR_TEMPERATURE,
                service=TrpcService.REPORT,
                fields={"address": 1201, "temp": 1500},
            ),
        )
        assert state.device_snapshots[1201].runtime.current_floor_temperature is None

    def test_confirmed_heat_write_updates_only_the_affected_cache_slots(self) -> None:
        method = TrpcMethod.HEAT_SETPOINT
        for enabled, expected_away in ((1, 30), (0, 44), (None, None)):
            with self.subTest(enabled=enabled):
                state = _state()
                if enabled is not None:
                    state.apply_message(
                        TrpcPacket.create(
                            service=TrpcService.REPORT,
                            method=TrpcMethod.SETBACK_ENABLE,
                            fields={"enable": enabled},
                        ),
                    )
                state.apply_message(
                    TrpcPacket.create(
                        service=TrpcService.REPORT,
                        method=TrpcMethod.SETBACK_STATE,
                        fields={"address": 1201, "setback": ThaSetback.WAKE_4},
                    ),
                )
                for slot, value in ((ThaSetback.CURRENT, 38), (ThaSetback.AWAY, 30)):
                    state.apply_message(
                        TrpcPacket.create(
                            service=TrpcService.REPORT,
                            method=method,
                            fields={
                                "address": 1201,
                                "setback": slot,
                                "setpoint": value,
                            },
                        ),
                    )
                command = TrpcCommand.update(
                    method,
                    {"address": 1201, "setback": ThaSetback.OCC_4, "setpoint": 44},
                )
                ack = TrpcPacket.create(
                    service=TrpcService.RESPONSE_UPDATE,
                    method=method,
                    fields=command.fields,
                )
                state.apply_message_result(ack, command)
                values = state.device_snapshots[1201].runtime.heat_setpoints
                assert values.current == values.wake_4 == values.occ_4 == 44
                assert values.away == expected_away
                assert ThaSetback.OCC_2 not in values.values

    def test_enabled_fan_write_keeps_away_distinct_from_unoccupied(self) -> None:
        runtime = DeviceRuntime.create(
            info=DiscoveredDevice(address=1201),
            fan_percent=SetpointValues.from_mapping(
                {ThaSetback.AWAY: 80, ThaSetback.SLEEP_4: 20},
                active_setback=ThaSetback.AWAY,
            ),
        )
        values = runtime.with_setpoint(
            DeviceValue.FAN_PERCENT,
            ThaSetback.UNOCC_2,
            40,
            confirmed_write=True,
            setbacks_enabled=True,
        ).fan_percent
        assert values.unocc_2 == values.sleep_4 == 40
        assert values.away == values.current == 80

    def test_schedule_setbacks_for_events(self) -> None:
        assert schedule_setbacks_for_events(0) == ()
        assert schedule_setbacks_for_events(2) == SETBACK_2_EVENT_SETBACKS
        assert schedule_setbacks_for_events(4) == SETBACK_4_EVENT_SETBACKS
        assert schedule_setbacks_for_events(7) is None
        assert schedule_setbacks_for_events(None) is None

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
        assert runtime.setpoint_target == 1400
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
        gateway = GatewaySnapshot(
            info=GatewayInfo(),
            setpoint_groups=raw_groups,
        )
        runtime = GatewayRuntime(
            metadata=GatewayInfo(),
            setpoint_groups=raw_groups,
        )
        raw_groups[1] = False

        decoded = DecodedSetbackValues(
            {ThaSetback.OCC_2: "occupied"},
            active_setback=ThaSetback.OCC_2,
        )

        assert values.current == 50
        assert gateway.setpoint_groups is not None
        assert gateway.setpoint_groups[1] is True
        assert runtime.setpoint_groups[1] is True
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

    def test_explicit_unavailable_active_slot_does_not_revive_cached_target(
        self,
    ) -> None:
        slot = ThaSetback.OCC_2
        values = SetpointValues(active_setback=slot).with_value(ThaSetback.CURRENT, 42)
        for raw in (0xFF, 0, 44):
            values = values.with_value(slot, raw)
            decoded = values.decoded(decode_dege)
            assert decoded.current == decode_dege(values.current)
            assert decoded.get(ThaSetback.CURRENT) == decode_dege(raw)
            assert decoded.as_dict()["current"] == decode_dege(raw)
