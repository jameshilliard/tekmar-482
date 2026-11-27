import unittest

from tekmar_482 import (
    AirFilterUsage,
    GatewayTraceGap,
    NativePath,
    NativeState,
    ThermostatEquipment,
    ThermostatRelayStatus,
    WritableBlock,
)
from tekmar_482.cli_status import equipment_fields

from .native_fixtures import TEST_PATH, block, frame, identity

_OTHER = NativePath(2, TEST_PATH.subnet, TEST_PATH.node)
_MODEL = 105102


def _equipment(
    state: NativeState,
    path: NativePath = TEST_PATH,
) -> ThermostatEquipment:
    climate = state.climate(path)
    assert climate is not None
    return climate.equipment


class EquipmentTests(unittest.TestCase):
    def test_reported_words_are_separate_and_keep_their_source(self) -> None:
        state = NativeState()
        for path in (TEST_PATH, _OTHER):
            state.observe(identity(_MODEL, path=path))
            state.observe(block(0x99, bytes.fromhex("03 0000"), path))
        relays = block(0x1B, bytes.fromhex("17 01 fe 00 02"))
        for report in (
            relays,
            block(0x0A, bytes.fromhex("0d ff 7856")),
            block(0x1C, bytes.fromhex("03 3412")),
            block(0x99, bytes.fromhex("03 ffff")),
            block(0x9A, bytes.fromhex("02 ab")),
        ):
            assert state.observe(report) == (TEST_PATH,)
            assert not state.observe(report)
        equipment = _equipment(state)
        assert equipment.relay_status and equipment.relay_status.frame is relays
        assert equipment.relay_status.value == ThermostatRelayStatus(
            1,
            254,
            0,
            None,
            2,
            None,
        )
        assert (
            equipment.filter_usage
            and equipment.filter_usage.value == AirFilterUsage(None, 0x5678)
        )
        assert [(r.block, r.raw, r.hours) for r in equipment.runtimes] == [
            (WritableBlock.HEAT_STAGE1_RUNTIME, 0x1234, None),
            (WritableBlock.W1_HEAT_RUNTIME, 0xFFFF, 0xFFFF),
            (WritableBlock.W1_COOL_RUNTIME, None, None),
        ]
        assert _equipment(state, _OTHER).runtimes[0].hours == 0
        assert equipment_fields(equipment)["relays"] == {
            "heat_stage1": 1,
            "heat_stage2": 254,
            "cool_stage1": 0,
            "cool_stage2": None,
            "fan1": 2,
            "fan2": None,
            "received_ms": 1,
        }

    def test_edit_proposal_never_becomes_a_runtime_readback(self) -> None:
        state = NativeState()
        state.observe(identity(_MODEL))
        state.observe(block(0x99, bytes.fromhex("03 3412")))
        state.observe(block(0x9A, bytes.fromhex("03 0000")))
        sender = NativePath(TEST_PATH.port, 0, 0)
        update = bytes([TEST_PATH.subnet, TEST_PATH.node, 0x99, 0, 0, 0])
        assert not state.observe(frame(0x2DF, update[:-1], sender))
        assert state.observe(frame(0x2DF, update, sender)) == (TEST_PATH,)
        assert [r.block for r in _equipment(state).runtimes] == [
            WritableBlock.W1_COOL_RUNTIME,
        ]
        state.observe(block(0x99, bytes.fromhex("03 0000")))
        assert [r.hours for r in _equipment(state).runtimes] == [0, 0]
        state.observe(GatewayTraceGap(1, 1 << TEST_PATH.port))
        assert state.climate(TEST_PATH) is None

    def test_heat_pump_runtime_uses_reported_instance_and_availability(self) -> None:
        state = NativeState()
        state.observe(identity(_MODEL))
        state.observe(block(0x97, bytes.fromhex("03 09 08")))
        state.observe(block(0x98, bytes.fromhex("07 00 2301")))
        state.observe(block(0x98, bytes.fromhex("07 03 4503")))
        assert [(r.selector, r.hours) for r in _equipment(state).runtimes] == [
            (0, 0x123),
            (3, 0x345),
        ]
        state.observe(block(0x97, bytes.fromhex("03 08 08")))
        assert [(r.selector, r.hours) for r in _equipment(state).runtimes] == [
            (3, 0x345),
        ]

    def test_onboard_sensor_selection_is_not_the_humidity_mode_or_sample(self) -> None:
        state = NativeState()
        state.observe(identity(_MODEL))
        state.observe(block(0xA3, bytes.fromhex("03 00 01")))
        state.observe(block(0xA8, bytes.fromhex("01 01")))
        sample = frame(0x323, bytes.fromhex("00 00 32 0000 32"))
        state.observe(sample)
        current = state.thermostats[0].adjustments
        assert current.onboard_humidity_enabled is True
        assert current.humidity and current.humidity.value.relative_percent == 50
        state.observe(block(0xA8, bytes.fromhex("01 00")))
        updated = state.thermostats[0].adjustments
        assert updated.onboard_humidity_enabled is False and updated.humidity is None
        assert updated.humidity_setup == current.humidity_setup
        state.observe(block(0xA8, bytes.fromhex("01 ff")))
        assert state.thermostats[0].adjustments.onboard_humidity_enabled is None
