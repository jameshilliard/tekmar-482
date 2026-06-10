import unittest

from tekmar_482 import (
    decode_active_demand,
    decode_dege,
    decode_degh,
    decode_device_mode,
    decode_device_values,
    decode_network_error,
    decoded_to_dict,
)


class DecodingTest(unittest.TestCase):
    def test_decode_gateway_values(self) -> None:
        outdoor = decode_degh(1550)
        error = decode_network_error(0x00C6)

        assert outdoor is not None
        assert outdoor.fahrenheit == 70.0
        assert outdoor.celsius == 21.1
        assert error is not None
        assert error.low_byte == 0xC6
        assert error.description == "Snow/Ice Sensor Temperature Drift"

    def test_decode_invalid_or_not_available_temperatures(self) -> None:
        assert decode_dege(0xFF) is None
        assert decode_degh(0xFFFF) is None

    def test_decode_degh_does_not_apply_arbitrary_temperature_bounds(self) -> None:
        decoded = decode_degh(0x4000)

        assert decoded is not None
        assert decoded.raw == 0x4000
        assert decoded.fahrenheit == 1553.4

    def test_decode_device_values_to_json_ready_dict(self) -> None:
        decoded = decode_device_values(
            {
                "current_temperature": 1550,
                "active_demand": 1,
                "mode_setting": 2,
                "heat_setpoints": {"current": 42},
            },
        )

        payload = decoded_to_dict(decoded)

        assert payload == {
            "current_temperature": {
                "raw": 1550,
                "celsius": 21.1,
                "fahrenheit": 70.0,
            },
            "active_demand": {
                "raw": 1,
                "name": "heat",
                "description": None,
            },
            "mode_setting": {
                "raw": 2,
                "name": "auto",
                "description": None,
            },
            "heat_setpoints": {
                "current": {
                    "raw": 42,
                    "celsius": 21.0,
                    "fahrenheit": 69.8,
                },
            },
        }

    def test_decode_unknown_enum_values(self) -> None:
        demand = decode_active_demand(99)
        mode = decode_device_mode(99)

        assert demand is not None
        assert demand.name == "unknown"
        assert mode is not None
        assert mode.name == "unknown"
