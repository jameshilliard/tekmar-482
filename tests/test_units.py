import unittest

from tekmar_482.units import (
    celsius_to_dege,
    celsius_to_degh,
    dege_to_celsius,
    degh_to_celsius,
    degh_to_fahrenheit,
    fahrenheit_to_degh,
)


class UnitsTest(unittest.TestCase):
    def test_dege_setpoint_conversion(self) -> None:
        assert celsius_to_dege(21.5) == 43
        assert dege_to_celsius(43) == 21.5
        assert dege_to_celsius(0xFF) is None

    def test_degh_temperature_conversion(self) -> None:
        assert fahrenheit_to_degh(70) == 1550
        assert degh_to_fahrenheit(1550) == 70
        assert round(degh_to_celsius(1550) or 0, 1) == 21.1
        assert celsius_to_degh(21.1111111111) == 1550
        assert degh_to_celsius(0xFFFF) is None
