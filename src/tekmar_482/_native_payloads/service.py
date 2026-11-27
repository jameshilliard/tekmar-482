"""Known tN4 service-frame layouts; these are not appliance capabilities."""

from .plant import MIXING_UPDATE

CALIBRATION_MODE = 0x287
CYCLE_LENGTH_REQUEST = 0x33f
NETWORK_TEST_TIME = 0x387
NETWORK_TEST_MODE = 0x38f

_BASE_LENGTHS = {
    CALIBRATION_MODE: 1,
    CYCLE_LENGTH_REQUEST: 2,
    MIXING_UPDATE: 3,
    NETWORK_TEST_TIME: 2,
    NETWORK_TEST_MODE: 3,
}


def service_frame_supported(message_id: int, payload: bytes) -> bool:
    """Allow only known base layouts in Normal mode; preserve tails in Raw."""
    return len(payload) == _BASE_LENGTHS.get(message_id)
