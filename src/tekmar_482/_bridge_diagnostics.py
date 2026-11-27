"""Current custom bridge wire version and little-endian diagnostic replies."""

from enum import IntEnum
from struct import Struct

WIRE_VERSION = 2
PANEL = Struct("<2I2Q3I")
CAPTURE_COUNTERS = Struct("<2I5Q5I")
PORT_DROPS = Struct("<BQQ")
TRACE_STATE = Struct("<B5Q")
LINK_SERVICE_TIMING = Struct("<4I")
PORT_TRANSMIT_TIMING = Struct("<B4I")
ERROR_COUNTER = Struct("<BBIQ")
ERROR_REPORTING_METHOD = 0x544BF00C
LINK_TIMING_METHOD = 0x544BF014
MAX_COUNTER = (1 << 64) - 1


class ErrorSource(IntEnum):
    PORT_0 = 0
    PORT_1 = 1
    PORT_2 = 2
    PORT_3 = 3
    PORT_4 = 4
    GATEWAY = 5


class ErrorReason(IntEnum):
    STUFFING = 1
    CRC = 2
    CRC_DELIMITER = 3
    BAD_END = 4
    RX_NO_ACK = 5
    EARLY_START = 6
    STUCK_DOMINANT = 7
    RESPONSE_DRIVE = 8
    TX_NO_ACK = 9
    ARBITRATION_LOST = 10
    TX_ECHO_MISMATCH = 11
    TX_BAD_END = 12
    TX_MISSED_TIMING = 13
    PIN_UNAVAILABLE = 14
    RAW_VALID_DROP = 15
    RAW_INVALID_DROP = 16
    TRACE_DROP = 17
    NATIVE_READ_DROP = 18
    UART_OVERRUN = 0x80
    UART_PARITY = 0x81
    UART_FRAMING = 0x82
    UART_BREAK = 0x83
    TPCK_CHECKSUM = 0x84
    TPCK_FRAMING = 0x85
    TPCK_RESYNC = 0x86
    ADDRESS_NOTICE_DROP = 0x87

    def supports(self, source: ErrorSource) -> bool:
        """UART and gateway-wide faults do not belong to a physical port."""
        return (self >= ErrorReason.UART_OVERRUN) == (source is ErrorSource.GATEWAY)
