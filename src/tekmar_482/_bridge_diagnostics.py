"""Current custom bridge wire version and little-endian diagnostic replies."""

from struct import Struct

WIRE_VERSION = 2
PANEL = Struct("<2I2Q3I")
CAPTURE_COUNTERS = Struct("<2I5Q5I")
PORT_DROPS = Struct("<BQQ")
TRACE_STATE = Struct("<B5Q")
MAX_COUNTER = (1 << 64) - 1
