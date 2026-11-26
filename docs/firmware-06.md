# Firmware 06 compatibility

This page covers stock tRPC. For the custom bridge, see the firmware project's
[native tN4 frame reference](https://github.com/jameshilliard/tekmar-482-firmware/blob/master/docs/tn4-frames.md).

Reviewed image: `101702_482_J1196J.hex` (SHA-256
`659e5a10e39137485614db0a3174c4e85dcf1a1ef206470ae47bc6111e2522ba`).
This image reports protocol version 3. Addresses below are code offsets
in this image, not stable identifiers for other releases.

## Wire format and endpoints

The TPCK encoder at `0xbb20` escapes CA, 35 and 2F with 2F. Its checksum
is the low byte of length + packet type + payload sum. An escaped 35 is data;
only an unescaped 35 terminates a frame. Packet type 6 carries a service
byte followed by a little-endian 32-bit method ID (`0xbd94`).

The table covers the input dispatcher at `0xae8c` and calls to the tRPC
output routine at `0xbe80`. Byte counts exclude the five-byte header.
"Readback" means Update returns the cached value without changing it.
These are firmware behaviors, not a list of recommended commands.

| Methods (hex) | Output bytes | Request / Update behavior |
| --- | ---: | --- |
| 000 Null | 0 | Fallback reply for an unrecognized method |
| 107 NetworkError | 2 | Read / readback |
| 10F ReportingState, 16F SetbackEnable | 1 | Read / set gateway flag |
| 117 OutdoorTemp | 2 | Read / update |
| 11F DeviceAttributes | 4 | Read / readback |
| 127 ModeSetting | 3 | Read / update; unavailable with setpoint-device bit 7 |
| 12F ActiveDemand | 3 | Read / readback |
| 137 CurrentTemperature, 138 CurrentFloorTemperature | 4 | Read / readback; floor value requires attribute bit 2 or 8 |
| 13D SetpointGroupEnable | 2 | Read / send group command; no direct update acknowledgment |
| 13E SetpointDevice | 5 | Read / update; attribute bit 7 |
| 13F HeatSetpoint, 147 CoolSetpoint, 14F SlabSetpoint, 157 FanPercent | 4 | Read / update; capability-gated |
| 150 RelativeHumidity | 3 | Read only |
| 151 HumiditySetMax, 152 HumiditySetMin | 3 | Read / update; separate capability bits |
| 15F TakingAddress | 4 | Unsolicited report: old and new addresses |
| 167 DeviceInventory | 2 | Walk or single lookup; Update is not a read |
| 177 SetbackState, 17F SetbackEvents | 3 | Read / readback |
| 187 FirmwareRevision, 18F ProtocolVersion | 2 | Read only |
| 197 DeviceType, 19F DeviceVersion | 6 | Read only |
| 1A7 DateTime | 7 | Update only; rejection returns seven FF bytes |

## What the client can infer

- Inventory request zero walks the device table and ends with zero. A nonzero
  address returns exactly one address or FFFF, with no following terminator
  (`0xa82c`). Do not infer a missing device from a timed-out walk.
- A named slot in a reply to a CURRENT request or update identifies the
  device-wide active period: the indexed handlers and SetbackState read the
  same device byte at offset 2E. An unsolicited named-slot report or an
  ambiguous echo does not establish which slot was requested.
- With setbacks enabled, Wake and both Occupied selectors share a cache
  bucket; Sleep and both Unoccupied selectors share another; Away is
  separate. With setbacks disabled a write fills every bucket for that
  setting (e.g. heat at `0x29f4`). A fan bus write key is not a cache index.
- Mode and setpoint write replies describe the gateway cache, not an
  independent confirmation from the thermostat. There is no transaction ID.
  An echo cannot confirm two commands, and a timed-out write is not retried.
- The stock mode request (`0x93b4`) emits its cached response before returning
  to the dispatcher. ProtocolVersion (`0xab18`) uses the same serialized
  send path (`0xbe80`). Its solicited response marks a boundary for preceding
  inline cache reads, not for native bus operations still in progress.
- Group IDs are 1 through 12. The update handler at `0x97b0` submits a bus
  command without a direct reply. The group-status callback updates the cache
  even when reporting is off, but checks the reporting flag (`0x234c`) before
  emitting a group report. A subsequent matching report provides evidence
  of gateway state, not causation or slab demand. Enabled flags can expire silently.
- Floor readback at `0x95f4` populates the temperature only if bit 2 or 8
  is set. Without capability evidence the client neither requests it nor
  promotes a received floor value into device state.

The tests check the output IDs and lengths, parse and frame boundaries,
and replay documented state transitions without a gateway. They cannot
establish bus timing, thermostat-side application of writes, or behavior
on other firmware revisions.
