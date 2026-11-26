# tekmar-482

`tekmar-482` is an asyncio Python client for the tekmar 482 tN4 gateway.

The default protocol is the [custom native tN4 bridge](https://github.com/jameshilliard/tekmar-482-firmware).
For the original vendor firmware, explicitly select `--vendor` in the CLI or
`vendor=True` in `open_gateway()`. The library does not silently try the other
protocol or install an image when identification fails. Both protocols support:

- Raw TCP streams, suitable for transparent ser2net-style serial bridges
- Local RS-232 adapters through the optional `serial` extra
- serialx URLs such as `socket://host:port` and `rfc2217://host:port` when the
  optional `serial` extra is installed

## Install

```bash
uv add tekmar-482
```

Direct serial support is optional:

```bash
uv add "tekmar-482[serial]"
```

For local development:

```bash
uv sync --dev
uv run pre-commit install
```

## Native bridge

```python
from tekmar_482 import BridgeClient, GatewayEndpoint, NativeFrame, open_gateway

async def read_frame() -> None:
    endpoint = GatewayEndpoint.tcp("192.0.2.10", 3001)
    async with open_gateway(endpoint) as bridge:
        assert isinstance(bridge, BridgeClient)
        print(await bridge.panel())
        async with bridge.stream() as stream:
            while True:
                frame = await stream.read()
                if isinstance(frame, NativeFrame):
                    print(frame.port, frame.message_id, frame.decoded)
                    break
        print(stream.diagnostics)
```

`monitor` streams native frames. `discover` sends one read-only block-zero
request per observed port/subnet to identify all answering nodes, including
quiet thermostats and plant masters. `dump` then reads the cataloged and
previously observed blocks for those receive paths and prints the panel
state, native frames and receiver summaries. See the firmware's
[frame catalog](https://github.com/jameshilliard/tekmar-482-firmware/blob/master/docs/tn4-frames.md)
and [binary UART protocol](https://github.com/jameshilliard/tekmar-482-firmware/blob/master/docs/tpck-bridge.md).

Discovery reports receive paths, **not a count of physical devices**:
a plant controller can forward the same identity onto multiple lines.
An unknown model or a missing reply never promotes a link ACK to an
identity. The node lock bit does not create a second device on a path.
Each result distinguishes the catalog's candidate blocks from those
actually reported with supported bytes during the scan.

```bash
tekmar-482 discover --tcp 192.0.2.10:3001 --duration 45 --json
```

Use `--probe-subnet PORT:SUBNET` to include a known silent subnet, or
`--passive` to skip all native requests. A solitary peer can need a link
ACK to deliver its frames: `--ack-port PORT` is explicit and repeatable.
By default the gateway does not ACK incoming Raw frames. Use a longer
window if devices have not yet answered. Scanning does not change setpoints.

```python
from tekmar_482 import discover_native

async with bridge.stream(ack_ports=(0,)) as stream:
    result = await discover_native(stream, duration=45)
for device in result.devices:
    print(device.port, device.address, device.identity)
```

The scanner owns the capture consumer for the window; `on_record` can
observe the same traffic. For an ongoing subscription, feed frames from
your own receive loop to `NativeInventory.observe()` instead.

For a model-aware read of one device, use its physical port, native subnet
and node from discovery, for example:

```bash
tekmar-482 read-device 2 0x12 3 --tcp 192.0.2.10:3001 --json
```

`read_native_snapshot(stream, devices)` implements this command and the
device-read phase of `dump`. Its block reader, `read_native_devices()`,
confirms identity before expanding a model's block list.
Unknown models can still be queried for blocks and
selectors previously observed on that path. An explicit zero availability
mask advertises no instances; a missing mask does not. Read batches group
nodes sharing a subnet, block and selector into one request. Results contain
the link outcome, matching frames (including raw bytes and field support
masks), and any nodes that didn't reply. A missing reply means unknown.
Use `--block-timeout` on `dump` or `--timeout` on `read-device` to
adjust the per-request deadline (default 20 seconds). Missing peers get
one read-only retry by default; `--block-attempts 1` disables it. Peers
that already replied are not queried again in the retry. A missing TX
result stops that port, since its send slot may still be occupied. `--duration` on
`dump` bounds discovery only. No settings or counters are written.

Use `bridge.normal()` to run tRPC on the **same** custom-firmware connection:

```python
async with bridge.normal() as client:
    addresses = await client.get_inventory_addresses()
```

Normal and Raw sessions take exclusive turns on the link. The Normal client
is valid only inside its context. A cold gateway needs time to hear devices;
its inventory is not the same as the list of all native frame sources. See
[Normal method coverage](https://github.com/jameshilliard/tekmar-482-firmware/blob/master/docs/normal-mode.md)
before using other vendor client methods on custom firmware.

The CLI `request` command uses Normal mode on custom firmware by default.
For example, with a previously discovered tHA address:

```bash
tekmar-482 request CurrentTemperature address=1001 --tcp 192.0.2.10:3001 --json
```

Use `client.reports()` inside `bridge.normal()` for unsolicited temperature
and demand updates; enable them with `await client.set_reporting_state(enabled=True)`.

In custom Normal mode the mode, heat/cool/slab setpoint, fan and humidity
limit setters wait for device readback. Inspect the returned `mode`,
`setpoint` or `percent`: the device can
keep its previous value or return `0xff` (unavailable). A timeout does not
prove the write failed; read before retrying. For a quiet device use
`timeout=12` to allow the gateway's ten-second transaction to finish.
Stock firmware uses its original write-echo matching.

For an addressed setpoint controller, use `set_setpoint_device_celsius()`
or `set_latest_setpoint_device()` (raw degH). Custom Normal mode returns
the verified 16-bit target in `temp` and the resolved `setback`. These
are not thermostat HeatSetpoint bytes or snowmelt group enable commands.

`await client.get_outdoor_temperature_celsius()` returns the selected
network temperature, or `None` if there is no source. An unanswered read
raises `TimeoutError`. To provide a fallback source, call
`await client.set_outdoor_temperature_celsius(value)` with a fresh sample
before four minutes elapse. A network sensor takes priority: the Update
reply's `temp` is the selected value in degH, not necessarily the supplied
value. An unsent update superseded by a newer sample returns `None`.
The client does not refresh an old sample on its own.

`set_humidity_min()` controls humidification and `set_humidity_max()`
controls dehumidification; measuring RH does not imply either is enabled.
The low-level fan client uses the device's wire units. On a discovered
device the high-level fan setter and decoded runtime use physical percent,
including the 10-percent steps on the 544, 545 and 546.

For an addressed native read, use the physical port and wire address
from a frame observed on that line. These are not tRPC decimal addresses:

```python
import asyncio

from tekmar_482 import NativeAddress, NativeBlockQuery

source = NativeAddress(0, 0)  # Host-supplied gateway address.
query = NativeBlockQuery(frame.message.source, 0x10)
async with asyncio.timeout(20):
    report = await stream.read_block(frame.port, query, source=source)
print(report.decoded)
```

`read_block()` waits for a new report from that port, subnet and node for
the requested block. Indexed blocks require a selector, including zero;
for example, `NativeBlockQuery(peer, SnowmeltBlock.SLAB_HIGH, selector=0)`.
No records are taken from the normal capture stream. One block read can
be pending per port. Choose an application deadline: some physical
controllers take over ten seconds to report. There is no request ID, so
a matching report is a device observation, not proof that a particular
request caused it. A zero support mask conveys no value. An indexed
report without a supported selector remains in the capture stream, but
cannot complete a read for a particular selector.

Keep the full source node from a report for diagnostics. For an addressed
write, the 483's block writer and the original 482 address the five-bit
node, not its lock flag. `NativeMessage.update_block()` makes this conversion
and rejects node zero; it does not change the caller's source address.

The shared decoder covers all 166 addressed block layouts registered in
the 483 SW 11 reference. For example, on a 402,
`NativeBlockQuery(peer, 0x44, selector=129)` reads the DHW pump's
`pump_hours`; selector 0 reads the primary pump. Check that the field
is present before using it: an unsupported byte is not zero hours.
The firmware's [addressed block catalog](https://github.com/jameshilliard/tekmar-482-firmware/blob/master/docs/tn4-blocks.md)
lists the plant, thermostat, heat pump, schedule and snowmelt layouts.

For an addressed edit use `write_block()`. It reads the selected peer,
preserves all sibling bytes and flags, sends at most one update, and
requests a fresh report. For example, to change only the wake heating
setpoint on a thermostat:

```python
from tekmar_482 import NativeField, NativeWriteStatus, WritableBlock, celsius_to_dege

query = NativeBlockQuery(peer, WritableBlock.HEATING_SETPOINTS)
result = await stream.write_block(
    port, query, {NativeField.WAKE: celsius_to_dege(20)}, source=source
)
if result.status is NativeWriteStatus.DIFFERENT and result.readback is not None:
    print(result.readback.require_report().decoded)  # Actual device value.
```

Values are native wire units, not universally degrees or percentages.
Use `tekmar-482 write-fields` offline to list the known block and field
names; the catalog is not a device capability advertisement. A missing
support bit or incomplete positional source prevents a write. An unchanged
value never sends an update. Only the requested fields are compared on
readback. A timeout after sending leaves the outcome unknown and does not
retry. The client serializes edits on the same port, but tN4 does not
lock out another device or a manual change.

The CLI previews the complete proposed frame by default. Add `--confirm`
to apply the edit and see the separate link result and device readback:

```bash
tekmar-482 write-fields 0x05
tekmar-482 write-block 2 0x12 3 0x05 wake=40 --tcp 192.0.2.10:3001 --json
```

The CLI uses the same reader. For example, with an **observed** native node
on a dedicated single-peer line (addresses below are examples):

```bash
tekmar-482 read-block 2 0x12 3 0x71 --ack --tcp 192.0.2.10:3001 --json
```

`--ack` supplies a link ACK to valid incoming frames on that port. It is
off by default. Use `--selector` for indexed blocks and `--timeout` to
change the CLI's 20-second deadline.

`send()` transmits once and waits for that port's terminal link result:
`ACK`, `REJECTED` before transmission, or `FAILED` during transmission.
A link ACK does not confirm an application change. A block read can still
succeed on a real report if the link ACK was missed; other send failures
raise `NativeTransmitError` with the underlying `TxResult`.

Different ports can send concurrently, without a capture consumer. A second
send on the same port raises `ProtocolError` until its previous result
arrives, even if the caller cancelled or timed out. There is no implicit retry.
`stream.read()` delivers native frames, invalid captures, and non-terminal
port errors in receive order. A slow consumer gets a `CaptureGap` with the
number of oldest records dropped by the host; set `capture_backlog` on
`bridge.stream()` to change the default of 512 records. Gateway drop counts
are separate and available in `stream.diagnostics` after a normal exit.

For a matched, complete weekly event report:

```python
from calendar import Day
from datetime import time

from tekmar_482 import WeeklyEvent

week = WeeklyEvent.from_frame(report)
print(week.event.name, week.time_on(Day.MONDAY))
change = week.with_time(Day.MONDAY, time(6, 30))
update = change.update(source)  # Construct only; nothing is sent here.
readback = change.request(source)
```

Each block is one event across seven days, not seven complete schedules.
Use `None` for a disabled event. Edits preserve the other day bytes and
the indexed schedule selector. Sparse or extended reports are available
through `frame.decoded` but cannot be used to construct a whole-week edit.
After an intentional write, read the same block back and compare it with
the intended change. Neither the local transmit echo nor a link ACK
confirms the setting.

On an identified snowmelt controller, use `SnowmeltBlock` for addressed
configuration, including melt/storm group IDs, runtimes and slab targets.
A complete target report can construct a native setpoint update:

```python
from tekmar_482 import SnowmeltBlock, SnowmeltSetpoint

query = NativeBlockQuery(peer, SnowmeltBlock.IDLE_SETPOINT)
async with asyncio.timeout(20):
    report = await stream.read_block(port, query, source=source)
slab = SnowmeltSetpoint.from_frame(report)
print(slab.celsius, slab.is_off)
update = slab.update_celsius(2.0, source=source)  # Constructs only.
```

`None` requests Off for Idle or Storm; Melt has no Off setting.
A temperature update does not start a melt cycle. After an intentional
send, read the same block again and check the device's reported value.

**Live status queries.** A device's operating state and slab temperature
are reported as snowmelt status keys, separate from its configuration
blocks. `read_snowmelt_status(stream, port, peer, source=source)` reads
the application and configured zone or sensor number before sending the status request.
It requests the 483 application's keys by default. Pass `keys=bytes([...])`
to select other keys, including melt and storm group IDs. The public
`NativeStatusQuery.snowmelt(role, number, keys)` takes the **one-based**
configured number, not a native node or the zero-based 17F wire destination.
Both `read-device` and active `dump` use the configuration they already
read from an identified snowmelt controller to request live telemetry.

```bash
tekmar-482 read-status snowmelt 2 0x12 3 --keys 19221b --tcp 192.0.2.10:3001 --json
tekmar-482 read-status clock 2 --peer 0x12 3 --tcp 192.0.2.10:3001 --json
tekmar-482 read-status master 2 --tcp 192.0.2.10:3001 --json
```

Other services are `outdoor`, `wwsd`, `zone-sync`, `io-discovery`,
`io-channel` and `humidity`. Active `dump` also requests the five shared
line services once per responding port; use `--no-line-status` to skip them.
`read_native_snapshot(..., include_line_status=True)` does the same.
`stream.read_status(port, query, source=source, peer=peer)` returns a
`NativeStatusBatch` with the link result, actual reporting source, and
fresh frames. Without an explicit peer it follows the first matching
source. Keys may arrive out of order or in several frames. Missing keys
and a link ACK without a report never constitute a complete answer.
Partial data are available even when the CLI exits with status 1. These
commands do not change device settings or enable a snowmelt group.

### Installation and updates

Only the **source distribution (sdist)** is released. Its build compiles
the 12 MHz LPC2146 flash image from the pinned public submodule. Installing
from the sdist retains that image without requiring Rust, Git, or a runtime
firmware download. Installing the Python package does **not** flash a gateway.
Stop any other serial client before programming.

An editable Git checkout has no flash image. To build the release locally,
initialize the submodule with `git submodule update --init firmware`, install
GNU Arm binutils and the firmware's pinned Rust toolchain, and run
`uv build --sdist`. The build stages the image only while creating the archive.
Use the flash installer for development hardware and persistent deployment.
To test a local revision, run the bundled CLI directly from the sdist
(tested with uv 0.11.20):
`uvx --from 'dist/tekmar_482-0.1.0.tar.gz[serial]' tekmar-482 update --serial "$PORT"`.
An already-current custom build is not rewritten.

```bash
# A custom dev image is running. This may replace an unversioned build.
tekmar-482 install --confirm --serial /dev/ttyUSB0
# For initial conversion from stock, enter ROM ISP with the hardware Test
# button first; only use this on the supported LPC2146 board.
tekmar-482 install --confirm --from-rom --serial /dev/ttyUSB0
# Install the bundled build unless the running custom build matches it.
tekmar-482 update --serial /dev/ttyUSB0
```

Use `auto_update=True` or `--auto-update` to check on connection. If an
identified custom build does not match the revision stamped in the bundled
binary, the updater replaces it, including newer, forked and unversioned
builds. It never converts a running stock device. **A default custom
connection to a 482 already in ROM ISP automatically installs the bundled
firmware**, even without `--auto-update`. ROM cannot identify the previous
application or the board from its MCU ID: select only a compatible 482.
`--vendor` never probes or programs ROM. Flashing verifies the ROM part ID, page checksums,
flash compare and the booted build ID. The first 64 bytes are hidden by ROM
remapping during compare. Do not interrupt power. After an interruption,
use physical ISP entry and the explicit installer; it does not retry an
ambiguous flash operation. No stock backup is required. To return to stock,
use the vendor's firmware matching your hardware lot and its update procedure.
When using a TCP serial bridge, set `--baudrate` to its actual UART rate.

## Vendor client

The following tRPC API and push controller are for stock firmware, not the
native frame stream. `Tekmar482Client` is the low-level vendor-only client.

```python
import asyncio

from tekmar_482 import Tekmar482Client


async def main() -> None:
    async with Tekmar482Client.serial("/dev/serial/by-id/usb-adapter") as client:
        await client.request_firmware_revision()
        message = await client.read_message(timeout=5)
        print(message)


asyncio.run(main())
```

For a raw TCP serial bridge:

```python
async with Tekmar482Client.tcp("192.0.2.10", 3001) as client:
    addresses = await client.get_inventory_addresses(timeout=10)
```

The default inventory request (address zero) walks the whole table and ends
with a zero address. A nonzero `start_address` is an exact lookup, not the
start of a range: it returns a one-address tuple if present, or an empty tuple
if the gateway explicitly replies `0xFFFF`. There is no second end marker.
An inventory timeout raises `TimeoutError`, not an empty or partial device
list. The next scan waits for the outstanding inventory to finish. If that wait
expires after the original walk was abandoned, the client raises
`InventoryDrainTimeoutError`; reconnect before retrying. A running controller
handles this reconnect automatically. Discovery uses one timeout for the scan, including
time waiting to send and reading device metadata. If a metadata reply is
missing, rediscovery raises `TimeoutError` and leaves the controller's last
complete topology and capabilities in place.

During a live refresh, a missed runtime reply leaves the last observation in
place for an identified device; an explicit unavailable value replaces it.
Reconnecting starts with fresh readings. With `discover(manage_reporting=True)`,
success requires acknowledgment that reporting is back on. If the scan fails
after the disable was sent, restoration can take up to another five seconds
(or the scan timeout, whichever is shorter).

For a serialx socket URL:

```python
async with Tekmar482Client.serial_url("socket://192.0.2.10:3001") as client:
    await client.request_protocol_version()
```

## Session dispatcher

Queued commands belong to the connection on which they were submitted. Closing
and reopening the client does not replay writes from the previous connection.
Closing cannot undo a write the gateway has already received.
When a newer write replaces an unsent one, the surviving writes keep their
submission order. This matters when the current and an explicit schedule slot
refer to the same setpoint.

For requests and setpoint writes, the exchange timeout starts when the command
takes the wire, not while it is queued. It covers the transport write and any
required acknowledgment. If a previous read timed out or was cancelled, an
indistinguishable retry waits for the old reply within that budget. The
session compares method and response service, device address or group ID,
and schedule slot when applicable. A `CURRENT` reply can name any slot.
Unrelated reads can proceed, and unsolicited reports are still delivered.

For an inline, non-indexed cache read whose reply never arrived, a successful
`await client.request_response(TrpcMethod.PROTOCOL_VERSION)` establishes that
the gateway has finished sending any earlier cache reply. The controller does
this before retrying a stock mode read. It does **not** establish completion of
a custom firmware bus read, an indexed exchange, an inventory walk, or a
write. Those need their own reply, terminator, or a fresh session on a known
quiescent gateway. A timed-out write may already have been applied; the
library does not retry it automatically.

`Tekmar482Client` uses a `Tekmar482Session` internally. The session owns the
single packet reader for the serial/TCP stream, matches request/response calls,
and keeps unsolicited tRPC messages available for monitors or report consumers:

```python
async with Tekmar482Client.tcp("192.0.2.10", 3001) as client:
    async with client.reports() as reports:
        await client.set_reporting_state(enabled=True)
        async for message in reports:
            print(message)
```

The lower-level `Tekmar482Session` can also be used directly with any
`PacketTransport` implementation when building integrations that need custom
lifecycle or reconnect handling.

For user-facing controls that may emit repeated setpoint updates, the client
also exposes latest-value helpers. These abandon older unsent commands with the
same command key, while never canceling a command that has already started
writing to the gateway:

```python
await client.set_latest_heat_setpoint_celsius(address=1201, celsius=21.0)
```

With `confirm=True`, a latest-value helper returns the acknowledgment, returns
`None` if a newer command replaced it before transmission, and raises
`TimeoutError` if no matching acknowledgment arrived after transmission. A
timeout does not establish whether the thermostat applied the write. If the
gateway could echo the same acknowledgment for a later write (for example,
the same temperature for `AWAY` and `CURRENT`), the later write waits for
the missing reply or times out unsent. This also applies when the first
write used `confirm=False`: it returns after transmission, but its echo
cannot confirm a later write. This includes repeated writes of the same
thermostat mode; a targeted mode read can still proceed. Neither write is retried.
With setbacks disabled or not yet known, writes to different slots of the
same setpoint on one device keep their submission order. With setbacks
enabled, a write to an independent slot can proceed.
`get_setpoint_group(group_id)` reads recent activity without transmitting
on tN4. An idle group does not identify an installed device. For an observed
snowmelt zone, request its melt and storm group IDs (keys 0x25/0x26)
with `read_snowmelt_status()` in Raw mode without changing them.

`set_setpoint_group()` waits for a matching group report. Stock firmware
06 and custom Normal mode report only when reporting is enabled; neither directly acknowledges
the write. A timeout does not prove the command failed, and a matching report
does not prove that a snowmelt slab is heating.

`DiscoveredDevice.setback_events` is a daily event count (0, 2 or 4),
or `None` if unknown. Raw tRPC packets keep the configuration code (0, 1
or 2). A schedule-master report updates metadata without an inventory walk.

`await client.get_device_attributes(address)` reads that device's observed
capability mask without requesting its identity or writing a setting. Zero
means no capability bits are set; it is not an absent-device indicator.
The controller applies capability reports and matched reads without a new
inventory walk, retaining measurements for controls that still exist.

## Push controller

See the [firmware 06 compatibility notes](docs/firmware-06.md) for the scope
of the protocol review and the distinction between gateway and device state.

`gateway.reporting_enabled` is the software flag read from the gateway.
`gateway.reporting_dip_on` is the last physical switch position reported
during a front-panel Test, or `None` if none was observed. A DIP change does
not turn push on or off mid-session. The custom firmware returns a numeric
FirmwareRevision of 0 (unnumbered); `await bridge.revision()` gives its build ID.

In Normal mode, both the custom firmware and vendor firmware 06 support
`await client.set_datetime(value)`. Supply local wall time in years 2000
through 2255; seconds and timezone offsets are not transmitted. A successful
echo confirms the gateway queued a clock broadcast, not that each thermostat
applied it. A rejection raises `ProtocolError`; timeout returns `None`.
There is no Normal-mode clock read or report. Discovery and dumps do not
manufacture a live clock from the last accepted write.

In Raw mode, `NativeMessage.request_clock(source=...)` builds a native 317
for one line. Listen for 31F frames and use the frame's port and source to
identify the responding clock; a link ACK is not a clock reading.
`NativeMessage.clock_report(value, source=...)` builds the five-byte 482
payload. Supply `dst` and optionally `dst_mode` for peers using those fields.

Humidity measurement and minimum/maximum humidity setpoints use independent
device attribute bits. When attributes are unavailable, a humidity-capable model
can indicate measurement support, but does not establish either setpoint.

`Tekmar482Controller` owns the Home Assistant-style lifecycle: connect, disable
reporting during discovery, seed an initial runtime snapshot, enable reports,
apply live reports to state, and reconnect if the stream drops. On reconnect
it repeats discovery, reapplies the configured setback setting (if any), and
publishes the new inventory and runtime snapshot only after reporting is
confirmed. If a ReportingState readback shows reporting unexpectedly disabled,
the controller reconnects and reads fresh state, since updates may have been
missed. There is no periodic full-state poll. With setpoint groups enabled,
the controller rechecks only groups last seen on, after 60 seconds without a
refresh. Firmware 06 can clear these flags silently. An off reply stops the
checks, including when it was a reply to a diagnostic read on the same client.
No reply makes the group unknown until a new observation. This reads
the gateway flag, not the slab's heating state. A mode write that times out is
checked with a targeted read, never by repeating the write. A targeted mode
read that times out gets one retry unless a newer trigger supersedes it. Overlapping writes
to the same thermostat settle before that read takes the wire.

While reconnecting, `controller.data` remains the last published snapshot.
Controller writes and repeated calls to `start()` raise `ConnectionClosedError`
until the replacement connection has confirmed its inventory and reporting.
Writes attempted during this window are not queued for the new connection.
Rediscovery also rejects new controller writes until the inventory is confirmed
again; already admitted device writes finish before the inventory walk starts.
On a `TakingAddress` report the controller immediately rejects new and unsent
device writes for the old and new addresses and clears their runtime readings.
Only a successful inventory started after the report clears that restriction.
Even when the model is unchanged, readings from before identification are not
reused for the replacement device.
If a scan fails while reporting is paused, the controller reconnects and seeds
fresh state even if its attempt to restore reporting succeeds.

Snapshots apply read replies and unsolicited reports in the order the gateway
sends them. On a same-connection refresh, a missed read keeps the last known
value for the same device; an explicit unavailable value clears the decoded
reading. A scan interrupted by reconnection fails instead of combining readings
from two connections. A reconnect starts fresh. A reply to a `CURRENT` setpoint request can
identify the active schedule slot. An unsolicited report for a specific slot
updates that slot without selecting it. Firmware 06 shares cached values
between Wake and both Occupied slots, and between Sleep and both Unoccupied
slots; Away is separate. A confirmed write with setbacks disabled updates
all cached slots for that setpoint. If the setback setting is unknown, the
controller clears other slots that may have changed.

```python
import asyncio

from tekmar_482 import Tekmar482Client, Tekmar482Controller


async def main() -> None:
    client = Tekmar482Client.tcp("192.0.2.10", 3001)
    controller = Tekmar482Controller(client)

    def updated(data):
        runtime = data.device_runtime(1201)
        if runtime is not None:
            print(runtime.current_temperature)

    controller.add_listener(updated)
    async with controller:
        await asyncio.Event().wait()


asyncio.run(main())
```

The lower-level polling methods remain available for setup flows, diagnostics,
and command-line inspection. Runtime scans read current targets across devices
before optional schedule slots. They use the advertised schedule when known
and probe all named slots when it is unknown. The diagnostic `dump` requests
all slots by default, regardless of the advertised schedule.

## Runtime models

Snapshots keep the raw protocol values for compatibility, and also expose typed
runtime views:

```python
snapshot = await client.dump_available_info()
device = snapshot.devices[0]

print(device.values["current_temperature"])
print(device.runtime.current_temperature)
print(device.runtime.heat_setpoints.current)
```

Specialized runtime classes identify known categories such as thermostats,
setpoint devices, and snowmelt controllers.

## Trace replay

Tests and offline tools can replay scripted protocol traces without hardware:

```python
from tekmar_482 import (
    Tekmar482Client,
    TraceReplayTransport,
    TraceStep,
    TrpcMethod,
    TrpcPacket,
    TrpcService,
)

transport = TraceReplayTransport(
    [
        TraceStep.request(
            TrpcMethod.PROTOCOL_VERSION,
            response=TrpcPacket.create(
                service=TrpcService.RESPONSE_REQUEST,
                method=TrpcMethod.PROTOCOL_VERSION,
                fields={"version": 3},
            ),
        )
    ]
)

async with Tekmar482Client(transport) as client:
    print(await client.request_response(TrpcMethod.PROTOCOL_VERSION))
```

Use `dump_trace()` and `load_trace()` to store these replay steps as JSON
fixtures.

## Home Assistant integration shape

The library is designed for a Home Assistant integration to create a
`Tekmar482Controller` during config-entry setup and use its callbacks to feed a
push-only `DataUpdateCoordinator`:

```python
client = Tekmar482Client.tcp("192.0.2.10", 3001)
controller = Tekmar482Controller(client)
controller.add_listener(coordinator.async_set_updated_data)
snapshot = await controller.start()
```

For setup validation and one-shot inspection, integrations can still perform a
direct discovery/runtime poll:

```python
async with Tekmar482Client.tcp("192.0.2.10", 3001) as client:
    discovery = await client.discover()
    snapshot = await client.poll_runtime(discovery)
```

## CLI

The package installs a `tekmar-482` command:

```bash
tekmar-482 gateway-info --tcp 192.0.2.10:3001
tekmar-482 discover --tcp 192.0.2.10:3001 --json
tekmar-482 dump --tcp 192.0.2.10:3001 --json
tekmar-482 dump --serial /dev/serial/by-id/usb-adapter
tekmar-482 monitor --tcp 192.0.2.10:3001
tekmar-482 --vendor request DeviceType address=1001 --tcp 192.0.2.10:3001 --json
```

Generic request/response calls and CLI requests awaiting a reply match the
address, setpoint group, and setback slot when the method echoes those fields.
Use `get_inventory_addresses()` to collect an entire inventory walk; a generic
request waits for just one matching response. A `CURRENT` setpoint request may
be answered with the active slot.

Direct serial usage requires the optional serial extra:

```bash
tekmar-482 gateway-info --serial /dev/serial/by-id/usb-adapter
```

## Development checks

```bash
uv run ruff format src tests
uv run ruff check --fix src tests
uv run mypy src tests
uv run python -m unittest
```
