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

`monitor` streams native peer frames in Normal mode without resetting
inventory or interrupting stock tRPC reporting. `discover` sends one read-only block-zero
request per observed port/subnet to identify all answering nodes, including
quiet thermostats and plant masters. `dump` then reads the cataloged and
previously observed blocks for those receive paths and prints the panel
state, native frames and receiver summaries. See the firmware's
[frame catalog](https://github.com/jameshilliard/tekmar-482-firmware/blob/master/docs/tn4-frames.md)
and [binary UART protocol](https://github.com/jameshilliard/tekmar-482-firmware/blob/master/docs/tpck-bridge.md).

`NativeMessage.destination_nodes` lists explicit native destinations for
block operations and the seven stage demand, purge and override commands.
An empty tuple means the bitmap selected no nodes; `None` means the message
doesn't establish a native destination. Group IDs, I/O payload endpoints
and RH selectors are not assumed to be native node addresses.
For a stage command, `message.stage_selection` exposes its subnet, selected
nodes, stage bit and raw output level. See [stage commands](#stage-commands)
for sending them without leaving Normal mode. Neither an observed command
nor a link ACK is a report of the actual output state.

Discovery reports receive paths, **not a count of physical devices**:
a plant controller can forward the same identity onto multiple lines.
An unknown model or a missing reply never promotes a link ACK to an
identity. The node lock bit does not create a second device on a path.
Each result distinguishes the catalog's candidate blocks from those
actually reported with supported bytes during the scan.

Early 541, 542, 161 and 162 devices can report byte-swapped J numbers.
The native decoder recognizes the documented product/J pairs; the 537
also has a known zero-product/J1161 pair. Raw identification fields
remain available in captures. A supported J of zero is a corrupt
identification, not a missing field or evidence of a replacement.
Normal inventory can also label identified reset controls, mixing
modules, timers, zone modules, power managers and switches. Recognizing
the model does not create tHA temperature, mode or fan controls.

```bash
tekmar-482 discover --tcp 192.0.2.10:3001 --duration 45 --json
```

Use `--probe-subnet PORT:SUBNET` to include a known silent subnet, or
`--passive` to skip all native requests. Discovery and dump stay in Normal
mode by default, including while probing. The gateway attempts one read of
identity block 00 per observed or specified subnet; only a peer frame
establishes a device, not a link ACK. The listening window stops new probes;
already submitted Normal probes finish before a subsequent scan reuses the
ports. Active and passive thermostat and device watches also stay in Normal
mode. Use `--raw` to
explicitly select Raw capture. A solitary peer in Raw can need a link
ACK to deliver its frames: `--ack-port PORT` is explicit and repeatable.
By default the gateway does not ACK incoming Raw frames. Use a longer
window if devices have not yet answered. Scanning does not change setpoints.

```python
from tekmar_482 import BridgeManagement, discover_native

async with bridge.normal(select_mode=False) as client:
    async with BridgeManagement(client).native_session() as native:
        result = await discover_native(native, duration=45)
for device in result.devices:
    print(device.port, device.address, device.identity)
```

The scanner owns the capture consumer for the window; `on_record` can
observe the same traffic. For an ongoing subscription, feed records (including gaps) from
your own receive loop to `NativeInventory.observe()` instead.

For a long-running read-only consumer, use
`async with BridgeManagement(client).peer_capture(ports) as peers` on an
open Normal session. `await peers.read()` returns peer frames, invalid
receptions, or an ordered loss notice. Gateway transmissions, early-start
frames and tRPC packets are not peer updates. A `GatewayTraceGap` gives
the affected ports and the total number of lost gateway records; an ordinary
`CaptureGap` indicates host backlog. Feed gaps to `NativeState.observe()`
as well as frames: an address or a group master could have changed.
The trace mask is restored on exit, including cancellation. Older firmware
without ordered loss notices is rejected, not silently switched into Raw.

For a model-aware read of one device, use its physical port, native subnet
and node from discovery, for example:

```bash
tekmar-482 read-device 2 0x12 3 --tcp 192.0.2.10:3001 --json
```

`read-device`, `read-sensors` and `read-schedule` default to Normal mode;
use `--raw` explicitly to select Raw. Pass either an open
`BridgeManagement(client)` or a Raw stream to the read helpers.
`read_native_snapshot(stream, devices)` implements the device scan and the
device-read phase of `dump`. Its block reader, `read_native_devices()`,
confirms identity before expanding a model's block list.
Unknown models can still be queried for blocks and
selectors previously observed on that path. An explicit zero availability
mask advertises no instances; a missing mask does not. Read batches group
nodes sharing a subnet, block and selector into one request. Results contain
the link outcome, matching frames (including raw bytes and field support
masks), and any nodes that didn't reply. A missing reply means unknown.
Use `--block-timeout` on `dump` or `--timeout` on `read-device` to
adjust the deadline (default 20 seconds for dump, 25 for device reads). Missing peers get
one read-only retry by default; `--block-attempts 1` disables it. Peers
that already replied are not queried again in the retry. A missing Raw TX
result stops that port; its send slot may still be occupied. An explicit
Normal gateway expiry frees the slot even if it never transmitted. `--duration` on
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

Custom Normal mode implements all 26 input method IDs in stock J1196J
and acknowledges valid incoming tN4 frames, including on a single-peer
line. It sends a startup probe on quiet ports and queries for a master
on link-up. It does not continuously scan. Raw sessions initiate neither
probe, and Raw receive ACK remains opt-in. Normal mode yields to a winning
sender and can reattempt a pending operation; neither mode replays a write
just because its ACK was missing. Normal mode drives a six-bit error flag
on a bad frame or a missing ACK for its own send. Raw mode never emits that
flag, even on ports selected for receive ACKs. For fault handling and
remaining differences from stock, see the
[bus compatibility notes](https://github.com/jameshilliard/tekmar-482-firmware/blob/master/docs/normal-mode.md#bus-compatibility).

The CLI `request` command uses Normal mode on custom firmware by default.
For example, with a previously discovered tHA address:

```bash
tekmar-482 request CurrentTemperature address=1001 --tcp 192.0.2.10:3001 --json
```

Use `client.reports()` inside `bridge.normal()` for unsolicited temperature
and demand updates; enable them with `await client.set_reporting_state(enabled=True)`.

**Debug a physical thermostat change.** On custom firmware, trace both
directions of one tN4 port while the ordinary gateway keeps running:

```bash
tekmar-482 trace-normal --port 1 --duration 60 --tcp 192.0.2.10:3001 --json
tekmar-482 request HeatSetpoint address=1001 setback=7 --trace-port 1 --tcp 192.0.2.10:3001 --json
```

Check the air-group status (`0x36F` member or `0x377` master) for the new
**active target**, then look for a tRPC HeatSetpoint or CoolSetpoint report.
This distinguishes a change not yet seen on the bus from one already received
by the gateway. A different schedule period's stored setting need not appear
in the live target. Check the final drop counts before interpreting silence.
Repeat `--port` to select several lines; omitting it traces all five. Native
records carry gateway uptime; every packet row has a host monotonic observation
time. Normal replies can overtake buffered diagnostics on a busy UART.

For an existing Normal client, subscribe alongside requests and reports:

```python
async with BridgeManagement(client).trace([1]) as trace:
    async for event in trace:
        print(event)
```

The bounded observer doesn't steal responses. It exposes
native RX, the origin and outcome of gateway TX attempts, invalid captures
and both directions of tRPC. Exiting restores the previous trace mask.
Tracing never enables reporting, changes mode, or grants raw transmit access.
Use `trace-normal --status` to inspect the gateway's mask and lifetime
drop counts, or `trace-normal --stop` to clear a trace after a disconnect.

For instrumentation, wrap custom connection creation in
`tekmar_482.wire_trace.observe_bridge_packets(callback)`. The synchronous
callback receives `(packet, outbound)` for each received application packet
and submitted transmission, including operations from other tasks on that
connection. This observer does not enable tracing or capture ROM ISP bytes.
`tekmar_482.raw.decode_raw_packet(packet)` decodes a gateway-to-host Raw
record without taking ownership of the stream.

If the gateway advertises `BridgeFeature.NORMAL_IDENTITY`, use
`await BridgeManagement(client).request_identity(native_path)` while
tracing that port in Normal mode. It queues only an addressed read of
native identification block 00. Match the actual port, subnet and node of
the resulting report. A successful request or link ACK does not confirm
a model, and a node without a response remains unidentified.

Raw and Normal trace carry the same native tN4 body inside TPCK. Their
envelopes differ: Raw type `0x01` carries bridge operations; trace type
`0xD0` identifies Normal RX or a gateway TX attempt with its origin and
outcome. Trace is not an alternate encoding for sending Raw requests.

**Gateway traffic counters.** With `BridgeFeature.TRAFFIC_COUNTS`, use
`BridgeManagement(client).traffic` in Normal mode or `stream.traffic` in
Raw mode. Await `traffic.port(port)` or `traffic.device(NativePath(...))`,
or enumerate `async for device in traffic.devices()`. These read the 482's
saturating 64-bit counters since boot, including time without a host. RX
excludes local echoes; invalid RX and unaddressed TX remain port totals.
TX ACK measures a link outcome, not application acceptance.

**Gateway error diagnostics.** The custom firmware counts errors even when no
host is listening. `errors` subscribes to changed counters without changing
the current Normal/Raw mode, trace, thermostat reporting or link ACKs.

```console
tekmar-482 errors --source 1 --source gateway --duration 60 --tcp 192.0.2.10:3001 --json
tekmar-482 errors --source 1 --read crc --tcp 192.0.2.10:3001
```

Each report identifies the port or gateway, reason, last-seen gateway uptime
in milliseconds and lifetime count. Repeats can share one notification.
Counts saturate at 64 bits and reset at boot. This is not a frame capture or
a device's network alarm word. Use `errors --status` to see enabled sources
or `errors --stop` after a host disconnect. A clean watch exit restores the
previous selection. [Wire format and reason codes](https://github.com/jameshilliard/tekmar-482-firmware/blob/master/docs/tpck-bridge.md#error-reporting).

```python
from tekmar_482 import ErrorReason, ErrorReporting, ErrorSource

# On an open tRPC client; use stream.error_reporting inside bridge.stream().
errors = ErrorReporting(client)
crc = await errors.counter(ErrorSource.PORT_1, ErrorReason.CRC)
async with errors.watch([ErrorSource.PORT_1, ErrorSource.GATEWAY]) as reports:
    async for counter in reports:
        print(counter.source.name, counter.reason.name, counter.count)
```

Custom bridge wire version 2 uses saturating 64-bit counters for capture
and trace drops, edge counts and panel events. They reset at gateway boot,
not on a mode change. Older custom wire versions are rejected; only stock
commands have a backward-compatibility guarantee. Per-frame uptime and
hardware timer readings are still 32-bit timestamps.

Custom Normal mode obtains missing fields from the device on its own port.
A tHA ModeSetting Request returns the thermostat's recent confirmed mode,
even when another command is using its port. With a missing or invalidated
sample, overlapping readers share a native heating-setup read. A change in
heat/cool enablement, demand, setback or active target can trigger that read;
none of these is itself a configured mode. Reporting publishes confirmed modes
on enable and on change. A controller using `bridge.normal()` consumes these
reports directly; the stock client retains targeted reads after related
reports because stock firmware doesn't reliably push configured mode. The gateway
does not poll the inventory on a quiet bus or pause all ports after
`WaterCycleStart`. In Raw mode the frame is available as native plant
telemetry. A passive Python watch (`source=None`) sends nothing.

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

When editing a device selected from a monitor snapshot, pass its
`NativeIdentity` as `expected_identity` to the edit method. For example,
look up `state.identity(route.preferred)` after confirming that the route
has a preferred path. The edit reads block zero from that same physical
port. Missing or conflicting identity fields raise
`NativeIdentityMismatchError` before any update. This verifies observed
identity fields; it does not distinguish two units with identical product
and revision numbers.

The CLI previews the complete proposed frame by default. Block, thermostat,
scene-response and schedule edits use Normal mode without changing trace,
reporting or link-ACK settings. Add `--confirm` to apply an edit. The
gateway first rereads the full baseline from the same device. If a sibling
or the support mask changed, the result is `baseline_changed`: **nothing
was written**. If the gateway sees the target address taken or vacated
before transmission, it returns `target_moved`, also without writing.
Only a separate readback can produce `confirmed`. A link
ACK alone cannot. Use `--raw` explicitly for a Raw edit without this
gateway-side comparison. Neither path automatically repeats the update.

```bash
tekmar-482 write-fields 0x05
tekmar-482 write-block 2 0x12 3 0x05 wake=40 --tcp 192.0.2.10:3001 --json
```

For a thermostat, use `set-thermostat-target` or
`BridgeManagement(client).write_thermostat_target(port, peer, change)`
to edit a stored slot in physical units. Both default to preview. For example:

```bash
tekmar-482 set-thermostat-target 2 0x12 3 heat wake 21.5 --tcp 192.0.2.10:3001
```

`ThermostatTargetChange(ThermostatTarget.HEAT, TargetSlot.WAKE, 21.5)`
uses Celsius. Fan and humidity use percentages; fan 0 means Auto, humidity
0 means Off. Fan targets on 544/545/546 use ten-percent steps. The client
reads the destination's identity and complete target block. Floor, fan and
humidity edits also require the corresponding setup or sensor assignment.
An edit does not enable an output, change the operating mode or select a
different setback. A confirmed edit reads the target back; a missing support
bit, compact pre-write block or unrepresentable value cannot be written.

To read a single block without interrupting Normal operation (addresses
below are examples):

```bash
tekmar-482 read-block 2 0x12 3 0x71 --tcp 192.0.2.10:3001 --json
tekmar-482 read-block 2 0x12 3 0x38 --selector 3 --tcp 192.0.2.10:3001 --json
tekmar-482 read-block 2 0x12 3 0x00 --also-node 5 7 --tcp 192.0.2.10:3001 --json
```

`await BridgeManagement(client).read_block(port, query)` returns the peer's
full timestamped `NativeFrame` on an open Normal session. It needs neither
tracing nor tHA reporting. Distinct ports can read concurrently; a second
read on an occupied port is rejected. Silence is a timeout, not an empty
block. Use `read_block_group(port, NativeBlockQuery(NativeTargets(subnet, mask), block))`
to read multiple peers with **one** on-wire request. Node 1 is bit 0;
node 31 is bit 30. The gateway waits ten seconds to send, then ten seconds
between distinct peer replies; duplicates never extend the wait. For a slow
line, allow over twenty seconds for the host's initial wait to cover both
windows. After each part `--timeout` starts again. A gateway expiry returns
the actual transmit outcome, any replies, and the remaining `missing_nodes`.
A host timeout or gateway control-queue drop is an error, not evidence of
device absence. Reads and edits
require the advertised `BLOCK_TRANSFERS` feature. They do not silently fall
back to Raw or to cached values.

Use `read-block --raw` to explicitly select Raw mode. In that mode,
`--ack` supplies link ACKs to valid incoming frames on that port and is
off by default. Only use that flag where the gateway should supply ACKs.

`send()` transmits once and waits for that port's terminal link result:
`ACK`, `REJECTED` before transmission, or `FAILED` during transmission.
A link ACK does not confirm an application change. A block read can still
succeed on a real report if the link ACK was missed; other send failures
raise `NativeTransmitError` with the underlying `TxResult`.
For block edits and snowmelt group changes, a bad frame ending, echo
mismatch, missed timing or unknown transmit failure does not prove the
device ignored the write. The client reads the device again, keeping the
original link result; it never repeats the write. A pre-send rejection or
loss of arbitration instead ends the edit without readback.

Different ports can send concurrently, without a capture consumer. A second
send on the same port raises `ProtocolError` until its previous result
arrives, even if the caller cancelled or timed out. There is no implicit retry.
`stream.read()` delivers native frames, invalid captures, and non-terminal
port errors in receive order. A slow consumer gets a `CaptureGap` with the
number of oldest records dropped by the host; set `capture_backlog` on
`bridge.stream()` to change the default of 512 records. Gateway drop counts
are separate and available in `stream.diagnostics` after a normal exit.

### Thermostat mode and demand

For a live view of **configured mode and actual demand**, select the paths
returned by native discovery. The first command only reads; the second previews
a mode change. One capture reader can watch multiple thermostats.

```bash
tekmar-482 watch-climate --device 2 0x12 3 --device 2 0x12 4 --duration 60 --tcp 192.0.2.10:3001
tekmar-482 set-climate-mode 2 0x12 3 heat --tcp 192.0.2.10:3001
```

`watch_climate(stream, paths, on_change=callback)` runs until cancellation or
an optional deadline. Pass a `BridgeManagement.native_session()` context's
value to watch without disrupting tHA clients. Active watches request blocks
from the gateway's own source address on the selected trace ports. They
require firmware that orders read completions after earlier trace frames
and loss notices, so an address change or gap retires an obsolete read.
Passive watches never send. A Raw stream remains available for explicit
source addresses or link ACKs. The watcher reads identity and cataloged mode setup once.
There is no periodic poll. An addressed setup update or a change in operating
channel, enable, demand, setback or active target queues a targeted read,
with at most one pending follow-up
per source and block. If initial identification or a mode read went
unanswered, later air traffic can trigger a retry once per distinct
operating state, not once per repeated sample. A source's confirmed mode
comes from block `0003`,
never from the update request or an air-group demand flag. A setting change
with no distinguishable bus traffic cannot be detected by this mechanism.
A quiet device can have a confirmed mode but no observed demand. A finite
active watch lists unanswered blocks and exits with status 1 if a selected
path never reported its configured mode.

AirGroupMasterConfig announcements are exposed separately from stored
thermostat configuration. `NativeFrame.air_group_configuration` and
`state.air_group_configuration(path)` retain the reported current and
previous roles. A different group or master flag prompts an addressed
HeatingSetup read in an active watch. It does not overwrite the configured
mode or group before readback. Identical roles do not start polling, and
an in-flight read from before the change cannot confirm the new setup.

The callback receives a `ClimateChange` with a `NativePath` and immutable
current state, or `None` on invalidation. It runs on the event loop and must
not block. Use `source=None` (CLI: `--passive`) to send no requests; omitting
paths in that case watches all observed thermostat identities. Link ACKs
are off unless explicitly enabled with `--ack-port`. Code which
already owns the stream's capture reader can feed its records to
`NativeState.observe()` directly and inspect `thermostats`, `water_sources`
and `block(path, block_id, selector)`. Supply a new state on reconnect.

`stream.write_thermostat_mode(port, address, ThermostatMode.HEAT, source=...)`
shares the port with the watcher's queued reads. It identifies the device,
checks the requested mode against its model, revision and relevant setup,
reads the current heating setup, and preserves the cycle rate and group ID.
The CLI needs `--confirm` to transmit. A change is confirmed only by a
matching post-write device read, not a link ACK. The native decoder names
Humidity and Backup when devices report them; neither is offered by the
483's remote selector.

### Thermostat sensors

```bash
tekmar-482 read-sensors 2 0x12 3 --tcp 192.0.2.10:3001 --json
```

This read-only command identifies the thermostat before querying its room
statistics and individual sensor inputs. Output separates the setup byte,
reported byte, revision-corrected status role and display role. Unknown or
conflicting roles and unusable words are not reported as temperatures.
`unanswered_blocks` lists missing replies, not absent hardware.
`read-device` and active `dump` include the same interpretation without
extra reads.

Use `await read_thermostat_sensors(stream, port, address)` for an addressed
read. Its `device.sensors` contains the room statistics, per-slot readings
and floor mean; `batches` retains the underlying responses. For passive
updates, pass every record from `stream.read()` to a
`ThermostatSensorMonitor`. It starts interpreting inputs after seeing the
source's identity. A capture gap clears its observations; an address move
invalidates the affected paths. Start a new monitor on reconnect.

An active `watch-native` or `NativeMonitor` also reads the identified
thermostat's cataloged room statistics and input blocks. A reported role
change discards the affected input samples and room statistics and queues
fresh reads. Values are the last device samples, not continuous polling;
the separate air-group and slab reports remain the live operating readings.
Use `--no-thermostat-sensors` to skip the extra reads without hiding passive
observations or setup needed by other features.

### Boiler, mixing and tank status

```bash
tekmar-482 read-water 0 3 --duration 60 --tcp 192.0.2.10:3001 --json
```

`read-water` stays in Normal mode by default. It asks each
selected port for its master's loop map, identifies an addressable
responder with block zero, and reads the loop setup, equipment banks and
runtime registers cataloged for that plant controller within the same
observation window, including time spent waiting for an occupied port.
A master query does not force a temperature or output report.
The command returns 1 if it heard no operating data; it still prints the
setup and link results. Use `--no-plant-settings` for topology-only queries
or `--passive` to send nothing on tN4. Use `--raw` only to switch the gateway
out of Normal mode. A custom source address or link ACK requires `--raw`.

`read_water(stream, ports, duration=seconds)` accepts a Normal native session
or a Raw stream; a Normal peer capture also works with `source=None`.
It reports gateway trace loss separately from host capture loss. A gateway
gap invalidates observations only on the affected ports; host loss affects
all selected ports. The scan keeps listening after a loss but does not use
the interrupted master claim for further equipment reads. `unfinished_ports`
lists probes still pending when the window ended, not negative replies.
For push updates on one port, feed its records to a `WaterMonitor`; start a
new monitor on reconnect. The native `dump` also includes water sources.

Each source is a physical port, full subnet and node. Prefer an explicit
loop setup over the master's advertised boiler/mix name. A report can be
repeated on another port without that port owning the loop; an unresolved
assignment stays unlabelled. Master and plant-loop status are independent.
DHW status exposes temperatures; the separate tank status retains raw
readings by tank ID because their scale is not established.

For ports that are known to share a tN4 network, pass `--shared-network` to
`read-water` to show one logical system for each shared MasterPresent slot
map. The Python equivalents are `state.water_systems(ports=(0, 2))` and
`result.water_systems(ports=(0, 2))` on a `WaterRead`. Each loop's `source`
is the one path that claims the full subnet with that map and has a known
assignment. Copied status without an announcement is still visible in
`water_sources` but cannot supply that route. Competing claims, duplicate
slots, and setup changes without fresh reports leave the source unresolved.
This is a map of loops, not proof that several routes are one physical
controller. The CLI scopes each port independently unless asked to group.

For example, `state.water_source(path).equipment.outputs` contains the
latest plant reports by output subtype and reported device ID on that path.
A staged boiler reports separate available and operating stage masks; a heat
pump reports counts, and a modulating boiler reports actual modulation.
The advertised count is not a physical inventory. Mixing capability reports
are separate from measurements, and a mixing command is not a measurement.
See the [plant output layouts](https://github.com/jameshilliard/tekmar-482-firmware/blob/master/docs/tn4-frames.md#plant-output-subtypes)
for definitions and the fields that remain raw.

`state.plant_registers(path)` and `read_water(...).plant_configurations`
separate the equipment banks, runtime hours and missing block/selector
pairs from operating reports. For example, on a 402, pump-hour selectors
0 and 129 refer to the primary and DHW pumps. A zero availability mask
advertises no instances; a missing mask or a partial hours word is not
zero. An equipment change invalidates readings for the affected instances.
Plant scans also record the supported reset curve, DHW setup and setpoint
setup fields (`02`, `24`, `25`) where the device supplies them. On a 402,
`boiler_energy_hours` is the time-based estimate described in the
[402 monitor menu](https://www.watts.com/dfsmedia/0533dbba17714b1ab581ab07a4cbb521/81499-source/638820695310000000/402-d-07.pdf#page=19),
not a measured energy reading. `energy_savings_hours` is time spent in the
savings state. Missing or partial words are not substituted with zero.

`state.peer_link_counters(path)` and `snapshot.peer_links` expose the peer's
own bus error and event octets (`48`, `49`) and received-message word (`62`)
when observed. The reported `port_id` is local to the peer; it isn't the
gateway port in `path`. These aren't the gateway's 64-bit counters. An absent
field is unknown, not zero. For automatic scans, the 483 provides a port
selector only for reset controls on nonzero water buses. Other peers need
an observed or explicit selector. No periodic counter reads are added.

### Stage commands

Preview a staged heat demand request, start or end purge, or start or end
an on/off override. Choose one physical port, the native target subnet,
nodes 1 through 24, a stage bit and a raw output level (0 through 127,
not a percentage):

```bash
tekmar-482 stage-command start-purge 3 0xab --nodes 1 16 24 --stage 1 --output-level 117 --json
```

This preview does not connect. Add `--confirm` and a transport for an
intentional send. The gateway stays in Normal mode and reports the link
outcome of one attempt. A busy port rejects the operation, and a host
timeout leaves delivery unknown. Neither causes a retry. These commands
can change equipment outputs; use them only with a verified target and
observe actual plant telemetry separately. A link ACK does not confirm
that any selected device acted.

```python
from tekmar_482 import StageCommand, StageSelection

selection = StageSelection.from_nodes(0xAB, (1, 16, 24), stage=1, output_level=117)
tx = await manager.send_stage_command(3, StageCommand.START_PURGE, selection)
```

`None` means the gateway expired the command before transmission. For
an explicitly Raw session, build the same payload with
`NativeMessage.stage_command(command, selection, source=source, priority=0)`
and pass it to `stream.send`.

### Service frames

The 483 references define base layouts for CalibrationMode,
CycleLengthRequest, MixingModuleUpdate, NetworkTestMode and NetworkTestTime.
Build an exact frame with the corresponding `NativeMessage` constructor,
supplying the native source and priority. For example, this **encodes only**:

```python
from tekmar_482 import NativeAddress, NativeMessage

frame = NativeMessage.cycle_length_request(
    3, 0x20, source=NativeAddress(0x43, 7), priority=2,
)
print(frame.to_bytes().hex())
```

For an intentional transmit, call `await manager.send_service_frame(port, frame)`
on a Normal gateway advertising `NORMAL_SERVICE_FRAME`. It attempts the
frame once on the selected physical port without changing modes. An ACK
reports link delivery, not a device's acceptance. A result of `None`
means the request expired before starting.

The example uses synthetic wire values. The air-group ID is not a device
address, and neither an ACK nor a later AirGroupMasterStatus confirms that
the group applied the requested length. These layouts do not establish
the required source, valid mode codes, timer units or supporting models
for an installation. Normal mode accepts only the base layouts. An
extended mixing frame stays opaque past the base fields and requires an
explicit Raw send to preserve its full bytes.

### Weekly schedules

Read an addressed device's assignment and the event times it actually
owns. A zone schedule belongs to the device; a system schedule belongs
to its master, not its members. The 033 timer can own up to four
separate system schedules. For example:

```bash
tekmar-482 read-schedule 2 0x12 3 --tcp 192.0.2.10:3001 --json
tekmar-482 read-schedule 2 0x12 3 --channel cool --tcp 192.0.2.10:3001
tekmar-482 read-schedule 1 0x14 5 --system 2 --tcp 192.0.2.10:3001
tekmar-482 write-schedule 2 0x12 3 occupied monday 07:30 --tcp 192.0.2.10:3001
```

`write-schedule` previews by default. Use the **displayed** event name;
in a two-event schedule Occupied is the Wake wire row, Unoccupied is
the Sleep row. Use a local ten-minute time to add or change an event,
or `disabled` to remove it. A change covers every day in the device's
24-hour, 5-2, 5-1-1 or 7-day grouping. With `--confirm` the client
reads the owner, updates only the selected fields and verifies by a
fresh addressed read. It does not activate a schedule or change a
thermostat's mode as a side effect.

```python
from calendar import Day
from datetime import time

from tekmar_482 import ScheduleChange, ScheduleEvent, ScheduleTarget, read_weekly_schedule

target = ScheduleTarget(peer)
week = await read_weekly_schedule(stream, port, target, source=source)
for row in week.rows:
    print(row.display_event.name, row.days)  # Monday first; None means unknown.
change = ScheduleChange(ScheduleEvent.OCCUPIED, Day.MONDAY, time(7, 30))
preview = await stream.write_schedule_time(port, target, change, source=source, preview=True)
```

Calendar rows retain raw ten-minute slots (144 = disabled), including unknown
codes. `row.time_on(Day.MONDAY)` returns a time only for a known enabled slot.
Missing times and conflicting timer headers mark a table incomplete. `dump` and
`read-device` also assemble calendars from the block replies they already
received. Given a native snapshot, `snapshot.schedule_directory.system(2,
ports=(0, 1))` returns observed masters and members within those lines;
`owner` is absent if there is no master or more than one distinct master.
The library never forwards a member edit to a guessed owner.

For low-level work `WeeklyEvent.from_frame(report, profile=week.config.profile)`
parses a complete row and preserves its indexed header. Early 552/557
revisions use a different weekday order, so a single-schedule row requires
identification before it can be interpreted as calendar days. Plain
`frame.decoded` shows the nominal wire slots, not a revision-corrected calendar.
Named weekday edits through `write-block` also identify their target first.
Use setup block `0x0f` to assign a capable device its own zone schedule
(`heating_schedule_id=1`) or a shared role. These changes are separate from
editing event times; the client refuses setup rewrites that could silently
demote a master. See the [native schedule reference](https://github.com/jameshilliard/tekmar-482-firmware/blob/master/docs/tn4-frames.md#weekly-schedules).

Use `configure-schedule` to change an assignment or format, optionally with
one or more event times in the resulting format. For example, preview
starting a local two-event schedule on an identified device:

```bash
tekmar-482 configure-schedule 2 0x12 3 --binding zone --cycle seven_day \
  --events 2 --time occupied monday 07:30 --time unoccupied monday 22:00 \
  --tcp 192.0.2.10:3001 --json
```

Apply that preview only after checking the scope and existing times. Add
`--expect-binding none --confirm` if the device is currently off. Other
assignments include `member_2` and `master_2`. Changing an assignment does
not select an owner automatically or alter another device's assignment.
`--channel` selects which assignment byte to edit on devices with separate
heat and cool assignments; it does not select a different stored calendar.

The operation requires fresh, supported rows before activating a schedule.
It preserves times you did not specify and refuses to merge conflicting
days without an explicit replacement. The format edit is last for a local
table. For an already-owned 033 slot, use `--system 1` through `--system 4`
to edit the format on all four indexed rows, preserving the selector and
master bit. Remote enabling or disabling of timer slots is not established.

In Python use `await manager.configure_schedule(port, target,
ScheduleSetupChange(binding=ScheduleBinding.ZONE, times=(change,)))`.
Preview is the default. Pass `preview=False` and `expected_binding` to
apply; Raw additionally takes `source=`. An interactive client can pass
`expected_config=week.config` instead to reject a changed assignment, format
or indexed timer setup before planning an edit. The client rechecks its baseline,
and Normal firmware also guards each write. A fresh matching readback, not
a link ACK, verifies a step. tN4 has no atomic multi-block update. A failure
stops the plan and returns the completed or attempted steps and remaining
unsent blocks. The client does not roll back or replay an edit.

### Setpoint controllers

Select the application and the exact stored field to edit a 161 or 162:

```bash
tekmar-482 set-setpoint-target 1 0x31 7 dhw wake_occupied 55 --tcp 192.0.2.10:3001
```

The five fields are `wake_occupied`, `unoccupied_sleep`, `away`, `wake`, and
`sleep`. The last two require a separate table with the selected word
supported; they never fall back to the combined fields. The 161 only lets
you adjust the occupied target. See the
[161](https://www.watts.com/dfsmedia/0533dbba17714b1ab581ab07a4cbb521/42727-source/161-dl-06.pdf)
and [162](https://www.watts.com/dfsmedia/0533dbba17714b1ab581ab07a4cbb521/42733-source/638820695640000000/162-d-06.pdf)
manuals for the applications and target ranges.

The command previews by default. With `--confirm`, the client checks the
peer's identity, current application, and supported target fields. It
checks the reported maximum if present, changes one word, and reads it
back. Stored targets use degH even for DHW. For a hot tub target, `off`
uses the word defined by the 483 SW 11 setpoint model; Off encodings for
other applications are not established. This edit does not send a group
enable, change the application, or activate a schedule.

```python
from tekmar_482 import SetpointApplication, SetpointSlot, SetpointTargetChange

change = SetpointTargetChange(SetpointApplication.DHW, SetpointSlot.WAKE_OCCUPIED, 55)
preview = await manager.write_setpoint_target(port, address, change)
```

Pass `preview=False` to send and verify. The Raw stream takes the same
change plus an explicit `source=`. Read `control.target(slot)` on a
`NativeSetpointControl` for the stored word and its Celsius value, if
known. `control.writable_target(slot)` also checks model support and usable
values. Its `bounds_celsius` includes a reported maximum if available.
`control.readings` remains separate live status. When compact write readback
omits known sibling fields, the live watch requests the full report once.

### Snowmelt

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
`io-channel`, `humidity` and `gateway-access` (read-only access status).
All ten `read-status` services default to the gateway's existing Normal
session. They do not enable trace or change reporting. In Python use
`BridgeManagement(client).read_status(port, query, peer=peer)` or
`read_snowmelt_status(port, peer, keys=...)` on that manager. Reads on
different ports can overlap. A concurrent block or status read on the same
port is rejected. Use the subcommand's `--raw` only for an explicitly Raw
session, custom request source or link-ACK controls.

In Normal mode the gateway returns every matching part followed by its link
outcome and completion status. A gateway deadline returns the partial batch;
host timeout, lost serial connection or gateway output overflow are errors,
not evidence that the peer is absent.
The gateway starts its reply window after transmitting and renews it when a
multipart reply covers a new requested key or channel. The Normal
`read_status` timeout bounds each wait for a part or the terminal, not the
total length of a reply train. Allow at least 20 seconds plus transport
latency for the initial wait; the default is 25 seconds. In Raw mode the
host sets a single deadline for the exchange.
Active `dump` also requests the six shared
line services once per responding port; use `--no-line-status` to skip them.
`read_native_snapshot(..., include_line_status=True)` does the same.
`stream.read_status(port, query, source=source, peer=peer)` returns a
`NativeStatusBatch` with the link result, actual reporting source, and
fresh frames. Without an explicit peer it follows the first matching
source. Keys may arrive out of order or in several frames. Missing keys
and a link ACK without a report never constitute a complete answer.
Partial data are available even when the CLI exits with status 1. These
commands do not change device settings or enable a snowmelt group.

After a master query, use the responding frame's source for addressed
register reads, not the boiler subnet in its payload. In Raw mode, frames
received before the request's completed TxResult remain available to
passive listeners but cannot satisfy an active read or confirm a write.

**Read native I/O channels.** I/O discovery, explicit channel replies and
unsolicited changes are separate from block reads. For example, with
reporter `0x12:3` on port 2 and I/O payload address `0x43:0xa5`:

```bash
tekmar-482 read-status io-discovery 2 0x43 0xa5 --peer 0x12 3 --tcp 192.0.2.10:3001 --json
tekmar-482 read-status io-channel 2 0x43 0xa5 7 --peer 0x12 3 --tcp 192.0.2.10:3001 --json
tekmar-482 read-device 2 0x12 3 --io-endpoint 0x43 0xa5 --tcp 192.0.2.10:3001 --json
```

Keep both I/O address bytes exactly as reported; do not convert the node
to a block bitmap or derive it from a thermostat display address. The
same channel number can exist on multiple endpoints, peers and physical
ports. The CLI's `dump` and `read_native_snapshot` query endpoints seen
during discovery or explicitly supplied in `NativeDevice.io_endpoints`.
If a fresh identification reply establishes an unlocked 553 with no known
endpoint, the scan tries its wire address once. A node-zero I/O reporter
needs no block request. Other models and locked 553 nodes still require
an observed or explicit I/O endpoint.

Start a live watch of a known endpoint without waiting for an I/O change:

```bash
tekmar-482 watch-native --io-endpoint 2 0x12 3 0x43 0xa5 --tcp 192.0.2.10:3001 --json
```

The first three fields are the reporting path, not the payload endpoint.
Repeat the option for more endpoints or add `--device` for more paths. In
Python use `watch_native(stream, [path], io_endpoints={path: (endpoint,)})`.
Each supplied pair seeds one read. A reply must match both the source and
the two payload bytes. If there is no matching directory, the summary
lists the endpoint as incomplete; it does not create empty channels or
poll. A capture gap or move invalidates the previous evidence. This option
is only for active watches with I/O discovery enabled.

`frame.io_report` provides the typed value, raw type ID and all six
discovery label bytes. `label_text` removes NUL bytes and exposes a
nonempty printable ASCII label when present; it is not the model's
terminal name. `IoDirectory.from_batch(answer)` describes one discovery
exchange. `state.io_directory(path, endpoint)` combines live observations;
a complete, source-matched discovery can retire missing channels, but a
partial scan or silence cannot. A newer change seen during a scan wins.
Stream frames have a connection-local `capture_index` in addition to the
gateway's millisecond timestamp. The live watcher uses receive order, so
two reports in the same timestamp tick cannot reverse a channel update.
A gateway trace gap invalidates routes and pending readbacks only on the
ports in its mask. A host capture gap has no port information and
invalidates every observed route. Address moves and identity replacements
invalidate the affected paths. A board-terminal name is not a discovered
label or proof of an output. These services do not include a generic
I/O channel write.

An active `watch-native` discovers the first time it sees an I/O endpoint
and can rediscover when a later report supplies a new channel, type or count.
It can also start from fresh 553 identification without an I/O announcement.
Without explicit device paths it prefers the port claiming the full subnet;
a sole observed path with no conflicting master announcement also works.
An ambiguous route needs more evidence or an explicit path. Silence never
creates channels or starts periodic discovery.
On each port, identity, mode and I/O discovery run ahead of bulk
configuration reads without interrupting a request already on the wire.
For a partial directory, it can send one explicit read per missing ID from
a previously complete directory on that route, or from a known model's
local terminal layout when the advertised count and observed IDs agree.
Each missing ID must reply on the same port, reporter and payload endpoint.
A completed send without a reply leaves that channel unknown but does not
hide later channels. An incomplete frame or a missing transmit result stops
the follow-up reads. The host does not infer IDs from a count alone,
reissue the directory, or drop old channels based on a supplemented result.
`--no-io-discovery`
keeps I/O observation passive while other requested reads continue.

`read_native_snapshot` applies the same recovery after a matching identity
read. For a single endpoint, call `read_io_directory(reader, port, endpoint,
peer=address, identity=identity)` with a confirmed identity, or supply a
previously complete `before=` directory. The returned `IoDirectoryRead`
keeps the original discovery and individual follow-ups separate. A direct
reply can complete channel coverage, but does not supply a discovery label
or per-channel count. Unknown labels stay unknown; a live watch can retain
a previously reported label for the same channel type.

**Watch several devices on one connection.** With no `--device`, `watch-native`
automatically identifies peers that report on its captured ports. Repeat
`--port` to limit the watch to specific physical lines. It reads identity
at the observed address and does not merge copies on different ports even
when their products and revisions match. When Normal firmware supports
subnet discovery, a fresh, unambiguous MasterPresent header also prompts
one bitmap request for identity on that port and subnet. Quiet peers can
answer without first sending telemetry. A link outcome is not an inventory;
silence or repeated headers do not restart the scan. A capture gap retires
the affected scan; new master evidence can start another. Fixed-path, Raw
and passive watches do not send these automatic subnet probes.
A node-zero report can supply telemetry but has no addressed block slot.
Use `--device` instead to explicitly watch a path, even before it speaks.
Ordinary monitoring never sends a setting update.

For a snowmelt control the watcher
requests the missing status keys for that application, while continuing to
receive other devices' reports. After an incomplete request, a new valid
report from either role can prompt a retry to the configured primary role.
With the same keys still missing it waits five more reports, matching the
483's retry cadence. A complete profile and a silent controller generate
no retries. There is no timer poll. The final summary shows unanswered
blocks, link outcomes and which keys the latest status requests received.

```bash
tekmar-482 watch-native --port 0 --port 2 --duration 60 --tcp 192.0.2.10:3001 --json
tekmar-482 watch-native --device 0 0x14 6 --device 2 0x12 3 --duration 60 --tcp 192.0.2.10:3001 --json
```

Schedule-capable devices read their local assignment first. A 033 timer
reads the Wake header for each of its four slots instead. The watcher
requests calendar rows only from confirmed owners and only for active
events. A native `2F7` role change invalidates the affected assignment and
rows; ordinary phase changes and countdowns update the live signal without
sending a request. An addressed schedule write invalidates its destination
but is never treated as a readback. After a missing setup reply, a new
role announcement can prompt one retry. Use `--no-schedules` to omit calendar
reads and output.

`state.climate(path).adjustments` separates stored heat, cool, floor, fan,
humidify and dehumidify tables from live air, fan/duct, slab and RH reports.
`NativeClimate.active_air_target` is the target supplied by the latest
eligible master/member status: heat or cool, setback selector, raw byte and
Celsius. It is `None` when that frame supplies no eligible target, and
`celsius` is `None` for an explicit FF. It does not overwrite a stored
schedule slot. `air` preserves the full report, including inactive channels.

Read a stored slot with `adjustments.table(ThermostatTarget.HEAT)`, then
`table.value(TargetSlot.WAKE)` (Celsius or percent) or `table.raw(...)`.
A missing table is not a table of zeros. The floor table can report a maximum;
the three separate room limit tables expose supported raw bytes by slot.
An addressed update or a changed active target triggers a targeted read;
sensor fluctuations and fan cycling do not. Select `--no-thermostat-targets`
to skip these extra reads. Passive mode still decodes any observed table.
An RH reading does not establish that humidification or dehumidification is
installed, and fan output states are not ventilation percentages.

For a setpoint controller, `state.setpoint_control(path)` separates the live
`22F` status from stored targets and group assignment. Status retains relay,
enable, direction, demand, reason codes and raw words even for an unidentified
source. On an identified 161/162, `control.readings` presents the live target
and first sensor in Fahrenheit only when the application was known at receipt.
The 483 uses E for live DHW and H for other applications. The second sensor
stays raw. A later setup change never reinterprets an old sample.

The watcher reads application setup, operating parameters, override time and
the selected primary target table. All **stored** target words use H, including
DHW. The optional wake/sleep table is not assumed; a six-byte floor table
has no reported maximum. `--no-setpoint-settings` skips these reads.
`snapshot.setpoint_controls` exposes configuration from a block scan, not
a synthetic live status: there is no known request for a fresh `22F`.

The library equivalent is `await watch_native(stream, ports=(0, 2), on_change=callback)`.
Pass explicit `paths` instead of `ports` to watch a fixed set of addresses.
An automatic watch follows TakingAddress on the same wire without moving
old measurements to the new address. A trace gap retires only affected
routes; fresh reports can discover replacements.
If a Normal read is refused as busy, only that port waits for the previous
operation's terminal report before reconsidering the queued read. A read
that was already admitted is not replayed after a host response deadline.
Other ports continue receiving and reading throughout.
Set `stale_after=seconds` (CLI `--stale-after`) to discard a path after
that much silence on the connection. Unchanged frames renew the interval;
expiry neither sends a probe nor claims the device has been removed.
Without this option, silence alone does not invalidate the latest sample.

For a long-running application that owns the connection, run
`NativeMonitor(endpoint, ports=(0, 2)).run()` as a task. The monitor opens
an already-running Normal gateway, discovers reporting peers, and rebuilds
state on reconnect without flashing or replaying an operation. Consumers
can wait on its coalesced revisions:

```python
import asyncio

from tekmar_482 import GatewayEndpoint, NativeMonitor

monitor = NativeMonitor(GatewayEndpoint.tcp("192.0.2.10", 3001))
async with asyncio.TaskGroup() as tasks:
    observer = tasks.create_task(monitor.run())
    revision = -1
    try:
        while True:
            revision = await monitor.wait_for_change(revision)
            print(monitor.state.thermostats)
    finally:
        observer.cancel()
```

`monitor.management` is the shared command channel, or `None` when
disconnected. Fetch it again after reconnect rather than retaining an
outdated handle. Connection loss invalidates the old observations; a
firmware that explicitly rejects the required protocol is not retried.
If no peer trace arrives for `idle_check_interval` (15 seconds by default),
the monitor checks the gateway's trace subscription on the host link. This
detects a gateway reboot even when the USB adapter or TCP bridge stays
connected. A missing subscription or response makes the monitor reopen
the link and rebuild state. This check never sends on a tN4 line, including
when the monitor is passive.
An active monitor also checks a port when its first addressable peer is
observed without a MasterPresent header. The one MasterQuery uses the
same per-port queue as other reads; a quiet line does not cause polling.
Inventory consumers can wait for `state.port_subnets_checked(port)`
before registering peers. A fresh header establishes the claimed subnet.
A query that completed transmission with no reply permits an unclaimed,
port-scoped view but does not prove a subnet or absence of a master. An
unsent query, timeout, or reply missing from the ordered trace does not
settle the port. Trace loss invalidates this evidence on affected ports;
reconnecting starts a fresh check.

The synchronous callback receives `(path, state)`. Use
`state.snowmelt(path)`, `state.climate(path)`, `state.schedule(path)`,
`state.water_source(path)` or `state.plant_registers(path)` to
obtain an immutable snapshot. `source=None` (CLI: `--passive`) sends no
requests; link ACKs remain off unless explicitly enabled on a port. A
finite active watch lists missing identities, modes, thermostat blocks, calendar
data, plant registers and snowmelt keys and returns status 1 for incomplete
coverage. In automatic discovery, the first valid MasterStatus or
PlantLoopStatus on a port without its own MasterPresent map prompts one
read-only MasterQuery. A missing or empty answer does not start a poll.
Only a received peer frame provides the map. Reconnect, trace loss, or
withdrawal of the last claim on a port allows the next loop report to
trigger another query. This keeps existing connections to running gateways
useful even when the startup announcement was missed. Fixed-path watches
do not broadcast this query. `--no-plant-settings` skips these discovery
queries and addressed plant register reads without hiding observed reports.

When several ports are known to belong to the same network,
`state.device_routes(ports=(0, 3))` groups observations with the same full
subnet and logical node. Use `watch-native --shared-network --port 0 --port 3`
to emit these route changes alongside the physical trace-derived state.
MasterPresent headers provide the local subnet even when their payload is
empty. A unique matching port with a locally observed identity is preferred;
competing claims or incompatible identities leave the route unresolved.
Only a single-port scope can select an observed path without a master claim.
Measurements and settings stay on their original paths. This view does not
prove a unit's serial identity or replace fresh addressed validation before
a write.

`state.schedule(path).signals` retains the reported current and next phases
per advertised system. These are not proof that a device owns a table or that
a member uses that phase locally. Use `state.system_schedule(n, ports=(0, 3))`
for ports known to share a network. It selects only the preferred path's
schedule and keeps unverified master claims in `unresolved_masters`. An
unverified distinct claim or multiple confirmed masters leaves `owner`
unset. Use `state.schedule_directory.system(n, ports=(port,))` for physical
observations without route selection. Calendar rows are never combined
across ports. Missing rows are unknown, not disabled. Passive rows expose
their `report` without inventing a transmit result.

A single 654 can report a zone **and** a sensor. `device.channels` keeps
their words separate, even if both use the same number. `device.primary`
selects the configured zone in a control application, or the configured
sensor in sensor-only mode. It is absent until that endpoint reports.
`channel.operating_state`, `water_detection` and `pump_on` are typed
readings. Use `channel.raw(key)` for any reported word, including unknown
keys. A key has its own gateway receive timestamp; repeated keys use the
latest wire occurrence. The full ordered pairs remain on
`frame.snowmelt_report`. `snapshot.snowmelt_devices`, `read-device` and
`dump` expose the same interpretation. A new occupant, an address change
or a capture gap invalidates the old state.

**Remote Melt and Storm.** The [654 manual](https://www.watts.com/dfsmedia/0533dbba17714b1ab581ab07a4cbb521/81803-source/638820695340000000/654-d-03.pdf)
specifies a setpoint-group enable, not a write to an individual zone's
operating state. Preview the actual group and frame first:

```bash
tekmar-482 set-snowmelt-group 0 0x14 6 melt on --tcp 192.0.2.10:3001 --json
```

The command reads the product, application and configured zone. It requests
**both** group assignments from that zone in one status exchange. When the
exchange ends without both keys, it reads both addressed group registers
instead and rejects any disagreement with the partial status. An invalid
complete reply does not fall back to older data or defaults.
To broadcast, explicitly add `--expect-group N --confirm`.
This can affect **every device using that group**, not just the selected
zone. Missing, changed, ambiguous or sensor-only bindings cannot transmit.
`await BridgeManagement(client).set_snowmelt_group(port, address,
SnowmeltOperation.MELT, enabled=True)` uses Normal mode and also defaults
to preview. An intentional send requires `preview=False, expected_group=N`.
Pass `expected_zone` to pin a previously displayed zone as well as the group.
The Raw stream offers the same operation with an explicit source address.

After sending once, the client requests operating state from that zone.
A link ACK, group report, or another sensor's report is not confirmation.
The result distinguishes a reported state, a different operation, an
unanswered read and an unknown send outcome. A local demand can keep a
zone melting after a group is disabled. The CLI exits nonzero if the
observed operation does not match, and never retries the broadcast. During
a watch, a change in the configured group's enable on the observed port
triggers an operating-state read, not an optimistic mode change.
`device.reported_groups` holds a paired observation from the configured
zone. It is separate from `device.setup.groups`, which reflects addressed
registers. The watch asks for the pair once when the zone is established
and the assignments are not already known. It uses the group registers if
the pair is unavailable. After an incomplete exchange with a terminal link
result, fresh valid telemetry from that controller can prompt one more
paired read if assignments are still missing. There is no timer retry.
Neither cached view authorizes a send.

Use `device.setpoint(SnowmeltBlock.MELT_SETPOINT)` for the stored slab target,
not the operating slab setpoint in a status report.
`await manager.write_snowmelt_setpoint(port, address, block, celsius)` reads
the controller's application and previews the edit. Pass `preview=False`
to compare the baseline, write and read the stored block back. Pin the
observed application with `expected_application` when editing from a UI.
Use `None` for Off on Idle or Storm. Zero Celsius is a temperature, not Off;
Melt does not support Off. This operation never issues a group request.

### Line announcements

`watch-native --passive` emits a `line_report` when an observed clock, scene,
setpoint group, outdoor temperature, warm-weather shutdown, network lock,
gateway-access status or zone-sync announcement changes. `dump` includes the
latest `line_reports` per capture window. Each includes the port, source
address, gateway capture time and exact native body. No query is needed.

Use `state.line_reports_for(path)` to enumerate a sender's observations, or
`state.line_report(path, LineReportKind.SETPOINT_GROUP, group=4)` to select
one. Requested and actual group enables are separate. A source can be a line
master or the gateway itself: a scene announcement is not proof that every
device changed setback. Repeats refresh the timestamp; use the frame stream
to see each repetition. An observed capture gap, source replacement or address
change discards the affected state rather than reporting a fabricated value.

**Network faults.** `watch-native --passive` reports native `3D7` alarms and
explicit clears without sending a query. `dump` includes the last report
per source for each continuous capture window. In Python,
`state.fault_report(path)` is `None` until that source has reported. Otherwise
`report.fault` exposes the exact word, code, six-bit instance, two-bit
priority and, where documented, circuit state or descriptor detail. A clear
has `report.fault.is_clear` set; it does not clear another source. Equal
reports refresh the receive timestamp without generating a change. A gap
or address reassignment makes the affected observation unknown, not clear.

Normal-mode `NetworkError` carries the same two bytes but no source.
`decode_network_error(word).fault` exposes their native interpretation;
the last Normal-mode word is not a list of all faults on the installation.

These commands preview the exact native frame offline. Sending requires
`--confirm`, a transport and an explicit list of physical ports:

```bash
tekmar-482 network-init --ports 2 --json
tekmar-482 set-clock 2030-07-15T00:30 --dst --ports 0 2 --json
tekmar-482 set-group 4 on --ports 1 --json
tekmar-482 set-zone-sync --cycle 0 --purge on --target outdoor-reset --ports 2 --json
```

`LineBroadcast.clock(...)`, `.group(...)`, `.zone_sync(...)` and `.scene(...)`
provide the equivalent Python API. Inspect `announcement.message.to_bytes()`
or call `await announcement.send(BridgeManagement(client))` on an open Normal
session. These commands default to Normal and do not interrupt inventory,
tracing or stock clients. Use `--raw` (or pass an open Raw stream) when a
custom source address is needed. No command silently falls back to Raw mode.
Each selected port has its own outcome. A gateway expiry means it never
started; a host timeout leaves the outcome unknown. Neither causes a retry.
A link ACK does not mean every controller applied the announcement.

`LineBroadcast.network_init((2,))` builds the stock 482's empty class-1
`3DF` probe. Raw mode sends it only on request. There is no defined
application reply: a link ACK does not identify a master or a thermostat.
Use `read-status master` to request MasterPresent instead.

To look up a 161/162's remote-enable assignment, use
`set-device-group PORT SUBNET NODE on --tcp HOST:PORT`. The default only
reads the identity and setup. To transmit, add **both** `--expect-group N`
and `--confirm`: if the freshly read group differs, no enable is sent.
The command broadcasts to the group, not solely to the selected device.
A DHW, pool or unassigned application has no remote-enable group. In
Python use `await DeviceGroupBinding.read(BridgeManagement(client), port, address)` and
`binding.announcement(expected_group, enabled=True)`. Incoming
`frame.group_report` exposes the source's requested and actual bits
separately. An echoed command is not a group report.

### Scenes

Scenes are system-wide announcements, not a thermostat mode or schedule
edit. Use the numbers **1 through 8** printed on user switches. Preview the
exact frame and the physical lines it would reach without opening a connection:

```bash
tekmar-482 select-scene 6 --ports 0 2 --json
```

To transmit, add an endpoint and `--confirm`. There is one attempt per
selected port; the CLI reports each link's outcome and exits nonzero if any
port was not link-acknowledged. An ACK does **not** mean every device on a
line acted. The Python equivalent in an active Normal session is:

```python
from tekmar_482 import BridgeManagement, NativeAddress, NativeScene, SceneSelection

selection = SceneSelection(NativeScene.TEMPORARY_OCCUPIED, (0, 2), NativeAddress(0, 0))
results = await selection.send(BridgeManagement(client))
```

`monitor` decodes announcements. In Python, `NativeFrame.scene_report`
provides the received ID, duration and any extension; the frame identifies
the line and sender. Captures can include the host's own transmission.
There is no known current-scene request: do not infer the active scene on
startup or from a device's configured response.

To inspect one device's participation and response settings, read block
`0x0e` or use `await SceneSetup.read(stream, port, peer, source=source)`.
Only supported slots are reported. To preview a change to one such slot:

```bash
tekmar-482 scene-response 2 0x12 3 4 schedule --tcp 192.0.2.10:3001 --json
```

Adding `--confirm` edits the device and reads the field back. It does not
activate the scene. See the [wire IDs, durations and response table](https://github.com/jameshilliard/tekmar-482-firmware/blob/master/docs/tn4-frames.md#system-scenes).

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
An application advertising an unsupported custom wire version is not
programmed through that interface. Enter ROM ISP with its matching tool
or the hardware Test button, then use the same `--from-rom` installer.

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

`DiscoveredDevice.setback_events` decodes the tHA count (0, 2 or 4);
reserved or unavailable codes map to `None`. Raw tRPC packets retain
the original byte. The stock 482 starts an inventoried address at code 0
and copies the two event bits from each native schedule announcement
without checking the role. Zero does not prove the device lacks scheduling,
and a positive count does not prove the device owns the schedule. Consult
the native schedule role and setup to identify the owner.

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
payload. Supply **local civil time**: the encoder subtracts a daylight
hour before encoding the date, weekday and hour. A timezone-aware value
can supply the DST adjustment; use `dst=True` for an explicitly daylight
wall time. An offset alone is not DST. `dst_mode` adds the optional sixth
byte. On receive, `frame.clock.wall_datetime` is local time without a
timezone and is absent for time-only, invalid or contradictory reports.

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
tekmar-482 timing --tcp 192.0.2.10:3001 --json
tekmar-482 discover --tcp 192.0.2.10:3001 --json
tekmar-482 dump --tcp 192.0.2.10:3001 --json
tekmar-482 dump --serial /dev/serial/by-id/usb-adapter
tekmar-482 monitor --tcp 192.0.2.10:3001
tekmar-482 --vendor request DeviceType address=1001 --tcp 192.0.2.10:3001 --json
```

On custom firmware advertising `LINK_TIMING`, `timing` reports the
longest gateway service phases and each port's last missed transmit
deadline. Use repeatable `--port` to limit the per-port results. The
equivalent Python reads are `BridgeManagement.link_service_timing()`
and `port_transmit_timing(port)`. They work without changing modes,
enabling trace, or sending on tN4.

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

CI also runs the Python client over loopback TCP against the pinned production
ARM image. The emulator checks the physical tN4 port and wire bytes against
synthetic peers. This exercises unsolicited mode reports, guarded writes,
weekly schedules, snowmelt control and status, and plant and I/O queries
without a gateway or live device changes. On Linux with Rust and Arm binutils
installed:

```bash
git submodule update --init
make -C firmware image
TEKMAR_TEST_FLASH_IMAGE=firmware/target/thumbv4t-none-eabi/release/tekmar-482-firmware.bin \
    uv run python -m unittest -v tests.test_firmware_interop
```
