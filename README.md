# tekmar-482

`tekmar-482` is an asyncio Python client for the tekmar 482 tN4 gateway.

The package talks directly to the gateway's tHA/tRPC protocol over binary TPCK
framing. It supports:

- Raw TCP streams carrying TPCK frames, suitable for ser2net-style bridges
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

## Example

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
    await client.request_device_inventory()
```

For a serialx socket URL:

```python
async with Tekmar482Client.serial_url("socket://192.0.2.10:3001") as client:
    await client.request_protocol_version()
```

## Session dispatcher

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

## Push controller

`Tekmar482Controller` owns the Home Assistant-style lifecycle: connect, disable
reporting during discovery, seed an initial runtime snapshot, enable reports,
apply live reports to state, and reconnect if the stream drops.

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
and command-line inspection.

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
tekmar-482 request DeviceType address=1001 --tcp 192.0.2.10:3001 --json
```

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
