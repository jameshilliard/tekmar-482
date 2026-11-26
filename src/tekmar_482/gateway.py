"""Choose the native bridge, or explicitly opt into vendor tRPC."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from .bridge import BridgeClient
from .client import Tekmar482Client
from .firmware import install_firmware, provision_if_rom

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from ._wire import GatewayEndpoint


@asynccontextmanager
async def open_gateway(
    endpoint: GatewayEndpoint,
    *,
    vendor: bool = False,
    auto_update: bool = False,
) -> AsyncIterator[BridgeClient | Tekmar482Client]:
    if vendor:
        if auto_update:
            msg = "vendor mode does not install custom firmware"
            raise ValueError(msg)
        client = (
            Tekmar482Client.serial_url(endpoint.address, baudrate=endpoint.baudrate)
            if endpoint.port is None
            else Tekmar482Client.tcp(endpoint.address, endpoint.port)
        )
        async with client:
            yield client
        return
    if auto_update:
        await install_firmware(endpoint)
    else:
        await provision_if_rom(endpoint)
    async with endpoint.connect() as wire:
        bridge = BridgeClient(wire)
        await bridge.identify()
        yield bridge
