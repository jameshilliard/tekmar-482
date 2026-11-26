"""Byte-stream endpoints shared by the custom application and ROM installer."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Self

from .exceptions import ConnectionClosedError, ProtocolError, TransportError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


@dataclass(frozen=True, slots=True)
class GatewayEndpoint:
    """A local serial device or a transparent TCP-to-serial bridge."""

    address: str
    port: int | None = None
    baudrate: int = 9600

    @classmethod
    def serial(cls, path: str, *, baudrate: int = 9600) -> Self:
        return cls(path, baudrate=baudrate)

    @classmethod
    def tcp(cls, host: str, port: int, *, baudrate: int = 9600) -> Self:
        """For firmware programming, baudrate must match the bridge's UART."""
        return cls(host, port, baudrate)

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[Wire]:
        if self.baudrate <= 0:
            msg = "baudrate must be positive"
            raise ValueError(msg)
        if self.port is not None:
            reader, writer = await asyncio.open_connection(self.address, self.port)
        else:
            try:
                from serialx import open_serial_connection  # noqa: PLC0415
            except ImportError as err:
                msg = "direct serial requires tekmar-482[serial]"
                raise TransportError(msg) from err
            reader, writer = await open_serial_connection(
                self.address,
                self.baudrate,
                exclusive=True,
            )
        try:
            yield Wire(reader, writer, self.baudrate)
        finally:
            writer.close()
            async with asyncio.timeout(3):
                await writer.wait_closed()


@dataclass(slots=True)
class Wire:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    baudrate: int

    async def write(self, data: bytes) -> None:
        self.writer.write(data)
        async with asyncio.timeout(3):
            await self.writer.drain()

    async def line(self) -> bytes:
        try:
            data = await self.reader.readuntil(b"\n")
        except asyncio.IncompleteReadError as err:
            msg = "gateway disconnected before the end of the reply"
            raise ConnectionClosedError(msg) from err
        if not data.endswith(b"\r\n") or len(data) > 266:  # noqa: PLR2004
            msg = "malformed gateway line"
            raise ProtocolError(msg)
        return data[:-2]

    async def expect(self, expected: bytes) -> None:
        async with asyncio.timeout(5):
            actual = await self.line()
        if actual != expected:
            msg = f"expected {expected!r}, received {actual!r}"
            raise ProtocolError(msg)
