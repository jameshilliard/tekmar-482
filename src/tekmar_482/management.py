"""Negotiated firmware management over the ordinary vendor tRPC session."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import IntEnum, IntFlag
from typing import TYPE_CHECKING

from .exceptions import ProtocolError
from .trpc import ResponseMatch, TrpcCommand, TrpcMethod, TrpcService

if TYPE_CHECKING:
    from .client import Tekmar482Client
    from .trpc import TrpcPacket

_CAPABILITY_BYTES = 4


class BridgeMode(IntEnum):
    NORMAL = 0
    RAW = 1


class BridgeFeature(IntFlag):
    RAW = 1
    ROM_UPDATE = 2


class BridgeMethod(IntEnum):
    CAPABILITIES = 0x544BF001
    MODE = 0x544BF002
    STATS = 0x544BF003
    BUILD = 0x544BF004
    ENTER_UPDATE = 0x544BF005
    ERROR = 0x544BF006
    PANEL = 0x544BF007
    LED_TEST = 0x544BF008
    ACK_MASK = 0x544BF009
    COUNTERS = 0x544BF00A


@dataclass(frozen=True, slots=True)
class BridgeCapabilities:
    wire_version: int
    mode: BridgeMode
    ports: int
    features: BridgeFeature


class _ManagementMatch(ResponseMatch):
    def matches(self, message: TrpcPacket) -> bool:
        if self.service_ids is None or message.service_id not in self.service_ids:
            return False
        if message.method_id == TrpcMethod.NULL_METHOD:
            return not message.extra
        if message.method_id == BridgeMethod.ERROR:
            return message.extra[:4] == self.method_id.to_bytes(4, "little")
        return super().matches(message)


class BridgeManagement:
    """Use with an open Tekmar482Client. Does not open or replace its transport."""

    def __init__(self, client: Tekmar482Client) -> None:
        self._session = client.session

    async def _exchange(self, command: TrpcCommand) -> TrpcPacket:
        exchange = (
            self._session.update_response
            if command.service == TrpcService.UPDATE
            else self._session.request_response
        )
        packet = await exchange(
            command,
            response_match=_ManagementMatch.for_command(command),
        )
        if packet is None:
            msg = f"no reply to management method {int(command.method):#x}"
            raise TimeoutError(msg)
        if packet.method_id == BridgeMethod.ERROR:
            reason = packet.extra[4:]
            if reason == b"\x01":
                msg = "gateway is busy; firmware update was not accepted"
            elif reason == b"\x02":
                msg = "gateway rejected the management service or payload"
            else:
                msg = f"unexpected management error: {reason.hex()}"
            raise ProtocolError(msg)
        return packet

    async def capabilities(self) -> BridgeCapabilities | None:
        """Return None only for a stock-style NullMethod, never on timeout."""
        reply = await self._exchange(TrpcCommand.request(BridgeMethod.CAPABILITIES))
        if reply.method_id == TrpcMethod.NULL_METHOD:
            return None
        if len(reply.extra) == _CAPABILITY_BYTES:
            version, mode, ports, flags = reply.extra
            if version == 1 and mode in BridgeMode and ports:
                return BridgeCapabilities(
                    version,
                    BridgeMode(mode),
                    ports,
                    BridgeFeature(flags),
                )
        msg = f"unsupported bridge capabilities: {reply.extra.hex()}"
        raise ProtocolError(msg)

    async def build_revision(self) -> str:
        reply = await self._exchange(TrpcCommand.request(BridgeMethod.BUILD))
        if not re.fullmatch(rb"(?:[0-9a-f]{40}|unversioned)", reply.extra):
            msg = "missing or invalid custom firmware revision"
            raise ProtocolError(msg)
        return reply.extra.decode("ascii")

    async def enter_update_mode(self) -> None:
        """Request ROM ISP once. The caller must still verify the ROM part ID."""
        caps = await self.capabilities()
        if caps is None or not caps.features & BridgeFeature.ROM_UPDATE:
            msg = "gateway did not advertise the firmware update extension"
            raise ProtocolError(msg)
        reply = await self._exchange(TrpcCommand.update(BridgeMethod.ENTER_UPDATE))
        if reply.method_id != BridgeMethod.ENTER_UPDATE or reply.extra:
            msg = "gateway did not acknowledge ROM handoff"
            raise ProtocolError(msg)
