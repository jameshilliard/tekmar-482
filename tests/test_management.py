import unittest
from collections import deque
from unittest.mock import patch

from tekmar_482 import (
    BridgeCapabilities,
    BridgeFeature,
    BridgeManagement,
    BridgeMode,
    NullTransport,
    ProtocolError,
    Tekmar482Client,
)
from tekmar_482.packet import TYPE_TRPC, Packet


class Script(NullTransport):
    def __init__(self, *rows: tuple[str, str]) -> None:
        super().__init__()
        self.rows = deque(rows)

    async def write_packet(self, packet: Packet) -> None:
        request, replies = self.rows.popleft()
        assert packet == Packet(TYPE_TRPC, bytes.fromhex(request))
        for reply in replies.split():
            self.queue_packet(Packet(TYPE_TRPC, bytes.fromhex(reply)))


class ManagementTest(unittest.IsolatedAsyncioTestCase):
    async def test_timeout_is_not_stock_identification(self) -> None:
        async with Tekmar482Client(Script()) as client:
            with patch.object(client.session, "request_response", return_value=None):
                with self.assertRaises(TimeoutError):
                    await BridgeManagement(client).enter_update_mode()

    async def test_identify_revision_then_enter_isp(self) -> None:
        caps = ("0101f04b54", "0401f04b5402000502")
        revision = "a" * 40
        script = Script(
            caps,
            ("0104f04b54", "0404f04b54" + revision.encode().hex()),
            caps,
            ("0005f04b54", "0305f04b54"),
        )
        async with Tekmar482Client(script) as client:
            management = BridgeManagement(client)
            assert await management.capabilities() == BridgeCapabilities(
                2,
                BridgeMode.NORMAL,
                5,
                BridgeFeature.ROM_UPDATE,
            )
            assert await management.build_revision() == revision
            await management.enter_update_mode()
        assert not script.rows

    async def test_stock_and_unknown_wire_version_never_enter_isp(self) -> None:
        for response in (
            "0400000000",
            "0401f04b5401000502",
            "0401f04b54ff000502",
            "0401f04b5402000500",
        ):
            script = Script(("0101f04b54", response))
            async with Tekmar482Client(script) as client:
                with self.assertRaises(ProtocolError):
                    await BridgeManagement(client).enter_update_mode()
            assert not script.rows

    async def test_capability_probe_and_busy_result_are_correlated(self) -> None:
        script = Script(
            ("0101f04b54", "0400000000"),
            ("0101f04b54", "0300000000 0401f04b5402000502"),
            ("0005f04b54", "0306f04b5404f04b5401 0306f04b5405f04b5401"),
        )
        async with Tekmar482Client(script) as client:
            management = BridgeManagement(client)
            assert await management.capabilities() is None
            with self.assertRaisesRegex(ProtocolError, "busy"):
                await management.enter_update_mode()
        assert not script.rows
