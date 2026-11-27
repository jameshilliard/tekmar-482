import unittest
from collections import deque
from unittest.mock import patch

from tekmar_482 import (
    BridgeCapabilities,
    BridgeFeature,
    BridgeManagement,
    BridgeMode,
    LinkServiceTiming,
    NativePath,
    NullTransport,
    PortTransmitTiming,
    ProtocolError,
    Tekmar482Client,
    TransmitTimingMiss,
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
    async def test_read_timing_in_either_mode_without_sending_on_tn4(self) -> None:
        for mode in BridgeMode:
            with self.subTest(mode=mode):
                script = Script(
                    ("0101f04b54", f"0401f04b5402{mode:02x}050040"),
                    ("0114f04b54", "0414f04b54000200000f00000012000000feffffff"),
                    (
                        "0114f04b5403",
                        "0414f04b5403900200008002000071020000ffffffff",
                    ),
                    (
                        "0114f04b5404",
                        "0414f04b540440000000000000000000000000000000",
                    ),
                )
                async with Tekmar482Client(script) as client:
                    manager = BridgeManagement(client)
                    assert await manager.capabilities() == BridgeCapabilities(
                        2,
                        mode,
                        5,
                        BridgeFeature.LINK_TIMING,
                    )
                    assert await manager.link_service_timing() == LinkServiceTiming(
                        512,
                        15,
                        18,
                        0xFFFFFFFE,
                    )
                    assert await manager.port_transmit_timing(3) == PortTransmitTiming(
                        3,
                        656,
                        TransmitTimingMiss(640, 625, 0xFFFFFFFF),
                    )
                    assert await manager.port_transmit_timing(4) == PortTransmitTiming(
                        4,
                        64,
                        None,
                    )
                assert not script.rows

    async def test_link_timing_rejects_unavailable_or_inconsistent_data(self) -> None:
        script = Script(
            ("0114f04b54", "0400000000"),
            ("0114f04b54", "0414f04b54010203"),
            ("0114f04b5401", "0414f04b540000000000000000000000000000000000"),
            ("0114f04b5401", "0414f04b540101000000000000000000000001000000"),
        )
        async with Tekmar482Client(script) as client:
            manager = BridgeManagement(client)
            with self.assertRaisesRegex(ValueError, "invalid tN4 port"):
                await manager.port_transmit_timing(5)
            with self.assertRaisesRegex(
                ProtocolError,
                "does not implement link timing",
            ):
                await manager.link_service_timing()
            with self.assertRaisesRegex(ProtocolError, "invalid diagnostic length"):
                await manager.link_service_timing()
            for _ in range(2):
                with self.assertRaisesRegex(
                    ProtocolError,
                    "invalid port transmit timing",
                ):
                    await manager.port_transmit_timing(1)
        assert not script.rows

    async def test_normal_identity_never_sets_or_changes_mode(self) -> None:
        target = NativePath(2, 0x42, 31)
        request = "010df04b5402421f"
        script = Script(
            (request, "040df04b5402421f"),
            (request, "0406f04b540df04b5401"),
            (request, "040df04b5402421e"),
        )
        async with Tekmar482Client(script) as client:
            manager = BridgeManagement(client)
            with self.assertRaises(ValueError):
                await manager.request_identity(NativePath(2, 0x42, 0))
            await manager.request_identity(target)
            with self.assertRaisesRegex(ProtocolError, "busy"):
                await manager.request_identity(target)
            with self.assertRaisesRegex(ProtocolError, "did not accept"):
                await manager.request_identity(target)
        assert not script.rows

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
