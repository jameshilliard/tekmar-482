import asyncio
import json
import unittest
from io import StringIO

from tekmar_482 import (
    BridgeManagement,
    BroadcastIssue,
    DeviceGroupBinding,
    FailureReason,
    LineBroadcast,
    NativeAddress,
    NativeMessage,
    ProtocolError,
    SnowmeltGroupStatus,
    SnowmeltOperation,
    Tekmar482Client,
    TxResult,
)
from tekmar_482.cli import CliError, build_parser, run

from .test_bridge import listen, management
from .test_management import Script
from .test_normal_block import Wire

_CAP = "0101f04b54"
_CAPS = "0401f04b54020005bf03"
_ADMIT = "0312f04b54"
_UPDATE = "0012f04b54"
_END = "0212f04b54"
_GROUP = "031f020401"
_PEER = NativeAddress(0x12, 1)


def block_read(block: int, fields: bytes) -> tuple[str, str]:
    bid = block.to_bytes(2, "little")
    key = "031201" + bid.hex() + "0000"
    peer = NativeMessage(0x2E7, _PEER, bid + fields).to_bytes().hex()
    return "010ff04b54" + key, "040ff04b54" + key + " 020ff04b5403000001000000" + peer


class NormalAnnounceTests(unittest.IsolatedAsyncioTestCase):
    async def test_normal_cli_over_tcp_keeps_mode_and_separates_port_results(
        self,
    ) -> None:
        async def gateway(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            first = await reader.readexactly(1)
            if first == b"?":
                assert await reader.readexactly(5) == b"\r\nJ\r\n"
                assert not await reader.read()
                return
            caps = management(1, 1)
            support = management(1, 4, bytes.fromhex("020005bf03"))
            assert first + await reader.readexactly(len(caps) - 1) == caps
            writer.write(support)
            for port in (2, 3):
                assert await reader.readexactly(len(caps)) == caps
                writer.write(support)
                key = bytes([port]) + bytes.fromhex("ff02 07e001")
                update = management(18, 0, key)
                assert await reader.readexactly(len(update)) == update
                writer.write(management(18, 3, key))
            writer.write(
                management(18, 2, bytes.fromhex("0301"))
                + management(18, 2, bytes.fromhex("0200")),
            )
            assert not await reader.read()

        parser = build_parser()
        args = ["select-scene", "8", "--ports", "2", "3"]
        with self.assertRaisesRegex(CliError, "require select-scene --raw"):
            await run(parser.parse_args([*args, "--source-node", "1"]))
        async with listen(gateway) as ep, asyncio.timeout(5):
            output = StringIO()
            assert (
                await run(
                    parser.parse_args(
                        [
                            *args,
                            "--confirm",
                            "--json",
                            "--tcp",
                            f"{ep.address}:{ep.port}",
                        ],
                    ),
                    stdout=output,
                )
                == 1
            )
        assert [json.loads(line) for line in output.getvalue().splitlines()] == [
            {"port": 2, "tx_status": "ACK", "tx_reason": None},
            {"port": 3, "tx_status": "FAILED", "tx_reason": 1},
        ]

    async def test_busy_unsent_arbitration_and_timeout_are_distinct(self) -> None:
        cmd = LineBroadcast.group((3,), 4, enabled=True)
        for reply, expected in (
            ("0306f04b5412f04b5401", BroadcastIssue.PORT_BUSY),
            (_ADMIT + _GROUP + " " + _END + "03ff", BroadcastIssue.EXPIRED_NOT_SENT),
            (_ADMIT + _GROUP + " " + _END + "0302", FailureReason.ARBITRATION_LOST),
            (_ADMIT + _GROUP, BroadcastIssue.DEADLINE),
        ):
            script = Script((_CAP, _CAPS), (_UPDATE + _GROUP, reply))
            async with Tekmar482Client(script) as client:
                (result,) = await cmd.send(BridgeManagement(client), timeout=0.1)
                assert (
                    not client.session.report_subscription_count
                    and not result.link_acknowledged
                )
                assert (
                    result.outcome.reason
                    if isinstance(result.outcome, TxResult)
                    else result.outcome
                ) is expected
            assert not script.rows

    async def test_cancellation_cannot_assign_old_result_to_next_send(self) -> None:
        wire = Wire()
        cmd = LineBroadcast.group((3,), 4, enabled=True)
        async with Tekmar482Client(wire) as client:
            manager = BridgeManagement(client)
            first = asyncio.create_task(manager.send_announcement(3, cmd.message))
            await wire.expect(_CAP)
            wire.feed(_CAPS)
            await wire.expect(_UPDATE + _GROUP)
            first.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await first
            second = asyncio.create_task(manager.send_announcement(3, cmd.message))
            await wire.expect(_CAP)
            wire.feed(_ADMIT + _GROUP)
            wire.feed(_CAPS)
            await wire.expect(_UPDATE + _GROUP)
            wire.feed(_END + "0300")
            wire.feed(_ADMIT + _GROUP)
            wire.feed(_END + "0301")
            tx = await second
            assert tx and tx.reason is FailureReason.NO_ACK

    async def test_unknown_feature_invalid_body_and_foreign_sources_do_not_send(
        self,
    ) -> None:
        cmd = LineBroadcast.network_init((2,))
        for caps in ("0401f04b54020005bf01", "0401f04b54020105bf03", "0400000000"):
            script = Script((_CAP, caps))
            async with Tekmar482Client(script) as client:
                manager = BridgeManagement(client)
                for wrong in (
                    NativeMessage(0x2DF, _PEER, b""),
                    LineBroadcast.network_init((2,), source=_PEER).message,
                    NativeMessage(0x3DF, NativeAddress(0, 0), b"", priority=0),
                ):
                    with self.assertRaises(ValueError):
                        await manager.send_announcement(2, wrong)
                with self.assertRaisesRegex(ProtocolError, "does not support"):
                    await cmd.send(manager)
        for end in ("02", "020000", "02fd"):
            key = "02df03"
            script = Script(
                (_CAP, _CAPS),
                (_UPDATE + key, _ADMIT + key + " " + _END + end),
            )
            async with Tekmar482Client(script) as client:
                with self.assertRaises(ProtocolError):
                    await cmd.send(BridgeManagement(client))

    async def test_setpoint_control_uses_reported_group_not_its_node(self) -> None:
        script = Script(
            (_CAP, _CAPS),
            block_read(0, b"\x07" + (101102).to_bytes(3, "little")),
            (_CAP, _CAPS),
            block_read(0x26, bytes.fromhex("41 04 04")),
            (_CAP, _CAPS),
            (_UPDATE + _GROUP, _ADMIT + _GROUP + " " + _END + "0300"),
        )
        async with Tekmar482Client(script) as client:
            manager = BridgeManagement(client)
            binding = await DeviceGroupBinding.read(manager, 3, _PEER)
            with self.assertRaisesRegex(ValueError, "expected remote-enable group"):
                binding.announcement(_PEER.node, enabled=True)
            (result,) = await binding.announcement(4, enabled=True).send(manager)
            assert result.link_acknowledged and not script.rows

    async def test_snowmelt_reads_live_binding_and_only_zone_state_confirms(
        self,
    ) -> None:
        setup = []
        for block, fields in [
            (0, b"\x07" + (105801).to_bytes(3, "little")),
            (0xD0, b"\x01\x00"),
            (0xC0, b"\x01\x03"),
            (0xC2, b"\x01\x04"),
            (0xC3, b"\x01\x06"),
        ]:
            setup.extend([(_CAP, _CAPS), block_read(block, fields)])
        query = "030112017f0111011b"
        peer_state = (
            NativeMessage(0x17E, _PEER, bytes.fromhex("19011b0002")).to_bytes().hex()
        )
        for outcome in (None, "ff", "01"):
            rows = list(setup)
            if outcome is not None:
                rows.extend(
                    [
                        (_CAP, _CAPS),
                        (
                            _UPDATE + _GROUP,
                            _ADMIT + _GROUP + " " + _END + "03" + outcome,
                        ),
                    ],
                )
            if outcome == "01":
                rows.extend(
                    [
                        (_CAP, _CAPS),
                        (
                            "0110f04b54" + query,
                            "0410f04b54"
                            + query
                            + " 0210f04b54030001000000"
                            + peer_state
                            + " 0210f04b54030100",
                        ),
                    ],
                )
            script = Script(*rows)
            async with Tekmar482Client(script) as client:
                result = await BridgeManagement(client).set_snowmelt_group(
                    3,
                    _PEER,
                    SnowmeltOperation.MELT,
                    enabled=True,
                    expected_group=4,
                    preview=outcome is None,
                )
            assert not script.rows and result.group == 4 and result.zone == 3
            if outcome is None:
                assert result.status is SnowmeltGroupStatus.PREVIEW
            elif outcome == "ff":
                assert (
                    result.status is SnowmeltGroupStatus.EXPIRED_NOT_SENT
                    and result.readback is None
                )
            else:
                assert result.status is SnowmeltGroupStatus.STATE_REPORTED
                assert result.matches_requested_state is False
