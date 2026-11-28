import asyncio
import io
import json
import unittest
from calendar import Day
from datetime import time
from unittest.mock import MagicMock, patch

from tekmar_482 import (
    BridgeClient,
    BridgeManagement,
    FailureReason,
    NativeAddress,
    NativeBlockQuery,
    NativeField,
    NativeMessage,
    NativePortBusyError,
    NativeWriteStatus,
    ProtocolError,
    ScheduleChange,
    ScheduleEvent,
    ScheduleTarget,
    Tekmar482Client,
    TxStatus,
)
from tekmar_482.cli import build_parser, run

from .test_management import Script
from .test_normal_block import Wire

_CAP = ("0101f04b54", "0401f04b54040005bf04")
_KEY = "02120100000003000000"
_EDIT = "021201030000000703010409000409"
_OLD = "6e712846030007010409"
_NEW = "6e712846030007000409"
_QUERY = NativeBlockQuery(NativeAddress(0x12, 33), 3)
_CHANGES = {NativeField.MODE_SETTING: 0}


def block(terminal: str) -> tuple[str, str]:
    return "010ff04b54" + _KEY, "040ff04b54" + _KEY + " 020ff04b5402" + terminal


def peer(frame: str) -> tuple[str, str]:
    # The actual request got no link ACK; its peer still returned a valid block.
    return block("000134120000" + frame)


def edit(terminal: str) -> tuple[str, str]:
    return "0011f04b54" + _EDIT, "0311f04b54" + _EDIT + " 0211f04b5402" + terminal


class NormalEditTests(unittest.IsolatedAsyncioTestCase):
    async def test_normal_weekend_schedule_edit_accepts_compact_confirmation(
        self,
    ) -> None:
        target = NativeAddress(0x23, 5)

        def read(block_id: int, values: bytes) -> tuple[str, str]:
            block_bytes = block_id.to_bytes(2, "little")
            key = "012310000000" + block_bytes.hex() + "0000"
            msg = NativeMessage(0x2E7, target, block_bytes + values)
            return (
                "010ff04b54" + key,
                "040ff04b54" + key + " 020ff04b5401000101000000" + msg.to_bytes().hex(),
            )

        week = bytes.fromhex("0306090c0f1215")
        edit_key = "012305130000007f07" + week.hex() + "0306090c0f3030"
        script = Script(
            _CAP,
            read(0, b"\x07" + (105102).to_bytes(3, "little")),
            _CAP,
            read(0x0F, bytes.fromhex("0d010101")),
            _CAP,
            read(0x13, b"\x7f" + week),
            _CAP,
            ("0011f04b54" + edit_key, "0311f04b54" + edit_key + " 0211f04b5401000101"),
            _CAP,
            read(0x13, bytes.fromhex("603030")),
        )
        async with Tekmar482Client(script) as client:
            result = await BridgeManagement(client).write_schedule_time(
                1,
                ScheduleTarget(target),
                ScheduleChange(ScheduleEvent.UNOCCUPIED, Day.SUNDAY, time(8)),
                preview=False,
            )
            assert result.status is NativeWriteStatus.CONFIRMED and not script.rows

    async def test_ack_is_not_confirmation_and_ambiguous_faults_read_back_once(
        self,
    ) -> None:
        for code, report, expected in (
            ("000000", peer(_OLD), NativeWriteStatus.DIFFERENT),
            ("000101", peer(_NEW), NativeWriteStatus.CONFIRMED),
            ("000003", peer(_NEW), NativeWriteStatus.CONFIRMED),
            ("000000", block("0100"), NativeWriteStatus.UNCONFIRMED),
            (
                "000000",
                ("010ff04b54" + _KEY, "040ff04b54" + _KEY),
                NativeWriteStatus.UNCONFIRMED,
            ),
        ):
            with self.subTest(code=code, report=report):
                script = Script(_CAP, peer(_OLD), _CAP, edit(code), _CAP, report)
                async with Tekmar482Client(script) as client:
                    result = await BridgeManagement(client).write_block(
                        2,
                        _QUERY,
                        _CHANGES,
                        preview=False,
                        timeout=0.2,
                    )
                    assert result.status is expected and not script.rows
                    assert result.guard_result and result.guard_result.completed_frame
                    assert (
                        result.tx_result
                        and result.proposed.to_bytes().hex() == "6df0000712010300000409"
                    )
                    if result.status is NativeWriteStatus.CONFIRMED:
                        assert result.readback and result.readback.tx_result
                        assert result.readback.tx_result.reason is FailureReason.NO_ACK

    async def test_known_non_delivery_does_not_retry_or_read_back(self) -> None:
        for code, expected, attempted in (
            ("0100ff", NativeWriteStatus.BASELINE_CHANGED, None),
            ("0500ff", NativeWriteStatus.TARGET_MOVED, None),
            ("05ffff", NativeWriteStatus.TARGET_MOVED, None),
            ("0201ff", NativeWriteStatus.NOT_SENT, None),
            ("02ffff", NativeWriteStatus.NOT_SENT, None),
            ("03feff", NativeWriteStatus.NOT_SENT, None),
            ("0303ff", NativeWriteStatus.NOT_SENT, None),
            ("0400fe", NativeWriteStatus.LINK_FAILED, TxStatus.REJECTED),
            ("000002", NativeWriteStatus.LINK_FAILED, TxStatus.FAILED),
        ):
            script = Script(_CAP, peer(_OLD), _CAP, edit(code))
            async with Tekmar482Client(script) as client:
                result = await BridgeManagement(client).write_block(
                    2,
                    _QUERY,
                    _CHANGES,
                    preview=False,
                )
                assert result.status is expected and result.readback is None
                assert (result.tx_result and result.tx_result.status) is attempted
            assert not script.rows

    async def test_no_write_for_preview_noop_unsupported_or_invalid_evidence(
        self,
    ) -> None:
        for frame, preview, expected in (
            (_OLD, True, NativeWriteStatus.PREVIEW),
            (_NEW, False, NativeWriteStatus.UNCHANGED),
        ):
            script = Script(_CAP, peer(frame))
            async with Tekmar482Client(script) as client:
                result = await BridgeManagement(client).write_block(
                    2,
                    _QUERY,
                    _CHANGES,
                    preview=preview,
                )
                assert result.status is expected and result.tx_result is None
            assert not script.rows
        for invalid in (
            "010000",
            "0000ff",
            "0003ff",
            "0300ff",
            "00ff00",
            "040002",
            "020200",
            "050000",
            "0503ff",
        ):
            script = Script(_CAP, peer(_OLD), _CAP, edit(invalid))
            async with Tekmar482Client(script) as client:
                with self.assertRaises(ProtocolError):
                    await BridgeManagement(client).write_block(
                        2,
                        _QUERY,
                        _CHANGES,
                        preview=False,
                    )
            assert not script.rows
        for caps in ("0401f04b54040005ff", "0401f04b54040105bf01", "0400000000"):
            async with Tekmar482Client(Script((_CAP[0], caps))) as client:
                with self.assertRaisesRegex(ProtocolError, "does not support"):
                    await BridgeManagement(client).write_block(
                        2,
                        _QUERY,
                        _CHANGES,
                        preview=False,
                    )

    async def test_second_manager_cannot_reuse_cancelled_update_or_its_readback(
        self,
    ) -> None:
        wire = Wire()
        async with Tekmar482Client(wire) as client:
            first, second = BridgeManagement(client), BridgeManagement(client)
            task = asyncio.create_task(
                first.write_block(2, _QUERY, _CHANGES, preview=False),
            )
            await wire.until_read(_KEY)
            wire.feed(peer(_OLD)[1].split()[0])
            wire.feed(peer(_OLD)[1].split()[1])
            await wire.expect(_CAP[0])
            wire.feed(_CAP[1])
            await wire.expect(edit("")[0])
            with self.assertRaises(NativePortBusyError):
                await second.read_block(2, _QUERY)
            wire.feed("0311f04b54" + _EDIT)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            again = asyncio.create_task(second.write_block(2, _QUERY, _CHANGES))
            await wire.until_read(_KEY)
            wire.feed("0211f04b5402000000")  # previous update, never a fresh block
            for packet in peer(_NEW)[1].split():
                wire.feed(packet)
            assert (await again).status is NativeWriteStatus.UNCHANGED
            assert wire.out.empty() and not client.session.report_subscription_count

    async def test_cli_defaults_to_normal_and_never_writes_without_confirm(
        self,
    ) -> None:
        args = build_parser().parse_args(
            [
                "write-block",
                "2",
                "0x12",
                "33",
                "3",
                "mode_setting=0",
                "--json",
                "--tcp",
                "localhost:1234",
            ],
        )
        script = Script(_CAP, peer(_OLD))
        async with Tekmar482Client(script) as client:
            gateway = MagicMock(spec=BridgeClient)
            gateway.normal.return_value.__aenter__.return_value = client
            out = io.StringIO()
            with patch("tekmar_482.cli.open_gateway") as opened:
                opened.return_value.__aenter__.return_value = gateway
                assert await run(args, stdout=out) == 0
            assert json.loads(out.getvalue())["status"] == "preview"
            gateway.normal.assert_called_once_with(select_mode=False)
            gateway.stream.assert_not_called()
        assert not script.rows
