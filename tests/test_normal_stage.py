import asyncio
import json
import unittest
from io import StringIO

from tekmar_482 import (
    BridgeManagement,
    FailureReason,
    ProtocolError,
    StageCommand,
    StageSelection,
    Tekmar482Client,
    TxResult,
    TxStatus,
)
from tekmar_482.cli import CliError, build_parser, run

from .test_management import Script
from .test_normal_block import Wire

_CAP = "0101f04b54"
_SUPPORT = "0401f04b54020005bffe"
_UPDATE = "0015f04b54"
_ADMIT = "0315f04b54"
_END = "0215f04b54"
_KEY = "039f03abf5018080"
_NODES = StageSelection.from_nodes(0xAB, (24, 1, 16), stage=1, output_level=117)


class NormalStageTests(unittest.IsolatedAsyncioTestCase):
    async def test_cli_previews_exact_body_without_opening_the_gateway(self) -> None:
        parser = build_parser()
        args = [
            "stage-command",
            "start-purge",
            "3",
            "0xab",
            "--nodes",
            "24",
            "1",
            "16",
            "--stage",
            "1",
            "--output-level",
            "117",
            "--json",
        ]
        output = StringIO()
        assert await run(parser.parse_args(args), stdout=output) == 0
        assert json.loads(output.getvalue()) == {
            "preview": {
                "port": 3,
                "message_id": 0x39F,
                "target_subnet": 0xAB,
                "target_nodes": [1, 16, 24],
                "stage": 1,
                "output_level": 117,
                "body": "39f00005abf5018080",
            },
        }
        with self.assertRaisesRegex(CliError, "specify --serial or --tcp"):
            await run(parser.parse_args([*args, "--confirm"]), stdout=output)

    async def test_bad_targets_and_unavailable_modes_never_send(self) -> None:
        for nodes in ((), (0,), (25,)):
            with self.subTest(nodes=nodes), self.assertRaises(ValueError):
                StageSelection.from_nodes(0xAB, nodes, stage=1, output_level=117)
        for caps in ("0401f04b54020005bf7e", "0401f04b54020105bffe"):
            with self.subTest(caps=caps):
                script = Script((_CAP, caps))
                async with Tekmar482Client(script) as client:
                    manager = BridgeManagement(client)
                    with self.assertRaisesRegex(ValueError, "requires a physical port"):
                        await manager.send_stage_command(
                            3,
                            StageCommand.START_PURGE,
                            StageSelection(1, 0, 0, 0),
                        )
                    with self.assertRaisesRegex(ProtocolError, "does not support"):
                        await manager.send_stage_command(
                            3,
                            StageCommand.START_PURGE,
                            _NODES,
                        )
                assert not script.rows

    async def test_admission_is_not_the_result_and_outcomes_remain_distinct(
        self,
    ) -> None:
        for end, expected in (
            ("00", TxResult(3, TxStatus.ACK)),
            ("01", TxResult(3, TxStatus.FAILED, FailureReason.NO_ACK)),
            ("02", TxResult(3, TxStatus.FAILED, FailureReason.ARBITRATION_LOST)),
            ("ff", None),
        ):
            with self.subTest(end=end):
                script = Script(
                    (_CAP, _SUPPORT),
                    (_UPDATE + _KEY, f"{_ADMIT}{_KEY} {_END}03{end}"),
                )
                async with Tekmar482Client(script) as client:
                    result = await BridgeManagement(client).send_stage_command(
                        3,
                        StageCommand.START_PURGE,
                        _NODES,
                    )
                    assert result == expected and not script.rows

    async def test_cancelled_send_cannot_assign_its_result_to_the_next_command(
        self,
    ) -> None:
        wire = Wire()
        async with Tekmar482Client(wire) as client, asyncio.timeout(5):
            manager = BridgeManagement(client)
            first = asyncio.create_task(
                manager.send_stage_command(3, StageCommand.START_PURGE, _NODES),
            )
            await wire.expect(_CAP)
            wire.feed(_SUPPORT)
            await wire.expect(_UPDATE + _KEY)
            first.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await first
            second = asyncio.create_task(
                manager.send_stage_command(3, StageCommand.END_PURGE, _NODES),
            )
            await wire.expect(_CAP)
            wire.feed(_ADMIT + _KEY)
            wire.feed(_SUPPORT)
            end_key = "039703abf5018080"
            await wire.expect(_UPDATE + end_key)
            wire.feed(_END + "0300")
            wire.feed(_ADMIT + end_key)
            wire.feed(_END + "0301")
            assert await second == TxResult(3, TxStatus.FAILED, FailureReason.NO_ACK)
