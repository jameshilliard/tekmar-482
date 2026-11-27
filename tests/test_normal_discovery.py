import asyncio
import json
import unittest
from io import StringIO
from unittest.mock import MagicMock, patch

from tekmar_482 import (
    BridgeClient,
    BridgeManagement,
    ConnectionClosedError,
    FailureReason,
    NativeAddress,
    NativePortBusyError,
    PanelState,
    ProtocolError,
    Tekmar482Client,
    discover_native,
)
from tekmar_482.cli import CliError, build_parser, run

from .test_management import Script
from .test_normal_block import Wire
from .test_peer_capture import restore, start, trace

_CAP = "0101f04b54"
_SUPPORT = "0401f04b54020005bf1e"
_ASK = "0113f04b54"
_ACCEPT = "0413f04b54"
_END = "0213f04b54"
_ID = "6e7161c8 00001f 0d8e01 7904"


async def admit(wire: Wire, key: str) -> None:
    await wire.expect(_CAP)
    wire.feed(_SUPPORT)
    await wire.expect(_ASK + key)
    wire.feed(_ACCEPT + key)


class NormalDiscoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_discovery_validates_selection_and_drains_only_admitted_probes(
        self,
    ) -> None:
        wire = Wire()
        async with Tekmar482Client(wire) as client, asyncio.TaskGroup() as group:
            manager = BridgeManagement(client)

            async def scan() -> None:
                async with manager.native_session([2, 4]) as native:
                    for paths, src in (
                        ([(1, 22)], NativeAddress(0, 0)),
                        ([], NativeAddress(0, 1)),
                    ):
                        with self.assertRaises(ValueError):
                            await discover_native(native, subnets=paths, source=src)
                    result = await discover_native(
                        native,
                        subnets=[(4, 9), (4, 10)],
                        duration=0.08,
                    )
                    assert [
                        (
                            d.port,
                            d.identity.product.model
                            if d.identity and d.identity.product
                            else None,
                        )
                        for d in result.devices
                    ] == [(2, "481")]
                    assert [(p.port, p.subnet) for p in result.probes] == [
                        (2, 22),
                        (4, 9),
                    ]
                    assert (
                        result.probes[0].result
                        and result.probes[0].result.reason is FailureReason.NO_ACK
                    )
                    assert (
                        result.probes[1].result
                        and result.probes[1].result.completed_frame
                    )
                with self.assertRaises(ConnectionClosedError):
                    await discover_native(native, subnets=[(2, 22)])

            task = group.create_task(scan())
            await start(wire, "14")
            await admit(wire, "0409")
            wire.queue_packet(trace("00 02 01000000 01 " + _ID))
            await admit(wire, "0216")
            # A valid peer report is independent of this outcome.
            wire.feed(_END + "0201")
            await asyncio.sleep(0.1)
            assert not task.done() and wire.out.empty()
            wire.feed(_END + "0400")  # No peer reported on port 4. No new probe starts.
            await restore(wire)

    async def test_invalid_unsupported_busy_and_malformed_never_mean_no_peers(
        self,
    ) -> None:
        key = "03ff"
        script = Script(
            (_CAP, "0401f04b54020005bf0e"),
            (_CAP, _SUPPORT),
            (_ASK + key, "0406f04b5413f04b5401"),
            (_CAP, _SUPPORT),
            (_ASK + key, _ACCEPT + key + " " + _END + "03fd"),
            (_CAP, _SUPPORT),
            (_ASK + key, _ACCEPT + key + " " + _END + "03ff"),
        )
        async with Tekmar482Client(script) as client:
            manager = BridgeManagement(client)
            for port, subnet in ((3, -1), (5, 0), (0, 256)):
                with self.assertRaises(ValueError):
                    await manager.discover_subnet(port, subnet)
            with self.assertRaisesRegex(ProtocolError, "does not support"):
                await manager.discover_subnet(3, 255)
            with self.assertRaises(NativePortBusyError):
                await manager.discover_subnet(3, 255)
            with self.assertRaisesRegex(ProtocolError, "transmit outcome"):
                await manager.discover_subnet(3, 255)
            assert await manager.discover_subnet(3, 255) is None
        assert not script.rows

    async def test_default_dump_keeps_one_normal_session_from_probe_to_readback(
        self,
    ) -> None:
        wire = Wire()
        parser = build_parser()
        output = StringIO()

        async def gateway() -> None:
            await start(wire, "1f")
            await admit(wire, "0216")
            wire.feed(_END + "0200")
            wire.queue_packet(trace("00 02 00000000 00 7d7161c2 8fe1"))
            await wire.until_read("0216 40000000 0000 0000")
            wire.feed("040ff04b54 0216 40000000 0000 0000")
            wire.feed("020ff04b54 020100")  # No answering identity.
            await restore(wire)

        async with Tekmar482Client(wire) as client, asyncio.TaskGroup() as tasks:
            bridge = MagicMock(spec=BridgeClient)
            bridge.panel.return_value = PanelState(0, 0, 0, 0, 0, 0, testing=False)
            bridge.revision.return_value = "unversioned"
            bridge.normal.return_value.__aenter__.return_value = client
            with patch("tekmar_482.cli.open_gateway") as opened:
                opened.return_value.__aenter__.return_value = bridge
                args = ["dump", "--tcp", "localhost:4321", "--json"]
                with self.assertRaisesRegex(CliError, "link ACKs require"):
                    await run(parser.parse_args([*args, "--ack-port", "2"]))
                opened.assert_not_called()
                tasks.create_task(gateway())
                code = await run(
                    parser.parse_args(
                        [
                            *args,
                            "--duration",
                            "0.08",
                            "--probe-subnet",
                            "2:22",
                            "--block-attempts",
                            "1",
                            "--no-line-status",
                        ],
                    ),
                    stdout=output,
                )
                assert code == 0
            bridge.normal.assert_called_once_with(select_mode=False)
            bridge.stream.assert_not_called()
        records = list(map(json.loads, output.getvalue().splitlines()))
        summary = next(r for r in records if "identity_probes" in r)
        assert summary["devices"][0]["model"] is None
        assert len(summary["identity_probes"]) == 1
        batch = next(r["block_read"] for r in records if "block_read" in r)
        assert not batch["reports"]
