import asyncio
import json
import unittest
from io import StringIO
from unittest.mock import MagicMock, patch

from tekmar_482 import (
    BridgeClient,
    BridgeManagement,
    ConnectionClosedError,
    GatewayTraceGap,
    InvalidCapture,
    NativeFrame,
    NativePath,
    NativeState,
    NormalPeerCapture,
    ProtocolError,
    Tekmar482Client,
    discover_native,
    watch_native,
)
from tekmar_482.cli import CliError, build_parser, run
from tekmar_482.packet import Packet

from .test_management import Script
from .test_normal_block import Wire

_CAPS = "0401f04b54030005bf0e"
_IDENTITY = "6e7161c8 00001f 0d8e01 7904"
_STATUS = "7d7161c2 8fe1"


def trace(data: str) -> Packet:
    return Packet(0xD0, bytes.fromhex(data))


async def start(wire: Wire, mask: str) -> None:
    await wire.expect("0101f04b54")
    wire.feed(_CAPS)
    await wire.expect("010bf04b54")
    wire.feed("040bf04b5410" + "00" * 40)
    await wire.expect("000bf04b54" + mask)
    wire.queue_packet(trace("00 04 00000000 00 " + _STATUS))
    wire.feed("030bf04b54" + mask)


async def restore(wire: Wire, *, tail: bool = False) -> None:
    await wire.expect("000bf04b5410")
    if tail:
        wire.queue_packet(trace("00 02 03000000 00 " + _STATUS))
    wire.feed("030bf04b5410")
    wire.queue_packet(trace("00 04 04000000 00 " + _STATUS))
    await wire.expect("010bf04b54")
    wire.feed("040bf04b5410" + "00" * 40)


class PeerCaptureTests(unittest.IsolatedAsyncioTestCase):
    async def test_fences_filter_in_one_uart_burst_and_restore(self) -> None:
        wire = Wire()

        async def gateway() -> None:
            await start(wire, "14")
            wire.feed("02070100000000")
            for row in (
                "00 03 01000000 00 " + _STATUS,  # unselected port
                "01 02 01000000 03 00 " + _STATUS,  # gateway transmit
                "00 02 01000000 02 " + _STATUS,  # early start
                "00 02 01000000 80 " + _STATUS,  # unrecognized flags
                "05 01 0300000000000000",  # no selected port
                "00 02 01000000 01 " + _IDENTITY,
                "05 14 0300000000000000",
                "04 04 02000000 02 0900 8080",
            ):
                wire.queue_packet(trace(row))
            await restore(wire, tail=True)

        async with Tekmar482Client(wire) as client, asyncio.TaskGroup() as group:
            group.create_task(gateway())
            async with BridgeManagement(client).peer_capture(
                [2, 4],
                backlog=3,
            ) as peers:
                rx = await peers.read()
                assert isinstance(rx, NativeFrame)
                assert (rx.message_id, rx.capture_index, rx.port) == (0x2E7, 1, 2)
                gap = await peers.read()
                assert gap == GatewayTraceGap(3, 20) and gap.ports == (2, 4)
                assert isinstance(await peers.read(), InvalidCapture)
            (last,) = peers.drain()
            assert isinstance(last, NativeFrame) and (
                last.capture_index,
                last.port,
            ) == (2, 2)
            assert peers.host_drops == 0 and peers.gateway_drops == (0,) * 5
            with self.assertRaises(ConnectionClosedError):
                await peers.read()

    async def test_cancellation_restores_mask_but_not_to_a_reconnected_transport(
        self,
    ) -> None:
        for disconnect in (False, True):
            wire = Wire()
            entered = asyncio.Event()
            async with Tekmar482Client(wire) as client:

                async def listen(ready: asyncio.Event) -> None:
                    async with BridgeManagement(client).peer_capture([4]) as peers:
                        ready.set()
                        await peers.read()

                task = asyncio.create_task(listen(entered))
                await start(wire, "10")
                await entered.wait()
                if disconnect:
                    await client.close()
                    await client.open()
                    with self.assertRaises((ConnectionError, ConnectionClosedError)):
                        await task
                    assert wire.out.empty()
                else:
                    task.cancel()
                    await restore(wire)
                    with self.assertRaises(asyncio.CancelledError):
                        await task

    async def test_old_firmware_refused_before_touching_mask(self) -> None:
        script = Script(("0101f04b54", "0401f04b54030005bf06"))
        async with Tekmar482Client(script) as client:
            with self.assertRaisesRegex(ProtocolError, "Normal trace requires"):
                async with BridgeManagement(client).peer_capture():
                    self.fail("accepted unordered trace")
        assert not script.rows

    async def test_native_watch_preserves_snow_master_evidence_until_a_gap(
        self,
    ) -> None:
        packet = Packet(6, bytes.fromhex("030bf04b5404"))
        peers = NormalPeerCapture(4, 4, packet)
        peers.record(packet, outbound=False)
        for row in (
            "00 02 01000000 00 6e7161c8 00001f 499d01 ca04",
            "00 02 02000000 00 6f7161c3 84013c",
            "05 04 0100000000000000",
        ):
            peers.record(trace(row), outbound=False)
        history: list[tuple[str | None, int | None]] = []

        def changed(path: NativePath, state: NativeState) -> None:
            identity, calendar = state.identity(path), state.schedule(path)
            status = (
                calendar.signals[0].status if calendar and calendar.signals else None
            )
            history.append(
                (
                    identity.product.model if identity and identity.product else None,
                    status.events_raw if status and status.master else None,
                ),
            )

        result = await watch_native(
            peers,
            duration=0.01,
            source=None,
            on_change=changed,
        )
        assert history == [("654", None), ("654", 2), (None, None)]
        assert not result.state.paths and result.state.dropped == 1

    async def test_host_overflow_and_gateway_loss_invalidate_inventory(self) -> None:
        start_packet = Packet(6, bytes.fromhex("030bf04b5403"))
        peers = NormalPeerCapture(4, 3, start_packet)
        peers.record(start_packet, outbound=False)
        for row in (
            "00 00 00000000 00 " + _STATUS,
            "00 00 00000000 00 " + _IDENTITY,
            "00 01 00000000 00 " + _IDENTITY,
            "05 01 0200000000000000",
            "00 00 00000000 00 " + _STATUS,
        ):
            peers.record(trace(row), outbound=False)
        with self.assertRaisesRegex(ValueError, "passive"):
            await discover_native(peers)
        result = await discover_native(peers, duration=0.01, source=None)
        assert (result.host_records_dropped, result.gateway_trace_records_dropped) == (
            1,
            2,
        )
        assert not result.probes
        assert [(d.port, d.identity is not None) for d in result.devices] == [
            (0, False),
            (1, True),
        ]
        peers.record(trace("05 00 0100000000000000"), outbound=False)
        with self.assertRaisesRegex(ProtocolError, "trace record"):
            await peers.read()

    async def test_monitor_reads_normal_peers_by_default_and_never_enables_ack(
        self,
    ) -> None:
        wire = Wire()

        async def gateway() -> None:
            await start(wire, "1f")
            wire.queue_packet(trace("05 14 0300000000000000"))
            wire.queue_packet(trace("00 02 00000000 00 " + _STATUS))
            await restore(wire)

        parser = build_parser()
        output = StringIO()
        async with Tekmar482Client(wire) as client, asyncio.TaskGroup() as tasks:
            bridge = MagicMock(spec=BridgeClient)
            bridge.normal.return_value.__aenter__.return_value = client
            with patch("tekmar_482.cli.open_gateway") as opened:
                opened.return_value.__aenter__.return_value = bridge
                endpoint = ["--tcp", "localhost:4321", "--json"]
                with self.assertRaisesRegex(CliError, "link ACKs require"):
                    await run(
                        parser.parse_args(
                            ["discover", "--passive", "--ack-port", "2", *endpoint],
                        ),
                    )
                opened.assert_not_called()
                tasks.create_task(gateway())
                assert (
                    await run(
                        parser.parse_args(["monitor", "--limit", "1", *endpoint]),
                        stdout=output,
                    )
                    == 0
                )
            bridge.normal.assert_called_once_with(select_mode=False)
            bridge.stream.assert_not_called()
        gap, frame = map(json.loads, output.getvalue().splitlines())
        assert gap == {"gateway_trace_records_dropped": 3, "ports": [2, 4]}
        assert frame["id"] == 0x3D7
