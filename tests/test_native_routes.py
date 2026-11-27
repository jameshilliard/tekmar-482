import asyncio
import unittest
from unittest.mock import patch

from tekmar_482 import GatewayTraceGap, NativePath, NativeState, Packet
from tekmar_482.cli import CliError, build_parser, run
from tekmar_482.cli_native_watch import watch_all_from_args
from tekmar_482.raw import RawDispatcher

from .native_fixtures import TEST_PATH, frame, identity

_FORWARDED = NativePath(0, TEST_PATH.subnet, TEST_PATH.node)
_MASTER = NativePath(TEST_PATH.port, TEST_PATH.subnet, 0)
_SCOPE = (0, 3)
_THERMOSTAT = 107201


class RoutesTests(unittest.TestCase):
    def test_prefers_exact_header_claim_without_combining_telemetry(self) -> None:
        state = NativeState()
        state.observe(identity(_THERMOSTAT, path=_FORWARDED))
        state.observe(identity(_THERMOSTAT))
        state.observe(frame(0x36F, bytes.fromhex("01012a fa05 00 0000")))
        state.observe(
            frame(0x36F, bytes.fromhex("01012a 1806 00 0000"), _FORWARDED, stamp=10),
        )
        state.observe(frame(0x3FF, b"", _MASTER))
        (device,) = state.device_routes(ports=_SCOPE)

        assert device.address == TEST_PATH.address and device.preferred == TEST_PATH
        assert device.paths == (_FORWARDED, TEST_PATH) and device.claiming_ports == (3,)
        assert state.master_subnets(3) == (TEST_PATH.subnet,)
        assert not state.has_master_announcement(3) and not device.identity_conflict
        chosen, copy = state.climate(TEST_PATH), state.climate(_FORWARDED)
        assert (
            chosen
            and chosen.air
            and chosen.air.heat
            and chosen.air.heat.temperature_f == 68
        )
        assert copy and copy.air and copy.air.heat and copy.air.heat.temperature_f == 71

    def test_never_falls_back_to_a_copy_after_losing_the_claim(self) -> None:
        state = NativeState()
        state.observe(frame(0x3FF, b"", _MASTER))
        state.observe(identity(_THERMOSTAT, path=_FORWARDED))
        # The master claims port 3, but only the forwarded route has an identity.
        (device,) = state.device_routes(ports=_SCOPE)
        assert device.preferred is None and device.claiming_ports == (3,)
        state.observe(identity(_THERMOSTAT))
        assert state.device_routes(ports=_SCOPE)[0].preferred == TEST_PATH
        state.observe(GatewayTraceGap(1, 1 << TEST_PATH.port))
        (device,) = state.device_routes(ports=_SCOPE)
        assert device.paths == (_FORWARDED,) and device.preferred is None
        assert not state.master_subnets(TEST_PATH.port)

    def test_competing_header_claims_and_revision_conflicts_are_unresolved(
        self,
    ) -> None:
        state = NativeState()
        for path in (TEST_PATH, _FORWARDED):
            state.observe(identity(_THERMOSTAT, path=path))
            state.observe(frame(0x3FF, b"", NativePath(path.port, path.subnet, 0)))
        (device,) = state.device_routes(ports=_SCOPE)
        assert device.claiming_ports == _SCOPE and device.preferred is None
        state.forget(NativePath(_FORWARDED.port, _FORWARDED.subnet, 0))
        state.observe(identity(_THERMOSTAT, z=2, path=_FORWARDED))
        (device,) = state.device_routes(ports=_SCOPE)
        assert device.identity_conflict and device.preferred is None
        state.observe(identity(_THERMOSTAT, path=_FORWARDED))
        assert state.device_routes(ports=_SCOPE)[0].preferred == TEST_PATH
        state.observe(frame(0x3FF, b"", NativePath(3, TEST_PATH.subnet ^ 0x20, 0)))
        assert state.device_routes(ports=_SCOPE)[0].preferred is None

    def test_different_full_subnets_and_unrelated_ports_are_not_combined(self) -> None:
        state = NativeState()
        other = NativePath(0, TEST_PATH.subnet ^ 0x20, TEST_PATH.node)
        state.observe(identity(_THERMOSTAT))
        state.observe(identity(_THERMOSTAT, path=other))
        state.observe(frame(0x3FF, b"", _MASTER))
        state.observe(frame(0x3FF, b"", NativePath(4, TEST_PATH.subnet, 0)))
        second, first = state.device_routes(ports=_SCOPE)
        assert first.preferred == TEST_PATH and first.paths == (TEST_PATH,)
        assert second.preferred is None and second.paths == (other,)
        assert state.device_routes(ports=(0,))[0].preferred == other
        state.observe(frame(0x3FF, b"", NativePath(0, TEST_PATH.subnet, 0)))
        assert state.device_routes(ports=(0,))[0].preferred is None
        for ports in ((), (5,)):
            with self.subTest(ports=ports), self.assertRaises(ValueError):
                state.device_routes(ports=ports)

    def test_first_empty_master_report_is_new_evidence_for_an_existing_source(
        self,
    ) -> None:
        state = NativeState()
        state.observe(frame(0x3CF, bytes.fromhex("bc07 00c0 9828 2211 fe"), _MASTER))
        assert not state.master_subnets(TEST_PATH.port)
        assert state.observe(frame(0x3FF, b"", _MASTER)) == (_MASTER,)
        assert not state.observe(frame(0x3FF, b"", _MASTER, stamp=2))


class RouteWatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_shared_network_must_be_declared_before_opening_a_gateway(
        self,
    ) -> None:
        args = build_parser().parse_args(
            [
                "watch-native",
                "--shared-network",
                "--tcp",
                "localhost:1234",
            ],
        )
        with patch("tekmar_482.cli.open_gateway") as opened:
            with self.assertRaisesRegex(CliError, "requires explicitly selected"):
                await run(args)
            opened.assert_not_called()

    async def test_passive_watch_emits_route_changes_and_keeps_raw_paths(self) -> None:
        writes: asyncio.Queue[bytes] = asyncio.Queue()
        stream = RawDispatcher(writes.put, 64)
        uart: asyncio.Queue[Packet] = asyncio.Queue()
        await self.enterAsyncContext(stream.receiving(uart.get))
        args = build_parser().parse_args(
            [
                "watch-native",
                "--shared-network",
                "--port",
                "0",
                "--port",
                "3",
                "--passive",
                "--duration",
                "0.02",
                "--tcp",
                "localhost:1234",
            ],
        )
        for f in (
            identity(_THERMOSTAT),
            identity(_THERMOSTAT, path=_FORWARDED),
            frame(0x3FF, b"", _MASTER),
        ):
            stream.feed(Packet(1, bytes([0, f.port]) + bytes(4) + f.body))
        output: list[object] = []
        assert await watch_all_from_args(stream, args, output.append) == 0
        routes = [
            row["device_routes"]
            for row in output
            if isinstance(row, dict) and "device_routes" in row
        ]
        assert any(row["routes"][0]["preferred"] is None for row in routes)
        assert routes[-1]["routes"][0]["preferred"]["port"] == 3
        assert [p["port"] for p in routes[-1]["routes"][0]["paths"]] == [0, 3]
        assert writes.empty()
