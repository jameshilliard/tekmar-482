import asyncio
import io
import json
import unittest
from unittest.mock import MagicMock, patch

from tekmar_482 import (
    BridgeClient,
    BridgeManagement,
    FailureReason,
    NativeAddress,
    NativeBlockQuery,
    NativeStatusQuery,
    ProtocolError,
    SnowmeltRole,
    StatusRequest,
    Tekmar482Client,
    TxStatus,
)
from tekmar_482.cli import CliError, build_parser, run

from .test_management import Script
from .test_normal_block import Wire

_CAP = "0101f04b54"
_SUPPORT = "0401f04b54040005bf04"
_READ = "0110f04b54"
_ADMIT = "0410f04b54"
_REPORT = "0210f04b54"
_SNOW = "020100187f01010208ff"
_PART = _REPORT + "0200"
_FIRST = _PART + "01000000" + "17e0060509010835ca"
_LAST = _PART + "02000000" + "17e00e060901ff04057e"
_SNOW_QUERY = NativeStatusQuery.snowmelt(SnowmeltRole.ZONE, 1, bytes([8, 255]))
_PEER = NativeAddress(0, 24)


class NormalStatusTests(unittest.IsolatedAsyncioTestCase):
    async def test_normal_sensor_address_comes_from_two_native_configuration_reads(
        self,
    ) -> None:
        steps: list[tuple[str, str]] = []
        for key, frame in (
            ("011404000000d0000000", "6e7148c5d000030400"),
            ("011404000000c1000000", "6e7148c4c1000108"),
        ):
            steps.extend(
                (
                    (_CAP, _SUPPORT),
                    (
                        "010ff04b54" + key,
                        "040ff04b54" + key + " 020ff04b5401000001000000" + frame,
                    ),
                ),
            )
        key = "010114037f013a021922"
        steps.extend(
            (
                (_CAP, _SUPPORT),
                (
                    _READ + key,
                    " ".join(
                        (
                            _ADMIT + key,
                            _REPORT + "01000200000057e148c54201190200",
                            _REPORT + "010200",
                        ),
                    ),
                ),
            ),
        )
        script = Script(*steps)
        async with Tekmar482Client(script) as client:
            answer = await BridgeManagement(client).read_snowmelt_status(
                1,
                NativeAddress(0x14, 3),
                keys=bytes([0x19, 0x22]),
            )
            assert not answer.answered and answer.missing_keys == (0x22,)
            assert answer.tx_result and answer.tx_result.status is TxStatus.ACK
            assert answer.query.payload.hex() == "3a021922" and len(answer.reports) == 1
        assert not script.rows

    async def test_multipart_uses_admission_and_exact_peer_and_preserves_tails(
        self,
    ) -> None:
        script = Script(
            (_CAP, _SUPPORT),
            (
                _READ + _SNOW,
                " ".join(
                    (
                        _LAST,
                        _REPORT + "020201",
                        _ADMIT + _SNOW,
                        _REPORT + "0104fe",
                        _FIRST,
                        _LAST,
                        _REPORT + "020101",
                    ),
                ),
            ),
        )
        async with Tekmar482Client(script) as client:
            result = await BridgeManagement(client).read_status(
                2,
                _SNOW_QUERY,
                peer=_PEER,
            )
            assert result.answered and result.missing_keys == ()
            assert result.tx_result and result.tx_result.reason is FailureReason.NO_ACK
            assert [f.timestamp_ms for f in result.reports] == [1, 2]
            assert result.reports[-1].body.hex() == "17e00e060901ff04057e"
            assert (
                result.covered == {8, 255}
                and not client.session.report_subscription_count
            )
        assert not script.rows

    async def test_partial_deadline_and_real_link_faults_do_not_become_success(
        self,
    ) -> None:
        for events, missing, status in (
            ([_FIRST, _REPORT + "020200"], (255,), TxStatus.ACK),
            ([_REPORT + "0202ff"], (8, 255), None),
            ([_REPORT + "0204fe"], (8, 255), TxStatus.REJECTED),
            ([_REPORT + "020403"], (8, 255), TxStatus.FAILED),
        ):
            script = Script(
                (_CAP, _SUPPORT),
                (_READ + _SNOW, " ".join([_ADMIT + _SNOW, *events])),
            )
            async with Tekmar482Client(script) as client:
                batch = await BridgeManagement(client).read_status(
                    2,
                    _SNOW_QUERY,
                    peer=_PEER,
                )
            assert batch.missing_keys == missing and not batch.answered
            assert (batch.tx_result.status if batch.tx_result else None) is status

    async def test_host_waits_between_parts_not_just_from_admission(self) -> None:
        wire = Wire()
        async with Tekmar482Client(wire) as client, asyncio.timeout(4):
            job = asyncio.create_task(
                BridgeManagement(client).read_status(
                    2,
                    _SNOW_QUERY,
                    peer=_PEER,
                    timeout=0.8,
                ),
            )
            await wire.expect(_CAP)
            wire.feed(_SUPPORT)
            await wire.expect(_READ + _SNOW)
            wire.feed(_ADMIT + _SNOW)
            for part in (_FIRST, _LAST):
                await asyncio.sleep(0.5)
                wire.feed(part)
            wire.feed(_REPORT + "020100")
            assert (await job).answered

    async def test_reject_false_completeness_mismatched_parts_and_overflow(
        self,
    ) -> None:
        for event in (
            _REPORT + "020100",
            _REPORT + "020300",
            _REPORT + "020400",
            _REPORT + "020202",
            _REPORT + "0202ff00",
            _PART + "01000000" + "17e0004509010835ca",
            _PART + "00",
            " ".join((_FIRST, _REPORT + "0204fe")),
        ):
            script = Script(
                (_CAP, _SUPPORT),
                (_READ + _SNOW, " ".join((_ADMIT + _SNOW, event))),
            )
            async with Tekmar482Client(script) as client:
                with self.assertRaises(ProtocolError):
                    await BridgeManagement(client).read_status(
                        2,
                        _SNOW_QUERY,
                        peer=_PEER,
                    )
                assert not client.session.report_subscription_count

    async def test_block_and_status_clients_share_port_admission_not_other_ports(
        self,
    ) -> None:
        wire = Wire()
        async with Tekmar482Client(wire) as client:
            m = BridgeManagement(client)
            job = asyncio.create_task(m.read_status(2, _SNOW_QUERY, peer=_PEER))
            await wire.expect(_CAP)
            wire.feed(_SUPPORT)
            await wire.expect(_READ + _SNOW)
            wire.feed(_ADMIT + _SNOW)
            for port, result in [(2, "0406f04b540ff04b5401"), (3, "020ff04b540302")]:
                other = asyncio.create_task(
                    BridgeManagement(client).read_block(
                        port,
                        NativeBlockQuery(_PEER, 0),
                    ),
                )
                key = f"{port:02x}000000800000000000"
                await wire.until_read(key)
                if port == 3:
                    wire.feed("040ff04b54" + key)
                wire.feed(result)
                with self.assertRaises(ProtocolError):
                    await other
                assert not job.done()
            wire.feed(_FIRST)
            wire.feed(_LAST)
            wire.feed(_REPORT + "020100")
            assert (await job).answered

    async def test_cli_defaults_to_normal_and_validates_raw_only_options(self) -> None:
        args = ["read-status", "master", "3", "--tcp", "localhost:1234", "--json"]
        key = "03000000f703"
        script = Script(
            (_CAP, _SUPPORT),
            (
                _READ + key,
                " ".join(
                    (
                        _ADMIT + key,
                        _REPORT + "0300010000003ff12000",
                        _REPORT + "030100",
                    ),
                ),
            ),
        )
        parser = build_parser()
        out = io.StringIO()
        async with Tekmar482Client(script) as client:
            bridge = MagicMock(spec=BridgeClient)
            bridge.normal.return_value.__aenter__.return_value = client
            with patch("tekmar_482.cli.open_gateway") as opened:
                opened.return_value.__aenter__.return_value = bridge
                with self.assertRaisesRegex(CliError, "require read-status --raw"):
                    await run(parser.parse_args([*args, "--ack"]), stdout=out)
                opened.assert_not_called()
                assert await run(parser.parse_args(args), stdout=out) == 0
            bridge.normal.assert_called_once_with(select_mode=False)
            bridge.stream.assert_not_called()
        assert (
            json.loads(out.getvalue())["status_read"]["reports"][0]["body"]
            == "3ff12000"
        )

    async def test_unsupported_silent_host_and_invalid_peer(self) -> None:
        query = NativeStatusQuery(StatusRequest.MASTER)
        for caps in ("0401f04b540400057f", "0401f04b54040105ff", "0400000000"):
            async with Tekmar482Client(Script((_CAP, caps))) as client:
                with self.assertRaisesRegex(ProtocolError, "does not support"):
                    await BridgeManagement(client).read_status(0, query)
        script = Script((_CAP, _SUPPORT), (_READ + _SNOW, _ADMIT + _SNOW))
        async with Tekmar482Client(script) as client:
            with self.assertRaises(ValueError):
                await BridgeManagement(client).read_status(
                    2,
                    NativeStatusQuery.humidity(0, 1),
                )
            with self.assertRaises(TimeoutError):
                await BridgeManagement(client).read_status(
                    2,
                    _SNOW_QUERY,
                    peer=_PEER,
                    timeout=0.01,
                )
            assert not client.session.report_subscription_count
