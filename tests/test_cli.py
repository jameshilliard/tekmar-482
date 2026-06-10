from __future__ import annotations

import json
import unittest
from contextlib import redirect_stderr
from io import StringIO
from typing import TYPE_CHECKING

from tekmar_482 import (
    NullTransport,
    PacketTransport,
    Tekmar482Client,
    TraceReplayTransport,
    TraceStep,
    TrpcMethodId,
    TrpcPacket,
    TrpcService,
    method_from_name,
)
from tekmar_482.cli import build_parser, main, parse_fields, parse_host_port, run

if TYPE_CHECKING:
    from argparse import Namespace
    from collections.abc import Callable


def response(method: str | TrpcMethodId, **fields: int) -> TrpcPacket:
    return TrpcPacket.create(
        service=TrpcService.RESPONSE_REQUEST,
        method=method_from_name(method) if isinstance(method, str) else method,
        fields=fields,
    )


def request_step(
    method: str | TrpcMethodId,
    *,
    response: TrpcPacket | None = None,
    **fields: int,
) -> TraceStep:
    return TraceStep(
        sent=TrpcPacket.create(
            service=TrpcService.REQUEST,
            method=method_from_name(method) if isinstance(method, str) else method,
            fields=fields,
        ),
        received=() if response is None else (response,),
    )


def client_factory(
    transport: PacketTransport,
) -> Callable[[Namespace], Tekmar482Client]:
    def factory(args: Namespace) -> Tekmar482Client:
        del args
        return Tekmar482Client(transport)

    return factory


class CliTest(unittest.IsolatedAsyncioTestCase):
    def test_parse_host_port(self) -> None:
        assert parse_host_port("127.0.0.1:3001") == ("127.0.0.1", 3001)
        assert parse_host_port("[::1]:3001") == ("::1", 3001)

    def test_parse_fields(self) -> None:
        assert parse_fields(["address=1001", "setback=0x07", "enable=true"]) == {
            "address": 1001,
            "setback": 7,
            "enable": 1,
        }

    def test_main_reports_invalid_tcp_without_traceback(
        self,
    ) -> None:
        stderr = StringIO()
        with redirect_stderr(stderr):
            status = main(["gateway-info", "--tcp", "bad"])

        assert status == 2
        assert "error: invalid host:port value: 'bad'" in stderr.getvalue()
        assert "Traceback" not in stderr.getvalue()

    async def test_cli_gateway_info_json(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["gateway-info", "--tcp", "127.0.0.1:3001", "--json"])
        transport = TraceReplayTransport(
            [
                request_step(
                    "FirmwareRevision",
                    response=response("FirmwareRevision", revision=154),
                ),
                request_step(
                    "ProtocolVersion",
                    response=response("ProtocolVersion", version=3),
                ),
            ],
        )
        stdout = StringIO()

        status = await run(
            args,
            client_factory=client_factory(transport),
            stdout=stdout,
        )

        assert status == 0
        assert json.loads(stdout.getvalue()) == {
            "firmware_revision": 154,
            "protocol_version": 3,
        }

    async def test_cli_request_json_waits_for_response(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "request",
                "DeviceType",
                "address=1001",
                "--tcp",
                "127.0.0.1:3001",
                "--match-address",
                "1001",
                "--json",
            ],
        )
        transport = TraceReplayTransport(
            [
                request_step(
                    "DeviceType",
                    address=1001,
                    response=response("DeviceType", address=1001, type=100101),
                ),
            ],
        )
        stdout = StringIO()

        status = await run(
            args,
            client_factory=client_factory(transport),
            stdout=stdout,
        )

        payload = json.loads(stdout.getvalue())
        assert status == 0
        assert payload["sent"]["method"] == "DeviceType"
        assert payload["response"]["body"] == {"address": 1001, "type": 100101}

    async def test_cli_monitor_limit_text(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            ["monitor", "--tcp", "127.0.0.1:3001", "--limit", "1"],
        )
        transport = NullTransport()
        transport.queue_packet(response("ProtocolVersion", version=3).to_packet())
        stdout = StringIO()

        status = await run(
            args,
            client_factory=client_factory(transport),
            stdout=stdout,
        )

        assert status == 0
        assert stdout.getvalue() == "Response:Request ProtocolVersion <version=3>\n"

    async def test_cli_dump_json(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "dump",
                "--tcp",
                "127.0.0.1:3001",
                "--json",
                "--no-include-setbacks",
                "--write-delay",
                "0",
            ],
        )
        transport = TraceReplayTransport(
            [
                request_step(
                    "FirmwareRevision",
                    response=response("FirmwareRevision", revision=154),
                ),
                request_step(
                    "ProtocolVersion",
                    response=response("ProtocolVersion", version=3),
                ),
                request_step(
                    "DeviceInventory",
                    address=0,
                    response=response("DeviceInventory", address=0),
                ),
                request_step(
                    "ReportingState",
                    response=response("ReportingState", state=1),
                ),
                request_step(
                    "SetbackEnable",
                    response=response("SetbackEnable", enable=0),
                ),
                request_step(
                    "OutdoorTemp",
                    response=response("OutdoorTemp", temp=1550),
                ),
                request_step(
                    "NetworkError",
                    response=response("NetworkError", error=0),
                ),
                request_step(
                    "DateTime",
                    response=response(
                        "DateTime",
                        year=2026,
                        month=6,
                        day=9,
                        weekday=2,
                        hour=21,
                        minute=46,
                    ),
                ),
                *(
                    request_step(
                        "SetpointGroupEnable",
                        group_id=group,
                        response=response(
                            "SetpointGroupEnable",
                            group_id=group,
                            enable=0,
                        ),
                    )
                    for group in range(1, 13)
                ),
            ],
        )
        stdout = StringIO()

        status = await run(
            args,
            client_factory=client_factory(transport),
            stdout=stdout,
        )

        payload = json.loads(stdout.getvalue())
        assert status == 0
        assert payload["gateway"]["info"]["firmware_revision"] == 154
        assert payload["gateway"]["date_time"] == {
            "day": 9,
            "hour": 21,
            "minute": 46,
            "month": 6,
            "weekday": 2,
            "year": 2026,
        }
        assert payload["gateway"]["decoded"]["outdoor_temp"]["fahrenheit"] == 70.0
        assert (
            payload["gateway"]["decoded"]["network_error"]["description"] == "No Errors"
        )
        assert payload["gateway"]["outdoor_temp"] == 1550
        assert payload["gateway"]["setpoint_groups"]["1"] is False
        assert payload["devices"] == []
