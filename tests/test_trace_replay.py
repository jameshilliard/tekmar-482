import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tekmar_482 import (
    ProtocolError,
    Tekmar482Client,
    TraceReplayTransport,
    TraceStep,
    TrpcMethod,
    TrpcMethodId,
    TrpcPacket,
    TrpcService,
    dump_trace,
    load_trace,
)


def response(method: TrpcMethodId, **fields: int) -> TrpcPacket:
    return TrpcPacket.create(
        service=TrpcService.RESPONSE_REQUEST,
        method=method,
        fields=fields,
    )


class TraceReplayTest(unittest.IsolatedAsyncioTestCase):
    async def test_trace_replay_transport_responds_to_expected_requests(self) -> None:
        transport = TraceReplayTransport(
            [
                TraceStep.request(
                    TrpcMethod.FIRMWARE_REVISION,
                    response=response(TrpcMethod.FIRMWARE_REVISION, revision=154),
                ),
                TraceStep.request(
                    TrpcMethod.PROTOCOL_VERSION,
                    response=response(TrpcMethod.PROTOCOL_VERSION, version=3),
                ),
            ],
        )
        client = Tekmar482Client(transport)

        async with client:
            info = await client.get_gateway_info()

        assert info.firmware_revision == 154
        assert info.protocol_version == 3
        assert transport.pending_steps == 0
        transport.assert_complete()

    async def test_trace_replay_transport_rejects_unexpected_write(self) -> None:
        transport = TraceReplayTransport(
            [
                TraceStep.request(
                    TrpcMethod.PROTOCOL_VERSION,
                    response=response(TrpcMethod.PROTOCOL_VERSION, version=3),
                ),
            ],
        )
        client = Tekmar482Client(transport)

        await client.open()
        with self.assertRaisesRegex(ProtocolError, "expected tRPC write"):
            await client.request_firmware_revision()
        await client.close()

    def test_trace_steps_round_trip_json(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            trace_path = Path(tmp_dir) / "trace.json"
            steps = (
                TraceStep.request(
                    TrpcMethod.PROTOCOL_VERSION,
                    response=response(TrpcMethod.PROTOCOL_VERSION, version=3),
                ),
            )

            dump_trace(trace_path, steps)

            assert load_trace(trace_path) == steps
