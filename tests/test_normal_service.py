import unittest

from tekmar_482 import (
    BridgeManagement,
    FailureReason,
    NativeAddress,
    NativeFrame,
    NativeMessage,
    ProtocolError,
    Tekmar482Client,
    TxResult,
    TxStatus,
)

from .test_management import Script

_CAP = "0101f04b54"
_SUPPORT = "0401f04b54050005bffe01"
_SOURCE = NativeAddress(0x43, 7)
_CALIBRATION = "687431c103"
_CYCLE = "b3f431c20320"
_MODE = "b8f431c3025634"
_TIME = "b87431c23412"
_MIXING = "b57431c3cffe63"


class ServiceFrameTests(unittest.TestCase):
    def test_encode_and_decode_the_known_base_layouts(self) -> None:
        cases = (
            (
                NativeMessage.calibration_mode(3, source=_SOURCE, priority=1),
                _CALIBRATION,
                {"mode": 3},
            ),
            (
                NativeMessage.cycle_length_request(3, 0x20, source=_SOURCE, priority=2),
                _CYCLE,
                {"air_group_id": 3, "requested_length": 32},
            ),
            (
                NativeMessage.network_test_mode(2, 0x3456, source=_SOURCE, priority=2),
                _MODE,
                {"test_mode": 2, "expected_duration": 0x3456},
            ),
            (
                NativeMessage.network_test_time(0x1234, source=_SOURCE, priority=2),
                _TIME,
                {"additional_time": 0x1234},
            ),
            (
                NativeMessage.mixing_module_update(
                    source=_SOURCE,
                    priority=2,
                    output_mode=3,
                    cooling_logic=True,
                    clear_error=True,
                    target_raw=0x3FE,
                    target_pump_output=99,
                ),
                _MIXING,
                {
                    "output_mode": 3,
                    "cooling_logic": 1,
                    "clear_error": 1,
                    "reserved": 0,
                    "target_raw": 0x3FE,
                    "target_pump_output": 99,
                },
            ),
        )
        for message, expected, fields in cases:
            with self.subTest(message=message.message_id):
                body = bytes.fromhex(expected)
                assert message.to_bytes() == body
                assert NativeMessage.from_bytes(body) == message
                assert NativeFrame(3, 0, body).decoded == fields

    def test_wire_words_are_not_truncated(self) -> None:
        with self.assertRaises(ValueError):
            NativeMessage.calibration_mode(256, source=_SOURCE, priority=0)
        with self.assertRaises(ValueError):
            NativeMessage.network_test_mode(2, -1, source=_SOURCE, priority=0)
        with self.assertRaises(ValueError):
            NativeMessage.network_test_time(65536, source=_SOURCE, priority=0)
        for group, length in ((-1, 32), (256, 32), (3, -1), (3, 256)):
            with self.assertRaises(ValueError):
                NativeMessage.cycle_length_request(
                    group,
                    length,
                    source=_SOURCE,
                    priority=2,
                )
        with self.assertRaises(ValueError):
            NativeMessage.mixing_module_update(
                source=_SOURCE,
                priority=0,
                output_mode=3,
                cooling_logic=False,
                clear_error=False,
                target_raw=0x400,
                target_pump_output=0,
            )


class NormalServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_rejects_extensions_bad_ports_and_unguarded_edits(self) -> None:
        tail = bytes.fromhex("b57431c4cffe639a")
        extended = NativeMessage.from_bytes(tail)
        assert extended.to_bytes() == tail
        assert NativeFrame(3, 0, tail).decoded["extension_hex"] == "9a"
        script = Script()
        async with Tekmar482Client(script) as client:
            manager = BridgeManagement(client)
            for port, msg in (
                (5, NativeMessage.from_bytes(bytes.fromhex(_MODE))),
                (3, extended),
                (3, NativeMessage(0x287, _SOURCE, b"")),
                (3, NativeMessage(0x33F, _SOURCE, b"\x03")),
                (3, NativeMessage(0x33F, _SOURCE, b"\x03\x20\xcc")),
                (3, NativeMessage(0x2DF, _SOURCE, bytes.fromhex("4307030001"))),
            ):
                with (
                    self.subTest(port=port, msg=msg),
                    self.assertRaisesRegex(ValueError, "supported base service frame"),
                ):
                    await manager.send_service_frame(port, msg)

    async def test_checks_normal_capability_before_sending(self) -> None:
        for caps in ("0401f04b54050005bffe", "0401f04b54050105bffe01"):
            with self.subTest(caps=caps):
                script = Script((_CAP, caps))
                async with Tekmar482Client(script) as client:
                    with self.assertRaisesRegex(ProtocolError, "does not support"):
                        await BridgeManagement(client).send_service_frame(
                            3,
                            NativeMessage.from_bytes(bytes.fromhex(_MODE)),
                        )
                assert not script.rows

    async def test_preserves_the_header_and_returns_only_the_terminal_outcome(
        self,
    ) -> None:
        for terminal, expected in (
            ("00", TxResult(3, TxStatus.ACK)),
            ("01", TxResult(3, TxStatus.FAILED, FailureReason.NO_ACK)),
            ("02", TxResult(3, TxStatus.FAILED, FailureReason.ARBITRATION_LOST)),
            ("ff", None),
        ):
            with self.subTest(terminal=terminal):
                key = f"03{_CYCLE}"
                script = Script(
                    (_CAP, _SUPPORT),
                    (f"0016f04b54{key}", f"0316f04b54{key} 0216f04b5403{terminal}"),
                )
                async with Tekmar482Client(script) as client:
                    frame = NativeMessage.cycle_length_request(
                        3,
                        0x20,
                        source=_SOURCE,
                        priority=2,
                    )
                    assert frame.destination_nodes is None
                    outcome = await BridgeManagement(client).send_service_frame(
                        3,
                        frame,
                    )
                    assert outcome == expected and not script.rows
