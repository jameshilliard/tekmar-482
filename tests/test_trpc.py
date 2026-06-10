import unittest

from tekmar_482.packet import TYPE_TRPC, Packet
from tekmar_482.trpc import (
    SetbackSetpointFields,
    TrpcCommand,
    TrpcMethod,
    TrpcPacket,
    TrpcService,
    method_from_name,
)


class TrpcTest(unittest.TestCase):
    def test_trpc_heat_setpoint_encoding_matches_vendor_layout(self) -> None:
        message = TrpcPacket.create(
            service=TrpcService.REQUEST,
            method=TrpcMethod.HEAT_SETPOINT,
            fields={"address": 1001, "setback": 2, "setpoint": 70},
        )

        assert message.to_packet() == Packet(
            TYPE_TRPC,
            bytes.fromhex("013F010000E9030246"),
        )

    def test_trpc_accepts_typed_payload_objects(self) -> None:
        message = TrpcPacket.create(
            service=TrpcService.REQUEST,
            method=TrpcMethod.HEAT_SETPOINT,
            fields=SetbackSetpointFields(address=1001, setback=2, setpoint=70),
        )

        assert message.body == {"address": 1001, "setback": 2, "setpoint": 70}
        assert message.to_packet() == Packet(
            TYPE_TRPC,
            bytes.fromhex("013F010000E9030246"),
        )

    def test_trpc_command_fields_are_immutable_copy(self) -> None:
        fields = {"address": 1001}
        command = TrpcCommand.request(TrpcMethod.DEVICE_TYPE, fields)
        fields["address"] = 9999

        assert command.fields["address"] == 1001
        with self.assertRaises(TypeError):
            command.fields["address"] = 1201  # type: ignore[index]

    def test_trpc_parses_known_body_and_extra_data(self) -> None:
        message = TrpcPacket.from_bytes(bytes.fromhex("02170100002A00CAFE"))

        assert message.service == TrpcService.REPORT
        assert message.service_name == "Report"
        assert message.method == TrpcMethod.OUTDOOR_TEMP
        assert message.method_name == "OutdoorTemp"
        assert message.body == {"temp": 42}
        assert message.extra == bytes.fromhex("CAFE")

    def test_trpc_request_with_missing_fields_defaults_to_zero(self) -> None:
        message = TrpcPacket.create(
            service=TrpcService.REQUEST,
            method=TrpcMethod.DEVICE_INVENTORY,
        )

        assert message.body == {}
        assert message.to_bytes() == bytes.fromhex("01670100000000")

    def test_trpc_accepts_vendor_and_ha_field_aliases(self) -> None:
        group = TrpcPacket.create(
            service=TrpcService.REQUEST,
            method=TrpcMethod.SETPOINT_GROUP_ENABLE,
            fields={"groupid": 2, "Enable": 1},
        )
        humidity = TrpcPacket.create(
            service=TrpcService.REPORT,
            method=TrpcMethod.RELATIVE_HUMIDITY,
            fields={"address": 1001, "RHpercent": 44},
        )

        assert group.body == {"group_id": 2, "enable": 1}
        assert group.to_bytes() == bytes.fromhex("013D0100000201")
        assert humidity.body == {"address": 1001, "percent": 44}

    def test_trpc_accepts_outdoor_temperature_method_alias(self) -> None:
        method = method_from_name("OutdoorTemperature")
        message = TrpcPacket.create(service=TrpcService.REQUEST, method=method)

        assert message.method == TrpcMethod.OUTDOOR_TEMP
        assert message.method_name == "OutdoorTemp"
        assert message.to_bytes() == bytes.fromhex("01170100000000")
