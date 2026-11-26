import unittest

from tekmar_482.exceptions import ProtocolError
from tekmar_482.packet import TYPE_TRPC, Packet
from tekmar_482.trpc import (
    METHOD_SPECS,
    ResponseMatch,
    SetbackSetpointFields,
    TrpcCommand,
    TrpcMethod,
    TrpcPacket,
    TrpcService,
    method_from_name,
)


class TrpcTest(unittest.TestCase):
    def test_firmware_06_output_methods_and_body_lengths(self) -> None:
        # Method and byte count passed to the firmware's tRPC output routine.
        outputs = {
            0: (0,),
            1: (0x10F, 0x16F),
            2: (0x107, 0x117, 0x13D, 0x167, 0x187, 0x18F),
            3: (0x127, 0x12F, 0x150, 0x151, 0x152, 0x177, 0x17F),
            4: (0x11F, 0x137, 0x138, 0x13F, 0x147, 0x14F, 0x157, 0x15F),
            5: (0x13E,),
            6: (0x197, 0x19F),
            7: (0x1A7,),
        }
        assert {int(m): spec.size for m, spec in METHOD_SPECS.items()} == {
            method: size for size, methods in outputs.items() for method in methods
        }

    def test_known_output_services_require_complete_body(self) -> None:
        for method, spec in METHOD_SPECS.items():
            for service in (
                TrpcService.REPORT,
                TrpcService.RESPONSE_REQUEST,
                TrpcService.RESPONSE_UPDATE,
            ):
                with self.subTest(method=method, service=service):
                    frame = TrpcPacket.create(method=method, service=service).to_bytes()
                    assert TrpcPacket.from_bytes(frame).method == method
                    for missing in range(1, spec.size + 1):
                        with self.assertRaises(ProtocolError):
                            TrpcPacket.from_bytes(frame[:-missing])

    def test_default_selectors_and_explicit_overrides(self) -> None:
        group = TrpcCommand.request(TrpcMethod.SETPOINT_GROUP_ENABLE, {"IDnumber": 3})
        assert ResponseMatch.for_command(group).fields == {"group_id": 3}
        assert ResponseMatch.for_command(group, fields={"groupid": 4}).fields == {
            "group_id": 4,
        }
        slot = TrpcCommand.request(TrpcMethod.HEAT_SETPOINT, {"address": 1201})
        match = ResponseMatch.for_command(slot, address=2101)
        assert match.address == 2101 and match.fields == {"setback": 0}
        assert (
            ResponseMatch.for_command(
                slot,
                response_method=TrpcMethod.NETWORK_ERROR,
            ).address
            is None
        )
        assert (
            ResponseMatch.for_command(
                TrpcCommand.request(TrpcMethod.DEVICE_INVENTORY, {"address": 0}),
            ).address
            is None
        )
        # SetbackState returns a slot as data, not as an echoed selector.
        assert not ResponseMatch.for_command(
            TrpcCommand.request(TrpcMethod.SETBACK_STATE, {"address": 1201}),
        ).fields

    def test_reserved_setback_selectors_are_not_writable(self) -> None:
        for method, field in (
            (TrpcMethod.HEAT_SETPOINT, "setpoint"),
            (TrpcMethod.COOL_SETPOINT, "setpoint"),
            (TrpcMethod.SLAB_SETPOINT, "setpoint"),
            (TrpcMethod.SETPOINT_DEVICE, "temp"),
            (TrpcMethod.FAN_PERCENT, "percent"),
        ):
            for slot in (-1, 8, 255):
                with self.subTest(method=method, slot=slot):
                    fields = {"address": 1201, "setback": slot, field: 42}
                    with self.assertRaisesRegex(ValueError, "setback"):
                        TrpcCommand.update(method, fields)
                    with self.assertRaisesRegex(ValueError, "setback"):
                        TrpcPacket.create(
                            service=TrpcService.UPDATE,
                            method=method,
                            fields=fields,
                        ).to_bytes()

    def test_writable_temperature_wire_limits(self) -> None:
        for method, field, na in (
            (TrpcMethod.HEAT_SETPOINT, "setpoint", 0xFF),
            (TrpcMethod.COOL_SETPOINT, "setpoint", 0xFF),
            (TrpcMethod.SLAB_SETPOINT, "setpoint", 0xFF),
            (TrpcMethod.SETPOINT_DEVICE, "temp", 0xFFFF),
        ):
            with self.subTest(method=method):
                selection = {"address": 1201, "setback": 7}
                with self.assertRaisesRegex(ValueError, f"requires {field}"):
                    TrpcCommand.update(method, selection)
                with self.assertRaisesRegex(ValueError, f"requires {field}"):
                    TrpcPacket.create(
                        service=TrpcService.UPDATE,
                        method=method,
                        fields=selection,
                    ).to_bytes()
                for raw in (0, na - 1):
                    packet = TrpcCommand.update(method, {field: raw}).to_message()
                    assert (
                        TrpcPacket.from_bytes(packet.to_bytes()).require(field) == raw
                    )
                for raw in (-1, na, na + 1):
                    with self.assertRaises(ValueError):
                        TrpcCommand.update(method, {field: raw})
                    packet = TrpcPacket.create(
                        service=TrpcService.UPDATE,
                        method=method,
                        fields={field: raw},
                    )
                    with self.assertRaises(ValueError):
                        packet.to_bytes()
                packet = TrpcPacket.create(
                    service=TrpcService.REPORT,
                    method=method,
                    fields={field: na},
                )
                assert TrpcPacket.from_bytes(packet.to_bytes()).require(field) == na

    def test_wire_fields_do_not_wrap(self) -> None:
        for address in (-1, 65536):
            packet = TrpcPacket.create(
                method=TrpcMethod.DEVICE_TYPE,
                fields={"address": address},
            )
            with self.assertRaisesRegex(ValueError, "unsigned 2-byte field"):
                packet.to_bytes()

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
