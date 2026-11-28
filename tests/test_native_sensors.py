import asyncio
import unittest

from tekmar_482 import (
    CaptureGap,
    GatewayTraceGap,
    NativeAddress,
    NativeFrame,
    NativeMessage,
    NativePath,
    NativeSnapshot,
    NativeState,
    Packet,
    SensorRole,
    SensorSlot,
    ThermostatSensorMonitor,
    read_thermostat_sensors,
)
from tekmar_482.cli import build_parser
from tekmar_482.cli_status import sensor_fields
from tekmar_482.raw import RawDispatcher

_PEER = NativeAddress(0x23, 5)
_LOCKED = NativeAddress(_PEER.subnet, _PEER.node | 32)


def _block(
    block: int,
    data: bytes,
    *,
    port: int = 1,
    peer: NativeAddress = _PEER,
) -> NativeFrame:
    msg = NativeMessage(0x2E7, peer, block.to_bytes(2, "little") + data)
    return NativeFrame(port, 0, msg.to_bytes())


def _product(number: int, j: int, z: int) -> bytes:
    return (
        bytes([0x3F])
        + number.to_bytes(3, "little")
        + j.to_bytes(2, "little")
        + bytes([z])
    )


class SensorMonitorTests(unittest.TestCase):
    def test_unusable_identity_does_not_create_a_tracked_path(self) -> None:
        cache = ThermostatSensorMonitor()
        assert not cache.observe(_block(0, b"\x00"))
        assert not cache.observe(_block(0, _product(105102, 0, 3)))
        assert not cache.devices
        assert not cache.observe(CaptureGap(1))

    def test_shared_product_number_requires_a_distinguishing_j_number(self) -> None:
        cache = ThermostatSensorMonitor()
        assert cache.observe(_block(0, b"\x07" + (10599181).to_bytes(3, "little")))
        assert not cache.devices
        assert not cache.observe(_block(0x16, bytes.fromhex("07 02 f205")))
        assert cache.observe(_block(0, b"\x18" + (1116).to_bytes(2, "little")))
        (device,) = cache.devices
        assert device.sensors.product.model == "542" and not device.sensors.inputs

    def test_duplicate_roles_and_paths_are_not_flattened(self) -> None:
        cache = ThermostatSensorMonitor()
        old = _product(99203, 1127, 8)
        new = _product(99203, 1127, 9)
        report = bytes.fromhex("07 00 f205")
        assert not cache.observe(_block(0x16, report))
        assert cache.observe(_block(0, old))
        assert cache.observe(_block(0, new, port=2))
        other = NativeAddress(0x2B, _PEER.node)
        assert cache.observe(_block(0, new, peer=other))
        assert cache.observe(_block(0x16, report, peer=_LOCKED))
        assert not cache.observe(_block(0x16, report))
        assert cache.observe(_block(0x16, report, port=2))
        first, second, third = cache.devices
        assert first.address == _PEER and first.sensors.inputs[0].current_f == 67.2
        assert not second.sensors.inputs and second.address == other
        assert third.sensors.inputs[0].status_role is SensorRole.OFF
        assert third.sensors.inputs[0].current_f is None
        assert cache.observe(_block(0xD, bytes.fromhex("06 0202")))
        for block in (0x17, 0x18):
            assert cache.observe(_block(block, bytes.fromhex("07 02 f305")))
        assert cache.devices[0].sensors.average_floor_f == 67.3
        assert [s.slot.value for s in cache.devices[0].sensors.inputs] == [0, 1, 2]

    def test_identity_refinement_conflict_and_half_words(self) -> None:
        cache = ThermostatSensorMonitor()
        assert cache.observe(_block(0, b"\x07" + (99203).to_bytes(3, "little")))
        assert cache.observe(_block(0x16, bytes.fromhex("07 00 f205")))
        (before_revision,) = cache.devices
        assert before_revision.sensors.inputs[0].current_f is None
        assert cache.observe(
            _block(0, bytes.fromhex("20 08")),
        )  # Same occupant, supplies revision.
        assert cache.devices[0].sensors.inputs[0].current_f == 67.2
        assert not cache.observe(_block(0x16, bytes.fromhex("07 01")))  # Truncated.
        assert not cache.observe(
            _block(0x16, bytes.fromhex("11 00 01 02")),
        )  # Ambiguous.
        assert cache.observe(_block(0x16, bytes.fromhex("00")))
        assert cache.devices[0].sensors.inputs[0].current_raw is None
        for data in ("03 00 f2", "04 05"):
            assert cache.observe(_block(0x16, bytes.fromhex(data)))
            assert cache.devices[0].sensors.inputs[0].current_raw is None
        assert cache.observe(
            _block(0, bytes.fromhex("20 09")),
        )  # No matching product in this report.
        assert not cache.devices
        assert cache.observe(_block(0, _product(99203, 1127, 9)))
        assert not cache.devices[0].sensors.inputs
        assert cache.observe(_block(0x17, bytes.fromhex("07 02 f205")))
        assert cache.observe(
            _block(0, _product(107201, 1234, 0)),
        )  # Entirely different product.
        assert not cache.devices[0].sensors.inputs
        assert cache.observe(_block(0, _product(105801, 1226, 0)))  # Not a thermostat.
        assert not cache.devices

    def test_address_moves_and_capture_gaps_require_new_identity(self) -> None:
        cache = ThermostatSensorMonitor()
        other = NativeAddress(8, 3)
        for peer in (_PEER, other):
            assert cache.observe(_block(0, _product(100103, 1162, 3), peer=peer))
        move = NativeMessage(0x3E7, other, bytes([_LOCKED.subnet, _LOCKED.node]))
        assert cache.observe(NativeFrame(1, 5, move.to_bytes()))
        assert not cache.observe(_block(0x17, bytes.fromhex("07 ef 0e01"), peer=other))
        assert not cache.devices
        assert cache.observe(_block(0, _product(100103, 1162, 3), peer=other))
        assert cache.observe(CaptureGap(2))
        assert not cache.observe(CaptureGap(1))
        assert not cache.devices

    def test_setup_changes_and_updates_reconcile_like_native_state(self) -> None:
        cache = ThermostatSensorMonitor()
        state = NativeState()
        paths = tuple(NativePath(p, _PEER.subnet, _PEER.node) for p in (1, 2))
        for path in paths:
            for report in (
                _block(0, _product(105102, 1222, 3), port=path.port),
                _block(0x0D, bytes.fromhex("07 01 02 04"), port=path.port),
                _block(0x15, bytes.fromhex("03 0006"), port=path.port),
                _block(0x17, bytes.fromhex("07 02 f305"), port=path.port),
                _block(0x18, bytes.fromhex("07 04 c905"), port=path.port),
            ):
                assert cache.observe(report)
                state.observe(report)

        def matches() -> None:
            assert [(item.port, item.sensors) for item in cache.devices] == [
                (item.path.port, item.sensors) for item in state.thermostats
            ]

        matches()
        # A changed setup retires the affected input and aggregate, not the
        # other input or a thermostat on a different physical port.
        changed = _block(0x0D, bytes.fromhex("07 01 05 04"))
        assert cache.observe(changed)
        state.observe(changed)
        matches()
        first, second = cache.devices
        assert first.sensors.room is None
        first_inputs = {item.slot: item for item in first.sensors.inputs}
        assert first_inputs[SensorSlot.EXTERNAL_1].current_raw is None
        assert first_inputs[SensorSlot.EXTERNAL_2].current_raw == 0x05C9
        assert second.sensors.room and second.sensors.room.average_raw == 0x0600

        # An addressed update invalidates only that physical destination.
        # A short update has no effect and cannot erase supported readings.
        sender = NativeAddress(0, 0)
        payload = bytes([_PEER.subnet, _PEER.node, 0x0D, 0, 1, 2, 4, 0, 0])
        short = NativeFrame(2, 0, NativeMessage(0x2DF, sender, payload[:5]).to_bytes())
        assert not cache.observe(short) and not state.observe(short)
        update = NativeFrame(2, 0, NativeMessage(0x2DF, sender, payload).to_bytes())
        assert cache.observe(update)
        assert state.observe(update) == (paths[1],)
        matches()
        assert cache.devices[1].sensors.room is None
        assert cache.devices[0].sensors.inputs == first.sensors.inputs

    def test_gateway_gap_only_invalidates_affected_port(self) -> None:
        cache = ThermostatSensorMonitor()
        for port in (1, 2):
            assert cache.observe(_block(0, _product(105102, 1222, 3), port=port))
            assert cache.observe(_block(0x17, bytes.fromhex("07 02 f305"), port=port))
        unaffected = cache.devices[1]
        assert cache.observe(GatewayTraceGap(2, 1 << 1))
        assert cache.devices == (unaffected,)
        assert not cache.observe(GatewayTraceGap(1, 1 << 1))
        assert not cache.observe(_block(0x17, bytes.fromhex("07 02 f305")))
        assert cache.observe(_block(0, _product(105102, 1222, 3)))
        assert not cache.devices[0].sensors.inputs
        assert cache.observe(CaptureGap(1))
        assert not cache.devices


class WatchedSensorStateTests(unittest.TestCase):
    def test_setup_change_retires_only_affected_input_samples_on_its_port(self) -> None:
        state = NativeState()
        paths = tuple(NativePath(p, _PEER.subnet, _PEER.node) for p in (1, 2))
        for path in paths:
            state.observe(_block(0, _product(105102, 1222, 3), port=path.port))
            for b, data in (
                (0x0D, "07 01 02 04"),
                (0x15, "03 0006"),
                (0x16, "00"),  # The device replied without a supported value.
                (0x17, "07 02 f305"),
                (0x18, "07 04 c905"),
            ):
                state.observe(_block(b, bytes.fromhex(data), port=path.port))
            assert not state.missing_sensor_reads(path)

        first, second = paths
        assert state.observe(_block(0x0D, bytes.fromhex("07 01 05 04"))) == (first,)
        assert state.missing_sensor_reads(first) == {(0x15, None): 1, (0x17, None): 1}
        assert not state.missing_sensor_reads(second)
        before = state.climate(second)
        view = state.climate(first)
        assert before and before.sensors.room and view and view.sensors.room is None
        inputs = {s.slot: s for s in view.sensors.inputs}
        assert inputs[SensorSlot.EXTERNAL_1].current_raw is None
        assert inputs[SensorSlot.EXTERNAL_2].current_raw == 0x5C9
        state.observe(_block(0x17, bytes.fromhex("07 05 ca05")))
        updated = state.climate(first)
        assert updated and updated.sensors.inputs[1].display_role is SensorRole.COIL
        assert state.climate(second) == before


class SensorReadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.blocks = {
            0: _product(100103, 1162, 4),
            0x15: bytes.fromhex("03 f2 05"),
            0x17: bytes.fromhex("07 00 f4 05"),
        }
        self.pending: set[int] = set()
        self.sent: list[int] = []
        self.stream = RawDispatcher(self.send, 16)
        idle: asyncio.Queue[Packet] = asyncio.Queue()
        await self.enterAsyncContext(self.stream.receiving(idle.get))

    async def send(self, data: bytes) -> None:
        assert data[:2] == bytes([0, 1])
        cmd = NativeMessage.from_bytes(data[6:])
        assert cmd.message_id == 0x2D7 and cmd.payload[2] == _PEER.subnet
        assert int.from_bytes(cmd.payload[3:7], "little") == _PEER.request_mask()
        block = int.from_bytes(cmd.payload[:2], "little")
        self.sent.append(block)
        if block not in self.pending:
            self.stream.feed(Packet(1, bytes([2, 1, 0])))
        if (body := self.blocks.get(block)) is not None:
            self.stream.feed(
                Packet(
                    1,
                    bytes([0, 1, 0, 0, 0, 0]) + _block(block, body).body + bytes(1),
                ),
            )

    async def test_addressed_and_snapshot_match_with_missing_replies(self) -> None:
        del self.blocks[0x15]
        sample = await read_thermostat_sensors(self.stream, 1, _PEER, timeout=0.02)
        assert self.sent == [0, 0x15, 0x17] and sample.unanswered_blocks == (0x15,)
        assert NativeSnapshot(sample.batches, ()).thermostat_sensors == (sample.device,)
        assert sample.device.sensors.room is None
        (sensor,) = sample.device.sensors.inputs
        assert (
            sensor.status_role is SensorRole.OFF
            and sensor.display_role is SensorRole.FLOOR
        )
        assert (
            sensor.current_f == 67.4 and sample.device.sensors.average_floor_f is None
        )
        assert sensor_fields(sample.device)["model"] == "538"
        args = build_parser().parse_args(
            ["read-sensors", "1", "0x23", "5", "--tcp", "localhost:1234"],
        )
        assert (args.command, args.port, args.subnet, args.node) == (
            "read-sensors",
            1,
            0x23,
            5,
        )

    async def test_unfinished_transmit_stops_reads_even_if_a_report_arrived(
        self,
    ) -> None:
        self.pending.add(0x15)
        sample = await read_thermostat_sensors(self.stream, 1, _PEER, timeout=0.02)
        assert sample.unanswered_blocks == (0x15, 0x17)
        assert self.sent == [0, 0x15] and sample.device.sensors.room is None

    async def test_zero_support_retires_samples_in_read_and_snapshot(self) -> None:
        self.blocks[0x17] = bytes.fromhex("00")
        sample = await read_thermostat_sensors(self.stream, 1, _PEER, timeout=0.02)
        assert sample.unanswered_blocks == ()
        assert NativeSnapshot(sample.batches, ()).thermostat_sensors == (sample.device,)
        (sensor,) = sample.device.sensors.inputs
        assert sensor.current_raw is None and sensor.current_f is None

    async def test_non_thermostat_is_not_probed_for_thermostat_sensors(self) -> None:
        self.blocks[0] = _product(105801, 1226, 0)
        with self.assertRaisesRegex(ValueError, "identified thermostat"):
            await read_thermostat_sensors(self.stream, 1, _PEER, timeout=0.02)
        assert self.sent == [0]
