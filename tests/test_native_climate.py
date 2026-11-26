from __future__ import annotations

import asyncio
import unittest
from typing import TYPE_CHECKING

from tekmar_482 import (
    CaptureGap,
    ClimateChange,
    NativeAddress,
    NativeFrame,
    NativeMessage,
    NativePath,
    NativeState,
    NativeWriteStatus,
    Packet,
    ThermostatMode,
    watch_climate,
)
from tekmar_482.cli import build_parser
from tekmar_482.cli_status import climate_fields
from tekmar_482.raw import RawDispatcher

if TYPE_CHECKING:
    from collections.abc import Callable

_PATH = NativePath(3, 0x35, 7)
_HOST = NativeAddress(0, 0)


def frame(
    mid: int,
    data: bytes,
    path: NativePath = _PATH,
    *,
    stamp: int = 1,
) -> NativeFrame:
    return NativeFrame(
        path.port,
        stamp,
        NativeMessage(mid, path.address, data).to_bytes(),
    )


def block(b: int, mask_and_data: bytes, path: NativePath = _PATH) -> NativeFrame:
    return frame(0x2E7, b.to_bytes(2, "little") + mask_and_data, path)


def identity(number: int = 100103, z: int = 1, path: NativePath = _PATH) -> NativeFrame:
    return block(0, b"\x3f" + number.to_bytes(3, "little") + bytes([0, 0, z]), path)


def air(flags: int, temp: int = 0x600, path: NativePath = _PATH) -> NativeFrame:
    return frame(
        0x36F,
        bytes([1, flags, 45]) + temp.to_bytes(2, "little") + bytes(3),
        path,
    )


def update(mode: int, path: NativePath = _PATH) -> NativeFrame:
    return frame(
        0x2DF,
        bytes([path.subnet, path.node, 3, 0, mode, 9, 4]),
        NativePath(path.port, 0, 0),
    )


class StateTests(unittest.TestCase):
    def test_evidence_not_echoes_or_demand(self) -> None:
        state = NativeState()
        assert state.observe(air(3)) == (_PATH,)
        assert not state.thermostats
        assert state.observe(identity())
        device = state.thermostats[0]
        assert device.air and device.air.heat and device.air.heat.calling
        assert device.setup.mode is None
        state.observe(block(3, bytes.fromhex("07 00 04 05")))
        assert state.thermostats[0].setup.mode is ThermostatMode.OFF
        assert not state.observe(update(3))
        assert state.thermostats[0].setup.mode is ThermostatMode.OFF
        assert not state.observe(block(3, bytes.fromhex("07 01 04")))
        assert state.observe(block(3, bytes.fromhex("02 05")))
        (partial,) = state.thermostats
        assert partial.setup.mode is None
        state.observe(block(3, bytes.fromhex("01 fa")))
        reading = climate_fields(state.thermostats[0])
        assert reading["mode_raw"] == 250 and reading["mode"] is None
        assert reading["air_timestamp_ms"] == 1

    def test_selectors_paths_and_identity_replacement(self) -> None:
        state = NativeState()
        other = NativePath(2, _PATH.subnet, _PATH.node)
        for p in (_PATH, other):
            state.observe(identity(path=p))
            state.observe(air(3, path=p))
        for selector in (0, 2):
            report = block(0x5A, bytes([7, selector, 1, 0]))
            state.observe(report)
            assert state.block(_PATH, 0x5A, selector) == report
        assert not state.observe(block(0x5A, bytes.fromhex("06 02 00")))
        state.observe(identity(104104))
        assert state.block(_PATH, 0x5A, 0) is None and state.air_frame(_PATH) is None
        assert state.climate(_PATH) is None and state.climate(other) is not None
        assert NativePath(2, _PATH.subnet, _PATH.node | 32) == other

    def test_gap_and_repeated_move_do_not_transfer_old_occupants(self) -> None:
        state = NativeState()
        dest = NativePath(_PATH.port, 0x46, 8)
        take = frame(0x3E7, bytes([_PATH.subnet, _PATH.node | 32]), dest)
        state.observe(identity())
        state.observe(air(3))
        assert set(state.observe(take)) == {_PATH, dest}
        state.observe(block(3, bytes.fromhex("07 03 04 05"), dest))
        state.observe(identity(path=dest))
        (fresh,) = state.thermostats
        assert fresh.setup.mode is None
        state.observe(block(3, bytes.fromhex("01 05"), dest))
        assert not state.observe(take)
        assert state.thermostats[0].setup.mode is ThermostatMode.HUMIDITY
        assert state.observe(CaptureGap(4)) == (dest,)
        assert not state.thermostats and state.dropped == 4
        state.observe(air(3, path=dest))
        state.observe(identity(path=dest))
        assert state.thermostats[0].air is None


class ClimateSessionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tx: asyncio.Queue[bytes] = asyncio.Queue()
        self.changes: asyncio.Queue[ClimateChange] = asyncio.Queue()
        self.stream = RawDispatcher(self.tx.put, 64)
        idle: asyncio.Queue[Packet] = asyncio.Queue()
        await self.enterAsyncContext(self.stream.receiving(idle.get))

    def rx(self, f: NativeFrame) -> None:
        self.stream.feed(
            Packet(
                1,
                bytes([0, f.port]) + f.timestamp_ms.to_bytes(4, "little") + f.body,
            ),
        )

    def ack(self, port: int = _PATH.port) -> None:
        self.stream.feed(Packet(1, bytes([2, port, 0])))

    async def sent(self, mid: int, b: int, path: NativePath = _PATH) -> NativeMessage:
        async with asyncio.timeout(1):
            data = await self.tx.get()
        message = NativeMessage.from_bytes(data[2:])
        assert data[:2] == bytes([1, path.port]) and message.message_id == mid
        if mid == 0x2D7:
            assert message.payload[2] == path.subnet
            assert (
                int.from_bytes(message.payload[3:7], "little")
                == path.address.request_mask()
            )
            assert message.payload[:2] == b.to_bytes(2, "little")
        else:
            assert message.payload[:4] == bytes([path.subnet, path.node]) + b.to_bytes(
                2,
                "little",
            )
        return message

    async def changed(
        self,
        predicate: Callable[[ClimateChange], bool],
    ) -> ClimateChange:
        async with asyncio.timeout(1):
            while True:
                change = await self.changes.get()
                if predicate(change):
                    return change

    async def test_change_during_read_gets_one_followup_and_no_timer_poll(self) -> None:
        task = asyncio.create_task(
            watch_climate(
                self.stream,
                [_PATH],
                on_change=self.changes.put_nowait,
                duration=0.15,
            ),
        )
        await self.sent(0x2D7, 0)
        self.rx(air(1))
        self.rx(identity())
        self.ack()
        await self.sent(0x2D7, 3)
        self.ack()
        self.rx(block(3, bytes.fromhex("07 01 09 04")))
        await self.changed(
            lambda c: (
                c.current is not None and c.current.setup.mode is ThermostatMode.HEAT
            ),
        )
        self.rx(air(1, 0x610))
        await self.changed(
            lambda c: (
                c.current is not None
                and c.current.air is not None
                and c.current.air.heat is not None
                and c.current.air.heat.temperature_raw == 0x610
            ),
        )
        assert self.tx.empty()
        self.rx(update(2))
        await self.sent(0x2D7, 3)
        for flags in (3, 1, 3):
            self.rx(air(flags))
        self.rx(update(0))
        self.ack()
        self.rx(block(3, bytes.fromhex("07 02 09 04")))
        await self.sent(0x2D7, 3)
        self.ack()
        self.rx(block(3, bytes.fromhex("07 00 09 04")))
        done = await task
        assert self.tx.empty() and done.reads == 4
        assert done.thermostats[0].setup.mode is ThermostatMode.OFF
        assert done.thermostats[0].air and done.thermostats[0].air.heat
        assert done.thermostats[0].air.heat.calling

    async def test_timeout_keeps_transmit_slot_until_actual_result(self) -> None:
        task = asyncio.create_task(
            watch_climate(self.stream, [_PATH], duration=0.12, timeout=0.01),
        )
        await self.sent(0x2D7, 0)
        self.rx(identity())
        # Identity is visible to capture, but its request still has no TxResult.
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(self.tx.get(), 0.03)
        self.ack()
        await self.sent(0x2D7, 3)
        self.ack()
        self.rx(block(3, b"\x01\x03"))
        result = await task
        assert result.thermostats[0].setup.mode is ThermostatMode.COOL
        assert result.latest_reads[0].tx_result is None

    async def test_unanswered_identity_rearms_on_traffic_not_periodic_samples(
        self,
    ) -> None:
        task = asyncio.create_task(
            watch_climate(self.stream, [_PATH], duration=0.4, timeout=0.015),
        )
        await self.sent(0x2D7, 0)
        self.ack()
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(self.tx.get(), 0.04)
        self.rx(air(1))
        await self.sent(0x2D7, 0)
        self.ack()
        await asyncio.sleep(0.04)
        self.rx(air(1, 0x611))
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(self.tx.get(), 0.04)
        self.rx(air(3))
        await self.sent(0x2D7, 0)
        self.ack()
        self.rx(identity())
        await self.sent(0x2D7, 3)
        self.ack()
        result = await task
        assert result.thermostats[0].setup.mode_raw is None
        assert result.missing_mode_reports == (_PATH,)
        assert result.reads == 4 and self.tx.empty()

    async def test_watch_shares_the_port_with_verified_mode_edit(self) -> None:
        task = asyncio.create_task(watch_climate(self.stream, [_PATH], duration=0.14))
        await self.sent(0x2D7, 0)
        self.ack()
        self.rx(identity())
        await self.sent(0x2D7, 3)
        edit = asyncio.create_task(
            self.stream.write_thermostat_mode(
                _PATH.port,
                _PATH.address,
                ThermostatMode.OFF,
                source=_HOST,
            ),
        )
        self.ack()
        self.rx(block(3, bytes.fromhex("07 01 05 02")))
        await self.sent(0x2D7, 0)
        self.ack()
        self.rx(identity())
        await self.sent(0x2D7, 3)
        self.ack()
        self.rx(block(3, bytes.fromhex("07 01 05 02")))
        msg = await self.sent(0x2DF, 3)
        assert msg.payload[4:] == bytes([0, 5, 2])
        self.rx(update(0))
        self.ack()
        await self.sent(0x2D7, 3)
        self.ack()
        self.rx(block(3, bytes.fromhex("07 01 05 02")))
        assert (await edit).status is NativeWriteStatus.DIFFERENT
        await self.sent(0x2D7, 3)
        self.ack()
        self.rx(block(3, bytes.fromhex("07 01 05 02")))
        assert (await task).thermostats[0].setup.mode is ThermostatMode.HEAT

    async def test_restricted_revision_and_missing_setup_never_write(self) -> None:
        for number, z, mode, deps in (
            (100103, 6, ThermostatMode.OFF, ()),
            (105102, 1, ThermostatMode.AUTO, (4, 0x97)),
        ):
            edit = asyncio.create_task(
                self.stream.write_thermostat_mode(
                    _PATH.port,
                    _PATH.address,
                    mode,
                    source=_HOST,
                ),
            )
            await self.sent(0x2D7, 0)
            self.ack()
            self.rx(identity(number, z))
            for b in deps:
                await self.sent(0x2D7, b)
                self.ack()
                self.rx(block(b, bytes(1)))
            await self.sent(0x2D7, 3)
            self.ack()
            self.rx(block(3, bytes.fromhex("07 01 05 02")))
            with self.assertRaisesRegex(ValueError, "not confirmed available"):
                await edit
            assert self.tx.empty()

    async def test_preview_reads_cooling_setup_and_preserves_siblings(self) -> None:
        task = asyncio.create_task(
            self.stream.write_thermostat_mode(
                _PATH.port,
                _PATH.address,
                ThermostatMode.AUTO,
                source=_HOST,
                preview=True,
            ),
        )
        for b, report in (
            (0, identity(105102)),
            (4, block(4, bytes.fromhex("01 00"))),
            (0x97, block(0x97, bytes.fromhex("03 03 02"))),
            (3, block(3, bytes.fromhex("07 01 06 02"))),
        ):
            await self.sent(0x2D7, b)
            self.ack()
            self.rx(report)
        result = await task
        assert result.status is NativeWriteStatus.PREVIEW
        assert result.proposed.payload[4:] == bytes([2, 6, 2])
        assert self.tx.empty()

    async def test_silent_port_does_not_block_another(self) -> None:
        other = NativePath(4, 0x35, 2)
        watch = asyncio.create_task(
            watch_climate(
                self.stream,
                [_PATH, other],
                on_change=self.changes.put_nowait,
            ),
        )
        async with asyncio.timeout(1):
            sent = [await self.tx.get(), await self.tx.get()]
        assert {d[1] for d in sent} == {3, 4}
        self.ack(4)
        self.rx(identity(path=other))
        await self.sent(0x2D7, 3, other)
        self.ack(4)
        self.rx(block(3, bytes.fromhex("01 04"), other))
        await self.changed(
            lambda c: (
                c.path == other
                and c.current is not None
                and c.current.setup.mode is ThermostatMode.VENT
            ),
        )
        watch.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await watch
        self.ack(3)
        await self.stream.wait_transmit_idle(3)

    async def test_disconnect_wakes_transmit_wait_and_watch(self) -> None:
        broken = RawDispatcher(self.tx.put, 8)
        rx: asyncio.Queue[Packet] = asyncio.Queue()
        with self.assertRaisesRegex(ConnectionError, "test disconnect"):
            async with broken.receiving(rx.get):
                watch = asyncio.create_task(watch_climate(broken, [_PATH]))
                await self.sent(0x2D7, 0)
                drained = asyncio.create_task(broken.wait_transmit_idle(3))
                await asyncio.sleep(0)
                broken.fail(ConnectionError("test disconnect"))
                for task in (drained, watch):
                    with self.assertRaisesRegex(ConnectionError, "test disconnect"):
                        await task

    async def test_passive_never_queries_and_filters_requested_paths(self) -> None:
        self.rx(identity(path=NativePath(2, _PATH.subnet, _PATH.node)))
        self.rx(identity())
        self.rx(block(3, bytes.fromhex("07 07 05 02")))
        result = await watch_climate(self.stream, [_PATH], source=None, duration=0.01)
        assert result.reads == 0 and self.tx.empty()
        (device,) = result.thermostats
        assert device.path == _PATH and device.setup.mode is ThermostatMode.BACKUP
        for targets in ([], [NativePath(1, 0, 0)]):
            with self.assertRaises(ValueError):
                await watch_climate(self.stream, targets)
        parsed = build_parser().parse_args(
            ["watch-climate", "--device", "1", "0x35", "7", "--tcp", "localhost:1234"],
        )
        assert parsed.device == [[1, 0x35, 7]]
