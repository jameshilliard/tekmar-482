import asyncio
import unittest

from tekmar_482 import (
    CaptureGap,
    ConnectionClosedError,
    FailureReason,
    InvalidCapture,
    NativeAddress,
    NativeBlockQuery,
    NativeFrame,
    NativeMessage,
    NativeTargets,
    NativeTransmitError,
    Packet,
    PortError,
    PortErrorReason,
    ProtocolError,
    RejectReason,
    SnowmeltBlock,
    TxResult,
    TxStatus,
)
from tekmar_482.raw import RawDispatcher

_BODY = bytes.fromhex("12300000")


class RawTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.rx: asyncio.Queue[Packet] = asyncio.Queue()
        self.tx: asyncio.Queue[bytes] = asyncio.Queue()
        self.stream = RawDispatcher(self.tx.put, 2)

    def deliver(self, *data: int) -> None:
        self.rx.put_nowait(Packet(1, bytes(data)))

    async def test_group_accepts_each_selected_node_once_even_before_tx_result(
        self,
    ) -> None:
        host = NativeAddress(0, 0)
        query = NativeBlockQuery(NativeTargets(0x24, 0b101), 0x44, 129)

        def report(port: int, node: int, value: bytes) -> None:
            body = NativeMessage(0x2E7, NativeAddress(0x24, node), b"\x44\0" + value)
            self.deliver(0, port, 0, 0, 0, 0, *body.to_bytes())

        async with self.stream.receiving(self.rx.get), asyncio.timeout(2):
            with self.assertRaisesRegex(ValueError, "multiple nodes"):
                await self.stream.read_block(3, query, source=host)
            task = asyncio.create_task(
                self.stream.read_block_group(3, query, source=host),
            )
            assert await self.tx.get() == bytes((1, 3)) + query.request(host).to_bytes()
            report(3, 0, b"\x07\x81\x01\0")
            report(3, 2, b"\x07\x81\x01\0")
            report(4, 3, b"\x07\x81\x01\0")
            report(3, 1, b"\x07\x81\x04\0")
            report(3, 33, b"\x07\x81\x05\0")
            report(3, 3, b"\x07\x80\x01\0")
            report(3, 3, b"\0")
            report(3, 35, b"\x07\x81\x06\0")
            self.deliver(2, 3, TxStatus.FAILED, FailureReason.NO_ACK)
            batch = await task
            assert batch.tx_result == TxResult(3, TxStatus.FAILED, FailureReason.NO_ACK)
            assert not batch.missing_nodes
            assert [(f.node, f.decoded["pump_hours"]) for f in batch.reports] == [
                (1, 4),
                (35, 6),
            ]
            assert await self.stream.read() == CaptureGap(6)
            assert self.tx.empty()

    async def test_group_deadline_distinguishes_missing_peer_from_missing_tx(
        self,
    ) -> None:
        host = NativeAddress(0, 0)
        query = NativeBlockQuery(NativeTargets(8, 3), 0x15)
        async with self.stream.receiving(self.rx.get), asyncio.timeout(2):
            for lost_tx in (False, True):
                task = asyncio.create_task(
                    self.stream.read_block_group(0, query, source=host, timeout=0.025),
                )
                await self.tx.get()
                if not lost_tx:
                    self.deliver(2, 0, TxStatus.ACK)
                msg = NativeMessage(0x2E7, NativeAddress(8, 2), bytes.fromhex("150000"))
                self.deliver(0, 0, 8, 0, 0, 0, *msg.to_bytes())
                batch = await task
                assert batch.tx_result == (
                    None if lost_tx else TxResult(0, TxStatus.ACK)
                )
                assert batch.missing_nodes == (1,)
                assert batch.reports[0].decoded["support_mask"] == "0x00"
            with self.assertRaisesRegex(ProtocolError, "send already in progress"):
                await self.stream.send(0, query.request(host))
            self.stream.feed(Packet(1, bytes((2, 0, TxStatus.ACK))))
            self.stream.check_drained()

    async def test_block_reports_are_copied_not_consumed_or_taken_from_backlog(
        self,
    ) -> None:
        peer, host = NativeAddress(0x12, 3), NativeAddress(0, 0)
        query = NativeBlockQuery(peer, SnowmeltBlock.SNOW_ICE_SENSOR_TYPE, 0)
        body = NativeMessage(0x2E7, peer, bytes.fromhex("7a00030003")).to_bytes()
        async with self.stream.receiving(self.rx.get), asyncio.timeout(2):
            self.stream.feed(Packet(1, bytes((0, 1, 0, 0, 0, 0)) + body))
            first = asyncio.create_task(self.stream.read_block(1, query, source=host))
            assert await self.tx.get() == bytes((1, 1)) + query.request(host).to_bytes()
            with self.assertRaisesRegex(ProtocolError, "native read already"):
                await self.stream.read_block(1, query, source=host)
            other = asyncio.create_task(self.stream.read_block(2, query, source=host))
            await self.tx.get()
            self.deliver(2, 1, 0)
            self.deliver(0, 2, 1, 0, 0, 0, *body)
            # No link ACK; an actual matching report still counts.
            self.deliver(2, 2, 2, 1)
            assert await other == NativeFrame(2, 1, body)
            assert not first.done()
            self.deliver(0, 1, 2, 0, 0, 0, *body[:-2], 1, 4)  # Different sensor.
            self.deliver(0, 1, 3, 0, 0, 0, *body)
            assert await first == NativeFrame(1, 3, body)
            assert await self.stream.read() == CaptureGap(2)
            wrong = await self.stream.read()
            assert isinstance(wrong, NativeFrame) and wrong.decoded["sensor_id"] == 1
            assert await self.stream.read() == NativeFrame(1, 3, body)

    async def test_block_timeout_and_rejection_leave_no_report_waiter(self) -> None:
        host = NativeAddress(0, 0)
        query = NativeBlockQuery(NativeAddress(0, 4), SnowmeltBlock.MELT_SETPOINT)
        async with self.stream.receiving(self.rx.get), asyncio.timeout(2):
            task = asyncio.create_task(self.stream.read_block(0, query, source=host))
            await self.tx.get()
            self.deliver(2, 0, 0)
            self.deliver(3, 255, 2)
            await self.stream.read()  # TxResult has already been delivered.
            assert not task.done()  # ACK alone is not a block report.
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            for status, reason in (
                (TxStatus.REJECTED, RejectReason.INVALID_BODY),
                (TxStatus.FAILED, FailureReason.ARBITRATION_LOST),
                (TxStatus.FAILED, FailureReason.BAD_END),
                (TxStatus.FAILED, FailureReason.ECHO_MISMATCH),
                (TxStatus.FAILED, FailureReason.MISSED_TIMING),
                (TxStatus.FAILED, 0xFF),
            ):
                task = asyncio.create_task(
                    self.stream.read_block(0, query, source=host),
                )
                await self.tx.get()
                peer = NativeAddress(0, 4)
                report = NativeMessage(0x2E7, peer, bytes([query.block_id, 0, 1, 5]))
                self.deliver(0, 0, 0, 0, 0, 0, *report.to_bytes())
                self.deliver(2, 0, status, reason)
                with self.assertRaises(NativeTransmitError) as ctx:
                    await task
                assert ctx.exception.result == TxResult(0, status, reason)
                assert isinstance(await self.stream.read(), NativeFrame)
            hanging = asyncio.create_task(self.stream.read_block(0, query, source=host))
            await self.tx.get()
            self.deliver(2, 0, 0)
            self.deliver(3, 255, 2)
            await self.stream.read()
        with self.assertRaises(ConnectionClosedError):
            await hanging

    async def test_independent_completions_and_nonterminal_diagnostics(self) -> None:
        async with self.stream.receiving(self.rx.get), asyncio.timeout(2):
            first = asyncio.create_task(self.stream.send(1, _BODY))
            assert await self.tx.get() == bytes((1, 1)) + _BODY
            other = asyncio.create_task(self.stream.send(2, _BODY))
            assert await self.tx.get() == bytes((1, 2)) + _BODY
            self.deliver(3, 1, 1)
            self.deliver(2, 2, 2, 3)
            assert await other == TxResult(
                2,
                TxStatus.FAILED,
                FailureReason.ECHO_MISMATCH,
            )
            assert not first.done()
            assert await self.stream.read() == PortError(
                1,
                PortErrorReason.SEND_IN_PROGRESS,
            )
            with self.assertRaisesRegex(ProtocolError, "already in progress"):
                await self.stream.send(1, _BODY)
            self.deliver(2, 1, 0)
            assert await first == TxResult(1, TxStatus.ACK)
            for code, why in ((2, RejectReason.PIN_UNAVAILABLE), (255, 255)):
                pending = asyncio.create_task(self.stream.send(1, _BODY))
                assert await self.tx.get() == bytes((1, 1)) + _BODY
                self.deliver(2, 1, 1, code)
                assert await pending == TxResult(1, TxStatus.REJECTED, why)
            assert self.tx.empty()

    async def test_cancelled_send_reserves_port_until_actual_result(self) -> None:
        drain = asyncio.Event()

        async def write(data: bytes) -> None:
            self.tx.put_nowait(data)
            await drain.wait()

        for cancel_during_drain in (True, False):
            with self.subTest(cancel_during_drain=cancel_during_drain):
                if not cancel_during_drain:
                    drain.set()
                stream = RawDispatcher(write, 1)
                async with stream.receiving(self.rx.get), asyncio.timeout(2):
                    send = asyncio.create_task(stream.send(0, _BODY))
                    await self.tx.get()
                    send.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await send
                    with self.assertRaisesRegex(ProtocolError, "already in progress"):
                        await stream.send(0, _BODY)
                    self.deliver(2, 0, 0)
                    self.deliver(3, 255, 2)
                    assert await stream.read() == PortError(
                        255,
                        PortErrorReason.INVALID_PORT,
                    )
                    drain.set()
                    retry = asyncio.create_task(stream.send(0, _BODY))
                    await self.tx.get()
                    self.deliver(2, 0, 2, 1)
                    assert await retry == TxResult(
                        0,
                        TxStatus.FAILED,
                        FailureReason.NO_ACK,
                    )
                    assert self.tx.empty()

    async def test_capture_overflow_cannot_block_transmit_results(self) -> None:
        async with self.stream.receiving(self.rx.get), asyncio.timeout(2):
            pending = asyncio.create_task(self.stream.send(4, _BODY))
            await self.tx.get()
            for tick in range(3):
                self.deliver(0, 0, tick, 0, 0, 0, *_BODY)
            self.deliver(4, 0, 4, 0, 0, 0, 2, 3, 0, 0xA0)
            self.deliver(2, 4, 0)
            assert await pending == TxResult(4, TxStatus.ACK)
            assert await self.stream.read() == CaptureGap(2)
            assert await self.stream.read() == NativeFrame(0, 2, _BODY)
            assert await self.stream.read() == InvalidCapture(0, 4, 2, 3, b"\xa0")

    async def test_disconnect_or_bad_result_wakes_all_waiters(self) -> None:
        async def receive() -> Packet:
            pkt = await self.rx.get()
            if not pkt.data:
                msg = "link disconnected"
                raise ConnectionClosedError(msg)
            return pkt

        for record, error in (
            ((), ConnectionClosedError),
            ((2, 0, 0, 9), ProtocolError),
        ):
            with self.subTest(record=record), self.assertRaises(error):
                stream = RawDispatcher(self.tx.put, 1)
                async with stream.receiving(receive), asyncio.timeout(2):
                    sends = [asyncio.create_task(stream.send(p, _BODY)) for p in (0, 1)]
                    await self.tx.get()
                    await self.tx.get()
                    reader = asyncio.create_task(stream.read())
                    self.deliver(*record)
                    for task in [*sends, reader]:
                        with self.assertRaises(error):
                            await task
