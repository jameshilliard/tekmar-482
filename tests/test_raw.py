import asyncio
import unittest

from tekmar_482 import (
    CaptureGap,
    ConnectionClosedError,
    FailureReason,
    InvalidCapture,
    NativeFrame,
    Packet,
    PortError,
    PortErrorReason,
    ProtocolError,
    RejectReason,
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
