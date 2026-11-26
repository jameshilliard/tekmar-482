import asyncio
import unittest

from tekmar_482 import (
    FailureReason,
    NativeAddress,
    NativeBlockQuery,
    NativeField,
    NativeMessage,
    NativeTargets,
    NativeTransmitError,
    NativeWriteResult,
    NativeWriteStatus,
    Packet,
    ProtocolError,
    RejectReason,
    TxResult,
    TxStatus,
    WritableBlock,
)
from tekmar_482.raw import RawDispatcher

_PEER = NativeAddress(0x12, 3)
_HOST = NativeAddress(0, 0)
_QUERY = NativeBlockQuery(_PEER, WritableBlock.HEATING_SETPOINTS)
_BODY = bytes.fromhex("2122232425")
_PATCHED = bytes.fromhex("3122232425")


class NativeWriteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tx: asyncio.Queue[bytes] = asyncio.Queue()
        self.stream = RawDispatcher(self.tx.put, 32)
        wire: asyncio.Queue[Packet] = asyncio.Queue()
        await self.enterAsyncContext(self.stream.receiving(wire.get))

    def start(
        self,
        *,
        timeout: float = 1,
        preview: bool = False,
        value: int = 0x31,
    ) -> asyncio.Task[NativeWriteResult]:
        return asyncio.create_task(
            self.stream.write_block(
                1,
                _QUERY,
                {NativeField.WAKE: value},
                source=_HOST,
                timeout=timeout,
                preview=preview,
            ),
        )

    def result(self, *status: int, port: int = 1) -> None:
        self.stream.feed(Packet(1, bytes([2, port, *status])))

    def report(self, body: bytes, *, support: int = 31, port: int = 1) -> None:
        msg = NativeMessage(0x2E7, _PEER, bytes([5, 0, support]) + body)
        self.stream.feed(Packet(1, bytes([0, port, 7, 0, 0, 0]) + msg.to_bytes()))

    async def expect_read(self, port: int = 1) -> None:
        async with asyncio.timeout(2):
            assert (
                await self.tx.get()
                == bytes([1, port]) + _QUERY.request(_HOST).to_bytes()
            )

    async def baseline(self, body: bytes = _BODY, *, support: int = 31) -> None:
        await self.expect_read()
        self.result(TxStatus.ACK)
        self.report(body, support=support)

    async def expect_update(self, body: bytes = _PATCHED) -> None:
        expected = NativeMessage.update_block(5, body, source=_HOST, target=_PEER)
        async with asyncio.timeout(2):
            assert await self.tx.get() == bytes([1, 1]) + expected.to_bytes()

    async def test_unknown_fields_multicast_and_missing_source_never_write(
        self,
    ) -> None:
        for query, field in (
            (NativeBlockQuery(_PEER, 0x15), NativeField.WAKE),
            (NativeBlockQuery(NativeTargets(0x12, 3), 5), NativeField.WAKE),
            (_QUERY, NativeField.HOURS),
        ):
            with self.subTest(query=query, field=field), self.assertRaises(ValueError):
                await self.stream.write_block(1, query, {field: 1}, source=_HOST)
        with self.assertRaises(ValueError):
            await self.stream.write_block(
                1,
                _QUERY,
                {NativeField.WAKE: 1},
                source=_HOST,
                timeout=0,
            )
        assert self.tx.empty()
        for support, body in ((0x11, b"\x01\x02"), (0x10, _BODY)):
            task = self.start()
            await self.baseline(body, support=support)
            with self.assertRaises(ValueError):
                await task
            assert self.tx.empty()
        rejected = self.start()
        await self.expect_read()
        self.result(TxStatus.REJECTED, RejectReason.INVALID_BODY)
        with self.assertRaises(NativeTransmitError):
            await rejected
        unanswered = self.start(timeout=0.03)
        await self.expect_read()
        self.result(TxStatus.ACK)
        with self.assertRaises(TimeoutError):
            await unanswered
        assert self.tx.empty()

    async def test_preview_and_unchanged_use_only_the_initial_read(self) -> None:
        for preview, body, status in (
            (True, _BODY, NativeWriteStatus.PREVIEW),
            (False, _PATCHED, NativeWriteStatus.UNCHANGED),
        ):
            task = self.start(preview=preview)
            await self.baseline(body, support=1)
            result = await task
            assert result.status is status
            assert result.before.message.payload[3:] == body
            assert result.proposed.payload[4:] == _PATCHED
            assert result.tx_result is None and result.readback is None
            assert self.tx.empty()

    async def test_new_readback_confirms_only_edited_bits_even_without_link_ack(
        self,
    ) -> None:
        task = self.start()
        await self.baseline()
        await self.expect_update()
        self.report(_BODY)  # While update is in flight: never used as readback.
        self.result(TxStatus.FAILED, FailureReason.NO_ACK)
        await self.expect_read()
        self.report(_BODY, port=2)
        self.report(bytes.fromhex("3101"), support=0x11)
        self.result(TxStatus.ACK)
        result = await task
        assert result.status is NativeWriteStatus.CONFIRMED
        assert result.tx_result == TxResult(1, TxStatus.FAILED, FailureReason.NO_ACK)
        assert (
            result.readback
            and result.readback.require_report().decoded["support_mask"] == "0x11"
        )
        assert result.before == await self.stream.read()
        assert self.tx.empty()

    async def test_actual_readback_partial_rejected_or_absent(self) -> None:
        for support, status in (
            (31, NativeWriteStatus.DIFFERENT),
            (16, NativeWriteStatus.UNCONFIRMED),
            (None, NativeWriteStatus.UNCONFIRMED),
            (0, NativeWriteStatus.UNCONFIRMED),
        ):
            task = self.start(timeout=0.05)
            await self.baseline()
            await self.expect_update()
            self.result(TxStatus.ACK)
            await self.expect_read()
            if support == 0:
                self.result(TxStatus.REJECTED, RejectReason.PIN_UNAVAILABLE)
            else:
                self.result(TxStatus.ACK)
                if support is not None:
                    self.report(_BODY, support=support)
            result = await task
            assert result.status is status
            assert result.readback and result.readback.tx_result
            if support:
                assert result.readback.require_report().message.payload[3:] == _BODY
            assert self.tx.empty()

    async def test_uncertain_write_only_verifies_and_keeps_original_outcome(
        self,
    ) -> None:
        for reason in (
            FailureReason.ECHO_MISMATCH,
            FailureReason.BAD_END,
            FailureReason.MISSED_TIMING,
            0xFF,
        ):
            with self.subTest(reason=reason):
                task = self.start()
                await self.baseline()
                await self.expect_update()
                self.result(TxStatus.FAILED, reason)
                await self.expect_read()
                self.report(_BODY)
                self.result(TxStatus.ACK)
                result = await task
                assert result.tx_result == TxResult(1, TxStatus.FAILED, reason)
                assert result.status is NativeWriteStatus.DIFFERENT and self.tx.empty()

    async def test_update_rejection_and_unknown_outcome_are_not_retried(self) -> None:
        for status, reason in (
            (TxStatus.REJECTED, RejectReason.LOCAL_ACK_ACTIVE),
            (TxStatus.FAILED, FailureReason.ARBITRATION_LOST),
        ):
            task = self.start()
            await self.baseline()
            await self.expect_update()
            self.result(status, reason)
            result = await task
            assert (
                result.status is NativeWriteStatus.LINK_FAILED and not result.readback
            )
            assert self.tx.empty()
        for cancel in (False, True):
            task = self.start(timeout=0.05)
            await self.baseline()
            await self.expect_update()
            if cancel:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            else:
                result = await task
                assert result.status is NativeWriteStatus.UNCONFIRMED
                assert result.tx_result is None and result.readback is None
            with self.assertRaisesRegex(ProtocolError, "already in progress"):
                await self.start()
            self.result(TxStatus.ACK)  # Releases the wire slot, not a new operation.
            assert self.tx.empty()

    async def test_port_ownership_extends_until_readback_and_sibling_edits_serialize(
        self,
    ) -> None:
        first = self.start()
        await self.baseline()
        await self.expect_update()
        second = asyncio.create_task(
            self.stream.write_block(
                1,
                _QUERY,
                {NativeField.AWAY: 1},
                source=_HOST,
            ),
        )
        with self.assertRaisesRegex(ProtocolError, "write in progress"):
            await self.stream.read_block_group(1, _QUERY, source=_HOST)
        with self.assertRaisesRegex(ProtocolError, "write in progress"):
            await self.stream.send(1, _QUERY.request(_HOST))
        other = asyncio.create_task(self.stream.read_block(2, _QUERY, source=_HOST))
        await self.expect_read(2)
        self.report(_BODY, port=2)
        self.result(TxStatus.ACK, port=2)
        assert (await other).port == 2
        self.result(TxStatus.ACK)
        await self.baseline(_PATCHED)
        assert (await first).status is NativeWriteStatus.CONFIRMED
        await self.baseline(_PATCHED)
        final = _PATCHED[:-1] + b"\x01"
        await self.expect_update(final)
        self.result(TxStatus.ACK)
        await self.baseline(final)
        assert (await second).status is NativeWriteStatus.CONFIRMED
        assert self.tx.empty()

    async def test_pending_unrelated_read_protects_port(self) -> None:
        task = asyncio.create_task(self.stream.read_block(1, _QUERY, source=_HOST))
        await self.expect_read()
        self.result(TxStatus.ACK)
        with self.assertRaisesRegex(ProtocolError, "already in progress"):
            await self.start()
        self.report(_BODY)
        assert (await task).port == 1
        assert self.tx.empty()
