import asyncio
import unittest

from tekmar_482 import (
    FailureReason,
    NativeAddress,
    NativeBlockQuery,
    NativeFrame,
    NativeMessage,
    NativeStatusQuery,
    Packet,
    ProtocolError,
    RejectReason,
    SnowmeltRole,
    StatusRequest,
    TekmarError,
    TxStatus,
    read_snowmelt_status,
)
from tekmar_482.raw import RawDispatcher

_HOST = NativeAddress(0, 0)
_PEER = NativeAddress(0x23, 6)
_SNOW = NativeStatusQuery.snowmelt(SnowmeltRole.ZONE, 3, bytes.fromhex("23221922"))
_CLOCK = NativeStatusQuery(StatusRequest.CLOCK)


class NativeStatusTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tx: asyncio.Queue[bytes] = asyncio.Queue()
        rx: asyncio.Queue[Packet] = asyncio.Queue()
        self.stream = RawDispatcher(self.tx.put, 32)
        await self.enterAsyncContext(self.stream.receiving(rx.get))

    async def sent(self) -> NativeMessage:
        async with asyncio.timeout(2):
            data = await self.tx.get()
        assert data[:2] == b"\x01\x02"
        return NativeMessage.from_bytes(data[2:])

    def result(self, *outcome: int) -> None:
        self.stream.feed(Packet(1, bytes([2, 2, *outcome])))

    def capture(
        self,
        mid: int,
        payload: str,
        *,
        port: int = 2,
        peer: NativeAddress = _PEER,
    ) -> NativeFrame:
        body = NativeMessage(mid, peer, bytes.fromhex(payload)).to_bytes()
        self.stream.feed(Packet(1, bytes([0, port, 9, 0, 0, 0]) + body))
        return NativeFrame(port, 9, body)

    async def test_snowmelt_collects_distinct_keys_from_one_reporting_peer(
        self,
    ) -> None:
        stale = self.capture(0x17E, "19 03 230001 220002 190003")
        task = asyncio.create_task(
            self.stream.read_status(2, _SNOW, source=_HOST, peer=_PEER),
        )
        assert (await self.sent()).to_bytes() == bytes.fromhex("17f00006 110423221922")
        self.capture(0x17E, "19 01 230001", port=1)
        self.capture(0x17E, "19 01 230001", peer=NativeAddress(_PEER.subnet, 5))
        self.capture(
            0x17E,
            "11 01 230001",
        )  # Zero-based request address is not the report address.
        self.capture(0x17E, "1a 01 230001")  # Same number on a sensor, not the zone.
        self.capture(0x17E, "19 02 230001")  # Truncated declared pair count.
        self.capture(0x17E, "19 01 990001 230001")  # Extension is not a pair.
        first = self.capture(0x17E, "19 02 220101 220201")
        self.result(TxStatus.FAILED, FailureReason.NO_ACK)
        self.capture(0x17E, "19 01 220301")  # Repeats a key already received.
        last = self.capture(
            0x17E,
            "19 02 190001 230101",
            peer=NativeAddress(_PEER.subnet, 38),
        )
        batch = await task
        assert batch.answered and batch.complete and not batch.missing_keys
        assert batch.reports == (first, last) and batch.expected_count == 3
        assert batch.covered == {0x19, 0x22, 0x23}
        assert batch.tx_result and batch.tx_result.reason is FailureReason.NO_ACK
        assert (
            first.decoded["pair0_slab_temperature_raw"]
            != first.decoded["pair1_slab_temperature_raw"]
        )
        assert await self.stream.read() == stale

    async def test_partial_and_missing_tx_do_not_claim_a_complete_read(self) -> None:
        for terminal in (True, False):
            task = asyncio.create_task(
                self.stream.read_status(2, _SNOW, source=_HOST, timeout=0.03),
            )
            await self.sent()
            self.capture(0x17E, "19 01 220301")
            if terminal:
                self.result(TxStatus.ACK)
            batch = await task
            assert not batch.answered and not batch.complete
            assert batch.missing_keys == (0x23, 0x19)
            assert (batch.tx_result is not None) is terminal
        with self.assertRaisesRegex(ProtocolError, "send already in progress"):
            await self.stream.read_status(2, _CLOCK, source=_HOST)
        self.result(TxStatus.ACK)
        self.stream.check_drained()

    async def test_io_discovery_uses_channel_counts_from_one_source(self) -> None:
        query = NativeStatusQuery.io(0xA5, 0x81)
        task = asyncio.create_task(self.stream.read_status(2, query, source=_HOST))
        assert (await self.sent()).to_bytes() == bytes.fromhex("1f600002a581")
        self.result(TxStatus.ACK)
        first = self.capture(0x1F5, "a581 0702 414243444546 010200")
        self.capture(0x1F5, "a581 0702 414243444546 010300")
        self.capture(
            0x1F5,
            "a581 1002 414243444546 010400",
            peer=NativeAddress(0x23, 7),
        )
        self.capture(0x1F5, "a582 1002 414243444546 010500")
        last = self.capture(0x1F5, "a581 1002 414243444546 010600")
        batch = await task
        assert batch.answered and batch.covered == {7, 16}
        assert batch.reports == (first, last) and batch.peer == _PEER
        task = asyncio.create_task(
            self.stream.read_status(2, query, source=_HOST, timeout=0.03),
        )
        await self.sent()
        self.result(TxStatus.ACK)
        self.capture(0x1F5, "a581 0704 414243444546 010200")
        self.capture(0x1F5, "a581 1002 414243444546 010600")
        conflict = await task
        assert not conflict.complete and conflict.expected_count == 0

    async def test_reject_cancel_and_conflicting_port_use(self) -> None:
        for cancel in (True, False):
            task = asyncio.create_task(self.stream.read_status(2, _CLOCK, source=_HOST))
            assert (await self.sent()).to_bytes() == bytes.fromhex("31700000")
            with self.assertRaisesRegex(ProtocolError, "native read already"):
                await self.stream.read_block(
                    2,
                    NativeBlockQuery(_PEER, 0),
                    source=_HOST,
                )
            with self.assertRaisesRegex(ProtocolError, "native read already"):
                await self.stream.send(2, NativeMessage.request_clock(source=_HOST))
            if cancel:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                with self.assertRaisesRegex(ProtocolError, "send already in progress"):
                    await self.stream.read_status(2, _CLOCK, source=_HOST)
                self.result(TxStatus.ACK)
            else:
                self.result(TxStatus.REJECTED, RejectReason.PIN_UNAVAILABLE)
                batch = await task
                assert not batch.answered and not batch.reports
        with self.assertRaisesRegex(ValueError, "reporting peer"):
            await self.stream.read_status(
                2,
                NativeStatusQuery.humidity(2, 1),
                source=_HOST,
            )
        assert self.tx.empty()

    async def test_snowmelt_resolution_reads_only_the_model_specific_number(
        self,
    ) -> None:
        for app, id_block, addr in ((1, 0xC0, 0x09), (4, 0xC1, 0x0A)):
            call = asyncio.create_task(
                read_snowmelt_status(self.stream, 2, _PEER, source=_HOST, keys=b"\x22"),
            )
            for block, payload in (
                (0xD0, f"d00001{app:02x}"),
                (id_block, f"{id_block:02x}000102"),
            ):
                assert await self.sent() == NativeBlockQuery(_PEER, block).request(
                    _HOST,
                )
                self.result(TxStatus.ACK)
                self.capture(0x2E7, payload)
            assert (await self.sent()).to_bytes() == bytes.fromhex(
                f"17f00003 {addr:02x}0122",
            )
            self.result(TxStatus.ACK)
            self.capture(0x17E, f"{addr + 8:02x} 01 220002")
            assert (await call).answered
        call = asyncio.create_task(
            read_snowmelt_status(self.stream, 2, _PEER, source=_HOST),
        )
        await self.sent()
        self.result(TxStatus.ACK)
        self.capture(0x2E7, "d00000")
        with self.assertRaisesRegex(TekmarError, "did not report application_mode"):
            await call
        assert self.tx.empty()
