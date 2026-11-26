import asyncio
import unittest

from tekmar_482._mode import ModeReads


class ModeReadsTest(unittest.IsolatedAsyncioTestCase):
    async def test_timeout_retry_is_bounded(self) -> None:
        await self._exercise_timeout(confirm=False)

    async def test_confirm_cancels_timeout_retry(self) -> None:
        await self._exercise_timeout(confirm=True)

    async def _exercise_timeout(self, *, confirm: bool) -> None:
        reads: list[int] = []
        retrying = asyncio.Event()
        cancelled = asyncio.Event()

        async def timeout(address: int) -> None:
            reads.append(address)
            if len(reads) == 2:
                retrying.set()
                if confirm:
                    try:
                        await asyncio.Future[None]()
                    finally:
                        cancelled.set()
            raise TimeoutError

        tracker = ModeReads(timeout)
        async with asyncio.timeout(1):
            tracker.trigger(1201, quiet_window=0)
            await retrying.wait()
            if confirm:
                tracker.confirmed(1201)
                await cancelled.wait()
            await asyncio.sleep(0)
            assert reads == [1201, 1201]
        await tracker.clear()

    async def test_triggers_follow_wire_order_until_all_writes_settle(self) -> None:
        reads: asyncio.Queue[int] = asyncio.Queue()
        tracker = ModeReads(reads.put)
        async with asyncio.timeout(1):
            with tracker.writing(1201):
                with tracker.writing(1201):
                    tracker.trigger(1201, quiet_window=0)
                    tracker.confirmed(1201)
                    tracker.trigger(1201, quiet_window=0)
                tracker.trigger(2101, quiet_window=0)
                assert await reads.get() == 2101
                assert reads.empty()
            assert await reads.get() == 1201
            assert reads.empty()
        await tracker.clear()

    async def test_disconnect_discards_triggers_from_unsettled_write(self) -> None:
        reads: asyncio.Queue[int] = asyncio.Queue()
        tracker = ModeReads(reads.put)
        with tracker.writing(1201):
            tracker.trigger(1201, quiet_window=0)
            await tracker.clear()
        tracker.trigger(2101, quiet_window=0)
        assert await asyncio.wait_for(reads.get(), 1) == 2101
        assert reads.empty()
        await tracker.clear()
