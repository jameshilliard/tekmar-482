"""Verified addressed tN4 edits. Link delivery and application state are distinct."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

from .native import NativeMessage

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from ._native_payloads.writes import BlockPatch
    from .native import NativeAddress, NativeFrame
    from .raw import NativeBlockBatch, TxResult


class NativeWriteStatus(StrEnum):
    PREVIEW = "preview"
    UNCHANGED = "unchanged"
    CONFIRMED = "confirmed"
    DIFFERENT = "different"
    UNCONFIRMED = "unconfirmed"
    LINK_FAILED = "link_failed"


@dataclass(frozen=True, slots=True)
class NativeWriteResult:
    status: NativeWriteStatus
    before: NativeFrame
    proposed: NativeMessage
    tx_result: TxResult | None = None
    readback: NativeBlockBatch | None = None


async def write_verified_block(
    patch: BlockPatch,
    *,
    source: NativeAddress,
    read: Callable[[], Awaitable[NativeBlockBatch]],
    send: Callable[[NativeMessage], Awaitable[TxResult]],
    timeout: float,
    preview: bool,
) -> NativeWriteResult:
    before = (await read()).require_report()
    payload = before.message.payload
    body = patch.apply(payload[2], payload[3:])
    proposed = NativeMessage.update_block(
        patch.block,
        body,
        source=source,
        target=before.message.source,
    )
    if body == payload[3:]:
        return NativeWriteResult(NativeWriteStatus.UNCHANGED, before, proposed)
    if preview:
        return NativeWriteResult(NativeWriteStatus.PREVIEW, before, proposed)

    deadline = asyncio.timeout(timeout)
    try:
        async with deadline:
            result = await send(proposed)
    except TimeoutError:
        if not deadline.expired():
            raise
        return NativeWriteResult(NativeWriteStatus.UNCONFIRMED, before, proposed)
    if not result.may_have_reached_peer:
        return NativeWriteResult(
            NativeWriteStatus.LINK_FAILED,
            before,
            proposed,
            result,
        )

    # Start a separate addressed read after TX. An unsolicited update or link
    # ACK seen during the send does not verify the destination's current value.
    batch = await read()
    matches = None
    if batch.reports and batch.tx_result and batch.tx_result.may_have_reached_peer:
        payload = batch.reports[0].message.payload
        matches = patch.matches(payload[2], payload[3:])
    match matches:
        case True:
            status = NativeWriteStatus.CONFIRMED
        case False:
            status = NativeWriteStatus.DIFFERENT
        case None:
            status = NativeWriteStatus.UNCONFIRMED
    return NativeWriteResult(status, before, proposed, result, batch)
