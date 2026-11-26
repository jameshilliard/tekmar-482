"""Verified addressed tN4 edits. Link delivery and application state are distinct."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

from ._native_payloads.climate import (
    HEATING_SETUP,
    ThermostatConfiguration,
    ThermostatMode,
)
from ._native_payloads.schedules import SETUP_BLOCK, SINGLE_WEEK, ScheduleProfile
from ._native_payloads.writes import BlockPatch, NativeField
from .native import NativeBlockQuery, NativeMessage, require_enum
from .native_identity import NativeIdentity

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

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


@dataclass(frozen=True, slots=True)
class NativeBlockEditor:
    """Callbacks borrowed only while the dispatcher owns the physical port."""

    source: NativeAddress
    read: Callable[[NativeBlockQuery], Awaitable[NativeBlockBatch]]
    send: Callable[[NativeMessage], Awaitable[TxResult]]
    timeout: float

    async def write_mode(
        self,
        target: NativeAddress,
        mode: ThermostatMode,
        *,
        preview: bool,
    ) -> NativeWriteResult:
        require_enum(mode, ThermostatMode)
        report = (await self.read(NativeBlockQuery(target, 0))).require_report()
        identity = NativeIdentity.from_report(report.decoded)
        product = identity.product if identity else None
        if identity is None or product is None or product.family != "thermostat":
            msg = "mode adjustment requires an identified thermostat"
            raise ValueError(msg)
        blocks = {}
        for b in mode.setup_dependencies(product):
            batch = await self.read(NativeBlockQuery(target, b))
            if batch.tx_result is None:
                msg = "no terminal result for thermostat setup read"
                raise TimeoutError(msg)
            if batch.reports and batch.tx_result.completed_frame:
                blocks[b] = batch.reports[0].decoded
        query = NativeBlockQuery(target, HEATING_SETUP)
        before = (await self.read(query)).require_report()
        blocks[HEATING_SETUP] = before.decoded
        config = ThermostatConfiguration.from_blocks(product, identity.z_number, blocks)
        if config.offers(mode) is not True:
            msg = f"{product.model}: {mode.name.lower()} mode not confirmed available"
            raise ValueError(msg)
        patch = BlockPatch.from_fields(
            HEATING_SETUP,
            {NativeField.MODE_SETTING: mode.value},
        )
        return await self.write(query, patch, preview=preview, before=before)

    async def write(
        self,
        query: NativeBlockQuery,
        patch: BlockPatch,
        *,
        preview: bool,
        profile: ScheduleProfile | None = None,
        before: NativeFrame | None = None,
    ) -> NativeWriteResult:
        if query.block_id in SINGLE_WEEK or query.block_id == SETUP_BLOCK:
            if profile is None:
                identity = (
                    await self.read(NativeBlockQuery(query.target, 0))
                ).require_report()
                profile = ScheduleProfile.from_identification(identity.decoded)
            patch = patch.for_schedule(profile)
        if before is None:
            before = (await self.read(query)).require_report()
        return await _write_verified_block(
            patch,
            before,
            source=self.source,
            read=lambda: self.read(query),
            send=self.send,
            timeout=self.timeout,
            preview=preview,
        )


async def _write_verified_block(
    patch: BlockPatch,
    before: NativeFrame,
    *,
    source: NativeAddress,
    read: Callable[[], Awaitable[NativeBlockBatch]],
    send: Callable[[NativeMessage], Awaitable[TxResult]],
    timeout: float,
    preview: bool,
) -> NativeWriteResult:
    payload = before.message.payload
    body = patch.apply(payload[2], payload[3:])
    proposed = NativeMessage.update_block(
        patch.block,
        body,
        source=source,
        target=before.message.source,
    )
    if patch.matches(payload[2], payload[3:]) is True:
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
    if batch.reports and batch.tx_result and batch.tx_result.completed_frame:
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
