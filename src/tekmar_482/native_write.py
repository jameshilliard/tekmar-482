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
from .native_identity import NativeIdentity, check_identity

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from .native import NativeAddress, NativeFrame, NativeTargets
    from .raw import NativeBlockBatch, TxResult


class NativeWriteStatus(StrEnum):
    PREVIEW = "preview"
    UNCHANGED = "unchanged"
    CONFIRMED = "confirmed"
    DIFFERENT = "different"
    UNCONFIRMED = "unconfirmed"
    LINK_FAILED = "link_failed"
    BASELINE_CHANGED = "baseline_changed"
    TARGET_MOVED = "target_moved"
    NOT_SENT = "not_sent"


@dataclass(frozen=True, slots=True)
class NativeWriteResult:
    status: NativeWriteStatus
    before: NativeFrame
    proposed: NativeMessage
    tx_result: TxResult | None = None
    readback: NativeBlockBatch | None = None
    guard_result: TxResult | None = None


@dataclass(frozen=True, slots=True)
class BlockDelivery:
    tx: TxResult | None
    guard: TxResult | None = None
    rejection: NativeWriteStatus | None = None


@dataclass(frozen=True, slots=True)
class NativeBlockEditor:
    """Callbacks borrowed only while the dispatcher owns the physical port."""

    source: NativeAddress
    read: Callable[[NativeBlockQuery], Awaitable[NativeBlockBatch]]
    update: Callable[
        [NativeBlockQuery, NativeFrame, NativeMessage],
        Awaitable[BlockDelivery],
    ]
    timeout: float
    expected_identity: NativeIdentity | None = None

    async def identification(
        self,
        target: NativeAddress | NativeTargets,
    ) -> dict[str, str | int | float]:
        report = (await self.read(NativeBlockQuery(target, 0))).require_report()
        fields = report.decoded
        check_identity(self.expected_identity, fields)
        return fields

    async def write_mode(
        self,
        target: NativeAddress,
        mode: ThermostatMode,
        *,
        preview: bool,
    ) -> NativeWriteResult:
        require_enum(mode, ThermostatMode)
        identity = NativeIdentity.from_report(await self.identification(target))
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
        is_schedule = query.block_id in SINGLE_WEEK or query.block_id == SETUP_BLOCK
        if is_schedule:
            if profile is None:
                fields = await self.identification(query.target)
                profile = ScheduleProfile.from_identification(fields)
            patch = patch.for_schedule(profile)
        elif before is None and self.expected_identity is not None:
            # Typed edits identified the target before reading its settings.
            await self.identification(query.target)
        if before is None:
            before = (await self.read(query)).require_report()
        return await _write_verified_block(
            patch,
            before,
            source=self.source,
            read=lambda: self.read(query),
            send=lambda message: self.update(query, before, message),
            timeout=self.timeout,
            preview=preview,
        )


async def _write_verified_block(
    patch: BlockPatch,
    before: NativeFrame,
    *,
    source: NativeAddress,
    read: Callable[[], Awaitable[NativeBlockBatch]],
    send: Callable[[NativeMessage], Awaitable[BlockDelivery]],
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

    try:
        async with asyncio.timeout(timeout):
            delivery = await send(proposed)
    except TimeoutError:
        # A missing UART admission or terminal cannot prove non-delivery.
        return NativeWriteResult(NativeWriteStatus.UNCONFIRMED, before, proposed)
    result = delivery.tx
    if result is None:
        return NativeWriteResult(
            delivery.rejection or NativeWriteStatus.NOT_SENT,
            before,
            proposed,
            guard_result=delivery.guard,
        )
    if not result.may_have_reached_peer:
        return NativeWriteResult(
            NativeWriteStatus.LINK_FAILED,
            before,
            proposed,
            result,
            guard_result=delivery.guard,
        )

    # Start a separate addressed read after TX. An unsolicited update or link
    # ACK seen during the send does not verify the destination's current value.
    try:
        batch = await read()
    except TimeoutError:
        return NativeWriteResult(
            NativeWriteStatus.UNCONFIRMED,
            before,
            proposed,
            result,
            guard_result=delivery.guard,
        )
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
    return NativeWriteResult(status, before, proposed, result, batch, delivery.guard)
