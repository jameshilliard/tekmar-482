"""Resolve a snowmelt controller's remote group and read its state after a send."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from enum import Enum, StrEnum
from typing import TYPE_CHECKING

from ._native_payloads.layouts import SnowmeltBlock as Block
from ._native_payloads.line import GROUP_IDS
from ._native_payloads.queries import NativeStatusQuery
from ._native_payloads.snowmelt import (
    SnowmeltKey as Key,
)
from ._native_payloads.snowmelt import (
    SnowmeltOperatingState,
    SnowmeltRole,
)
from .native import NativeBlockQuery, NativeMessage, require_enum
from .native_identity import NativeIdentity
from .native_snowmelt import SnowmeltConfiguration

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from .native import NativeAddress, NativeFrame
    from .native_status import NativeStatusBatch
    from .native_write import NativeBlockEditor
    from .raw import TxResult


class SnowmeltOperation(Enum):
    MELT = Block.MELT_GROUP
    STORM = Block.STORM_GROUP


class SnowmeltGroupStatus(StrEnum):
    PREVIEW = "preview"
    LINK_FAILED = "link_failed"
    EXPIRED_NOT_SENT = "expired_not_sent"
    OUTCOME_UNKNOWN = "outcome_unknown"
    NO_STATE_REPORT = "no_state_report"
    STATE_REPORTED = "state_reported"


@dataclass(frozen=True, slots=True)
class SnowmeltGroupResult:
    status: SnowmeltGroupStatus
    operation: SnowmeltOperation
    enabled: bool
    group: int
    zone: int
    message: NativeMessage
    tx_result: TxResult | None = None
    readback: NativeStatusBatch | None = None

    @property
    def observed_state(self) -> SnowmeltOperatingState | None:
        if self.readback is not None and self.readback.answered:
            pair = self.readback.snowmelt_pair(Key.OPERATING_STATE)
            return pair.operating_state if pair is not None else None
        return None

    @property
    def matches_requested_state(self) -> bool | None:
        """Compare the observed operation; this cannot establish what caused it."""
        state = self.observed_state
        if state is None:
            return None
        operation = (
            SnowmeltOperatingState.MELTING
            if self.operation is SnowmeltOperation.MELT
            else SnowmeltOperatingState.STORM
        )
        return (state is operation) == self.enabled


async def apply_snowmelt_group(
    editor: NativeBlockEditor,
    send: Callable[[NativeMessage], Awaitable[TxResult | None]],
    read_status: Callable[[NativeStatusQuery], Awaitable[NativeStatusBatch]],
    target: NativeAddress,
    operation: SnowmeltOperation,
    *,
    enabled: bool,
    expected_group: int | None,
    preview: bool,
) -> SnowmeltGroupResult:
    require_enum(operation, SnowmeltOperation)
    if (not preview or expected_group is not None) and expected_group not in GROUP_IDS:
        msg = "a snowmelt broadcast needs an explicit expected group 1..12"
        raise ValueError(msg)

    async def read(block: int) -> NativeFrame:
        return (await editor.read(NativeBlockQuery(target, block))).require_report()

    identity = NativeIdentity.from_report((await read(0)).decoded)
    if (
        not identity
        or not identity.product
        or identity.product.family != "snowmelt_control"
    ):
        msg = "snowmelt group selection requires an identified snowmelt control"
        raise ValueError(msg)
    blocks: dict[tuple[int, int | None], NativeFrame] = {
        (Block.APPLICATION_MODE, None): await read(Block.APPLICATION_MODE),
    }
    app = SnowmeltConfiguration.from_blocks(blocks).application
    if app is None or app.role is not SnowmeltRole.ZONE:
        msg = "remote melt/storm requires a reported control application"
        raise ValueError(msg)
    for block in (Block.ZONE_ID, Block.MELT_GROUP, Block.STORM_GROUP):
        blocks[block, None] = await read(block)
    setup = SnowmeltConfiguration.from_blocks(blocks)
    address = setup.primary
    if address is None:
        msg = "missing or invalid configured snowmelt zone"
        raise ValueError(msg)
    group, other = setup.melt_group, setup.storm_group
    if operation is SnowmeltOperation.STORM:
        group, other = other, group
    if (
        group is None
        or group not in GROUP_IDS
        or other not in GROUP_IDS
        or group == other
    ):
        msg = "missing, invalid or ambiguous melt/storm group assignment"
        raise ValueError(msg)
    if expected_group is not None and group != expected_group:
        msg = (
            f"configured {operation.name.lower()} group {group} "
            f"differs from expected group {expected_group}"
        )
        raise ValueError(msg)
    message = NativeMessage.setpoint_group(
        group,
        enabled=enabled,
        source=editor.source,
        priority=0,
    )
    result = SnowmeltGroupResult(
        SnowmeltGroupStatus.PREVIEW,
        operation,
        enabled,
        group,
        address.number,
        message,
    )
    if preview:
        return result
    try:
        async with asyncio.timeout(editor.timeout):
            tx = await send(message)
    except TimeoutError:
        return replace(result, status=SnowmeltGroupStatus.OUTCOME_UNKNOWN)
    if tx is None:
        return replace(result, status=SnowmeltGroupStatus.EXPIRED_NOT_SENT)
    if not tx.may_have_reached_peer:
        return replace(result, status=SnowmeltGroupStatus.LINK_FAILED, tx_result=tx)
    query = NativeStatusQuery.snowmelt(
        address.role,
        address.number,
        bytes([Key.OPERATING_STATE]),
    )
    batch = await read_status(query)
    status = (
        SnowmeltGroupStatus.STATE_REPORTED
        if batch.answered
        else SnowmeltGroupStatus.NO_STATE_REPORT
    )
    return replace(result, status=status, tx_result=tx, readback=batch)
