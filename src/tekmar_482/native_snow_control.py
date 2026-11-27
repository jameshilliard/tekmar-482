"""Resolve a snowmelt controller's remote group and read its state after a send."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from enum import Enum, StrEnum
from typing import TYPE_CHECKING

from ._native_payloads.layouts import SNOW_SLAB_TARGETS
from ._native_payloads.layouts import SnowmeltBlock as Block
from ._native_payloads.line import GROUP_IDS
from ._native_payloads.queries import NativeStatusQuery
from ._native_payloads.snowmelt import (
    SnowmeltApplication,
    SnowmeltOperatingState,
    SnowmeltRole,
)
from ._native_payloads.snowmelt import (
    SnowmeltKey as Key,
)
from ._native_payloads.writes import BlockPatch, NativeField
from .native import NativeBlockQuery, NativeMessage, SnowmeltSetpoint, require_enum
from .native_identity import NativeIdentity
from .native_snowmelt import (
    SNOW_GROUP_BLOCKS,
    SNOW_GROUP_KEYS,
    SnowmeltConfiguration,
    SnowmeltGroups,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from ._native_payloads.snowmelt import SnowmeltAddress
    from .native import NativeAddress, NativeFrame
    from .native_status import NativeStatusBatch
    from .native_write import NativeBlockEditor, NativeWriteResult
    from .raw import TxResult


class SnowmeltOperation(Enum):
    MELT = Block.MELT_GROUP
    STORM = Block.STORM_GROUP


@dataclass(frozen=True, slots=True)
class SnowmeltGroupBinding:
    zone: int
    group: int

    @classmethod
    def from_setup(
        cls,
        setup: SnowmeltConfiguration,
        operation: SnowmeltOperation,
    ) -> SnowmeltGroupBinding | None:
        """Resolve an unambiguous group from a configured controller zone."""
        require_enum(operation, SnowmeltOperation)
        address = setup.primary
        if address is None or address.role is not SnowmeltRole.ZONE:
            return None
        if (groups := setup.groups) is None:
            return None
        return cls(address.number, _operation_group(groups, operation))


def _operation_group(
    groups: SnowmeltGroups,
    operation: SnowmeltOperation,
) -> int:
    return (
        groups.melt_group if operation is SnowmeltOperation.MELT else groups.storm_group
    )


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


async def _reported_application(
    editor: NativeBlockEditor,
    target: NativeAddress,
) -> tuple[NativeFrame, SnowmeltApplication]:
    identity = NativeIdentity.from_report(await editor.identification(target))
    if (
        identity is None
        or not identity.product
        or identity.product.family != "snowmelt_control"
    ):
        msg = "operation requires an identified snowmelt control"
        raise ValueError(msg)
    frame = (
        await editor.read(NativeBlockQuery(target, Block.APPLICATION_MODE))
    ).require_report()
    setup = SnowmeltConfiguration.from_blocks({(Block.APPLICATION_MODE, None): frame})
    if (app := setup.application) is None or app.role is not SnowmeltRole.ZONE:
        msg = "operation requires a reported control application"
        raise ValueError(msg)
    return frame, app


async def _reported_groups(
    editor: NativeBlockEditor,
    read_status: Callable[[NativeStatusQuery], Awaitable[NativeStatusBatch]],
    target: NativeAddress,
    address: SnowmeltAddress,
) -> SnowmeltGroups | None:
    status = await read_status(
        NativeStatusQuery.snowmelt(address.role, address.number, SNOW_GROUP_KEYS),
    )
    melt = status.snowmelt_pair(Key.MELT_GROUP)
    storm = status.snowmelt_pair(Key.STORM_GROUP)
    if status.answered:
        return SnowmeltGroups.from_values(
            melt.raw if melt is not None else None,
            storm.raw if storm is not None else None,
        )
    if status.transmit_pending:
        msg = "snowmelt group status request has no terminal transmit result"
        raise TimeoutError(msg)

    # A controller may not return both status keys. Read both blocks instead
    # of combining a partial status reply with a separate configuration read.
    blocks: dict[tuple[int, int | None], NativeFrame] = {
        (block, None): (
            await editor.read(NativeBlockQuery(target, block))
        ).require_report()
        for block in SNOW_GROUP_BLOCKS
    }
    groups = SnowmeltConfiguration.from_blocks(blocks).groups
    if groups is not None and (
        (melt is not None and melt.raw != groups.melt_group)
        or (storm is not None and storm.raw != groups.storm_group)
    ):
        msg = "snowmelt group status conflicts with addressed configuration"
        raise ValueError(msg)
    return groups


async def apply_snowmelt_group(
    editor: NativeBlockEditor,
    send: Callable[[NativeMessage], Awaitable[TxResult | None]],
    read_status: Callable[[NativeStatusQuery], Awaitable[NativeStatusBatch]],
    target: NativeAddress,
    operation: SnowmeltOperation,
    *,
    enabled: bool,
    expected_group: int | None,
    expected_zone: int | None,
    preview: bool,
) -> SnowmeltGroupResult:
    require_enum(operation, SnowmeltOperation)
    if (not preview or expected_group is not None) and expected_group not in GROUP_IDS:
        msg = "a snowmelt broadcast needs an explicit expected group 1..12"
        raise ValueError(msg)
    if (
        expected_zone is not None
        and not 1 <= expected_zone <= SnowmeltRole.ZONE.maximum
    ):
        msg = "invalid expected snowmelt zone"
        raise ValueError(msg)

    async def read(block: int) -> NativeFrame:
        return (await editor.read(NativeBlockQuery(target, block))).require_report()

    application, _ = await _reported_application(editor, target)
    zone = await read(Block.ZONE_ID)
    setup = SnowmeltConfiguration.from_blocks(
        {(Block.APPLICATION_MODE, None): application, (Block.ZONE_ID, None): zone},
    )
    address = setup.primary
    if address is None:
        msg = "missing or invalid configured snowmelt zone"
        raise ValueError(msg)
    if expected_zone is not None and address.number != expected_zone:
        msg = (
            f"configured zone {address.number} "
            f"differs from expected zone {expected_zone}"
        )
        raise ValueError(msg)
    groups = await _reported_groups(editor, read_status, target, address)
    if groups is None:
        msg = "missing, invalid or ambiguous melt/storm group assignment"
        raise ValueError(msg)
    group = _operation_group(groups, operation)
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


async def edit_snowmelt_setpoint(
    editor: NativeBlockEditor,
    target: NativeAddress,
    block: Block,
    celsius: float | None,
    *,
    expected_application: SnowmeltApplication | None,
    preview: bool,
) -> NativeWriteResult:
    """Check the peer and application before editing a stored slab target."""
    require_enum(block, Block)
    if block not in SNOW_SLAB_TARGETS:
        msg = "expected a melt, idle or storm target"
        raise ValueError(msg)
    if expected_application is not None:
        require_enum(expected_application, SnowmeltApplication)

    _, app = await _reported_application(editor, target)
    if expected_application is not None and app is not expected_application:
        msg = "configured snowmelt application changed"
        raise ValueError(msg)

    query = NativeBlockQuery(target, block)
    before = (await editor.read(query)).require_report()
    raw = SnowmeltSetpoint.from_frame(before).encode_celsius(celsius)
    patch = BlockPatch.from_fields(block, {NativeField.SLAB_TARGET: raw})
    return await editor.write(query, patch, before=before, preview=preview)
