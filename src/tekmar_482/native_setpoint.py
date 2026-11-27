"""Source-scoped operating samples and stored configuration of setpoint controls."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from math import isfinite
from typing import TYPE_CHECKING

from ._native_payloads.codec import fahrenheit
from ._native_payloads.line import SetpointApplication, SetpointAssignment
from ._native_payloads.setpoint import (
    APPLICATION_SETUP,
    OPERATION_SETUP,
    OVERRIDE_TIME,
    TARGET_BLOCKS,
    SetpointTargets,
)
from ._native_payloads.writes import BlockPatch, NativeField
from .native import NativeBlockQuery, require_enum
from .native_identity import NativeIdentity
from .units import celsius_to_degh, fahrenheit_to_celsius, fahrenheit_to_degh

if TYPE_CHECKING:
    from ._native_payloads.products import NativeProduct
    from ._native_payloads.setpoint import SetpointReadings, SetpointStatus
    from .native import BlockKey, NativeAddress, NativeFrame, NativeMessage
    from .native_state import NativePath
    from .native_write import NativeBlockEditor, NativeWriteResult

SETPOINT_BLOCKS = frozenset(
    (APPLICATION_SETUP, OPERATION_SETUP, OVERRIDE_TIME, *TARGET_BLOCKS),
)

# The 161/162 manuals give the target minima and application maximum ranges
# in Fahrenheit. The reported maximum can restrict the range further.
_TARGET_BOUNDS = {
    app: (fahrenheit_to_degh(low), fahrenheit_to_degh(high))
    for app, low, high in (
        (SetpointApplication.SETPOINT_ON_OFF, -4, 239),
        (SetpointApplication.SETPOINT_PWM, -4, 239),
        (SetpointApplication.HOT_TUB, 40, 104),
        (SetpointApplication.DHW, 40, 200),
        (SetpointApplication.FLOOR, 40, 110),
        (SetpointApplication.POOL, 40, 120),
    )
}
# SW 11 SetpointControl.adjustment_for explicitly names this Off encoding.
_HOT_TUB_OFF = 1240


class SetpointSlot(Enum):
    """Physical target fields; Wake and Sleep require a separate table."""

    WAKE_OCCUPIED = NativeField.WAKE_OCCUPIED
    UNOCCUPIED_SLEEP = NativeField.UNOCCUPIED_SLEEP
    AWAY = NativeField.AWAY
    WAKE = NativeField.WAKE
    SLEEP = NativeField.SLEEP

    @property
    def secondary(self) -> bool:
        return self in (SetpointSlot.WAKE, SetpointSlot.SLEEP)

    def raw_value(self, targets: SetpointTargets) -> int | None:
        match self:
            case SetpointSlot.WAKE_OCCUPIED:
                return targets.wake_occupied_raw
            case SetpointSlot.UNOCCUPIED_SLEEP:
                return targets.unoccupied_sleep_raw
            case SetpointSlot.AWAY:
                return targets.away_raw
            case SetpointSlot.WAKE:
                return targets.wake_raw
            case SetpointSlot.SLEEP:
                return targets.sleep_raw


@dataclass(frozen=True, slots=True)
class StoredSetpointTarget:
    """One supported word, including Off and other non-temperature values."""

    application: SetpointApplication
    raw: int

    @property
    def is_off(self) -> bool:
        return (
            self.application is SetpointApplication.HOT_TUB and self.raw == _HOT_TUB_OFF
        )

    @property
    def celsius(self) -> float | None:
        low, high = _TARGET_BOUNDS[self.application]
        if low <= self.raw <= high and (value := fahrenheit(self.raw)) is not None:
            return fahrenheit_to_celsius(value)
        return None


@dataclass(frozen=True, slots=True)
class SetpointTargetChange:
    """Edit one stored target in Celsius for an explicitly selected application."""

    application: SetpointApplication
    slot: SetpointSlot
    celsius: float | None

    def __post_init__(self) -> None:
        require_enum(self.application, SetpointApplication)
        require_enum(self.slot, SetpointSlot)
        if self.celsius is None:
            if self.application is not SetpointApplication.HOT_TUB:
                msg = "the native Off word is only documented for hot tub targets"
                raise ValueError(msg)
        elif isinstance(self.celsius, bool) or not isfinite(self.celsius):
            msg = "expected a finite setpoint temperature"
            raise ValueError(msg)

    def wire_value(self) -> int:
        if self.celsius is None:
            return _HOT_TUB_OFF
        raw = celsius_to_degh(self.celsius)
        low, high = _TARGET_BOUNDS[self.application]
        if not low <= raw <= high:
            msg = "temperature outside the application's target range"
            raise ValueError(msg)
        return raw


async def edit_setpoint_target(
    editor: NativeBlockEditor,
    target: NativeAddress,
    change: SetpointTargetChange,
    *,
    preview: bool,
) -> NativeWriteResult:
    raw = change.wire_value()
    device = NativeIdentity.from_report(await editor.identification(target))
    if (
        device is None
        or not (product := device.product)
        or product.family != "setpoint_control"
    ):
        msg = "target adjustment requires an identified setpoint controller"
        raise ValueError(msg)
    if product.model == "161" and change.slot is not SetpointSlot.WAKE_OCCUPIED:
        msg = "the 161 only has an adjustable occupied target"
        raise ValueError(msg)
    setup = (
        await editor.read(NativeBlockQuery(target, APPLICATION_SETUP))
    ).require_report()
    app = SetpointAssignment.from_fields(setup.decoded).application
    if app is not change.application:
        msg = "the setpoint controller did not confirm the selected application"
        raise ValueError(msg)
    if app.target_block not in product.candidate_blocks:
        msg = f"{product.model}: application not available"
        raise ValueError(msg)

    query = NativeBlockQuery(target, app.target_block)
    before = (await editor.read(query)).require_report()
    maximum = SetpointTargets.from_reports(before.decoded, {}).maximum_raw
    if (
        change.celsius is not None
        and maximum is not None
        and raw > maximum
        and fahrenheit(maximum) is not None
    ):
        msg = "target exceeds the reported maximum"
        raise ValueError(msg)
    if change.slot.secondary:
        query = NativeBlockQuery(target, app.target_block + 1)
        before = (await editor.read(query)).require_report()
    patch = BlockPatch.from_fields(query.block_id, {change.slot.value: raw})
    return await editor.write(query, patch, before=before, preview=preview)


@dataclass(frozen=True, slots=True)
class NativeSetpointControl:
    path: NativePath
    identity: NativeIdentity | None
    assignment: SetpointAssignment
    application_setup: NativeMessage | None
    operation_setup: NativeMessage | None
    targets: SetpointTargets | None
    manual_override_time_raw: int | None
    status: SetpointStatus | None
    sample_application: SetpointApplication | None
    status_frame: NativeFrame | None = field(compare=False)

    @property
    def readings(self) -> SetpointReadings | None:
        if self.status is None or self.sample_application is None:
            return None
        return self.status.readings(self.sample_application)

    def target(self, slot: SetpointSlot) -> StoredSetpointTarget | None:
        """Return the stored field, never an alias or the live target."""
        app, targets = self.assignment.application, self.targets
        if app is None or targets is None or (raw := slot.raw_value(targets)) is None:
            return None
        return StoredSetpointTarget(app, raw)


@dataclass(slots=True)
class SetpointBook:
    reports: dict[int, NativeFrame] = field(default_factory=dict)
    revisions: dict[int, int] = field(default_factory=dict)
    confirmed_tables: set[int] = field(default_factory=set)
    status: SetpointStatus | None = None
    frame: NativeFrame | None = None
    sample_application: SetpointApplication | None = None

    @property
    def assignment(self) -> SetpointAssignment:
        setup = self.reports.get(APPLICATION_SETUP)
        return SetpointAssignment.from_fields(setup.decoded if setup else {})

    def observe(
        self,
        sample: SetpointStatus,
        frame: NativeFrame,
        product: NativeProduct | None,
    ) -> bool:
        app = (
            self.assignment.application
            if product and product.family == "setpoint_control"
            else None
        )
        changed = sample != self.status or app != self.sample_application
        self.status, self.frame, self.sample_application = sample, frame, app
        return changed

    def _discard(self, block: int) -> None:
        self.reports.pop(block, None)
        self.revisions[block] = self.revisions.get(block, 0) + 1

    def _new_application(self) -> None:
        self.sample_application = None
        self.confirmed_tables.clear()
        for block in TARGET_BLOCKS:
            self._discard(block)

    def report(self, block: int, frame: NativeFrame) -> bool:
        before = self.reports.get(block)
        if (
            block == APPLICATION_SETUP
            and before is not None
            and (
                self.assignment.application_raw
                != SetpointAssignment.from_fields(frame.decoded).application_raw
            )
        ):
            self._new_application()
        if block in TARGET_BLOCKS and frame.decoded.keys() & {
            "wake_occupied_raw",
            "unoccupied_sleep_raw",
            "away_raw",
            "maximum_raw",
            "wake_raw",
            "sleep_raw",
        }:
            self.confirmed_tables.add(block)
        self.reports[block] = frame
        return before is None or before.message.payload != frame.message.payload

    def invalidate(self, block: int) -> None:
        self._discard(block)
        if block == APPLICATION_SETUP:
            self._new_application()

    def missing(self, product: NativeProduct | None) -> dict[BlockKey, int]:
        if not product or product.family != "setpoint_control":
            return {}
        required = {APPLICATION_SETUP, OPERATION_SETUP, OVERRIDE_TIME}
        if (app := self.assignment.application) is not None:
            for block in (app.target_block, app.target_block + 1):
                if block in product.candidate_blocks or block in self.confirmed_tables:
                    required.add(block)
        return {
            (b, None): self.revisions.get(b, 0)
            for b in sorted(required - self.reports.keys())
        }

    def snapshot(
        self,
        path: NativePath,
        identity: NativeIdentity | None,
    ) -> NativeSetpointControl | None:
        product = identity.product if identity else None
        identified = product is not None and product.family == "setpoint_control"
        if not identified and self.status is None:
            return None
        assignment = self.assignment if identified else SetpointAssignment(None, None)
        app = assignment.application
        setup = self.reports.get(APPLICATION_SETUP) if identified else None
        operation = self.reports.get(OPERATION_SETUP) if identified else None
        timer = self.reports.get(OVERRIDE_TIME) if identified else None
        override = timer.decoded.get("manual_override_time") if timer else None
        first = self.reports.get(app.target_block) if app is not None else None
        second = self.reports.get(app.target_block + 1) if app is not None else None
        return NativeSetpointControl(
            path,
            identity,
            assignment,
            setup.message if setup else None,
            operation.message if operation else None,
            SetpointTargets.from_reports(
                first.decoded if first else {},
                second.decoded if second else {},
            )
            if first or second
            else None,
            override if isinstance(override, int) else None,
            self.status,
            self.sample_application,
            self.frame,
        )
