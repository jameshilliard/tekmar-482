"""Source-scoped operating samples and stored configuration of setpoint controls."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ._native_payloads.line import SetpointAssignment
from ._native_payloads.setpoint import (
    APPLICATION_SETUP,
    OPERATION_SETUP,
    OVERRIDE_TIME,
    TARGET_BLOCKS,
    SetpointTargets,
)

if TYPE_CHECKING:
    from ._native_payloads.line import SetpointApplication
    from ._native_payloads.products import NativeProduct
    from ._native_payloads.setpoint import SetpointReadings, SetpointStatus
    from .native import BlockKey, NativeFrame, NativeMessage
    from .native_identity import NativeIdentity
    from .native_state import NativePath

SETPOINT_BLOCKS = frozenset(
    (APPLICATION_SETUP, OPERATION_SETUP, OVERRIDE_TIME, *TARGET_BLOCKS),
)


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
