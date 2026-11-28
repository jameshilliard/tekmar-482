"""Addressed device controls built on an exclusive Raw edit session."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Protocol

from ._native_payloads.writes import BlockPatch
from .native_schedule import edit_schedule_time
from .native_schedule_setup import edit_schedule_setup
from .native_setpoint import edit_setpoint_target
from .native_snow_control import apply_snowmelt_group, edit_snowmelt_setpoint
from .native_target_edit import edit_thermostat_target

if TYPE_CHECKING:
    from collections.abc import Mapping
    from contextlib import AbstractAsyncContextManager

    from ._native_payloads.climate import ThermostatMode
    from ._native_payloads.layouts import SnowmeltBlock
    from ._native_payloads.queries import NativeStatusQuery
    from ._native_payloads.schedules import ScheduleBinding
    from ._native_payloads.snowmelt import SnowmeltApplication
    from ._native_payloads.writes import NativeField
    from .native import NativeAddress, NativeBlockQuery, NativeMessage, NativeTargets
    from .native_identity import NativeIdentity
    from .native_schedule import ScheduleChange, ScheduleConfiguration, ScheduleTarget
    from .native_schedule_setup import ScheduleSetupChange, ScheduleSetupResult
    from .native_setpoint import SetpointTargetChange
    from .native_snow_control import SnowmeltGroupResult, SnowmeltOperation
    from .native_status import NativeStatusBatch
    from .native_target_edit import ThermostatTargetChange
    from .native_write import NativeBlockEditor, NativeWriteResult
    from .raw import TxResult


class DeviceOperations(Protocol):
    """Typed device commands on an extended Raw session."""

    async def write_block(
        self,
        port: int,
        query: NativeBlockQuery,
        changes: Mapping[NativeField, int],
        *,
        source: NativeAddress,
        expected_identity: NativeIdentity | None = None,
        timeout: float = 20,
        preview: bool = False,
    ) -> NativeWriteResult: ...
    async def write_thermostat_mode(
        self,
        port: int,
        target: NativeAddress,
        mode: ThermostatMode,
        *,
        source: NativeAddress,
        expected_identity: NativeIdentity | None = None,
        timeout: float = 20,
        preview: bool = False,
    ) -> NativeWriteResult: ...
    async def write_thermostat_target(
        self,
        port: int,
        target: NativeAddress,
        change: ThermostatTargetChange,
        *,
        source: NativeAddress,
        expected_identity: NativeIdentity | None = None,
        timeout: float = 20,
        preview: bool = True,
    ) -> NativeWriteResult: ...
    async def write_snowmelt_setpoint(
        self,
        port: int,
        target: NativeAddress,
        block: SnowmeltBlock,
        celsius: float | None,
        *,
        source: NativeAddress,
        expected_identity: NativeIdentity | None = None,
        expected_application: SnowmeltApplication | None = None,
        timeout: float = 20,
        preview: bool = True,
    ) -> NativeWriteResult: ...
    async def write_setpoint_target(
        self,
        port: int,
        target: NativeAddress,
        change: SetpointTargetChange,
        *,
        source: NativeAddress,
        expected_identity: NativeIdentity | None = None,
        timeout: float = 20,
        preview: bool = True,
    ) -> NativeWriteResult: ...
    async def set_snowmelt_group(
        self,
        port: int,
        target: NativeAddress,
        operation: SnowmeltOperation,
        *,
        enabled: bool,
        source: NativeAddress,
        expected_identity: NativeIdentity | None = None,
        expected_group: int | None = None,
        expected_zone: int | None = None,
        timeout: float = 20,
        preview: bool = True,
    ) -> SnowmeltGroupResult: ...
    async def write_schedule_time(
        self,
        port: int,
        target: ScheduleTarget,
        change: ScheduleChange,
        *,
        source: NativeAddress,
        expected_identity: NativeIdentity | None = None,
        timeout: float = 20,
        preview: bool = False,
    ) -> NativeWriteResult: ...
    async def configure_schedule(
        self,
        port: int,
        target: ScheduleTarget,
        change: ScheduleSetupChange,
        *,
        source: NativeAddress,
        expected_binding: ScheduleBinding | None = None,
        expected_config: ScheduleConfiguration | None = None,
        expected_identity: NativeIdentity | None = None,
        timeout: float = 20,
        preview: bool = True,
    ) -> ScheduleSetupResult: ...


class RawDeviceOperations(ABC):
    @abstractmethod
    def _editor(
        self,
        port: int,
        target: NativeAddress | NativeTargets,
        source: NativeAddress,
        timeout: float,
        *,
        expected_identity: NativeIdentity | None,
    ) -> AbstractAsyncContextManager[NativeBlockEditor]: ...

    @abstractmethod
    async def _send(self, port: int, body: bytes | NativeMessage) -> TxResult: ...

    @abstractmethod
    async def _collect_status(
        self,
        port: int,
        query: NativeStatusQuery,
        source: NativeAddress,
        peer: NativeAddress | None,
        timeout: float,
    ) -> NativeStatusBatch: ...

    async def write_block(
        self,
        port: int,
        query: NativeBlockQuery,
        changes: Mapping[NativeField, int],
        *,
        source: NativeAddress,
        expected_identity: NativeIdentity | None = None,
        timeout: float = 20,
        preview: bool = False,
    ) -> NativeWriteResult:
        """Read, optionally update once, then verify. The timeout is per exchange."""
        patch = BlockPatch.from_fields(query.block_id, changes)
        async with self._editor(
            port,
            query.target,
            source,
            timeout,
            expected_identity=expected_identity,
        ) as editor:
            return await editor.write(query, patch, preview=preview)

    async def write_thermostat_mode(
        self,
        port: int,
        target: NativeAddress,
        mode: ThermostatMode,
        *,
        source: NativeAddress,
        expected_identity: NativeIdentity | None = None,
        timeout: float = 20,
        preview: bool = False,
    ) -> NativeWriteResult:
        async with self._editor(
            port,
            target,
            source,
            timeout,
            expected_identity=expected_identity,
        ) as editor:
            return await editor.write_mode(target, mode, preview=preview)

    async def write_thermostat_target(
        self,
        port: int,
        target: NativeAddress,
        change: ThermostatTargetChange,
        *,
        source: NativeAddress,
        expected_identity: NativeIdentity | None = None,
        timeout: float = 20,
        preview: bool = True,
    ) -> NativeWriteResult:
        async with self._editor(
            port,
            target,
            source,
            timeout,
            expected_identity=expected_identity,
        ) as editor:
            return await edit_thermostat_target(editor, target, change, preview=preview)

    async def write_snowmelt_setpoint(
        self,
        port: int,
        target: NativeAddress,
        block: SnowmeltBlock,
        celsius: float | None,
        *,
        source: NativeAddress,
        expected_identity: NativeIdentity | None = None,
        expected_application: SnowmeltApplication | None = None,
        timeout: float = 20,
        preview: bool = True,
    ) -> NativeWriteResult:
        """Preview or verify a stored slab target without enabling a group."""
        async with self._editor(
            port,
            target,
            source,
            timeout,
            expected_identity=expected_identity,
        ) as editor:
            return await edit_snowmelt_setpoint(
                editor,
                target,
                block,
                celsius,
                expected_application=expected_application,
                preview=preview,
            )

    async def write_setpoint_target(
        self,
        port: int,
        target: NativeAddress,
        change: SetpointTargetChange,
        *,
        source: NativeAddress,
        expected_identity: NativeIdentity | None = None,
        timeout: float = 20,
        preview: bool = True,
    ) -> NativeWriteResult:
        """Preview or verify one stored target on the specified 161/162 application."""
        async with self._editor(
            port,
            target,
            source,
            timeout,
            expected_identity=expected_identity,
        ) as editor:
            return await edit_setpoint_target(editor, target, change, preview=preview)

    async def set_snowmelt_group(
        self,
        port: int,
        target: NativeAddress,
        operation: SnowmeltOperation,
        *,
        enabled: bool,
        source: NativeAddress,
        expected_identity: NativeIdentity | None = None,
        expected_group: int | None = None,
        expected_zone: int | None = None,
        timeout: float = 20,
        preview: bool = True,
    ) -> SnowmeltGroupResult:
        """Read the binding, optionally broadcast once, then query the selected zone."""
        async with self._editor(
            port,
            target,
            source,
            timeout,
            expected_identity=expected_identity,
        ) as editor:
            return await apply_snowmelt_group(
                editor,
                lambda msg: self._send(port, msg),
                lambda query: self._collect_status(
                    port,
                    query,
                    source,
                    target,
                    timeout,
                ),
                target,
                operation,
                enabled=enabled,
                expected_group=expected_group,
                expected_zone=expected_zone,
                preview=preview,
            )

    async def write_schedule_time(
        self,
        port: int,
        target: ScheduleTarget,
        change: ScheduleChange,
        *,
        source: NativeAddress,
        expected_identity: NativeIdentity | None = None,
        timeout: float = 20,
        preview: bool = False,
    ) -> NativeWriteResult:
        """Verify ownership and grouping before updating one display event."""
        async with self._editor(
            port,
            target.address,
            source,
            timeout,
            expected_identity=expected_identity,
        ) as editor:
            return await edit_schedule_time(editor, target, change, preview=preview)

    async def configure_schedule(
        self,
        port: int,
        target: ScheduleTarget,
        change: ScheduleSetupChange,
        *,
        source: NativeAddress,
        expected_binding: ScheduleBinding | None = None,
        expected_config: ScheduleConfiguration | None = None,
        expected_identity: NativeIdentity | None = None,
        timeout: float = 20,
        preview: bool = True,
    ) -> ScheduleSetupResult:
        """Prepare or apply a weekly schedule using addressed Raw transactions."""
        async with self._editor(
            port,
            target.address,
            source,
            timeout,
            expected_identity=expected_identity,
        ) as editor:
            return await edit_schedule_setup(
                editor,
                target,
                change,
                expected_binding=expected_binding,
                expected_config=expected_config,
                preview=preview,
            )
