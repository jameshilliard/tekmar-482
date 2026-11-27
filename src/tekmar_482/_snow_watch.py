"""Read policy for snowmelt devices on a shared native capture stream."""

from __future__ import annotations

from enum import Enum, auto
from typing import TYPE_CHECKING

from ._native_payloads.layouts import SnowmeltBlock
from ._native_payloads.line import GROUP_IDS, GROUP_REPORT, GROUP_UPDATE
from ._native_payloads.queries import NativeStatusQuery
from ._native_payloads.snowmelt import SNOW_REPORT, SnowmeltKey, SnowmeltRole
from .native_snowmelt import SNOW_SETUP
from .native_state import NativePath

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from ._native_payloads.snowmelt import SnowmeltAddress
    from .native import NativeFrame
    from .native_snowmelt import NativeSnowmelt
    from .native_state import NativeState
    from .raw import CaptureGap, NativeBlockBatch

_GROUP_WIDTH = 2
_RETRY_REPORTS = 5  # SW 11 retries incomplete snowmelt data every fifth report.


class SnowRead(Enum):
    MISSING = auto()
    OPERATING = auto()


class SnowWatch:
    def __init__(
        self,
        state: NativeState,
        queue: Callable[[NativePath, int | SnowRead], None],
    ) -> None:
        self.state = state
        self._queue = queue
        self.current: dict[NativePath, NativeSnowmelt] = {}
        self._group_flags: dict[tuple[NativePath, int, int], int] = {}
        self._retry: dict[NativePath, tuple[bytes, int]] = {}
        self._setup_hints: set[tuple[NativePath, int, SnowmeltAddress]] = set()
        self.routes: dict[NativePath, int] = {}

    def discard(self, path: NativePath) -> None:
        self.current.pop(path, None)
        self._retry.pop(path, None)
        self.routes.pop(path, None)
        self._setup_hints = {h for h in self._setup_hints if h[0] != path}
        self._group_flags = {k: v for k, v in self._group_flags.items() if k[0] != path}

    def capture_gap(self, gap: CaptureGap) -> None:
        self._group_flags = {
            k: v for k, v in self._group_flags.items() if not gap.affects(k[0].port)
        }

    def changed(self, path: NativePath) -> bool:
        current = self.state.snowmelt(path)
        if current is None:
            if path in self.current:
                self.discard(path)
            return False
        before = self.current.get(path)
        self.current[path] = current
        if before is None or before.identity != current.identity:
            for block in SNOW_SETUP:
                if self.state.block(path, block) is None:
                    self._queue(path, block)
        if before is None or (
            before.setup.application_raw,
            before.setup.primary,
        ) != (current.setup.application_raw, current.setup.primary):
            self._retry.pop(path, None)
            self.routes[path] = self.routes.get(path, 0) + 1
            if current.missing_keys:
                self._queue(path, SnowRead.MISSING)
            return True
        return False

    def _retry_setup(
        self,
        path: NativePath,
        frame: NativeFrame,
        current: NativeSnowmelt | None,
        latest: Mapping[tuple[NativePath, int], NativeBlockBatch],
    ) -> None:
        if self.state.identity(path) is None:
            block = 0
        elif current is None:
            return
        elif current.setup.application_raw is None:
            block = SnowmeltBlock.APPLICATION_MODE
        elif (
            app := current.setup.application
        ) is not None and current.setup.primary is None:
            block = (
                SnowmeltBlock.ZONE_ID
                if app.role is SnowmeltRole.ZONE
                else SnowmeltBlock.SENSOR_ID
            )
        else:
            return
        batch = latest.get((path, block))
        if batch is None or batch.reports:
            return
        for channel in self.state.snowmelt_channels(path):
            hint = (path, block, channel.address)
            if channel.last_frame is frame and hint not in self._setup_hints:
                self._setup_hints.add(hint)
                self._queue(path, block)

    def observe(
        self,
        frame: NativeFrame,
        latest: Mapping[tuple[NativePath, int], NativeBlockBatch],
        *,
        attempted: bool,
    ) -> None:
        path = NativePath.from_frame(frame)
        if frame.message_id == SNOW_REPORT:
            current = self.state.snowmelt(path)
            self._retry_setup(path, frame, current, latest)
            if (
                current is None
                or not attempted
                or not (missing := current.missing_keys)
            ):
                return
            # Either valid role proves the controller is transmitting, but the
            # request and its missing-key set still belong to the primary role.
            if not any(channel.last_frame is frame for channel in current.channels):
                return
            previous, remaining = self._retry.get(path, (b"", 0))
            if missing == previous and remaining > 1:
                self._retry[path] = (missing, remaining - 1)
                return
            self._retry[path] = (missing, _RETRY_REPORTS)
            self._queue(path, SnowRead.MISSING)
            return
        if frame.message_id not in (GROUP_REPORT, GROUP_UPDATE):
            return
        data = frame.message.payload
        if len(data) < _GROUP_WIDTH or data[0] not in GROUP_IDS:
            return
        group, flags = data[:_GROUP_WIDTH]
        if frame.message_id == GROUP_REPORT:
            flags &= 0x11
        elif flags not in (0, 1):
            return
        key = (path, frame.message_id, group)
        if self._group_flags.get(key) == flags:
            return
        self._group_flags[key] = flags
        for p, device in self.current.items():
            setup = device.setup
            if (
                p.port == path.port
                and setup.primary is not None
                and setup.primary.role is SnowmeltRole.ZONE
                and group in (setup.melt_group, setup.storm_group)
            ):
                self._queue(p, SnowRead.OPERATING)

    def query(self, path: NativePath, read: SnowRead) -> NativeStatusQuery | None:
        current = self.current.get(path)
        if current is None or (address := current.setup.primary) is None:
            return None
        if read is SnowRead.MISSING:
            return current.status_query(missing_only=True)
        if address.role is SnowmeltRole.ZONE:
            return NativeStatusQuery.snowmelt(
                address.role,
                address.number,
                bytes([SnowmeltKey.OPERATING_STATE]),
            )
        return None

    def updated_block(self, target: NativePath, block: int) -> None:
        if target in self.current and block in SNOW_SETUP:
            self._queue(target, block)
