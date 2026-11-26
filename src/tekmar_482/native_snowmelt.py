"""Reported snowmelt configuration and independent zone and sensor channels."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Self

from ._native_payloads.layouts import SnowmeltBlock as Block
from ._native_payloads.queries import NativeStatusQuery
from ._native_payloads.snowmelt import (
    SnowmeltAddress,
    SnowmeltApplication,
    SnowmeltKey,
    SnowmeltReport,
    SnowmeltRole,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ._native_payloads.snowmelt import (
        SnowmeltOperatingState,
        SnowmeltPair,
        WaterDetection,
    )
    from .native import NativeFrame
    from .native_identity import NativeIdentity
    from .native_state import NativePath

SNOW_SETUP = (
    Block.APPLICATION_MODE,
    Block.ZONE_ID,
    Block.SENSOR_ID,
    Block.MELT_GROUP,
    Block.STORM_GROUP,
    Block.TRACKED_ZONE,
    Block.MELT_SETPOINT,
    Block.IDLE_SETPOINT,
    Block.STORM_SETPOINT,
)


@dataclass(frozen=True, slots=True)
class SnowmeltConfiguration:
    application_raw: int | None
    application_type: int | None
    zone_id: int | None
    sensor_id: int | None
    melt_group: int | None
    storm_group: int | None
    tracked_zone: int | None
    melt_setpoint_raw: int | None
    idle_setpoint_raw: int | None
    storm_setpoint_raw: int | None

    @classmethod
    def from_blocks(cls, blocks: Mapping[tuple[int, int | None], NativeFrame]) -> Self:
        def value(block: Block, name: str) -> int | None:
            frame = blocks.get((block, None))
            raw = frame.decoded.get(name) if frame is not None else None
            return raw if isinstance(raw, int) else None

        return cls(
            value(Block.APPLICATION_MODE, "application_mode"),
            value(Block.APPLICATION_MODE, "application_type"),
            value(Block.ZONE_ID, "zone_id"),
            value(Block.SENSOR_ID, "sensor_id"),
            value(Block.MELT_GROUP, "melt_group_id"),
            value(Block.STORM_GROUP, "storm_group_id"),
            value(Block.TRACKED_ZONE, "tracked_zone_id"),
            value(Block.MELT_SETPOINT, "melt_setpoint_raw"),
            value(Block.IDLE_SETPOINT, "idle_setpoint_raw"),
            value(Block.STORM_SETPOINT, "storm_setpoint_raw"),
        )

    @property
    def application(self) -> SnowmeltApplication | None:
        if self.application_raw is not None and self.application_raw in range(
            len(SnowmeltApplication),
        ):
            return SnowmeltApplication(self.application_raw)
        return None

    @property
    def primary(self) -> SnowmeltAddress | None:
        app = self.application
        if app is None:
            return None
        number = self.sensor_id if app.role is SnowmeltRole.SENSOR else self.zone_id
        if number is None or not 1 <= number <= app.role.maximum:
            return None
        return SnowmeltAddress(app.role, number)


@dataclass(frozen=True, slots=True)
class SnowmeltSample:
    pair: SnowmeltPair
    timestamp_ms: int = field(compare=False)


@dataclass(frozen=True, slots=True)
class SnowmeltChannel:
    address: SnowmeltAddress
    samples: tuple[SnowmeltSample, ...]
    last_frame: NativeFrame = field(compare=False)

    def sample(self, key: int) -> SnowmeltSample | None:
        return next((s for s in self.samples if s.pair.key == key), None)

    def raw(self, key: int) -> int | None:
        return s.pair.raw if (s := self.sample(key)) is not None else None

    def temperature_f(self, key: SnowmeltKey) -> float | None:
        return s.pair.temperature_f if (s := self.sample(key)) is not None else None

    @property
    def operating_state(self) -> SnowmeltOperatingState | None:
        sample = self.sample(SnowmeltKey.OPERATING_STATE)
        return sample.pair.operating_state if sample is not None else None

    @property
    def water_detection(self) -> WaterDetection | None:
        sample = self.sample(SnowmeltKey.WATER_DETECTION)
        return sample.pair.water_detection if sample is not None else None

    @property
    def pump_on(self) -> bool | None:
        sample = self.sample(SnowmeltKey.SYSTEM_PUMP)
        return sample.pair.pump_on if sample is not None else None


class SnowmeltChannels:
    def __init__(self) -> None:
        self._channels: dict[SnowmeltAddress, SnowmeltChannel] = {}

    @property
    def channels(self) -> tuple[SnowmeltChannel, ...]:
        return tuple(self._channels[a] for a in sorted(self._channels))

    def invalidate(
        self,
        block: int,
        previous: NativeFrame,
        current: NativeFrame,
    ) -> None:
        for setting, name, role in (
            (Block.APPLICATION_MODE, "application_mode", None),
            (Block.ZONE_ID, "zone_id", SnowmeltRole.ZONE),
            (Block.SENSOR_ID, "sensor_id", SnowmeltRole.SENSOR),
        ):
            if setting == block:
                old = previous.decoded.get(name)
                if old is not None and old != current.decoded.get(name):
                    self._channels = {
                        a: v
                        for a, v in self._channels.items()
                        if role is not None and a.role is not role
                    }
                break

    def update(
        self,
        frame: NativeFrame,
        address: SnowmeltAddress,
        report: SnowmeltReport,
    ) -> bool:
        before = self._channels.get(address)
        samples = {s.pair.key: s for s in before.samples} if before is not None else {}
        for pair in report.pairs:
            samples[pair.key] = SnowmeltSample(pair, frame.timestamp_ms)
        current = SnowmeltChannel(
            address,
            tuple(samples[k] for k in sorted(samples)),
            frame,
        )
        self._channels[address] = current
        return current != before


@dataclass(frozen=True, slots=True)
class NativeSnowmelt:
    path: NativePath
    identity: NativeIdentity
    setup: SnowmeltConfiguration
    channels: tuple[SnowmeltChannel, ...]

    @property
    def primary(self) -> SnowmeltChannel | None:
        address = self.setup.primary
        return next((c for c in self.channels if c.address == address), None)

    @property
    def missing_keys(self) -> bytes | None:
        """Missing SW 11 profile keys, or None when there is no configured endpoint."""
        app = self.setup.application
        if app is None or self.setup.primary is None:
            return None
        channel = self.primary
        return bytes(
            k for k in app.status_keys if channel is None or channel.sample(k) is None
        )

    def status_query(self, *, missing_only: bool = False) -> NativeStatusQuery | None:
        address, app = self.setup.primary, self.setup.application
        if address is None or app is None:
            return None
        keys = self.missing_keys if missing_only else app.status_keys
        return (
            NativeStatusQuery.snowmelt(address.role, address.number, keys)
            if keys
            else None
        )
