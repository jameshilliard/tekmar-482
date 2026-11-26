"""Select system scenes without mistaking link results for appliance state."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Self

from ._native_payloads.scenes import NativeScene, SceneResponse
from ._native_payloads.writes import NativeField, WritableBlock
from .constants import TN4_PORTS
from .native import NativeBlockQuery, NativeMessage, require_enum
from .raw import NativePortBusyError, TxResult, TxStatus

if TYPE_CHECKING:
    from .native import NativeAddress, NativeFrame
    from .native_write import NativeWriteResult
    from .raw import BridgeStream


class SceneSendIssue(Enum):
    PORT_BUSY = "port_busy"
    DEADLINE = "outcome_unknown"


@dataclass(frozen=True, slots=True)
class ScenePortResult:
    port: int
    outcome: TxResult | SceneSendIssue

    @property
    def link_acknowledged(self) -> bool:
        return (
            isinstance(self.outcome, TxResult) and self.outcome.status is TxStatus.ACK
        )


@dataclass(frozen=True, slots=True)
class SceneSelection:
    """One announcement on each explicitly selected line, with no retry."""

    scene: NativeScene
    ports: tuple[int, ...]
    source: NativeAddress

    def __post_init__(self) -> None:
        require_enum(self.scene, NativeScene)
        if (
            not self.ports
            or len(set(self.ports)) != len(self.ports)
            or not all(port in TN4_PORTS for port in self.ports)
        ):
            msg = "select one or more distinct physical ports (0..4)"
            raise ValueError(msg)

    @property
    def message(self) -> NativeMessage:
        return NativeMessage.scene_report(self.scene, source=self.source)

    async def send(
        self,
        stream: BridgeStream,
        *,
        timeout: float = 20,
    ) -> tuple[ScenePortResult, ...]:
        """Return independent link outcomes; do not claim application readback."""
        if not timeout > 0:
            msg = "scene send timeout must be positive"
            raise ValueError(msg)
        message = self.message

        async def one(port: int) -> ScenePortResult:
            deadline = asyncio.timeout(timeout)
            try:
                async with deadline:
                    return ScenePortResult(port, await stream.send(port, message))
            except NativePortBusyError:
                return ScenePortResult(port, SceneSendIssue.PORT_BUSY)
            except TimeoutError:
                if not deadline.expired():
                    raise
                return ScenePortResult(port, SceneSendIssue.DEADLINE)

        results = await asyncio.gather(
            *(one(p) for p in self.ports),
            return_exceptions=True,
        )
        collected = []
        for result in results:
            if isinstance(result, BaseException):
                raise result
            collected.append(result)
        return tuple(collected)


@dataclass(frozen=True, slots=True)
class SceneSetup:
    """One device's supported scene settings, not the currently active scene."""

    frame: NativeFrame

    def __post_init__(self) -> None:
        query = NativeBlockQuery(self.frame.message.source, WritableBlock.SCENE_SETUP)
        if not query.matches(self.frame.message):
            msg = "expected a complete, unambiguous scene setup report"
            raise ValueError(msg)

    @classmethod
    async def read(
        cls,
        stream: BridgeStream,
        port: int,
        target: NativeAddress,
        *,
        source: NativeAddress,
        timeout: float = 20,
    ) -> Self:
        query = NativeBlockQuery(target, WritableBlock.SCENE_SETUP)
        batch = await stream.read_block_group(
            port,
            query,
            source=source,
            timeout=timeout,
        )
        return cls(batch.require_report())

    @property
    def enabled(self) -> bool | None:
        raw = self.frame.decoded.get("scenes_enabled")
        return bool(raw) if raw in (0, 1) else None

    def configured_response(self, scene: NativeScene) -> SceneResponse | int | None:
        """Get a reported slot, including unknown codes. Fixed scenes have no slot."""
        if (field := scene.response_field) is None:
            return None
        raw = self.frame.decoded.get(field)
        if not isinstance(raw, int):
            return None
        return SceneResponse(raw) if raw in SceneResponse else raw


async def write_scene_response(
    stream: BridgeStream,
    port: int,
    target: NativeAddress,
    scene: NativeScene,
    response: SceneResponse,
    *,
    source: NativeAddress,
    timeout: float = 20,
    preview: bool = False,
) -> NativeWriteResult:
    """Change one configurable response using a fresh full block and readback."""
    require_enum(scene, NativeScene)
    require_enum(response, SceneResponse)
    if (field := scene.response_field) is None:
        msg = f"scene {scene.value} has a fixed response"
        raise ValueError(msg)
    return await stream.write_block(
        port,
        NativeBlockQuery(target, WritableBlock.SCENE_SETUP),
        {NativeField[field.upper()]: response.value},
        source=source,
        timeout=timeout,
        preview=preview,
    )
