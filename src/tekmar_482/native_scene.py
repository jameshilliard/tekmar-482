"""Select system scenes without mistaking link results for appliance state."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Self

from ._native_payloads.scenes import NativeScene, SceneResponse
from ._native_payloads.writes import NativeField, WritableBlock
from .native import NativeBlockQuery, require_enum
from .native_line import BroadcastIssue, BroadcastResult, LineBroadcast

if TYPE_CHECKING:
    from .native import NativeAddress, NativeFrame, NativeMessage
    from .native_write import NativeWriteResult
    from .raw import BridgeStream


SceneSendIssue = BroadcastIssue
ScenePortResult = BroadcastResult


@dataclass(frozen=True, slots=True)
class SceneSelection:
    """One announcement on each explicitly selected line, with no retry."""

    scene: NativeScene
    ports: tuple[int, ...]
    source: NativeAddress
    broadcast: LineBroadcast = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "broadcast",
            LineBroadcast.scene(self.ports, self.scene, source=self.source),
        )

    @property
    def message(self) -> NativeMessage:
        return self.broadcast.message

    async def send(
        self,
        stream: BridgeStream,
        *,
        timeout: float = 20,
    ) -> tuple[BroadcastResult, ...]:
        """Return independent link outcomes; do not claim application readback."""
        return await self.broadcast.send(stream, timeout=timeout)


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
    return await stream.write_block(
        port,
        NativeBlockQuery(target, WritableBlock.SCENE_SETUP),
        scene_response_fields(scene, response),
        source=source,
        timeout=timeout,
        preview=preview,
    )


def scene_response_fields(
    scene: NativeScene,
    response: SceneResponse,
) -> dict[NativeField, int]:
    require_enum(scene, NativeScene)
    require_enum(response, SceneResponse)
    if (field := scene.response_field) is None:
        msg = f"scene {scene.value} has a fixed response"
        raise ValueError(msg)
    return {NativeField[field.upper()]: response.value}
