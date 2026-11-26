"""483 SW 11 scene announcements and per-device response settings."""

from dataclasses import dataclass
from enum import IntEnum

from .codec import Fields, Reader

SCENE_REPORT = 0x2ff


class SceneResponse(IntEnum):
    """SceneSetup response codes, not tHA setback IDs."""

    WAKE = 0
    UNOCCUPIED = 1
    OCCUPIED = 2
    SLEEP = 3
    AWAY = 4
    SCHEDULE = 5


class NativeScene(IntEnum):
    """Scene numbers as shown on tekmar devices."""

    NORMAL = 1
    AWAY = 2
    UNOCCUPIED = 3
    PERMANENT_4 = 4
    PERMANENT_5 = 5
    TEMPORARY_OCCUPIED = 6
    TEMPORARY_7 = 7
    TEMPORARY_8 = 8

    @property
    def wire_id(self) -> int:
        return self.value - 1

    @property
    def duration_minutes(self) -> int:
        return _DURATIONS.get(self, 0)

    @property
    def response_field(self) -> str | None:
        return _FIELDS.get(self)

    @property
    def fixed_response(self) -> SceneResponse | None:
        """Policy for participating devices, not observed operating state."""
        return _FIXED.get(self)

    def payload(self) -> bytes:
        return bytes([self.wire_id]) + self.duration_minutes.to_bytes(2, "little")


_DURATIONS = {NativeScene.TEMPORARY_OCCUPIED: 180, NativeScene.TEMPORARY_7: 240, NativeScene.TEMPORARY_8: 480}
_FIELDS = {NativeScene.NORMAL: "permanent_scene_1", NativeScene.PERMANENT_4: "permanent_scene_4",
           NativeScene.PERMANENT_5: "permanent_scene_5", NativeScene.TEMPORARY_7: "temporary_scene_7",
           NativeScene.TEMPORARY_8: "temporary_scene_8"}
SCENE_RESPONSE_FIELDS = frozenset(_FIELDS.values())
_FIXED = {NativeScene.AWAY: SceneResponse.AWAY, NativeScene.UNOCCUPIED: SceneResponse.UNOCCUPIED,
          NativeScene.TEMPORARY_OCCUPIED: SceneResponse.OCCUPIED}


@dataclass(frozen=True, slots=True)
class SceneReport:
    """Received bytes; even known scenes need not carry the 483 writer's duration."""

    wire_id: int
    duration_minutes: int
    extension: bytes = b""

    @property
    def scene(self) -> NativeScene | None:
        number = self.wire_id + 1
        return NativeScene(number) if 1 <= number <= len(NativeScene) else None

    @classmethod
    def from_payload(cls, payload: bytes) -> "SceneReport":
        if len(payload) < 3:
            raise ValueError("scene report requires an ID and a 16-bit duration")
        return cls(payload[0], int.from_bytes(payload[1:3], "little"), payload[3:])

    @classmethod
    def read(cls, reader: Reader) -> "SceneReport":
        head = reader.take(3)
        return cls.from_payload(head + reader.take(len(reader.remaining)))

    def fields(self) -> Fields:
        fields: Fields = {"scene": self.wire_id, "duration": self.duration_minutes,
                          "duration_minutes": self.duration_minutes}
        if self.scene is not None:
            fields.update(scene_number=self.scene.value, scene_name=self.scene.name.lower())
        if self.extension:
            fields["extension_hex"] = self.extension.hex()
        return fields
