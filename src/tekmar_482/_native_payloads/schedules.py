"""483 SW 11 schedule ownership, calendar layout and revision-specific behavior."""

from collections.abc import Mapping
from dataclasses import dataclass
from enum import IntEnum

from .layouts import MULTI_WEEK
from .products import NativeProduct, product_revision

SINGLE_WEEK = range(0x10, 0x14)
SETUP_BLOCK = 0x0f
DAY_NAMES = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")


class ScheduleEvent(IntEnum):
    WAKE = 0
    UNOCCUPIED = 1
    OCCUPIED = 2
    SLEEP = 3


class ScheduleBinding(IntEnum):
    NONE = 0
    ZONE = 1
    MASTER_1 = 2
    MASTER_2 = 3
    MASTER_3 = 4
    MASTER_4 = 5
    MEMBER_1 = 6
    MEMBER_2 = 7
    MEMBER_3 = 8
    MEMBER_4 = 9
    UNOCCUPIED = 10

    @property
    def owns_table(self) -> bool:
        return self.ZONE <= self <= self.MASTER_4

    @property
    def system_number(self) -> int | None:
        if self.MASTER_1 <= self <= self.MEMBER_4:
            return 1 + (self.value - self.MASTER_1) % 4
        return None


class ScheduleCycle(IntEnum):
    DAILY = 0
    FIVE_TWO = 1
    FIVE_ONE_ONE = 2
    SEVEN_DAY = 3

    def days_for(self, day: int) -> tuple[int, ...]:
        if day not in range(7):
            raise ValueError("expected a Monday-based weekday, 0..6")
        if self is self.DAILY:
            return tuple(range(7))
        if self is not self.SEVEN_DAY and day < 5:
            return tuple(range(5))
        if self is self.FIVE_TWO:
            return (5, 6)
        return (day,)


class DailyEvents(IntEnum):
    NONE = 0
    TWO = 1
    FOUR = 2

    @property
    def events(self) -> tuple[ScheduleEvent, ...]:
        if self is self.FOUR:
            return tuple(ScheduleEvent)
        return (ScheduleEvent.WAKE, ScheduleEvent.SLEEP) if self is self.TWO else ()

    def display_event(self, event: ScheduleEvent) -> ScheduleEvent:
        if event not in self.events:
            raise ValueError("event is not part of this schedule")
        if self is self.TWO:
            return ScheduleEvent.OCCUPIED if event is ScheduleEvent.WAKE else ScheduleEvent.UNOCCUPIED
        return event


@dataclass(frozen=True, slots=True)
class ScheduleProfile:
    product: NativeProduct | None
    z_number: int | None

    @classmethod
    def from_identification(cls, fields: Mapping[str, object]) -> "ScheduleProfile":
        return cls(*product_revision(fields))

    def wire_day(self, block: int, day: int) -> int:
        """Calendar weekday to physical slot; the same mapping is used for readback."""
        if day not in range(7):
            raise ValueError("expected a Monday-based weekday, 0..6")
        if block in MULTI_WEEK:
            return day
        if block not in SINGLE_WEEK or self.product is None:
            raise ValueError("identify the schedule owner before interpreting calendar weekdays")
        last = {"552": 13, "557": 2}.get(self.product.model)
        if last is not None:
            if self.z_number is None:
                raise ValueError("calendar weekdays require the owner's revision")
            if self.z_number <= last:
                return (day + 1) % 7
        return day

    @property
    def shares_heating_schedule(self) -> bool:
        # SW 11 block_support CSV: only these three models advertise a separate cooling ID.
        return bool(self.product and SETUP_BLOCK in self.product.candidate_blocks
                    and self.product.model not in ("540", "545", "546"))

    def check_setup_edit(self, support: int, before: bytes, after: bytes) -> None:
        if before == after:
            return
        if self.product is None:
            raise ValueError("identify the device before changing schedule setup")
        if self.product.model not in ("545", "546") or (self.z_number is not None and self.z_number > 10):
            return
        # Both models can master a separate heat or cool schedule. Do not
        # infer a safe role from unsupported bytes in a positional response.
        if support & 3 != 3 or any(v not in range(len(ScheduleBinding)) or 2 <= v <= 5
                                  for v in (*before[:2], *after[:2])):
            raise ValueError("schedule setup edit could drop 545/546 mastership on this revision")
