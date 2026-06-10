"""Small timeout/deadline helper for async operations."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Deadline:
    """Track the remaining time for a multi-step operation."""

    expires_at: float | None = None

    @classmethod
    def from_timeout(cls, timeout: float | None) -> Deadline:
        """Create a deadline from a timeout duration."""
        if timeout is None:
            return cls()
        return cls(asyncio.get_running_loop().time() + timeout)

    @property
    def expired(self) -> bool:
        """Return whether the deadline has expired."""
        remaining = self.remaining()
        return remaining is not None and remaining <= 0

    def remaining(self) -> float | None:
        """Return seconds remaining, or None for no deadline."""
        if self.expires_at is None:
            return None
        return max(0.0, self.expires_at - asyncio.get_running_loop().time())
