"""Task-scoped observation of application packets, not ROM ISP traffic."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from .packet import Packet

type PacketObserver = Callable[[Packet, bool], None]
_observer = ContextVar[PacketObserver | None]("tekmar_packet_observer", default=None)


@contextmanager
def observe_bridge_packets(callback: PacketObserver) -> Iterator[None]:
    """Observe decoded TPCK RX and submitted TX in custom sessions opened here.

    The second argument is true for host TX. Keep callbacks synchronous and
    nonblocking. Collecting packets does not enable tracing or send any request.
    """
    token = _observer.set(callback)
    try:
        yield
    finally:
        _observer.reset(token)


def current_observer() -> PacketObserver | None:
    """Capture the observer when a physical bridge connection is created."""
    return _observer.get()
