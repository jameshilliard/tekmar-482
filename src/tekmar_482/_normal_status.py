"""Wire events for a read-only, possibly multipart, Normal status exchange."""

from __future__ import annotations

from enum import IntEnum
from struct import Struct
from typing import TYPE_CHECKING

from ._native_payloads.queries import StatusRequest
from ._normal_block import peer_frame, tx_outcome
from .constants import TN4_PORTS
from .exceptions import ProtocolError
from .raw import FailureReason

if TYPE_CHECKING:
    from ._native_payloads.queries import NativeStatusQuery
    from .native import NativeAddress
    from .native_status import NativeStatusBatch, StatusCollector

_KEY = Struct("<BBBBH")
_EVENT_HEADER = 2
_END_SIZE = 3
_NODE_MASK = 31


class _Event(IntEnum):
    FRAME = 0
    COMPLETE = 1
    EXPIRED = 2
    CONTROL_FULL = 3
    TRANSMIT_FAULT = 4


def status_key(
    port: int,
    query: NativeStatusQuery,
    peer: NativeAddress | None,
) -> bytes:
    if port not in TN4_PORTS:
        msg = f"invalid physical port {port}"
        raise ValueError(msg)
    if query.kind is StatusRequest.HUMIDITY and (
        peer is None or query.payload[0] != peer.subnet
    ):
        msg = "humidity queries need the reporting peer on the requested subnet"
        raise ValueError(msg)
    return (
        _KEY.pack(
            port,
            int(peer is not None),
            peer.subnet if peer else 0,
            (peer.node & _NODE_MASK) if peer else 0,
            query.kind,
        )
        + query.payload
    )


def status_result(
    port: int,
    collector: StatusCollector,
    body: bytes,
) -> NativeStatusBatch | None:
    if len(body) < _EVENT_HEADER or body[0] != port or body[1] not in _Event:
        msg = "invalid Normal status event header"
        raise ProtocolError(msg)
    event = _Event(body[1])
    if event is _Event.FRAME:
        if not collector.add(peer_frame(port, body)):
            msg = "Normal status frame did not match the query and reporting peer"
            raise ProtocolError(msg)
        return None
    if len(body) != _END_SIZE:
        msg = "invalid Normal status terminal length"
        raise ProtocolError(msg)
    tx = tx_outcome(port, body[2])
    if event is _Event.CONTROL_FULL:
        msg = "gateway control queue could not return every native status frame"
        raise ProtocolError(msg)
    batch = collector.batch(port, tx)
    if (
        (tx is not None and tx.reason is FailureReason.ARBITRATION_LOST)
        or (event is _Event.COMPLETE) != batch.answered
        or (batch.reports and not (tx and tx.completed_frame))
        or (event is _Event.TRANSMIT_FAULT and (tx is None or tx.completed_frame))
    ):
        msg = "Normal status terminal disagrees with the link or peer evidence"
        raise ProtocolError(msg)
    return batch
