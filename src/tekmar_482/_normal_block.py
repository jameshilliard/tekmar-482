"""Control-channel evidence for addressed Normal block transactions."""

from __future__ import annotations

from enum import IntEnum
from struct import Struct
from typing import TYPE_CHECKING

from ._native_payloads import BLOCK_SELECTORS
from .constants import TN4_PORTS
from .exceptions import ProtocolError
from .native import NativeFrame
from .native_write import BlockDelivery
from .raw import FailureReason, NativeBlockBatch, RejectReason, TxResult, TxStatus

if TYPE_CHECKING:
    from .native import NativeBlockQuery, NativeMessage

_KEY = Struct("<BBBHBB")
_STAMP = Struct("<I")
_BLOCK_HEADER = 3
_EDIT_RESULT = Struct("<BBBB")
_PIN_UNAVAILABLE = 0xFE
_NOT_SENT = 0xFF


class _Status(IntEnum):
    REPORT = 0
    EXPIRED = 1
    PIN_UNAVAILABLE = 2
    LINK_FAULT = 3


class _EditEnd(IntEnum):
    ATTEMPTED = 0
    CHANGED = 1
    EXPIRED = 2
    BASELINE_FAILED = 3
    UPDATE_UNAVAILABLE = 4


def tx_outcome(port: int, code: int) -> TxResult | None:
    if code == _NOT_SENT:
        return None
    if code == _PIN_UNAVAILABLE:
        return TxResult(port, TxStatus.REJECTED, RejectReason.PIN_UNAVAILABLE)
    if code == 0:
        return TxResult(port, TxStatus.ACK)
    if code in FailureReason:
        return TxResult(port, TxStatus.FAILED, FailureReason(code))
    msg = f"invalid Normal transmit outcome {code:#x}"
    raise ProtocolError(msg)


def read_key(port: int, query: NativeBlockQuery) -> bytes:
    nodes = query.target.request_mask()
    if port not in TN4_PORTS or nodes.bit_count() != 1:
        msg = "a Normal block read requires one physical port and one target node"
        raise ValueError(msg)
    selector = BLOCK_SELECTORS.get(query.block_id)
    return _KEY.pack(
        port,
        query.target.subnet,
        nodes.bit_length(),
        query.block_id,
        selector[1] if selector else 0,
        query.selector or 0,
    )


def read_result(port: int, query: NativeBlockQuery, body: bytes) -> NativeBlockBatch:
    if len(body) < _BLOCK_HEADER or body[0] != port or body[1] not in _Status:
        msg = "invalid Normal block result header"
        raise ProtocolError(msg)
    status = _Status(body[1])
    tx = tx_outcome(port, body[2])
    if status is not _Status.REPORT:
        if len(body) != _BLOCK_HEADER or not (
            (status is _Status.EXPIRED and (tx is None or tx.completed_frame))
            or (status is _Status.PIN_UNAVAILABLE and body[2] == _PIN_UNAVAILABLE)
            or (
                status is _Status.LINK_FAULT
                and tx
                and tx.status is TxStatus.FAILED
                and not tx.completed_frame
                and tx.reason != FailureReason.ARBITRATION_LOST
            )
        ):
            msg = "invalid terminal evidence on failed Normal block read"
            raise ProtocolError(msg)
        return NativeBlockBatch(port, query, tx, ())
    frame = peer_frame(port, body, timestamp_offset=_BLOCK_HEADER)
    if tx is None or not tx.completed_frame or not query.matches(frame.message):
        msg = "Normal block result does not match the requested peer, block or selector"
        raise ProtocolError(msg)
    return NativeBlockBatch(port, query, tx, (frame,))


def peer_frame(port: int, body: bytes, *, timestamp_offset: int = 2) -> NativeFrame:
    start = timestamp_offset + _STAMP.size
    if len(body) < start:
        msg = "truncated Normal native frame result"
        raise ProtocolError(msg)
    (stamp,) = _STAMP.unpack_from(body, timestamp_offset)
    try:
        return NativeFrame(port, stamp, body[start:])
    except ValueError as err:
        msg = "invalid native frame in Normal result"
        raise ProtocolError(msg) from err


def edit_key(
    port: int,
    query: NativeBlockQuery,
    before: NativeFrame,
    proposed: NativeMessage,
) -> bytes:
    payload = before.message.payload
    return (
        read_key(port, query)
        + bytes((payload[2], len(payload) - 3))
        + payload[3:]
        + proposed.payload[4:]
    )


def edit_result(port: int, body: bytes) -> BlockDelivery:
    if len(body) != _EDIT_RESULT.size or body[0] != port or body[1] not in _EditEnd:
        msg = "invalid Normal block edit result"
        raise ProtocolError(msg)
    _, code, baseline, attempt = _EDIT_RESULT.unpack(body)
    guard, tx = tx_outcome(port, baseline), tx_outcome(port, attempt)
    ready = guard is not None and guard.completed_frame
    match _EditEnd(code):
        case _EditEnd.ATTEMPTED:
            valid = ready and tx is not None and tx.status != TxStatus.REJECTED
        case _EditEnd.CHANGED:
            valid = ready and tx is None
        case _EditEnd.EXPIRED:
            valid = (guard is None or ready) and tx is None
        case _EditEnd.BASELINE_FAILED:
            valid = (
                guard is not None
                and not ready
                and guard.reason is not FailureReason.ARBITRATION_LOST
                and tx is None
            )
        case _EditEnd.UPDATE_UNAVAILABLE:
            valid = ready and attempt == _PIN_UNAVAILABLE
    if not valid:
        msg = "Normal block edit terminal disagrees with its link evidence"
        raise ProtocolError(msg)
    return BlockDelivery(tx, guard, code == _EditEnd.CHANGED)
