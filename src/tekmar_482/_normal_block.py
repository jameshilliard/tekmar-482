"""Control-channel encoding for an addressed, read-only Normal transaction."""

from __future__ import annotations

from enum import IntEnum
from struct import Struct
from typing import TYPE_CHECKING

from ._native_payloads import BLOCK_SELECTORS
from .constants import TN4_PORTS
from .exceptions import ProtocolError
from .native import NativeFrame

if TYPE_CHECKING:
    from .native import NativeBlockQuery

_KEY = Struct("<BBBHBB")
_RESULT = Struct("<BBI")
_STATUS_BYTES = 2


class _Status(IntEnum):
    REPORT = 0
    EXPIRED = 1
    PIN_UNAVAILABLE = 2
    LINK_FAULT = 3


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


def read_result(port: int, query: NativeBlockQuery, body: bytes) -> NativeFrame:
    if len(body) < _STATUS_BYTES or body[0] != port or body[1] not in _Status:
        msg = "invalid Normal block result header"
        raise ProtocolError(msg)
    status = _Status(body[1])
    if status is not _Status.REPORT:
        if len(body) != _STATUS_BYTES:
            msg = "unexpected frame on failed Normal block read"
            raise ProtocolError(msg)
        if status is _Status.EXPIRED:
            msg = "gateway read deadline expired without a matching peer report"
            raise TimeoutError(msg)
        msg = f"gateway could not complete the native read: {status.name.lower()}"
        raise ProtocolError(msg)
    frame = peer_frame(port, body)
    if not query.matches(frame.message):
        msg = "Normal block result does not match the requested peer, block or selector"
        raise ProtocolError(msg)
    return frame


def peer_frame(port: int, body: bytes) -> NativeFrame:
    if len(body) < _RESULT.size:
        msg = "truncated Normal native frame result"
        raise ProtocolError(msg)
    _, _, stamp = _RESULT.unpack_from(body)
    try:
        return NativeFrame(port, stamp, body[_RESULT.size :])
    except ValueError as err:
        msg = "invalid native frame in Normal result"
        raise ProtocolError(msg) from err
