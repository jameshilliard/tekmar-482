"""Join native reads to a single consumer of the same connection's peer trace."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .exceptions import ConnectionClosedError, ProtocolError
from .management import BridgeFeature, BridgeMode
from .native import NativeAddress
from .normal_trace import NormalNativeSession, NormalPeerCapture
from .raw import BridgeStream

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ._native_payloads.queries import NativeStatusQuery
    from .native import NativeBlockQuery
    from .native_status import NativeStatusBatch
    from .raw import NativeBlockBatch

type ObservationStream = BridgeStream | NormalPeerCapture | NormalNativeSession
type Capture = BridgeStream | NormalPeerCapture


@dataclass(frozen=True, slots=True)
class OrderedNormalReads:
    session: NormalNativeSession

    async def read_block_group(
        self,
        port: int,
        query: NativeBlockQuery,
        *,
        source: NativeAddress,
        timeout: float,
        wait_for_edits: bool = False,
    ) -> NativeBlockBatch:
        batch = await self.session.management.read_block_group(
            port,
            query,
            source=source,
            timeout=timeout,
            wait_for_edits=wait_for_edits,
        )
        await self.session.peers.wait_consumed()
        return batch

    async def read_status(
        self,
        port: int,
        query: NativeStatusQuery,
        *,
        source: NativeAddress,
        peer: NativeAddress | None = None,
        timeout: float,
        wait_for_edits: bool = False,
    ) -> NativeStatusBatch:
        batch = await self.session.management.read_status(
            port,
            query,
            source=source,
            peer=peer,
            timeout=timeout,
            wait_for_edits=wait_for_edits,
        )
        await self.session.peers.wait_consumed()
        return batch


async def observation(
    stream: ObservationStream,
    ports: Iterable[int],
    source: NativeAddress | None,
) -> tuple[Capture, BridgeStream | OrderedNormalReads | None]:
    capture = stream.peers if isinstance(stream, NormalNativeSession) else stream
    if isinstance(capture, NormalPeerCapture):
        if capture.closed:
            msg = "Normal peer capture has ended"
            raise ConnectionClosedError(msg)
        if any(not capture.port_mask & (1 << p) for p in ports):
            msg = "selected paths must be on captured ports"
            raise ValueError(msg)
    if isinstance(stream, NormalPeerCapture):
        if source is not None:
            msg = "a Normal peer capture is passive; use source=None"
            raise ValueError(msg)
        return capture, None
    if source is None:
        return capture, None
    if isinstance(stream, NormalNativeSession):
        if source != NativeAddress(0, 0):
            msg = "Normal observation uses the gateway source address (0, 0)"
            raise ValueError(msg)
        caps = await stream.management.capabilities()
        if (
            caps is None
            or caps.mode != BridgeMode.NORMAL
            or not caps.features & BridgeFeature.ORDERED_NATIVE_RESULTS
        ):
            msg = "active Normal observation requires ordered native results"
            raise ProtocolError(msg)
        return capture, OrderedNormalReads(stream)
    return stream, stream
