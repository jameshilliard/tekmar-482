from tekmar_482 import NativeFrame, NativeMessage, NativePath, Packet

TEST_PATH = NativePath(3, 0x35, 7)


def extended_rx(sample: NativeFrame) -> Packet:
    return Packet(
        1,
        bytes([0, sample.port])
        + sample.timestamp_ms.to_bytes(4, "little")
        + sample.body
        + bytes([sample.rx_flags or 0]),
    )


def frame(
    mid: int,
    data: bytes,
    path: NativePath = TEST_PATH,
    *,
    stamp: int = 1,
) -> NativeFrame:
    return NativeFrame(
        path.port,
        stamp,
        NativeMessage(mid, path.address, data).to_bytes(),
    )


def block(b: int, mask_and_data: bytes, path: NativePath = TEST_PATH) -> NativeFrame:
    return frame(0x2E7, b.to_bytes(2, "little") + mask_and_data, path)


def identity(
    number: int = 100103,
    z: int = 1,
    path: NativePath = TEST_PATH,
) -> NativeFrame:
    return block(0, b"\x27" + number.to_bytes(3, "little") + bytes([z]), path)
