from tekmar_482 import NativeFrame, NativeMessage, NativePath

TEST_PATH = NativePath(3, 0x35, 7)


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
