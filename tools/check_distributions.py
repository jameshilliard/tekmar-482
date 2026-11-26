"""Check the shipped flash image and reject firmware sources in distributions."""

import argparse
import tarfile
from pathlib import Path, PurePosixPath

from tools.bundle_firmware import SOURCE


def check(paths: list[str], image: bytes) -> None:
    if (
        image
        != (
            SOURCE / "target/thumbv4t-none-eabi/release/tekmar-482-firmware.bin"
        ).read_bytes()
    ):
        msg = "distribution firmware differs from the tested image"
        raise ValueError(msg)
    for name in paths:
        path = PurePosixPath(name)
        if (
            "firmware" in path.parts
            or ("_images" in path.parts and path.name not in {"_images", "bridge.bin"})
            or path.suffix in {".rs", ".hex", ".elf", ".S"}
            or path.name in {"Cargo.lock", "Cargo.toml", ".gitmodules", "link.x"}
        ):
            msg = f"unexpected firmware source or build product: {name}"
            raise ValueError(msg)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    directory = parser.parse_args().directory
    (sdist,) = directory.glob("*.tar.gz")
    if any(directory.glob("*.whl")):
        msg = "publish only the sdist, not a standalone wheel"
        raise ValueError(msg)
    with tarfile.open(sdist) as source:
        paths = source.getnames()
        (binary,) = (p for p in paths if p.endswith("/tekmar_482/_images/bridge.bin"))
        stream = source.extractfile(binary)
        if stream is None:
            msg = "missing flash binary in sdist"
            raise ValueError(msg)
        check(paths, stream.read())


if __name__ == "__main__":
    main()
