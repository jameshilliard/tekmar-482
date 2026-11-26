"""Rebuild the distributable firmware from the checked-out public submodule."""

import argparse
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "firmware"
PACKAGE = ROOT / "src" / "tekmar_482"


def git(*args: str) -> str:
    return subprocess.check_output(  # noqa: S603
        ["git", *args],  # noqa: S607
        cwd=SOURCE,
        text=True,
    ).strip()


def main() -> None:
    """Synchronize the Python decoder sources; the sdist hook builds the binary."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    for name in ("__init__.py", "layouts.py"):
        data = (SOURCE / "tools" / "payloads" / name).read_bytes()
        path = PACKAGE / "_native_payloads" / name
        if args.check:
            if not path.is_file() or path.read_bytes() != data:
                parser.exit(1, f"Stale bundled file: {name}\n")
        else:
            path.parent.mkdir(exist_ok=True)
            path.write_bytes(data)


def build_image() -> Path:
    if not (SOURCE / ".git").exists() or git("status", "--porcelain"):
        msg = "An initialized, clean firmware submodule is required to build the sdist"
        raise RuntimeError(msg)
    revision = git("rev-parse", "HEAD")
    gitlink = subprocess.check_output(
        ["git", "ls-files", "--stage", "firmware"],  # noqa: S607
        cwd=ROOT,
        text=True,
    ).split()
    if gitlink[:2] != ["160000", revision]:
        msg = "Firmware checkout differs from the pinned submodule revision"
        raise RuntimeError(msg)
    subprocess.run(
        ["make", "image"],  # noqa: S607
        cwd=SOURCE,
        check=True,
        env={**os.environ, "TEKMAR_BUILD_REVISION": revision},
    )
    image = SOURCE / "target/thumbv4t-none-eabi/release/tekmar-482-firmware.bin"
    if image.read_bytes()[0x100:0x130] != b"TK48REV\0" + revision.encode("ascii"):
        msg = "Flash image does not identify the pinned firmware revision"
        raise RuntimeError(msg)
    return image


if __name__ == "__main__":
    main()
