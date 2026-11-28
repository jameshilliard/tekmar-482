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


def sync_decoders(tools: Path, package: Path, *, check: bool) -> None:
    source, target = tools / "payloads", package / "_native_payloads"
    if not (source / "__init__.py").is_file():
        msg = "Canonical decoder package missing; initialize the firmware submodule"
        raise RuntimeError(msg)
    modules = {p.relative_to(source) for p in source.rglob("*.py")}
    bundled = {p.relative_to(target) for p in target.rglob("*.py")}
    files = {target / name: source / name for name in modules}
    files[package / "_bridge_diagnostics.py"] = tools / "bridge_diagnostics.py"
    for path in sorted(files.keys() | {target / name for name in bundled}):
        origin = files.get(path)
        data = origin.read_bytes() if origin is not None else None
        if data is not None and path.is_file() and path.read_bytes() == data:
            continue
        if check:
            msg = f"Stale bundled file: {path.relative_to(package)}"
            raise RuntimeError(msg)
        if data is None:
            path.unlink()
        else:
            path.parent.mkdir(exist_ok=True, parents=True)
            path.write_bytes(data)


def main() -> None:
    """Synchronize the Python decoder sources; the sdist hook builds the binary."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    try:
        sync_decoders(SOURCE / "tools", PACKAGE, check=args.check)
    except RuntimeError as err:
        parser.exit(1, f"{err}\n")


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
    sync_decoders(SOURCE / "tools", PACKAGE, check=True)
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
