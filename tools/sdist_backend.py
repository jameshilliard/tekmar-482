"""Delegate to uv_build; stage the embedded image only for source releases."""

from __future__ import annotations

from contextlib import ExitStack
from importlib import import_module
from shutil import copyfileobj
from typing import TYPE_CHECKING

from .bundle_firmware import PACKAGE, ROOT, build_image

if TYPE_CHECKING:
    from collections.abc import Mapping


def __getattr__(name: str) -> object:
    return getattr(import_module("uv_build"), name)


def build_sdist(
    sdist_directory: str,
    config_settings: Mapping[str, object] | None = None,
) -> str:
    target = PACKAGE / "_images/bridge.bin"
    backend = import_module("uv_build")
    if not (ROOT / ".git").exists() and target.is_file():
        # Repacking an extracted sdist must not require a submodule or Rust.
        filename = backend.build_sdist(sdist_directory, config_settings)
    else:
        image = build_image()
        target.parent.mkdir(exist_ok=True)
        with ExitStack() as cleanup:
            with image.open("rb") as source, target.open("xb") as dest:
                cleanup.callback(target.unlink)
                copyfileobj(source, dest)
            filename = backend.build_sdist(sdist_directory, config_settings)
    if not isinstance(filename, str):
        msg = "uv_build did not return an sdist filename"
        raise TypeError(msg)
    return filename
