import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tools.bundle_firmware import sync_decoders


class DecoderBundleTests(unittest.TestCase):
    def test_check_and_sync_missing_changed_and_removed_modules(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "tools"
            package = root / "package"
            canonical = source / "payloads"
            canonical.mkdir(parents=True)
            (canonical / "__init__.py").touch()
            (source / "bridge_diagnostics.py").touch()
            sync_decoders(source, package, check=False)
            module = canonical / "service.py"
            copy = package / "_native_payloads" / module.name
            for value in (b"first", b"second", None):
                with self.subTest(value=value):
                    if value is None:
                        module.unlink()
                    else:
                        module.write_bytes(value)
                    with self.assertRaisesRegex(RuntimeError, r"service\.py"):
                        sync_decoders(source, package, check=True)
                    sync_decoders(source, package, check=False)
                    sync_decoders(source, package, check=True)
                    assert (copy.read_bytes() if copy.exists() else None) == value
