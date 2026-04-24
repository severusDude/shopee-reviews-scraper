from pathlib import Path
import json
import shutil
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from config import DEFAULT_CONFIG, load_config, save_config


class WorkspaceTempDir:
    def __enter__(self) -> Path:
        self.path = Path(__file__).resolve().parents[1] / ".tmp_test_config"
        if self.path.exists():
            shutil.rmtree(self.path)
        self.path.mkdir(parents=True, exist_ok=True)
        return self.path

    def __exit__(self, exc_type, exc, tb) -> bool:
        if self.path.exists():
            shutil.rmtree(self.path)
        return False


class ConfigTests(unittest.TestCase):
    def test_default_config_includes_logging_and_progress(self) -> None:
        self.assertTrue(DEFAULT_CONFIG["logging"]["enabled"])
        self.assertEqual(DEFAULT_CONFIG["logging"]["level"], "INFO")
        self.assertTrue(DEFAULT_CONFIG["progress"]["enabled"])
        self.assertEqual(DEFAULT_CONFIG["progress"]["style"], "notebook")

    def test_load_config_merges_nested_logging_keys(self) -> None:
        with WorkspaceTempDir() as temp_dir:
            path = temp_dir / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "logging": {
                            "level": "DEBUG",
                            "console": False,
                        },
                        "progress": {
                            "enabled": False,
                        },
                    }
                ),
                encoding="utf-8",
            )
            loaded = load_config(path)

        self.assertEqual(loaded["logging"]["level"], "DEBUG")
        self.assertFalse(loaded["logging"]["console"])
        self.assertTrue(loaded["logging"]["file"])
        self.assertFalse(loaded["progress"]["enabled"])
        self.assertEqual(loaded["progress"]["style"], "notebook")

    def test_save_config_round_trips_nested_sections(self) -> None:
        with WorkspaceTempDir() as temp_dir:
            path = temp_dir / "config.json"
            save_config(path, DEFAULT_CONFIG)
            loaded = load_config(path)

        self.assertIn("logging", loaded)
        self.assertIn("progress", loaded)


if __name__ == "__main__":
    unittest.main()
