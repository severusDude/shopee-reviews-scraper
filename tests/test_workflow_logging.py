from pathlib import Path
import shutil
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from config import DEFAULT_CONFIG
from workflow_logging import build_workflow_logger


class WorkspaceTempDir:
    def __enter__(self) -> Path:
        self.path = Path(__file__).resolve().parents[1] / ".tmp_test_workflow_logging"
        if self.path.exists():
            shutil.rmtree(self.path)
        self.path.mkdir(parents=True, exist_ok=True)
        return self.path

    def __exit__(self, exc_type, exc, tb) -> bool:
        if self.path.exists():
            shutil.rmtree(self.path)
        return False


class WorkflowLoggingTests(unittest.TestCase):
    def test_build_workflow_logger_writes_text_and_jsonl_logs(self) -> None:
        with WorkspaceTempDir() as temp_dir:
            logger = build_workflow_logger(temp_dir, DEFAULT_CONFIG, "snapshot")
            logger.stage_started("snapshot", total_products=2)
            logger.parse_result("snapshot", parsed_count=4)

            self.assertIsNotNone(logger.log_path)
            self.assertIsNotNone(logger.event_log_path)
            self.assertTrue(logger.log_path.exists())
            self.assertTrue(logger.event_log_path.exists())

            log_text = logger.log_path.read_text(encoding="utf-8")
            event_text = logger.event_log_path.read_text(encoding="utf-8")
            logger.close()

        self.assertIn("[snapshot] stage_started", log_text)
        self.assertIn('"event": "parse_result"', event_text)

    def test_progress_falls_back_when_notebook_tqdm_fails(self) -> None:
        with WorkspaceTempDir() as temp_dir:
            logger = build_workflow_logger(temp_dir, DEFAULT_CONFIG, "snapshot")
            with patch("workflow_logging.is_notebook_environment", return_value=True):
                with patch("workflow_logging.tqdm_notebook", side_effect=RuntimeError("frontend missing")):
                    with patch("workflow_logging.tqdm_auto", side_effect=lambda iterable, **_: iterable):
                        items = list(logger.progress([1, 2, 3], desc="Snapshot"))
            logger.close()

        self.assertEqual(items, [1, 2, 3])


if __name__ == "__main__":
    unittest.main()
