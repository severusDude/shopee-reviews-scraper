from __future__ import annotations

import json
import logging
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator

try:
    from tqdm.auto import tqdm as tqdm_auto
    from tqdm.notebook import tqdm as tqdm_notebook
except ImportError:  # pragma: no cover - exercised only before dependency install
    def tqdm_auto(iterable: Iterable[Any] | None = None, **_: Any) -> Any:
        return iterable if iterable is not None else _NullProgressBar(total=0, desc="")

    def tqdm_notebook(iterable: Iterable[Any] | None = None, **_: Any) -> Any:
        return iterable if iterable is not None else _NullProgressBar(total=0, desc="")


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _coerce_log_level(level_name: str | None) -> int:
    candidate = (level_name or "INFO").upper()
    return getattr(logging, candidate, logging.INFO)


def is_notebook_environment() -> bool:
    try:
        from IPython import get_ipython
    except ImportError:
        return False
    shell = get_ipython()
    if shell is None:
        return False
    return shell.__class__.__name__ == "ZMQInteractiveShell"


@dataclass
class WorkflowLogger:
    logger: logging.Logger
    log_path: Path | None
    event_log_path: Path | None
    progress_enabled: bool
    progress_style: str
    verbose_notebook_events: bool

    def close(self) -> None:
        for handler in list(self.logger.handlers):
            handler.flush()
            handler.close()
            self.logger.removeHandler(handler)

    def stage_started(self, stage: str, **context: Any) -> None:
        self.event(stage, "stage_started", level=logging.INFO, **context)

    def stage_finished(self, stage: str, **context: Any) -> None:
        self.event(stage, "stage_finished", level=logging.INFO, **context)

    def fetch_result(self, stage: str, **context: Any) -> None:
        self.event(stage, "fetch_result", level=logging.INFO, **context)

    def parse_result(self, stage: str, **context: Any) -> None:
        self.event(stage, "parse_result", level=logging.INFO, **context)

    def stop_condition(self, stage: str, **context: Any) -> None:
        self.event(stage, "stop_condition", level=logging.WARNING, **context)

    def export_result(self, stage: str, **context: Any) -> None:
        self.event(stage, "export_result", level=logging.INFO, **context)

    def warning(self, stage: str, event: str, **context: Any) -> None:
        self.event(stage, event, level=logging.WARNING, **context)

    def error(self, stage: str, event: str, **context: Any) -> None:
        self.event(stage, event, level=logging.ERROR, **context)

    def event(self, stage: str, event: str, level: int = logging.INFO, **context: Any) -> None:
        payload = {
            "timestamp": _utc_timestamp(),
            "stage": stage,
            "event": event,
            **{key: value for key, value in context.items() if value is not None},
        }
        message = self._format_message(payload)
        self.logger.log(level, message)
        if self.event_log_path is not None:
            with self.event_log_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def progress(
        self,
        iterable: Iterable[Any],
        *,
        desc: str,
        total: int | None = None,
        leave: bool = False,
    ) -> Iterator[Any]:
        if not self.progress_enabled:
            return iter(iterable)
        factory = tqdm_notebook if self._use_notebook_progress() else tqdm_auto
        try:
            return factory(iterable, total=total, desc=desc, leave=leave)
        except Exception:
            return tqdm_auto(iterable, total=total, desc=desc, leave=leave)

    @contextmanager
    def progress_context(self, total: int, *, desc: str, leave: bool = False) -> Iterator[Any]:
        if not self.progress_enabled:
            yield _NullProgressBar(total=total, desc=desc)
            return
        factory = tqdm_notebook if self._use_notebook_progress() else tqdm_auto
        progress_bar = factory(total=total, desc=desc, leave=leave)
        try:
            yield progress_bar
        finally:
            progress_bar.close()

    def _use_notebook_progress(self) -> bool:
        return self.progress_style == "notebook" and is_notebook_environment()

    def _format_message(self, payload: dict[str, Any]) -> str:
        stage = payload["stage"]
        event = payload["event"]
        context = []
        for key, value in payload.items():
            if key in {"timestamp", "stage", "event"}:
                continue
            if key == "message":
                continue
            context.append(f"{key}={value}")
        suffix = f" | {'; '.join(context)}" if context else ""
        if "message" in payload:
            return f"[{stage}] {event}: {payload['message']}{suffix}"
        return f"[{stage}] {event}{suffix}"


class _NullProgressBar:
    def __init__(self, total: int, desc: str):
        self.total = total
        self.desc = desc

    def update(self, _: int = 1) -> None:
        return None

    def set_postfix_str(self, _: str) -> None:
        return None

    def close(self) -> None:
        return None


def build_workflow_logger(project_root: str | Path, config: dict[str, Any], stage: str) -> WorkflowLogger:
    root = Path(project_root)
    logs_dir = root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    logging_config = dict(config.get("logging") or {})
    progress_config = dict(config.get("progress") or {})
    logger_name = f"workflow.{stage}.{id(root)}"
    logger = logging.getLogger(logger_name)
    for handler in list(logger.handlers):
        handler.flush()
        handler.close()
        logger.removeHandler(handler)
    logger.setLevel(_coerce_log_level(logging_config.get("level")))
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    log_path: Path | None = None
    if logging_config.get("file", True):
        log_path = logs_dir / f"{stage}.log"
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    if logging_config.get("console", True):
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    if not logger.handlers:
        logger.addHandler(logging.NullHandler())

    event_log_path = None
    if logging_config.get("event_jsonl", True):
        event_log_path = logs_dir / f"{stage}_events.jsonl"
        event_log_path.touch(exist_ok=True)

    return WorkflowLogger(
        logger=logger,
        log_path=log_path,
        event_log_path=event_log_path,
        progress_enabled=bool(progress_config.get("enabled", True)),
        progress_style=str(progress_config.get("style", "notebook")).strip().lower(),
        verbose_notebook_events=bool(logging_config.get("verbose_notebook_events", True)),
    )
