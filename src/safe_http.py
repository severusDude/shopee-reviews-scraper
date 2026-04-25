from __future__ import annotations

import json
import random
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
import traceback
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen


BLOCK_MARKERS = (
    "captcha",
    "verify you are human",
    "access denied",
    "forbidden",
    "login",
    "sign in",
)

TITLE_RE = re.compile(r"<title[^>]*>(?P<title>.*?)</title>", flags=re.IGNORECASE | re.DOTALL)
SCRIPT_STYLE_RE = re.compile(r"<(script|style)\b[^>]*>.*?</\1>", flags=re.IGNORECASE | re.DOTALL)
COMMENT_RE = re.compile(r"<!--.*?-->", flags=re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")


@dataclass
class BlockDetectionResult:
    reason: str | None
    signal_class: str | None = None
    signal_source: str | None = None
    matched_text: str | None = None


@dataclass
class FetchResult:
    url: str
    final_url: str
    status_code: int
    headers: dict[str, str]
    text: str
    elapsed_s: float
    size_bytes: int
    error: str | None = None


@dataclass
class BrowserReviewFallbackResult:
    rows: list[dict[str, Any]]
    payload_pages: list[dict[str, Any]]
    artifacts: list[dict[str, Any]]
    error_code: str | None = None
    error_type: str | None = None
    error_message: str | None = None
    error_repr: str | None = None
    error_traceback: str | None = None

    @property
    def error(self) -> str | None:
        if not any((self.error_code, self.error_type, self.error_message)):
            return None
        parts = [part for part in (self.error_code, self.error_type, self.error_message) if part]
        return ": ".join(parts)


def _extract_title(text: str) -> str:
    match = TITLE_RE.search(text or "")
    if not match:
        return ""
    return re.sub(r"\s+", " ", match.group("title")).strip().lower()


def _extract_visible_text(text: str) -> str:
    without_scripts = SCRIPT_STYLE_RE.sub(" ", text or "")
    without_comments = COMMENT_RE.sub(" ", without_scripts)
    without_tags = TAG_RE.sub(" ", without_comments)
    return re.sub(r"\s+", " ", without_tags).strip().lower()


def inspect_block_condition(
    status_code: int,
    text: str,
    final_url: str,
    stop_on_status: list[int],
    stop_on_keywords: list[str] | None = None,
) -> BlockDetectionResult:
    if status_code in stop_on_status:
        return BlockDetectionResult(
            reason=f"status_{status_code}",
            signal_class="status_code",
            signal_source="http_status",
            matched_text=str(status_code),
        )

    if not (text or "").strip():
        return BlockDetectionResult(
            reason="empty_payload",
            signal_class="payload",
            signal_source="body",
        )

    final_lower = (final_url or "").lower()
    if "login" in final_lower or "signin" in final_lower:
        return BlockDetectionResult(
            reason="forced_login_redirect",
            signal_class="redirect",
            signal_source="final_url",
            matched_text=final_url,
        )

    markers = [marker.lower().strip() for marker in (stop_on_keywords or list(BLOCK_MARKERS)) if marker.strip()]
    title_text = _extract_title(text)
    visible_text = _extract_visible_text(text)

    for marker in markers:
        if marker in {"login", "sign in"}:
            if marker in title_text:
                return BlockDetectionResult(
                    reason=f"keyword_{marker}",
                    signal_class="keyword",
                    signal_source="title",
                    matched_text=marker,
                )
            continue
        if marker in title_text:
            return BlockDetectionResult(
                reason=f"keyword_{marker}",
                signal_class="keyword",
                signal_source="title",
                matched_text=marker,
            )
        if marker in visible_text:
            return BlockDetectionResult(
                reason=f"keyword_{marker}",
                signal_class="keyword",
                signal_source="visible_text",
                matched_text=marker,
            )

    return BlockDetectionResult(reason=None)


def detect_block_condition(
    status_code: int,
    text: str,
    final_url: str,
    stop_on_status: list[int],
    stop_on_keywords: list[str] | None = None,
) -> str | None:
    return inspect_block_condition(
        status_code=status_code,
        text=text,
        final_url=final_url,
        stop_on_status=stop_on_status,
        stop_on_keywords=stop_on_keywords,
    ).reason


class SafeCrawler:
    def __init__(self, config: dict[str, Any], sleep: bool = True):
        self.config = config
        self.sleep = sleep
        self.request_count = 0
        self.consecutive_empty_payloads = 0

    def _maybe_delay(self) -> None:
        if self.request_count == 0 or not self.sleep:
            return
        delay = random.uniform(
            float(self.config["min_delay_s"]),
            float(self.config["max_delay_s"]),
        )
        time.sleep(delay)
        every = int(self.config["cooldown_every_n_requests"])
        if every and self.request_count % every == 0:
            cooldown = random.uniform(
                float(self.config["cooldown_min_s"]),
                float(self.config["cooldown_max_s"]),
            )
            time.sleep(cooldown)

    def fetch(self, url: str) -> FetchResult:
        if self.request_count >= int(self.config["max_requests_per_run"]):
            raise RuntimeError("per-run request cap reached")

        self._maybe_delay()
        self.request_count += 1
        started = time.perf_counter()
        request = Request(
            url,
            headers={
                "User-Agent": self.config["user_agent"],
                "Accept-Language": "id-ID,id;q=0.9,en;q=0.8",
            },
        )

        try:
            with urlopen(request, timeout=float(self.config["timeout_s"])) as response:
                raw = response.read()
                text = raw.decode("utf-8", errors="replace")
                result = FetchResult(
                    url=url,
                    final_url=response.geturl(),
                    status_code=getattr(response, "status", 200),
                    headers=dict(response.headers.items()),
                    text=text,
                    elapsed_s=time.perf_counter() - started,
                    size_bytes=len(raw),
                )
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            result = FetchResult(
                url=url,
                final_url=url,
                status_code=exc.code,
                headers=dict(exc.headers.items()) if exc.headers else {},
                text=body,
                elapsed_s=time.perf_counter() - started,
                size_bytes=len(body.encode("utf-8")),
                error=f"HTTPError: {exc}",
            )
        except URLError as exc:
            result = FetchResult(
                url=url,
                final_url=url,
                status_code=0,
                headers={},
                text="",
                elapsed_s=time.perf_counter() - started,
                size_bytes=0,
                error=f"URLError: {exc}",
            )

        if not result.text.strip():
            self.consecutive_empty_payloads += 1
        else:
            self.consecutive_empty_payloads = 0
        return result


def append_jsonl(path: str | Path, row: dict[str, Any]) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    return path


def write_json(path: str | Path, payload: dict[str, Any] | list[dict[str, Any]]) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def save_text(path: str | Path, text: str) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def absolutize_links(base_url: str, links: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for link in links:
        absolute = urljoin(base_url, link)
        if absolute not in seen:
            deduped.append(absolute)
            seen.add(absolute)
    return deduped


def _truncate_text(text: str | None, limit: int = 4000) -> str | None:
    if text is None:
        return None
    if len(text) <= limit:
        return text
    return text[:limit] + "...<truncated>"


def _browser_error_result(
    *,
    error_code: str,
    exc: Exception | None = None,
    artifacts: list[dict[str, Any]] | None = None,
    error_message: str | None = None,
) -> BrowserReviewFallbackResult:
    exc_type = type(exc).__name__ if exc is not None else None
    exc_message = error_message if error_message is not None else (str(exc) if exc is not None else None)
    exc_repr = repr(exc) if exc is not None else None
    tb_text = None
    if exc is not None:
        tb_text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    artifacts = list(artifacts or [])
    if any((error_code, exc_type, exc_message, exc_repr, tb_text)):
        artifacts.append(
            {
                "kind": "diagnostic_json",
                "page_no": "0",
                "source_url": "",
                "content": {
                    "error_code": error_code,
                    "error_type": exc_type,
                    "error_message": exc_message,
                    "error_repr": exc_repr,
                    "error_traceback": _truncate_text(tb_text),
                },
            }
        )
    return BrowserReviewFallbackResult(
        rows=[],
        payload_pages=[],
        artifacts=artifacts,
        error_code=error_code,
        error_type=exc_type,
        error_message=exc_message,
        error_repr=exc_repr,
        error_traceback=_truncate_text(tb_text),
    )


def _classify_browser_exception(exc: Exception, phase: str) -> str:
    message = str(exc or "").lower()
    if isinstance(exc, ImportError):
        return "playwright_import_error"
    if isinstance(exc, PermissionError):
        return "playwright_permission_denied"
    if isinstance(exc, NotImplementedError):
        return "playwright_event_loop_conflict"
    if "inside the asyncio loop" in message:
        return "playwright_event_loop_conflict"
    if "executable doesn't exist" in message or "browserType.launch" in message and "executable" in message:
        return "playwright_browser_missing"
    if "timeout" in message:
        return "playwright_navigation_timeout" if phase == "navigation" else "playwright_startup_failed"
    return "playwright_startup_failed" if phase == "startup" else "browser_fallback_error"


def fetch_reviews_with_browser_fallback(
    *,
    product_url: str,
    config: dict[str, Any],
    max_pages: int,
) -> BrowserReviewFallbackResult:
    request_payload = {
        "product_url": product_url,
        "config": {
            "browser_headless": bool(config.get("browser_headless", True)),
            "browser_startup_probe_timeout_s": float(config.get("browser_startup_probe_timeout_s", 10)),
            "browser_timeout_s": float(config.get("browser_timeout_s", 30)),
            "user_agent": str(config.get("user_agent") or ""),
        },
        "max_pages": max_pages,
    }
    helper_path = Path(__file__).with_name("browser_runner.py")
    command = [sys.executable, str(helper_path)]
    timeout_s = max(
        float(config.get("browser_timeout_s", 30)) + float(config.get("browser_startup_probe_timeout_s", 10)) + 15.0,
        30.0,
    )

    try:
        result = subprocess.run(
            command,
            input=json.dumps(request_payload, ensure_ascii=False),
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=timeout_s,
            cwd=str(Path(__file__).resolve().parents[1]),
        )
    except subprocess.TimeoutExpired as exc:
        return _browser_error_result(error_code="playwright_navigation_timeout", exc=exc)
    except Exception as exc:  # pragma: no cover - defensive around process launch
        return _browser_error_result(error_code="playwright_process_failed", exc=exc)

    stdout_text = (result.stdout or "").strip()
    stderr_text = (result.stderr or "").strip()
    if result.returncode != 0:
        return BrowserReviewFallbackResult(
            rows=[],
            payload_pages=[],
            artifacts=[
                {
                    "kind": "diagnostic_json",
                    "page_no": "0",
                    "source_url": "",
                    "content": {
                        "command": command,
                        "returncode": result.returncode,
                        "stdout": _truncate_text(stdout_text),
                        "stderr": _truncate_text(stderr_text),
                    },
                }
            ],
            error_code="playwright_process_failed",
            error_type="CalledProcessError",
            error_message=f"helper exited with code {result.returncode}",
            error_repr=f"CalledProcessError(returncode={result.returncode})",
            error_traceback=_truncate_text(stderr_text),
        )

    try:
        payload = json.loads(stdout_text)
    except json.JSONDecodeError as exc:
        return BrowserReviewFallbackResult(
            rows=[],
            payload_pages=[],
            artifacts=[
                {
                    "kind": "diagnostic_json",
                    "page_no": "0",
                    "source_url": "",
                    "content": {
                        "stdout": _truncate_text(stdout_text),
                        "stderr": _truncate_text(stderr_text),
                    },
                }
            ],
            error_code="playwright_process_output_invalid",
            error_type=type(exc).__name__,
            error_message=str(exc),
            error_repr=repr(exc),
            error_traceback=_truncate_text(stderr_text),
        )

    return BrowserReviewFallbackResult(
        rows=list(payload.get("rows") or []),
        payload_pages=list(payload.get("payload_pages") or []),
        artifacts=list(payload.get("artifacts") or []),
        error_code=payload.get("error_code"),
        error_type=payload.get("error_type"),
        error_message=payload.get("error_message"),
        error_repr=payload.get("error_repr"),
        error_traceback=payload.get("error_traceback"),
    )
