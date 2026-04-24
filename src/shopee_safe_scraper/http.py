from __future__ import annotations

import json
import random
import re
import time
from dataclasses import dataclass
from pathlib import Path
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
