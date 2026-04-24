from __future__ import annotations

import json
import random
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


def detect_block_condition(
    status_code: int,
    text: str,
    final_url: str,
    stop_on_status: list[int],
    stop_on_keywords: list[str] | None = None,
) -> str | None:
    if status_code in stop_on_status:
        return f"status_{status_code}"
    text_lower = (text or "").lower()
    markers = stop_on_keywords or list(BLOCK_MARKERS)
    for marker in markers:
        if marker.lower() in text_lower:
            return f"keyword_{marker.lower()}"
    final_lower = (final_url or "").lower()
    if "login" in final_lower or "signin" in final_lower:
        return "forced_login_redirect"
    if not text.strip():
        return "empty_payload"
    return None


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
