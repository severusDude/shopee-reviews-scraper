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


@dataclass
class BrowserReviewFallbackResult:
    rows: list[dict[str, Any]]
    payload_pages: list[dict[str, Any]]
    artifacts: list[dict[str, str]]
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


def fetch_reviews_with_browser_fallback(
    *,
    product_url: str,
    config: dict[str, Any],
    max_pages: int,
) -> BrowserReviewFallbackResult:
    from parser import parse_reviews_from_payload, parse_reviews_from_rendered_html

    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except ImportError as exc:  # pragma: no cover - depends on optional dependency
        return BrowserReviewFallbackResult(
            rows=[],
            payload_pages=[],
            artifacts=[],
            error=f"playwright_import_error: {exc}",
        )

    rows: list[dict[str, Any]] = []
    payload_pages: list[dict[str, Any]] = []
    artifacts: list[dict[str, str]] = []
    browser = None
    playwright = None

    def maybe_capture_payload(response: Any) -> None:
        resource_type = ""
        try:
            resource_type = response.request.resource_type
        except Exception:
            resource_type = ""
        if resource_type not in {"xhr", "fetch"}:
            return
        try:
            payload = response.json()
        except Exception:
            return
        parsed_rows = parse_reviews_from_payload(
            payload,
            product_url=product_url,
            review_page=len(payload_pages) + 1,
            source_url=getattr(response, "url", product_url),
        )
        if not parsed_rows:
            return
        payload_pages.append(
            {
                "page_no": len(payload_pages) + 1,
                "source_url": getattr(response, "url", product_url),
                "payload": payload,
                "rows": parsed_rows,
            }
        )
        artifacts.append(
            {
                "kind": "payload_json",
                "page_no": str(len(payload_pages)),
                "source_url": getattr(response, "url", product_url),
                "content": payload,
            }
        )
        rows.extend(parsed_rows)

    try:
        playwright = sync_playwright().start()
        browser = playwright.chromium.launch(headless=bool(config.get("browser_headless", True)))
        context = browser.new_context(
            user_agent=str(config.get("user_agent") or ""),
            locale="id-ID",
        )
        page = context.new_page()
        page.on("response", maybe_capture_payload)
        timeout_ms = int(float(config.get("browser_timeout_s", 30)) * 1000)
        page.goto(product_url, wait_until="domcontentloaded", timeout=timeout_ms)
        page.wait_for_timeout(1500)

        for selector in (
            "text=Penilaian Produk",
            "text=Ulasan",
            "text=Ratings",
            "[href*='rating']",
            "[href*='review']",
        ):
            try:
                page.locator(selector).first.click(timeout=2000)
                page.wait_for_timeout(1200)
                break
            except Exception:
                continue

        for _ in range(max(max_pages, 1)):
            page.mouse.wheel(0, 2500)
            page.wait_for_timeout(800)

        rendered_html = page.content()
        artifacts.append(
            {
                "kind": "rendered_html",
                "page_no": "1",
                "source_url": page.url,
                "content": rendered_html,
            }
        )

        if not rows:
            dom_rows = parse_reviews_from_rendered_html(
                rendered_html,
                product_url=product_url,
                source_url=page.url,
            )
            rows.extend(dom_rows)

        return BrowserReviewFallbackResult(
            rows=rows,
            payload_pages=payload_pages[:max_pages],
            artifacts=artifacts,
        )
    except PlaywrightTimeoutError as exc:
        return BrowserReviewFallbackResult(
            rows=[],
            payload_pages=[],
            artifacts=artifacts,
            error=f"playwright_timeout: {exc}",
        )
    except Exception as exc:  # pragma: no cover - defensive around browser runtime
        return BrowserReviewFallbackResult(
            rows=[],
            payload_pages=[],
            artifacts=artifacts,
            error=f"browser_fallback_error: {exc}",
        )
    finally:
        if browser is not None:
            browser.close()
        if playwright is not None:
            playwright.stop()
