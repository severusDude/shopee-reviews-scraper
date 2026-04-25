from __future__ import annotations

import json
import asyncio
import sys
import traceback
import warnings
from pathlib import Path
from typing import Any

from parser import parse_reviews_from_payload, parse_reviews_from_rendered_html


def _configure_windows_event_loop_for_playwright() -> None:
    if sys.platform != "win32":
        return
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        policy_factory = getattr(asyncio, "WindowsProactorEventLoopPolicy", None)
        if policy_factory is not None:
            asyncio.set_event_loop_policy(policy_factory())
    if policy_factory is None:
        return


def _truncate_text(text: str | None, limit: int = 4000) -> str | None:
    if text is None:
        return None
    if len(text) <= limit:
        return text
    return text[:limit] + "...<truncated>"


def _classify_browser_exception(exc: Exception, phase: str) -> str:
    message = str(exc or "").lower()
    if isinstance(exc, ImportError):
        return "playwright_import_error"
    if isinstance(exc, PermissionError):
        return "playwright_permission_denied"
    if isinstance(exc, NotImplementedError):
        return "playwright_event_loop_conflict"
    if "executable doesn't exist" in message or ("browsertype.launch" in message and "executable" in message):
        return "playwright_browser_missing"
    if "timeout" in message:
        return "playwright_navigation_timeout" if phase == "navigation" else "playwright_startup_failed"
    return "playwright_startup_failed" if phase == "startup" else "browser_fallback_error"


def _error_payload(
    *,
    error_code: str,
    exc: Exception | None = None,
    artifacts: list[dict[str, Any]] | None = None,
    error_message: str | None = None,
) -> dict[str, Any]:
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
    return {
        "rows": [],
        "payload_pages": [],
        "artifacts": artifacts,
        "error_code": error_code,
        "error_type": exc_type,
        "error_message": exc_message,
        "error_repr": exc_repr,
        "error_traceback": _truncate_text(tb_text),
    }


def _load_request() -> dict[str, Any]:
    return json.loads(sys.stdin.read())


def main() -> int:
    request = _load_request()
    product_url = str(request["product_url"])
    config = dict(request["config"])
    max_pages = int(request["max_pages"])
    _configure_windows_event_loop_for_playwright()

    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        print(json.dumps(_error_payload(error_code="playwright_import_error", exc=exc), ensure_ascii=False))
        return 0

    rows: list[dict[str, Any]] = []
    payload_pages: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []
    browser = None
    playwright = None
    context = None

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
        startup_probe_page = context.new_page()
        startup_timeout_ms = int(float(config.get("browser_startup_probe_timeout_s", 10)) * 1000)
        startup_probe_page.goto("about:blank", wait_until="domcontentloaded", timeout=startup_timeout_ms)
        startup_probe_page.close()

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

        print(
            json.dumps(
                {
                    "rows": rows,
                    "payload_pages": payload_pages[:max_pages],
                    "artifacts": artifacts,
                    "error_code": None,
                    "error_type": None,
                    "error_message": None,
                    "error_repr": None,
                    "error_traceback": None,
                },
                ensure_ascii=False,
            )
        )
        return 0
    except PlaywrightTimeoutError as exc:
        print(json.dumps(_error_payload(error_code="playwright_navigation_timeout", exc=exc, artifacts=artifacts), ensure_ascii=False))
        return 0
    except Exception as exc:
        phase = "startup" if browser is None else "navigation"
        print(
            json.dumps(
                _error_payload(
                    error_code=_classify_browser_exception(exc, phase),
                    exc=exc,
                    artifacts=artifacts,
                ),
                ensure_ascii=False,
            )
        )
        return 0
    finally:
        if context is not None:
            context.close()
        if browser is not None:
            browser.close()
        if playwright is not None:
            playwright.stop()


if __name__ == "__main__":
    raise SystemExit(main())
