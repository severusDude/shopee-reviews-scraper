from __future__ import annotations

import unittest


def run_browser_smoke() -> str:
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto("https://example.com")
        title = page.title()
        browser.close()
        return title


class BrowserSmokeTests(unittest.TestCase):
    def test_sync_playwright_smoke(self) -> None:
        try:
            title = run_browser_smoke()
        except Exception as exc:
            self.skipTest(f"browser smoke unavailable in this environment: {exc}")
        self.assertEqual(title, "Example Domain")


if __name__ == "__main__":
    print(run_browser_smoke())
