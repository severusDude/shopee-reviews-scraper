from pathlib import Path
import json
import shutil
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
import subprocess

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from config import DEFAULT_CONFIG, save_config
from parser import (
    detect_shell_page,
    discover_review_links,
    parse_product_snapshot,
    parse_reviews_from_html,
    parse_reviews_from_payload,
    parse_reviews_from_rendered_html,
)
from pipeline import ensure_project_layout, harvest_reviews, snapshot_seed_products
from safe_http import BrowserReviewFallbackResult, _classify_browser_exception, fetch_reviews_with_browser_fallback


SAMPLE_HTML = """
<html>
  <head>
    <title>Produk Contoh Shopee</title>
    <meta property="og:title" content="Produk Contoh Shopee" />
    <meta property="og:category" content="Elektronik > Audio" />
    <meta property="description" content="Deskripsi singkat produk." />
    <script type="application/ld+json">
      {
        "@context": "https://schema.org",
        "@type": "Product",
        "name": "Produk Contoh Shopee",
        "description": "Deskripsi singkat produk.",
        "aggregateRating": {
          "@type": "AggregateRating",
          "ratingValue": "4.9",
          "reviewCount": "128"
        },
        "offers": {
          "@type": "Offer",
          "price": "199000"
        }
      }
    </script>
    <script>
      window.__STATE__ = {
        "shop_name": "Toko Aman",
        "shop_url": "https://shopee.co.id/toko-aman",
        "ratings": [
          {
            "comment": "Barang bagus sekali",
            "rating_star": 5,
            "ctime": "2026-04-01",
            "variation": "Hitam",
            "author": "Lutfi",
            "images": ["a.jpg"],
            "seller_reply": {"comment": "Terima kasih"}
          },
          {
            "comment": "Packing rapi",
            "rating": 4,
            "created_at": "2026-04-02",
            "variant": "Putih",
            "username": "Budi"
          }
        ]
      };
    </script>
  </head>
  <body>
    <a href="/produk/contoh/reviews?page=2">next</a>
  </body>
</html>
"""

SHELL_HTML = (Path(__file__).resolve().parents[1] / "data" / "raw" / "html" / "review_page_001_001.html").read_text(encoding="utf-8")

SAMPLE_PAYLOAD = {
    "data": {
        "ratings": [
            {
                "comment": "Barang mantap",
                "rating_star": 5,
                "ctime": "2026-04-03",
                "variation": "Hitam",
                "author": "Sinta",
                "images": ["a.jpg"],
                "seller_reply": {"comment": "Terima kasih"},
            },
            {
                "comment_text": "Sesuai foto",
                "rating": 4,
                "created_at": "2026-04-04",
                "variant": "Putih",
                "username": "Budi",
            },
        ]
    }
}

RENDERED_REVIEW_HTML = """
<html>
  <body>
    <div class="review-card">
      5 star Barang bagus sekali 2026-04-05 variasi: Hitam 3 helpful
      <img src="a.jpg" />
      <div>Balasan penjual</div>
    </div>
    <div class="rating-card">
      4/5 Sesuai deskripsi 2026-04-06 variant: Putih
    </div>
  </body>
</html>
"""


class WorkspaceTempDir:
    def __enter__(self) -> Path:
        self.path = Path(__file__).resolve().parents[1] / ".tmp_test_parser"
        if self.path.exists():
            shutil.rmtree(self.path)
        self.path.mkdir(parents=True, exist_ok=True)
        return self.path

    def __exit__(self, exc_type, exc, tb) -> bool:
        if self.path.exists():
            shutil.rmtree(self.path)
        return False


class ParserTests(unittest.TestCase):
    def test_product_snapshot_extracts_required_fields(self) -> None:
        record = parse_product_snapshot(
            SAMPLE_HTML,
            product_url="https://shopee.co.id/produk-contoh-i.123.456",
        )
        self.assertEqual(record["title"], "Produk Contoh Shopee")
        self.assertEqual(record["shop_name"], "Toko Aman")
        self.assertEqual(record["rating_avg_display"], "4.9")
        self.assertGreaterEqual(record["required_field_completeness"], 0.5)

    def test_review_parser_extracts_review_rows(self) -> None:
        reviews = parse_reviews_from_html(
            SAMPLE_HTML,
            product_url="https://shopee.co.id/produk-contoh-i.123.456",
        )
        self.assertEqual(len(reviews), 2)
        self.assertTrue(bool(reviews[0]["media_flag"]))
        self.assertTrue(bool(reviews[0]["seller_reply_flag"]))

    def test_review_link_discovery_keeps_base_url(self) -> None:
        links = discover_review_links(
            SAMPLE_HTML,
            base_url="https://shopee.co.id/produk-contoh-i.123.456",
        )
        self.assertEqual(links[0], "https://shopee.co.id/produk-contoh-i.123.456")
        self.assertIn("page=2", links[1])

    def test_detect_shell_page_flags_saved_shell_html(self) -> None:
        shell_details = detect_shell_page(
            SHELL_HTML,
            review_rows=parse_reviews_from_html(
                SHELL_HTML,
                product_url="https://shopee.co.id/contoh-i.1.2",
            ),
            product_record=parse_product_snapshot(
                SHELL_HTML,
                product_url="https://shopee.co.id/contoh-i.1.2",
            ),
        )
        self.assertEqual(shell_details.reason, "shell_page_no_embedded_reviews")

    def test_parse_reviews_from_payload_extracts_rows(self) -> None:
        reviews = parse_reviews_from_payload(
            SAMPLE_PAYLOAD,
            product_url="https://shopee.co.id/produk-contoh-i.123.456",
        )
        self.assertEqual(len(reviews), 2)
        self.assertTrue(bool(reviews[0]["media_flag"]))
        self.assertTrue(bool(reviews[0]["seller_reply_flag"]))

    def test_parse_reviews_from_rendered_html_extracts_rows(self) -> None:
        reviews = parse_reviews_from_rendered_html(
            RENDERED_REVIEW_HTML,
            product_url="https://shopee.co.id/produk-contoh-i.123.456",
        )
        self.assertEqual(len(reviews), 2)
        self.assertEqual(str(reviews[0]["star_rating"]), "5")
        self.assertTrue(bool(reviews[0]["seller_reply_flag"]))

    def test_classify_browser_exception_maps_permission_denied(self) -> None:
        exc = PermissionError(13, "Access is denied", None, 5, None)
        self.assertEqual(_classify_browser_exception(exc, "startup"), "playwright_permission_denied")

    def test_classify_browser_exception_maps_event_loop_conflict(self) -> None:
        exc = RuntimeError("It looks like you are using Playwright Sync API inside the asyncio loop.")
        self.assertEqual(_classify_browser_exception(exc, "startup"), "playwright_event_loop_conflict")

    def test_classify_browser_exception_maps_subprocess_loop_conflict(self) -> None:
        exc = NotImplementedError()
        self.assertEqual(_classify_browser_exception(exc, "startup"), "playwright_event_loop_conflict")

    def test_fetch_reviews_with_browser_fallback_parses_helper_json(self) -> None:
        browser_rows = parse_reviews_from_payload(
            SAMPLE_PAYLOAD,
            product_url="https://shopee.co.id/produk-contoh-i.123.456",
        )
        completed = subprocess.CompletedProcess(
            args=["python", "browser_runner.py"],
            returncode=0,
            stdout=json.dumps(
                {
                    "rows": browser_rows,
                    "payload_pages": [
                        {
                            "page_no": 1,
                            "source_url": "https://shopee.co.id/api/v4/item/get_ratings",
                            "payload": SAMPLE_PAYLOAD,
                            "rows": browser_rows,
                        }
                    ],
                    "artifacts": [],
                    "error_code": None,
                    "error_type": None,
                    "error_message": None,
                    "error_repr": None,
                    "error_traceback": None,
                }
            ),
            stderr="",
        )
        with patch("safe_http.subprocess.run", return_value=completed):
            result = fetch_reviews_with_browser_fallback(
                product_url="https://shopee.co.id/produk-contoh-i.123.456",
                config=DEFAULT_CONFIG,
                max_pages=2,
            )
        self.assertEqual(len(result.rows), 2)
        self.assertIsNone(result.error_code)

    def test_fetch_reviews_with_browser_fallback_handles_helper_nonzero_exit(self) -> None:
        completed = subprocess.CompletedProcess(
            args=["python", "browser_runner.py"],
            returncode=7,
            stdout="",
            stderr="boom",
        )
        with patch("safe_http.subprocess.run", return_value=completed):
            result = fetch_reviews_with_browser_fallback(
                product_url="https://shopee.co.id/produk-contoh-i.123.456",
                config=DEFAULT_CONFIG,
                max_pages=2,
            )
        self.assertEqual(result.error_code, "playwright_process_failed")
        self.assertEqual(result.error_type, "CalledProcessError")

    def test_fetch_reviews_with_browser_fallback_handles_invalid_helper_output(self) -> None:
        completed = subprocess.CompletedProcess(
            args=["python", "browser_runner.py"],
            returncode=0,
            stdout="not-json",
            stderr="",
        )
        with patch("safe_http.subprocess.run", return_value=completed):
            result = fetch_reviews_with_browser_fallback(
                product_url="https://shopee.co.id/produk-contoh-i.123.456",
                config=DEFAULT_CONFIG,
                max_pages=2,
            )
        self.assertEqual(result.error_code, "playwright_process_output_invalid")

    def test_snapshot_seed_products_fails_fast_when_seed_urls_blank(self) -> None:
        with WorkspaceTempDir() as root:
            ensure_project_layout(root)
            save_config(root / "config.yaml", DEFAULT_CONFIG)
            pd.DataFrame(
                [
                    {
                        "product_url": "",
                        "category_quota": "Elektronik",
                        "chosen_reason": "manual seed slot 1",
                        "seed_date": "2026-04-24",
                    }
                ]
            ).to_csv(root / "data" / "interim" / "seed_products.csv", index=False)

            with self.assertRaisesRegex(ValueError, "no valid product_url values"):
                snapshot_seed_products(root, sleep=False)

    def test_snapshot_seed_products_writes_log_artifacts(self) -> None:
        class FakeCrawler:
            def __init__(self, config: dict[str, object], sleep: bool = True):
                self.config = config
                self.sleep = sleep
                self.request_count = 0

            def fetch(self, url: str):
                self.request_count += 1
                return type(
                    "FetchResult",
                    (),
                    {
                        "status_code": 200,
                        "text": SAMPLE_HTML,
                        "final_url": url,
                        "elapsed_s": 0.1,
                        "size_bytes": len(SAMPLE_HTML.encode("utf-8")),
                        "error": None,
                    },
                )()

        with WorkspaceTempDir() as root:
            ensure_project_layout(root)
            save_config(root / "config.yaml", DEFAULT_CONFIG)
            pd.DataFrame(
                [
                    {
                        "product_url": "https://shopee.co.id/produk-contoh-i.123.456",
                        "category_quota": "Elektronik",
                        "chosen_reason": "manual seed slot 1",
                        "seed_date": "2026-04-24",
                    }
                ]
            ).to_csv(root / "data" / "interim" / "seed_products.csv", index=False)

            with patch("pipeline.SafeCrawler", FakeCrawler):
                result = snapshot_seed_products(root, sleep=False)

            self.assertTrue(Path(result["log_path"]).exists())
            self.assertTrue(Path(result["event_log_path"]).exists())
            self.assertTrue((root / "logs" / "product_snapshot_summary.json").exists())

    def test_harvest_reviews_verbose_does_not_call_print(self) -> None:
        class FakeCrawler:
            def __init__(self, config: dict[str, object], sleep: bool = True):
                self.config = config
                self.sleep = sleep
                self.request_count = 0

            def fetch(self, url: str):
                self.request_count += 1
                return type(
                    "FetchResult",
                    (),
                    {
                        "status_code": 200,
                        "text": SAMPLE_HTML,
                        "final_url": url,
                        "elapsed_s": 0.1,
                        "size_bytes": len(SAMPLE_HTML.encode("utf-8")),
                        "error": None,
                    },
                )()

        with WorkspaceTempDir() as root:
            ensure_project_layout(root)
            save_config(root / "config.yaml", DEFAULT_CONFIG)
            pd.DataFrame(
                [
                    {
                        "product_url": "https://shopee.co.id/produk-contoh-i.123.456",
                        "category_quota": "Elektronik",
                        "chosen_reason": "manual seed slot 1",
                        "seed_date": "2026-04-24",
                    }
                ]
            ).to_csv(root / "data" / "interim" / "seed_products.csv", index=False)

            with patch("pipeline.SafeCrawler", FakeCrawler):
                with patch("builtins.print") as mock_print:
                    result = harvest_reviews(root, sleep=False, max_pages_per_product=2, verbose=True)

            self.assertFalse(mock_print.called)
            self.assertIn("log_path", result)
            self.assertTrue(Path(result["event_log_path"]).exists())

    def test_harvest_reviews_uses_browser_fallback_on_shell_page(self) -> None:
        class FakeCrawler:
            def __init__(self, config: dict[str, object], sleep: bool = True):
                self.config = config
                self.sleep = sleep
                self.request_count = 0

            def fetch(self, url: str):
                self.request_count += 1
                return type(
                    "FetchResult",
                    (),
                    {
                        "status_code": 200,
                        "text": SHELL_HTML,
                        "final_url": url,
                        "elapsed_s": 0.1,
                        "size_bytes": len(SHELL_HTML.encode("utf-8")),
                        "error": None,
                    },
                )()

        browser_rows = parse_reviews_from_payload(
            SAMPLE_PAYLOAD,
            product_url="https://shopee.co.id/produk-contoh-i.123.456",
        )

        browser_result = BrowserReviewFallbackResult(
            rows=browser_rows,
            payload_pages=[
                {
                    "page_no": 1,
                    "source_url": "https://shopee.co.id/api/v4/item/get_ratings",
                    "payload": SAMPLE_PAYLOAD,
                    "rows": browser_rows,
                }
            ],
            artifacts=[
                {
                    "kind": "payload_json",
                    "page_no": "1",
                    "source_url": "https://shopee.co.id/api/v4/item/get_ratings",
                    "content": SAMPLE_PAYLOAD,
                }
            ],
        )

        with WorkspaceTempDir() as root:
            ensure_project_layout(root)
            save_config(root / "config.yaml", DEFAULT_CONFIG)
            pd.DataFrame(
                [
                    {
                        "product_url": "https://shopee.co.id/produk-contoh-i.123.456",
                        "category_quota": "Elektronik",
                        "chosen_reason": "manual seed slot 1",
                        "seed_date": "2026-04-24",
                    }
                ]
            ).to_csv(root / "data" / "interim" / "seed_products.csv", index=False)

            with patch("pipeline.SafeCrawler", FakeCrawler):
                with patch("pipeline.fetch_reviews_with_browser_fallback", return_value=browser_result):
                    result = harvest_reviews(root, sleep=False, max_pages_per_product=2, verbose=True)

            self.assertEqual(len(result["reviews"]), 2)
            event_text = Path(result["event_log_path"]).read_text(encoding="utf-8")
            self.assertIn('"event": "shell_page_detected"', event_text)
            self.assertIn('"event": "browser_fallback_started"', event_text)
            self.assertIn('"event": "browser_payload_captured"', event_text)

    def test_harvest_reviews_logs_browser_fallback_failure(self) -> None:
        class FakeCrawler:
            def __init__(self, config: dict[str, object], sleep: bool = True):
                self.config = config
                self.sleep = sleep
                self.request_count = 0

            def fetch(self, url: str):
                self.request_count += 1
                return type(
                    "FetchResult",
                    (),
                    {
                        "status_code": 200,
                        "text": SHELL_HTML,
                        "final_url": url,
                        "elapsed_s": 0.1,
                        "size_bytes": len(SHELL_HTML.encode("utf-8")),
                        "error": None,
                    },
                )()

        browser_result = BrowserReviewFallbackResult(
            rows=[],
            payload_pages=[],
            artifacts=[],
            error_code="playwright_import_error",
            error_type="ModuleNotFoundError",
            error_message="missing",
            error_repr="ModuleNotFoundError('missing')",
        )

        with WorkspaceTempDir() as root:
            ensure_project_layout(root)
            save_config(root / "config.yaml", DEFAULT_CONFIG)
            pd.DataFrame(
                [
                    {
                        "product_url": "https://shopee.co.id/produk-contoh-i.123.456",
                        "category_quota": "Elektronik",
                        "chosen_reason": "manual seed slot 1",
                        "seed_date": "2026-04-24",
                    }
                ]
            ).to_csv(root / "data" / "interim" / "seed_products.csv", index=False)

            with patch("pipeline.SafeCrawler", FakeCrawler):
                with patch("pipeline.fetch_reviews_with_browser_fallback", return_value=browser_result):
                    result = harvest_reviews(root, sleep=False, max_pages_per_product=2, verbose=True)

            self.assertEqual(len(result["reviews"]), 0)
            event_text = Path(result["event_log_path"]).read_text(encoding="utf-8")
            self.assertIn('"event": "browser_fallback_failed"', event_text)
            self.assertIn('"error_code": "playwright_import_error"', event_text)
            self.assertIn('"error_type": "ModuleNotFoundError"', event_text)

    def test_harvest_reviews_skips_repeated_browser_startup_failure(self) -> None:
        class FakeCrawler:
            def __init__(self, config: dict[str, object], sleep: bool = True):
                self.config = config
                self.sleep = sleep
                self.request_count = 0

            def fetch(self, url: str):
                self.request_count += 1
                return type(
                    "FetchResult",
                    (),
                    {
                        "status_code": 200,
                        "text": SHELL_HTML,
                        "final_url": url,
                        "elapsed_s": 0.1,
                        "size_bytes": len(SHELL_HTML.encode("utf-8")),
                        "error": None,
                    },
                )()

        browser_result = BrowserReviewFallbackResult(
            rows=[],
            payload_pages=[],
            artifacts=[],
            error_code="playwright_permission_denied",
            error_type="PermissionError",
            error_message="[WinError 5] Access is denied",
            error_repr="PermissionError(13, 'Access is denied', None, 5, None)",
        )

        with WorkspaceTempDir() as root:
            ensure_project_layout(root)
            save_config(root / "config.yaml", DEFAULT_CONFIG)
            pd.DataFrame(
                [
                    {
                        "product_url": "https://shopee.co.id/produk-contoh-i.123.456",
                        "category_quota": "Elektronik",
                        "chosen_reason": "manual seed slot 1",
                        "seed_date": "2026-04-24",
                    },
                    {
                        "product_url": "https://shopee.co.id/produk-kedua-i.123.789",
                        "category_quota": "Elektronik",
                        "chosen_reason": "manual seed slot 2",
                        "seed_date": "2026-04-24",
                    },
                ]
            ).to_csv(root / "data" / "interim" / "seed_products.csv", index=False)

            with patch("pipeline.SafeCrawler", FakeCrawler):
                with patch("pipeline.fetch_reviews_with_browser_fallback", return_value=browser_result) as mock_fallback:
                    result = harvest_reviews(root, sleep=False, max_pages_per_product=2, verbose=True)

            self.assertEqual(len(result["reviews"]), 0)
            self.assertEqual(mock_fallback.call_count, 1)
            event_text = Path(result["event_log_path"]).read_text(encoding="utf-8")
            self.assertIn('"event": "browser_runtime_unavailable"', event_text)


if __name__ == "__main__":
    unittest.main()
