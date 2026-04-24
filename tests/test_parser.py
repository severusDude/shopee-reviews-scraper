from pathlib import Path
import shutil
import sys
import unittest
from unittest.mock import patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from config import DEFAULT_CONFIG, save_config
from parser import discover_review_links, parse_product_snapshot, parse_reviews_from_html
from pipeline import ensure_project_layout, harvest_reviews, snapshot_seed_products


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


if __name__ == "__main__":
    unittest.main()
