from pathlib import Path
import tempfile
import sys
import unittest

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shopee_safe_scraper.config import DEFAULT_CONFIG, save_config
from shopee_safe_scraper.parser import discover_review_links, parse_product_snapshot, parse_reviews_from_html
from shopee_safe_scraper.pipeline import ensure_project_layout, snapshot_seed_products


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
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
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


if __name__ == "__main__":
    unittest.main()
