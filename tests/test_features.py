from pathlib import Path
import sys
import unittest

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shopee_safe_scraper.features import build_product_aggregates, build_review_features


class FeatureTests(unittest.TestCase):
    def test_review_features_columns_exist(self) -> None:
        reviews = pd.DataFrame(
            [
                {
                    "product_id": "1",
                    "review_text": "baguuus banget!!!",
                    "star_rating": 5,
                    "variant_text": "Merah",
                    "review_time_display": "2026-01-01",
                }
            ]
        )
        featured = build_review_features(reviews)
        self.assertIn("text_length", featured.columns)
        self.assertIn("repeated_char_ratio", featured.columns)
        self.assertTrue(bool(featured.loc[0, "variant_present_flag"]))

    def test_product_aggregates_merge_product_fields(self) -> None:
        reviews = pd.DataFrame(
            [
                {
                    "product_id": "1",
                    "review_text": "mantap",
                    "star_rating": 5,
                    "variant_text": "",
                    "review_time_display": "2026-01-01",
                },
                {
                    "product_id": "1",
                    "review_text": "mantap",
                    "star_rating": 5,
                    "variant_text": "",
                    "review_time_display": "2026-01-02",
                },
            ]
        )
        products = pd.DataFrame(
            [
                {
                    "product_id": "1",
                    "category_breadcrumb": "elektronik",
                    "price_display": "150000",
                    "sold_count_display": "250",
                    "rating_avg_display": "4.8",
                    "rating_count_display": "100",
                }
            ]
        )
        agg = build_product_aggregates(reviews, products)
        self.assertEqual(agg.loc[0, "price_band"], "mid")
        self.assertEqual(agg.loc[0, "sold_count_band"], "high")
        self.assertGreaterEqual(agg.loc[0, "duplicate_ratio"], 0)


if __name__ == "__main__":
    unittest.main()
