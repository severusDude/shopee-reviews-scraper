from pathlib import Path
import sys
import unittest

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cleaning import clean_reviews_df, mask_identifier, normalize_text


class CleaningTests(unittest.TestCase):
    def test_normalize_text_collapses_whitespace(self) -> None:
        self.assertEqual(normalize_text("  Halo   Dunia \n"), "halo dunia")

    def test_mask_identifier_preserves_edges(self) -> None:
        self.assertEqual(mask_identifier("Budi"), "b**i")

    def test_clean_reviews_deduplicates(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "product_id": "1",
                    "review_text": "Bagus sekali",
                    "review_time_display": "2026-01-01",
                    "star_rating": "5",
                    "reviewer_name_masked": "Lutfi",
                    "media_flag": True,
                    "seller_reply_flag": False,
                },
                {
                    "product_id": "1",
                    "review_text": "  bagus   sekali ",
                    "review_time_display": "2026-01-01",
                    "star_rating": "5",
                    "reviewer_name_masked": "Lutfi",
                    "media_flag": True,
                    "seller_reply_flag": False,
                },
            ]
        )
        cleaned = clean_reviews_df(df)
        self.assertEqual(len(cleaned), 1)
        self.assertEqual(cleaned.loc[0, "reviewer_name_masked"], "l***i")


if __name__ == "__main__":
    unittest.main()
