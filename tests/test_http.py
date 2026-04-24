from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shopee_safe_scraper.http import detect_block_condition, inspect_block_condition
from shopee_safe_scraper.parser import parse_product_snapshot


class BlockDetectionTests(unittest.TestCase):
    def test_ignores_keyword_noise_inside_script_assets(self) -> None:
        html = """
        <html>
          <head>
            <title>Produk Aman</title>
            <script>
              window.__ASSETS__ = {
                "pcmall-anticrawler": "asset.js",
                "pcmall-antifraudcaptcha": "captcha.js"
              };
            </script>
          </head>
          <body>
            <h1>Produk Aman</h1>
          </body>
        </html>
        """
        result = inspect_block_condition(
            status_code=200,
            text=html,
            final_url="https://shopee.co.id/produk-aman-i.1.2",
            stop_on_status=[403, 429],
            stop_on_keywords=["captcha", "verify you are human", "login", "sign in"],
        )
        self.assertIsNone(result.reason)

    def test_detects_visible_challenge_phrase(self) -> None:
        html = """
        <html>
          <head><title>Verify You Are Human</title></head>
          <body><h1>Verify you are human</h1></body>
        </html>
        """
        result = inspect_block_condition(
            status_code=200,
            text=html,
            final_url="https://shopee.co.id/challenge",
            stop_on_status=[403, 429],
            stop_on_keywords=["captcha", "verify you are human", "login", "sign in"],
        )
        self.assertEqual(result.reason, "keyword_verify you are human")
        self.assertEqual(result.signal_source, "title")

    def test_detects_status_stop_immediately(self) -> None:
        reason = detect_block_condition(
            status_code=403,
            text="<html><body>ok</body></html>",
            final_url="https://shopee.co.id/produk",
            stop_on_status=[403, 429],
            stop_on_keywords=["captcha"],
        )
        self.assertEqual(reason, "status_403")

    def test_detects_forced_login_redirect(self) -> None:
        reason = detect_block_condition(
            status_code=200,
            text="<html><body>ok</body></html>",
            final_url="https://shopee.co.id/buyer/login",
            stop_on_status=[403, 429],
            stop_on_keywords=["captcha", "login"],
        )
        self.assertEqual(reason, "forced_login_redirect")

    def test_saved_product_snapshot_not_classified_as_block_and_parses(self) -> None:
        fixture_path = Path(__file__).resolve().parents[1] / "data" / "raw" / "html" / "product_snapshot_001.html"
        if not fixture_path.exists():
            self.skipTest(f"fixture missing: {fixture_path}")
        html = fixture_path.read_text(encoding="utf-8")
        reason = detect_block_condition(
            status_code=200,
            text=html,
            final_url="https://shopee.co.id/Lampu-Track-LED-Sorot-15W-20W-30W-Warm-White-i.254302311.10765278201",
            stop_on_status=[403, 429],
            stop_on_keywords=["captcha", "verify you are human", "login", "sign in", "forbidden", "access denied"],
        )
        record = parse_product_snapshot(
            html,
            product_url="https://shopee.co.id/Lampu-Track-LED-Sorot-15W-20W-30W-Warm-White-i.254302311.10765278201",
        )
        self.assertIsNone(reason)
        self.assertTrue(bool(record["title"]))
        self.assertGreater(record["required_field_completeness"], 0.0)


if __name__ == "__main__":
    unittest.main()
