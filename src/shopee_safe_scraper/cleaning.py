from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def mask_identifier(value: Any) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    if len(text) <= 2:
        return "*" * len(text)
    return text[0] + ("*" * (len(text) - 2)) + text[-1]


def to_numeric_string(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    text = text.replace(",", ".")
    return text


def clean_products_df(products: pd.DataFrame) -> pd.DataFrame:
    df = products.copy()
    for column in ("title", "category_breadcrumb", "shop_name", "product_description", "variant_summary"):
        if column in df.columns:
            df[column] = df[column].map(normalize_text)
    if "product_url" in df.columns:
        df["product_url"] = df["product_url"].astype(str).str.strip()
    if "product_id" in df.columns:
        df["product_id"] = df["product_id"].astype(str).str.strip()
    if "rating_avg_display" in df.columns:
        df["rating_avg_display"] = df["rating_avg_display"].map(to_numeric_string)
    if "rating_count_display" in df.columns:
        df["rating_count_display"] = df["rating_count_display"].map(to_numeric_string)
    if "sold_count_display" in df.columns:
        df["sold_count_display"] = df["sold_count_display"].map(to_numeric_string)
    return df.drop_duplicates(subset=["product_id", "product_url"], keep="first").reset_index(drop=True)


def clean_reviews_df(reviews: pd.DataFrame) -> pd.DataFrame:
    df = reviews.copy()
    for column in ("review_text", "variant_text", "purchase_variant", "review_time_display"):
        if column in df.columns:
            df[column] = df[column].map(normalize_text)
    if "reviewer_name_masked" in df.columns:
        df["reviewer_name_masked"] = df["reviewer_name_masked"].map(mask_identifier)
    if "star_rating" in df.columns:
        df["star_rating"] = pd.to_numeric(df["star_rating"], errors="coerce")
    if "media_flag" in df.columns:
        df["media_flag"] = df["media_flag"].fillna(False).astype(bool)
    if "seller_reply_flag" in df.columns:
        df["seller_reply_flag"] = df["seller_reply_flag"].fillna(False).astype(bool)
    df["normalized_review_text"] = df["review_text"].map(normalize_text)
    dedupe_cols = ["product_id", "normalized_review_text", "review_time_display", "star_rating"]
    dedupe_cols = [column for column in dedupe_cols if column in df.columns]
    df = df.drop_duplicates(subset=dedupe_cols, keep="first").reset_index(drop=True)
    return df


def export_dataframe(df: pd.DataFrame, preferred_path: str | Path) -> dict[str, Any]:
    preferred = Path(preferred_path)
    preferred.parent.mkdir(parents=True, exist_ok=True)
    csv_fallback = preferred.with_suffix(".csv")
    result: dict[str, Any] = {
        "preferred_path": str(preferred),
        "csv_fallback_path": str(csv_fallback),
        "parquet_written": False,
    }
    try:
        df.to_parquet(preferred, index=False)
        result["parquet_written"] = True
    except Exception as exc:  # pragma: no cover - depends on optional engine
        result["parquet_error"] = f"{type(exc).__name__}: {exc}"
    df.to_csv(csv_fallback, index=False)
    return result
