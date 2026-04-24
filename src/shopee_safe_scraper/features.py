from __future__ import annotations

import re
from typing import Any

import pandas as pd

from .cleaning import normalize_text


EMOJI_RE = re.compile(
    "["
    "\U0001F300-\U0001F5FF"
    "\U0001F600-\U0001F64F"
    "\U0001F680-\U0001F6FF"
    "\U0001F900-\U0001F9FF"
    "\u2600-\u26FF"
    "\u2700-\u27BF"
    "]+",
    flags=re.UNICODE,
)
REPEATED_CHAR_RE = re.compile(r"(.)\1{2,}")


def repeated_char_ratio(text: str) -> float:
    if not text:
        return 0.0
    repeated = sum(len(match.group(0)) for match in REPEATED_CHAR_RE.finditer(text))
    return round(repeated / max(len(text), 1), 4)


def build_review_features(reviews: pd.DataFrame) -> pd.DataFrame:
    df = reviews.copy()
    df["review_text"] = df["review_text"].fillna("").map(str)
    df["normalized_review_text"] = df["review_text"].map(normalize_text)
    df["text_length"] = df["review_text"].str.len()
    df["emoji_count"] = df["review_text"].map(lambda value: len(EMOJI_RE.findall(value)))
    df["repeated_char_ratio"] = df["review_text"].map(repeated_char_ratio)
    df["exclamation_count"] = df["review_text"].str.count("!")
    df["variant_present_flag"] = df.get("variant_text", "").fillna("").map(lambda value: bool(str(value).strip()))
    if "star_rating" in df.columns:
        df["extreme_rating_flag"] = df["star_rating"].isin([1, 5])
    else:
        df["extreme_rating_flag"] = False
    df["short_review_flag"] = df["text_length"] < 20
    if "product_id" in df.columns and "review_time_display" in df.columns:
        df["relative_review_order"] = (
            df.sort_values(["product_id", "review_time_display"])
            .groupby("product_id")
            .cumcount()
            + 1
        )
    else:
        df["relative_review_order"] = range(1, len(df) + 1)
    return df


def _band_numeric(series: pd.Series, bins: list[float], labels: list[str]) -> pd.Series:
    coerced = pd.to_numeric(series, errors="coerce")
    return pd.cut(coerced, bins=bins, labels=labels, include_lowest=True).astype(str)


def build_product_aggregates(reviews: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    review_features = build_review_features(reviews)
    base = (
        review_features.groupby("product_id", dropna=False)
        .agg(
            review_count=("review_text", "size"),
            duplicate_ratio=("normalized_review_text", lambda s: 1 - (s.nunique() / max(len(s), 1))),
            extreme_rating_ratio=("extreme_rating_flag", "mean"),
            short_review_ratio=("short_review_flag", "mean"),
        )
        .reset_index()
    )

    near_template = (
        review_features.assign(template_key=review_features["normalized_review_text"].str.replace(r"\d+", "<num>", regex=True))
        .groupby("product_id", dropna=False)["template_key"]
        .agg(lambda s: 1 - (s.nunique() / max(len(s), 1)))
        .reset_index(name="near_template_ratio")
    )

    product_table = products.copy()
    if "price_display" in product_table.columns:
        product_table["price_band"] = _band_numeric(
            product_table["price_display"].astype(str).str.replace(r"[^\d.]", "", regex=True),
            bins=[-1, 100000, 500000, 1000000, float("inf")],
            labels=["budget", "mid", "upper_mid", "premium"],
        )
    else:
        product_table["price_band"] = "unknown"

    if "sold_count_display" in product_table.columns:
        product_table["sold_count_band"] = _band_numeric(
            product_table["sold_count_display"].astype(str).str.replace(r"[^\d.]", "", regex=True),
            bins=[-1, 50, 200, 1000, float("inf")],
            labels=["low", "medium", "high", "very_high"],
        )
    else:
        product_table["sold_count_band"] = "unknown"

    merged = base.merge(near_template, on="product_id", how="left").merge(
        product_table[
            [
                column
                for column in (
                    "product_id",
                    "category_breadcrumb",
                    "rating_avg_display",
                    "rating_count_display",
                    "price_band",
                    "sold_count_band",
                )
                if column in product_table.columns
            ]
        ],
        on="product_id",
        how="left",
    )
    merged["review_density_proxy"] = merged["review_count"] / merged["review_count"].max()
    return merged.fillna(
        {
            "duplicate_ratio": 0.0,
            "extreme_rating_ratio": 0.0,
            "short_review_ratio": 0.0,
            "near_template_ratio": 0.0,
        }
    )
