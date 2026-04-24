from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from html import unescape
from typing import Any, Iterable
from urllib.parse import parse_qs, urlparse


SCRIPT_RE = re.compile(
    r"<script[^>]*>(?P<body>.*?)</script>",
    flags=re.IGNORECASE | re.DOTALL,
)
JSON_ASSIGN_RE = re.compile(
    r"(?:window\.)?(?:__INITIAL_STATE__|__NEXT_DATA__|__STATE__|INITIAL_STATE)\s*=\s*(\{.*?\})\s*;",
    flags=re.DOTALL,
)
JSON_LD_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(?P<body>.*?)</script>',
    flags=re.IGNORECASE | re.DOTALL,
)
META_RE = re.compile(
    r'<meta[^>]+(?:property|name)=["\'](?P<key>[^"\']+)["\'][^>]+content=["\'](?P<value>[^"\']*)["\']',
    flags=re.IGNORECASE,
)
TITLE_RE = re.compile(r"<title>(.*?)</title>", flags=re.IGNORECASE | re.DOTALL)
HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', flags=re.IGNORECASE)


PRODUCT_KEYS = {
    "title": ["title", "name", "product_name"],
    "price_display": ["price", "price_display", "price_min", "price_max"],
    "discount_display": ["discount", "discount_display", "discount_percent"],
    "sold_count_display": ["sold", "historical_sold", "sold_count"],
    "rating_avg_display": ["rating", "rating_star", "rating_avg", "rating_value"],
    "rating_count_display": ["review_count", "rating_count", "cmt_count"],
    "shop_name": ["shop_name", "seller_name", "merchant_name", "shop"],
    "shop_url": ["shop_url", "seller_url"],
    "product_description": ["description", "product_description"],
    "variant_summary": ["variation", "variant", "model_name"],
    "stock_display": ["stock", "stock_count"],
    "follower_count_display": ["follower_count", "followers"],
    "shop_location": ["shop_location", "location"],
}

REVIEW_TEXT_KEYS = {
    "comment",
    "comment_text",
    "content",
    "review_text",
    "review",
    "text",
    "message",
}
REVIEW_CONTAINER_HINTS = {
    "comments",
    "item_rating",
    "ratings",
    "reviews",
    "review_list",
}


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def strip_tags(text: str) -> str:
    no_tags = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", unescape(no_tags)).strip()


def extract_json_blobs(html: str) -> list[Any]:
    blobs: list[Any] = []
    for match in JSON_LD_RE.finditer(html):
        body = match.group("body").strip()
        try:
            blobs.append(json.loads(body))
        except json.JSONDecodeError:
            continue
    for match in JSON_ASSIGN_RE.finditer(html):
        body = match.group(1).strip()
        try:
            blobs.append(json.loads(body))
        except json.JSONDecodeError:
            continue
    for match in SCRIPT_RE.finditer(html):
        body = match.group("body").strip()
        body = body.removeprefix("window.__STATE__=")
        if body.startswith("{") and body.endswith("}"):
            try:
                blobs.append(json.loads(body))
            except json.JSONDecodeError:
                continue
    return blobs


def deep_find_values(payload: Any, keys: set[str]) -> list[Any]:
    found: list[Any] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key.lower() in keys:
                found.append(value)
            found.extend(deep_find_values(value, keys))
    elif isinstance(payload, list):
        for value in payload:
            found.extend(deep_find_values(value, keys))
    return found


def first_scalar(values: Iterable[Any]) -> str | None:
    for value in values:
        if isinstance(value, (str, int, float)) and str(value).strip():
            return str(value).strip()
    return None


def extract_meta_map(html: str) -> dict[str, str]:
    meta: dict[str, str] = {}
    for match in META_RE.finditer(html):
        meta[match.group("key").lower()] = unescape(match.group("value")).strip()
    return meta


def extract_product_id(product_url: str) -> str:
    parsed = urlparse(product_url)
    query = parse_qs(parsed.query)
    for key in ("itemid", "product_id"):
        if key in query and query[key]:
            return query[key][0]
    digits = re.findall(r"\d+", parsed.path)
    return digits[-1] if digits else stable_hash(product_url)[:16]


def extract_breadcrumb(meta: dict[str, str], blobs: list[Any]) -> str | None:
    for blob in blobs:
        if isinstance(blob, dict) and blob.get("@type") == "BreadcrumbList":
            names = []
            for node in blob.get("itemListElement", []):
                name = (
                    node.get("name")
                    if isinstance(node, dict)
                    else None
                )
                if name:
                    names.append(str(name).strip())
            if names:
                return " > ".join(names)
    for key in ("og:category", "product:category"):
        value = meta.get(key)
        if value:
            return value
    return None


def parse_product_snapshot(html: str, product_url: str, scrape_time: str | None = None) -> dict[str, Any]:
    scrape_time = scrape_time or utcnow_iso()
    meta = extract_meta_map(html)
    blobs = extract_json_blobs(html)
    title_match = TITLE_RE.search(html)
    title_fallback = strip_tags(title_match.group(1)) if title_match else None

    record: dict[str, Any] = {
        "product_id": extract_product_id(product_url),
        "product_url": product_url,
        "scrape_time": scrape_time,
        "title": meta.get("og:title") or title_fallback,
        "category_breadcrumb": extract_breadcrumb(meta, blobs),
        "price_display": meta.get("product:price:amount"),
        "discount_display": None,
        "sold_count_display": None,
        "rating_avg_display": None,
        "rating_count_display": None,
        "shop_name": None,
        "shop_url": None,
        "shop_location": None,
        "follower_count_display": None,
        "stock_display": None,
        "variant_summary": None,
        "product_description": meta.get("description"),
        "parse_status": "partial",
    }

    for blob in blobs:
        if isinstance(blob, dict) and blob.get("@type") == "Product":
            record["title"] = record["title"] or blob.get("name")
            record["product_description"] = record["product_description"] or blob.get("description")
            aggregate = blob.get("aggregateRating", {})
            if isinstance(aggregate, dict):
                record["rating_avg_display"] = record["rating_avg_display"] or str(aggregate.get("ratingValue") or "").strip() or None
                record["rating_count_display"] = record["rating_count_display"] or str(aggregate.get("reviewCount") or "").strip() or None
            offers = blob.get("offers", {})
            if isinstance(offers, dict):
                record["price_display"] = record["price_display"] or str(offers.get("price") or "").strip() or None
            brand = blob.get("brand")
            if isinstance(brand, dict):
                record["shop_name"] = record["shop_name"] or brand.get("name")

    for field, aliases in PRODUCT_KEYS.items():
        if record.get(field):
            continue
        values = []
        for alias in aliases:
            values.extend(deep_find_values(blobs, {alias.lower()}))
        record[field] = first_scalar(values) or record.get(field)

    required = [
        "product_id",
        "product_url",
        "scrape_time",
        "title",
        "category_breadcrumb",
        "price_display",
        "rating_avg_display",
        "rating_count_display",
        "shop_name",
    ]
    completeness = sum(bool(record.get(key)) for key in required) / len(required)
    record["required_field_completeness"] = round(completeness, 3)
    record["response_hash"] = stable_hash(html)
    record["parse_status"] = "ok" if completeness >= 0.5 else "partial"
    return record


def _iter_review_candidates(payload: Any) -> Iterable[dict[str, Any]]:
    if isinstance(payload, dict):
        lowered = {str(key).lower() for key in payload.keys()}
        has_review_text = bool(lowered & REVIEW_TEXT_KEYS)
        has_review_context = any(
            key in lowered
            for key in (
                "rating",
                "rating_star",
                "score",
                "star",
                "ctime",
                "created_at",
                "time",
                "date",
                "mtime",
                "author",
                "username",
                "buyer_username",
                "user_name",
                "variation",
                "variant",
                "model_name",
                "images",
                "image_urls",
                "videos",
                "video_info_list",
            )
        )
        if has_review_text and has_review_context:
            yield payload
        for key, value in payload.items():
            if str(key).lower() in REVIEW_CONTAINER_HINTS and isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        yield item
            yield from _iter_review_candidates(value)
    elif isinstance(payload, list):
        for item in payload:
            yield from _iter_review_candidates(item)


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    if value in (None, "", 0, "0"):
        return False
    return True


def parse_reviews_from_html(
    html: str,
    product_url: str,
    review_page: int = 1,
    scrape_time: str | None = None,
    source_url: str | None = None,
) -> list[dict[str, Any]]:
    scrape_time = scrape_time or utcnow_iso()
    product_id = extract_product_id(product_url)
    blobs = extract_json_blobs(html)
    reviews: list[dict[str, Any]] = []
    source_url = source_url or product_url

    for candidate in _iter_review_candidates(blobs):
        text = first_scalar(
            candidate.get(key)
            for key in (
                "comment",
                "comment_text",
                "content",
                "review_text",
                "review",
                "text",
                "message",
            )
        )
        if not text:
            continue

        star_value = first_scalar(
            candidate.get(key)
            for key in ("rating_star", "rating", "score", "star")
        )
        review_time = first_scalar(
            candidate.get(key)
            for key in ("ctime", "created_at", "time", "date", "mtime")
        )
        variant = first_scalar(
            candidate.get(key)
            for key in ("variation", "variant", "model_name", "purchase_variant")
        )
        reviewer = first_scalar(
            candidate.get(key)
            for key in ("author", "username", "buyer_username", "user_name")
        )
        helpful = first_scalar(
            candidate.get(key)
            for key in ("helpful_count", "liked_count", "upvote_count")
        )

        reply_present = _coerce_bool(
            candidate.get("seller_reply")
            or candidate.get("reply")
            or candidate.get("shop_reply")
        )
        images = candidate.get("images") or candidate.get("image_urls") or []
        videos = candidate.get("videos") or candidate.get("video_info_list") or []

        reviews.append(
            {
                "product_id": product_id,
                "review_page": review_page,
                "scrape_time": scrape_time,
                "review_text": str(text).strip(),
                "star_rating": star_value,
                "review_time_display": review_time,
                "variant_text": variant,
                "media_flag": bool(images or videos),
                "seller_reply_flag": reply_present,
                "reviewer_name_masked": reviewer,
                "helpful_count_display": helpful,
                "purchase_variant": variant,
                "image_count": len(images) if isinstance(images, list) else 0,
                "video_flag": bool(videos),
                "response_hash": stable_hash(html),
                "source_url": source_url,
            }
        )

    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in reviews:
        key = stable_hash(
            "|".join(
                [
                    row["product_id"],
                    str(row["review_page"]),
                    row["review_text"],
                    str(row.get("star_rating") or ""),
                    str(row.get("review_time_display") or ""),
                ]
            )
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def discover_review_links(html: str, base_url: str, max_pages: int = 10) -> list[str]:
    hrefs = [match.group(1) for match in HREF_RE.finditer(html)]
    candidates: list[str] = []
    for href in hrefs:
        href_lower = href.lower()
        if "review" in href_lower or "rating" in href_lower or "page=" in href_lower:
            candidates.append(href)
    unique = []
    seen = set()
    for href in candidates:
        if href not in seen:
            unique.append(href)
            seen.add(href)
    resolved = []
    from .http import absolutize_links

    for link in absolutize_links(base_url, unique)[:max_pages]:
        resolved.append(link)
    if base_url not in resolved:
        resolved.insert(0, base_url)
    return resolved[:max_pages]
