from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from html import unescape
from dataclasses import dataclass
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
BODY_RE = re.compile(r"<body[^>]*>(?P<body>.*?)</body>", flags=re.IGNORECASE | re.DOTALL)
REVIEW_CARD_RE = re.compile(
    r"<div[^>]+(?:class|data-testid)=['\"][^'\"]*(?:review|rating)[^'\"]*['\"][^>]*>(?P<body>.*?)</div>",
    flags=re.IGNORECASE | re.DOTALL,
)
STAR_RE = re.compile(r"([1-5])(?:\s*/\s*5|\s*bintang|\s*star)", flags=re.IGNORECASE)


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


@dataclass(frozen=True)
class ShellPageDetectionResult:
    reason: str | None
    signal_count: int
    visible_text_length: int
    blob_count: int
    review_candidate_count: int
    product_signal_count: int


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def strip_tags(text: str) -> str:
    no_tags = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", unescape(no_tags)).strip()


def extract_visible_text(html: str) -> str:
    text = re.sub(r"<(script|style)\b[^>]*>.*?</\1>", " ", html or "", flags=re.IGNORECASE | re.DOTALL)
    return strip_tags(text)


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


def _count_review_candidates(payload: Any) -> int:
    return sum(1 for _ in _iter_review_candidates(payload))


def detect_shell_page(
    html: str,
    *,
    review_rows: list[dict[str, Any]] | None = None,
    product_record: dict[str, Any] | None = None,
) -> ShellPageDetectionResult:
    body_match = BODY_RE.search(html or "")
    body_html = body_match.group("body") if body_match else (html or "")
    visible_text = extract_visible_text(body_html)
    blobs = extract_json_blobs(html)
    review_candidate_count = sum(_count_review_candidates(blob) for blob in blobs)
    product_signals = 0
    if product_record:
        for key in ("title", "price_display", "rating_avg_display", "shop_name", "product_description"):
            if product_record.get(key):
                product_signals += 1

    signals = 0
    if 'id="main"' in html or "id='main'" in html:
        signals += 1
    if "text/shopee-page-manifest" in html or "window.__ASSETS__" in html:
        signals += 1
    if len(visible_text) < 400:
        signals += 1
    if not review_rows:
        signals += 1
    if review_candidate_count == 0:
        signals += 1
    if product_signals <= 1:
        signals += 1

    reason = "shell_page_no_embedded_reviews" if signals >= 4 else None
    return ShellPageDetectionResult(
        reason=reason,
        signal_count=signals,
        visible_text_length=len(visible_text),
        blob_count=len(blobs),
        review_candidate_count=review_candidate_count,
        product_signal_count=product_signals,
    )


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


def _normalize_review_candidate(
    candidate: dict[str, Any],
    *,
    product_id: str,
    review_page: int,
    scrape_time: str,
    source_url: str,
    response_hash: str,
) -> dict[str, Any] | None:
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
        return None

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

    return {
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
        "response_hash": response_hash,
        "source_url": source_url,
    }


def _dedupe_reviews(reviews: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
    response_hash = stable_hash(html)

    for candidate in _iter_review_candidates(blobs):
        row = _normalize_review_candidate(
            candidate,
            product_id=product_id,
            review_page=review_page,
            scrape_time=scrape_time,
            source_url=source_url,
            response_hash=response_hash,
        )
        if row is not None:
            reviews.append(row)

    return _dedupe_reviews(reviews)


def parse_reviews_from_payload(
    payload: Any,
    *,
    product_url: str,
    review_page: int = 1,
    scrape_time: str | None = None,
    source_url: str | None = None,
    response_hash: str | None = None,
) -> list[dict[str, Any]]:
    scrape_time = scrape_time or utcnow_iso()
    product_id = extract_product_id(product_url)
    source_url = source_url or product_url
    response_hash = response_hash or stable_hash(json.dumps(payload, sort_keys=True, ensure_ascii=False))
    reviews: list[dict[str, Any]] = []

    for candidate in _iter_review_candidates(payload):
        row = _normalize_review_candidate(
            candidate,
            product_id=product_id,
            review_page=review_page,
            scrape_time=scrape_time,
            source_url=source_url,
            response_hash=response_hash,
        )
        if row is not None:
            reviews.append(row)

    return _dedupe_reviews(reviews)


def parse_reviews_from_rendered_html(
    html: str,
    *,
    product_url: str,
    review_page: int = 1,
    scrape_time: str | None = None,
    source_url: str | None = None,
) -> list[dict[str, Any]]:
    scrape_time = scrape_time or utcnow_iso()
    source_url = source_url or product_url
    product_id = extract_product_id(product_url)
    response_hash = stable_hash(html)
    reviews: list[dict[str, Any]] = []

    for match in REVIEW_CARD_RE.finditer(html):
        block = match.group("body")
        text = strip_tags(block)
        if len(text) < 8:
            continue
        star_match = STAR_RE.search(text)
        variant_match = re.search(r"(variasi|variant)[:\s]+([^\n|]+)", text, flags=re.IGNORECASE)
        time_match = re.search(r"(\d{4}-\d{2}-\d{2}|\d{1,2}\s+\w+\s+\d{4})", text, flags=re.IGNORECASE)
        helpful_match = re.search(r"(\d+)\s*(orang|helpful|terbantu|like)", text, flags=re.IGNORECASE)
        candidate = {
            "comment": text,
            "rating_star": star_match.group(1) if star_match else None,
            "time": time_match.group(1) if time_match else None,
            "variation": variant_match.group(2).strip() if variant_match else None,
            "liked_count": helpful_match.group(1) if helpful_match else None,
            "images": re.findall(r"<img\b", block, flags=re.IGNORECASE),
            "videos": re.findall(r"<video\b", block, flags=re.IGNORECASE),
            "seller_reply": bool(re.search(r"balasan penjual|seller reply|shop reply", block, flags=re.IGNORECASE)),
        }
        row = _normalize_review_candidate(
            candidate,
            product_id=product_id,
            review_page=review_page,
            scrape_time=scrape_time,
            source_url=source_url,
            response_hash=response_hash,
        )
        if row is not None:
            reviews.append(row)

    return _dedupe_reviews(reviews)


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
    from safe_http import absolutize_links

    for link in absolutize_links(base_url, unique)[:max_pages]:
        resolved.append(link)
    if base_url not in resolved:
        resolved.insert(0, base_url)
    return resolved[:max_pages]
