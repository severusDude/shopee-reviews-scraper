from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from cleaning import clean_products_df, clean_reviews_df, export_dataframe
from config import load_config
from safe_http import SafeCrawler, append_jsonl, inspect_block_condition, save_text, write_json
from parser import discover_review_links, parse_product_snapshot, parse_reviews_from_html, utcnow_iso
from features import build_product_aggregates, build_review_features
from workflow_logging import WorkflowLogger, build_workflow_logger


@dataclass
class CrawlSummary:
    requests_made: int
    stop_reason: str | None
    rows_written: int


def _build_logger(config: dict[str, Any], root: Path, stage: str) -> WorkflowLogger | None:
    logging_config = dict(config.get("logging") or {})
    if not logging_config.get("enabled", True):
        return None
    return build_workflow_logger(root, config, stage)


def _log_event(logger: WorkflowLogger | None, method_name: str, *args: Any, **kwargs: Any) -> None:
    if logger is None:
        return
    getattr(logger, method_name)(*args, **kwargs)


def _progress_iter(
    logger: WorkflowLogger | None,
    iterable: Any,
    *,
    desc: str,
    total: int | None = None,
    leave: bool = False,
) -> Any:
    if logger is None:
        return iterable
    return logger.progress(iterable, desc=desc, total=total, leave=leave)


def project_root_from_cwd(cwd: str | Path) -> Path:
    cwd = Path(cwd).resolve()
    return cwd.parent if cwd.name == "notebooks" else cwd


def ensure_project_layout(project_root: str | Path) -> dict[str, Path]:
    root = Path(project_root)
    paths = {
        "root": root,
        "data": root / "data",
        "raw": root / "data" / "raw",
        "raw_html": root / "data" / "raw" / "html",
        "interim": root / "data" / "interim",
        "processed": root / "data" / "processed",
        "logs": root / "logs",
        "notebooks": root / "notebooks",
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths


def build_seed_template(config: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    today = pd.Timestamp.utcnow().date().isoformat()
    for category in config["categories"]:
        for slot in range(1, int(config["target_products_per_category"]) + 1):
            rows.append(
                {
                    "product_url": "",
                    "category_quota": category,
                    "chosen_reason": f"manual seed slot {slot}",
                    "seed_date": today,
                }
            )
    return pd.DataFrame(rows)


def _load_seed_products(project_root: str | Path) -> pd.DataFrame:
    seed_path = Path(project_root) / "data" / "interim" / "seed_products.csv"
    if not seed_path.exists():
        raise FileNotFoundError(f"Seed file not found: {seed_path}")
    df = pd.read_csv(seed_path)
    if "product_url" not in df.columns:
        raise ValueError("seed_products.csv must include a product_url column")
    df["product_url"] = df["product_url"].fillna("").astype(str).str.strip()
    valid_rows = df[df["product_url"] != ""].reset_index(drop=True)
    if valid_rows.empty:
        raise ValueError(
            "seed_products.csv has no valid product_url values. Fill template rows with public product URLs first."
        )
    return valid_rows


def snapshot_seed_products(
    project_root: str | Path,
    limit: int | None = None,
    sleep: bool = True,
) -> dict[str, Any]:
    paths = ensure_project_layout(project_root)
    config = load_config(paths["root"] / "config.yaml")
    logger = _build_logger(config, paths["root"], "product_snapshot")
    try:
        seeds = _load_seed_products(paths["root"])
        if limit is not None:
            seeds = seeds.head(limit)

        _log_event(
            logger,
            "stage_started",
            "product_snapshot",
            total_products=len(seeds),
            sleep=sleep,
            limit=limit,
        )
        crawler = SafeCrawler(config=config, sleep=sleep)
        manifest_path = paths["logs"] / "crawl_manifest.jsonl"
        rows: list[dict[str, Any]] = []
        stop_reason: str | None = None

        for row_idx, row in _progress_iter(
            logger,
            seeds.iterrows(),
            desc="Product snapshots",
            total=len(seeds),
        ):
            url = row["product_url"]
            result = crawler.fetch(url)
            stop_details = inspect_block_condition(
                status_code=result.status_code,
                text=result.text,
                final_url=result.final_url,
                stop_on_status=config["stop_on_status"],
                stop_on_keywords=config["stop_on_keywords"],
            )
            stop_candidate = stop_details.reason
            save_text(paths["raw_html"] / f"product_snapshot_{row_idx + 1:03d}.html", result.text)
            manifest_row = {
                "stage": "product_snapshot",
                "source_url": url,
                "final_url": result.final_url,
                "status_code": result.status_code,
                "elapsed_s": round(result.elapsed_s, 3),
                "size_bytes": result.size_bytes,
                "error": result.error,
                "stop_reason": stop_candidate,
                "stop_signal_class": stop_details.signal_class,
                "stop_signal_source": stop_details.signal_source,
                "stop_matched_text": stop_details.matched_text,
                "scrape_time": utcnow_iso(),
            }
            append_jsonl(manifest_path, manifest_row)
            _log_event(
                logger,
                "fetch_result",
                "product_snapshot",
                product_index=row_idx + 1,
                total_products=len(seeds),
                source_url=url,
                final_url=result.final_url,
                status_code=result.status_code,
                elapsed_s=round(result.elapsed_s, 3),
                size_bytes=result.size_bytes,
                request_count=crawler.request_count,
                error=result.error,
                stop_reason=stop_candidate,
            )
            if stop_candidate:
                stop_reason = stop_candidate
                _log_event(
                    logger,
                    "stop_condition",
                    "product_snapshot",
                    product_index=row_idx + 1,
                    source_url=url,
                    stop_reason=stop_reason,
                    signal_class=stop_details.signal_class,
                    signal_source=stop_details.signal_source,
                )
                break
            record = parse_product_snapshot(result.text, product_url=url)
            if "category_quota" in row:
                record["seed_category_quota"] = row["category_quota"]
            rows.append(record)
            _log_event(
                logger,
                "parse_result",
                "product_snapshot",
                product_index=row_idx + 1,
                product_id=record.get("product_id"),
                parse_status=record.get("parse_status"),
                required_field_completeness=record.get("required_field_completeness"),
                rows_written=len(rows),
            )

        products = pd.DataFrame(rows)
        interim_path = paths["interim"] / "product_snapshots.csv"
        if not products.empty:
            products.to_csv(interim_path, index=False)

        summary = CrawlSummary(
            requests_made=crawler.request_count,
            stop_reason=stop_reason,
            rows_written=len(rows),
        )
        write_json(paths["logs"] / "product_snapshot_summary.json", asdict(summary))
        _log_event(
            logger,
            "stage_finished",
            "product_snapshot",
            requests_made=crawler.request_count,
            rows_written=len(rows),
            stop_reason=stop_reason,
            output_path=str(interim_path),
        )
        return {
            "products": products,
            "summary": asdict(summary),
            "output_path": str(interim_path),
            "log_path": str(logger.log_path) if logger and logger.log_path else None,
            "event_log_path": str(logger.event_log_path) if logger and logger.event_log_path else None,
        }
    finally:
        if logger is not None:
            logger.close()


def harvest_reviews(
    project_root: str | Path,
    limit_products: int | None = None,
    sleep: bool = True,
    max_pages_per_product: int = 5,
    verbose: bool = False,
) -> dict[str, Any]:
    paths = ensure_project_layout(project_root)
    config = load_config(paths["root"] / "config.yaml")
    logger = _build_logger(config, paths["root"], "review_harvest")
    try:
        seeds = _load_seed_products(paths["root"])
        if limit_products is not None:
            seeds = seeds.head(limit_products)

        _log_event(
            logger,
            "stage_started",
            "review_harvest",
            total_products=len(seeds),
            sleep=sleep,
            limit_products=limit_products,
            max_pages_per_product=max_pages_per_product,
            verbose=verbose,
        )
        crawler = SafeCrawler(config=config, sleep=sleep)
        manifest_path = paths["logs"] / "crawl_manifest.jsonl"
        rows: list[dict[str, Any]] = []
        product_review_counts: dict[str, int] = {}
        stop_reason: str | None = None

        for product_idx, seed in _progress_iter(
            logger,
            seeds.iterrows(),
            desc="Review products",
            total=len(seeds),
        ):
            product_url = seed["product_url"]
            if verbose:
                _log_event(
                    logger,
                    "event",
                    "review_harvest",
                    "product_fetch_started",
                    product_index=product_idx + 1,
                    total_products=len(seeds),
                    source_url=product_url,
                )
            first_page = crawler.fetch(seed["product_url"])
            stop_details = inspect_block_condition(
                status_code=first_page.status_code,
                text=first_page.text,
                final_url=first_page.final_url,
                stop_on_status=config["stop_on_status"],
                stop_on_keywords=config["stop_on_keywords"],
            )
            stop_candidate = stop_details.reason
            append_jsonl(
                manifest_path,
                {
                    "stage": "review_harvest",
                    "source_url": seed["product_url"],
                    "final_url": first_page.final_url,
                    "status_code": first_page.status_code,
                    "elapsed_s": round(first_page.elapsed_s, 3),
                    "size_bytes": first_page.size_bytes,
                    "error": first_page.error,
                    "stop_reason": stop_candidate,
                    "stop_signal_class": stop_details.signal_class,
                    "stop_signal_source": stop_details.signal_source,
                    "stop_matched_text": stop_details.matched_text,
                    "scrape_time": utcnow_iso(),
                },
            )
            save_text(paths["raw_html"] / f"review_page_{product_idx + 1:03d}_001.html", first_page.text)
            _log_event(
                logger,
                "fetch_result",
                "review_harvest",
                product_index=product_idx + 1,
                page_no=1,
                source_url=seed["product_url"],
                final_url=first_page.final_url,
                status_code=first_page.status_code,
                elapsed_s=round(first_page.elapsed_s, 3),
                size_bytes=first_page.size_bytes,
                request_count=crawler.request_count,
                error=first_page.error,
                stop_reason=stop_candidate,
            )
            if stop_candidate:
                stop_reason = stop_candidate
                _log_event(
                    logger,
                    "stop_condition",
                    "review_harvest",
                    product_index=product_idx + 1,
                    page_no=1,
                    source_url=product_url,
                    stop_reason=stop_reason,
                    signal_class=stop_details.signal_class,
                    signal_source=stop_details.signal_source,
                )
                break

            page_urls = discover_review_links(first_page.text, first_page.final_url, max_pages=max_pages_per_product)
            if verbose:
                _log_event(
                    logger,
                    "event",
                    "review_harvest",
                    "pages_queued",
                    product_index=product_idx + 1,
                    product_url=product_url,
                    queued_pages=len(page_urls),
                )
            parse_failures = 0
            with logger.progress_context(total=len(page_urls), desc=f"Pages p{product_idx + 1}", leave=False) if logger else _null_context() as page_progress:
                for page_no, page_url in enumerate(page_urls, start=1):
                    if verbose and logger and logger.verbose_notebook_events:
                        logger.event(
                            "review_harvest",
                            "page_started",
                            product_index=product_idx + 1,
                            page_no=page_no,
                            total_pages=len(page_urls),
                            source_url=page_url,
                        )
                    if page_progress:
                        page_progress.set_postfix_str(f"page={page_no}/{len(page_urls)}")
                    page_result = first_page if page_no == 1 else crawler.fetch(page_url)
                    if page_no > 1:
                        stop_details = inspect_block_condition(
                            status_code=page_result.status_code,
                            text=page_result.text,
                            final_url=page_result.final_url,
                            stop_on_status=config["stop_on_status"],
                            stop_on_keywords=config["stop_on_keywords"],
                        )
                        stop_candidate = stop_details.reason
                        append_jsonl(
                            manifest_path,
                            {
                                "stage": "review_harvest",
                                "source_url": page_url,
                                "final_url": page_result.final_url,
                                "status_code": page_result.status_code,
                                "elapsed_s": round(page_result.elapsed_s, 3),
                                "size_bytes": page_result.size_bytes,
                                "error": page_result.error,
                                "stop_reason": stop_candidate,
                                "stop_signal_class": stop_details.signal_class,
                                "stop_signal_source": stop_details.signal_source,
                                "stop_matched_text": stop_details.matched_text,
                                "scrape_time": utcnow_iso(),
                            },
                        )
                        save_text(paths["raw_html"] / f"review_page_{product_idx + 1:03d}_{page_no:03d}.html", page_result.text)
                        _log_event(
                            logger,
                            "fetch_result",
                            "review_harvest",
                            product_index=product_idx + 1,
                            page_no=page_no,
                            source_url=page_url,
                            final_url=page_result.final_url,
                            status_code=page_result.status_code,
                            elapsed_s=round(page_result.elapsed_s, 3),
                            size_bytes=page_result.size_bytes,
                            request_count=crawler.request_count,
                            error=page_result.error,
                            stop_reason=stop_candidate,
                        )
                        if stop_candidate:
                            stop_reason = stop_candidate
                            _log_event(
                                logger,
                                "stop_condition",
                                "review_harvest",
                                product_index=product_idx + 1,
                                page_no=page_no,
                                source_url=page_url,
                                stop_reason=stop_reason,
                                signal_class=stop_details.signal_class,
                                signal_source=stop_details.signal_source,
                            )
                            break

                    parsed = parse_reviews_from_html(
                        page_result.text,
                        product_url=product_url,
                        review_page=page_no,
                        source_url=page_url,
                    )
                    if not parsed:
                        parse_failures += 1
                    else:
                        parse_failures = 0
                    rows.extend(parsed)
                    product_id = parsed[0]["product_id"] if parsed else None
                    if product_id:
                        product_review_counts[product_id] = product_review_counts.get(product_id, 0) + len(parsed)
                    _log_event(
                        logger,
                        "parse_result",
                        "review_harvest",
                        product_index=product_idx + 1,
                        page_no=page_no,
                        parsed_count=len(parsed),
                        parse_failures=parse_failures,
                        product_id=product_id,
                        request_count=crawler.request_count,
                        rows_written=len(rows),
                    )
                    if parse_failures >= 3:
                        _log_event(
                            logger,
                            "warning",
                            "review_harvest",
                            "empty_parse_threshold_reached",
                            product_index=product_idx + 1,
                            page_no=page_no,
                            parse_failures=parse_failures,
                        )
                        break
                    if product_id and product_review_counts.get(product_id, 0) >= int(config["max_reviews_per_product"]):
                        _log_event(
                            logger,
                            "warning",
                            "review_harvest",
                            "max_reviews_reached",
                            product_index=product_idx + 1,
                            page_no=page_no,
                            product_id=product_id,
                            max_reviews_per_product=config["max_reviews_per_product"],
                            rows_written=product_review_counts.get(product_id, 0),
                        )
                        break
                    if page_progress:
                        page_progress.update(1)
            if stop_reason:
                break

        reviews = pd.DataFrame(rows)
        interim_path = paths["interim"] / "reviews_raw.csv"
        if not reviews.empty:
            reviews.to_csv(interim_path, index=False)

        summary = CrawlSummary(
            requests_made=crawler.request_count,
            stop_reason=stop_reason,
            rows_written=len(rows),
        )
        write_json(paths["logs"] / "review_harvest_summary.json", asdict(summary))
        _log_event(
            logger,
            "stage_finished",
            "review_harvest",
            requests_made=crawler.request_count,
            rows_written=len(rows),
            stop_reason=stop_reason,
            output_path=str(interim_path),
        )
        return {
            "reviews": reviews,
            "summary": asdict(summary),
            "output_path": str(interim_path),
            "log_path": str(logger.log_path) if logger and logger.log_path else None,
            "event_log_path": str(logger.event_log_path) if logger and logger.event_log_path else None,
        }
    finally:
        if logger is not None:
            logger.close()


def clean_validate_artifacts(project_root: str | Path) -> dict[str, Any]:
    paths = ensure_project_layout(project_root)
    config = load_config(paths["root"] / "config.yaml")
    logger = _build_logger(config, paths["root"], "clean_validate")
    product_path = paths["interim"] / "product_snapshots.csv"
    review_path = paths["interim"] / "reviews_raw.csv"
    if not product_path.exists():
        raise FileNotFoundError(f"Missing product snapshots: {product_path}")
    if not review_path.exists():
        raise FileNotFoundError(f"Missing raw reviews: {review_path}")

    _log_event(
        logger,
        "stage_started",
        "clean_validate",
        product_path=str(product_path),
        review_path=str(review_path),
    )
    products = clean_products_df(pd.read_csv(product_path))
    reviews = clean_reviews_df(pd.read_csv(review_path))

    product_export = export_dataframe(products, paths["processed"] / "products.parquet")
    review_export = export_dataframe(reviews, paths["processed"] / "reviews.parquet")

    manifest_payload = {
        "products_rows": len(products),
        "reviews_rows": len(reviews),
        "product_export": product_export,
        "review_export": review_export,
    }
    write_json(paths["processed"] / "crawl_manifest.json", manifest_payload)
    _log_event(
        logger,
        "export_result",
        "clean_validate",
        products_rows=len(products),
        reviews_rows=len(reviews),
        product_export_path=product_export["preferred_path"],
        review_export_path=review_export["preferred_path"],
    )
    _log_event(
        logger,
        "stage_finished",
        "clean_validate",
        products_rows=len(products),
        reviews_rows=len(reviews),
    )
    if logger is not None:
        logger.close()
    return {
        "products": products,
        "reviews": reviews,
        "summary": manifest_payload,
        "log_path": str(logger.log_path) if logger and logger.log_path else None,
        "event_log_path": str(logger.event_log_path) if logger and logger.event_log_path else None,
    }


def build_metadata_feature_artifacts(project_root: str | Path) -> dict[str, Any]:
    paths = ensure_project_layout(project_root)
    config = load_config(paths["root"] / "config.yaml")
    logger = _build_logger(config, paths["root"], "metadata_features")
    processed_dir = paths["processed"]
    product_csv = processed_dir / "products.csv"
    review_csv = processed_dir / "reviews.csv"
    if not product_csv.exists():
        raise FileNotFoundError(f"Missing cleaned products CSV fallback: {product_csv}")
    if not review_csv.exists():
        raise FileNotFoundError(f"Missing cleaned reviews CSV fallback: {review_csv}")

    _log_event(
        logger,
        "stage_started",
        "metadata_features",
        product_csv=str(product_csv),
        review_csv=str(review_csv),
    )
    products = pd.read_csv(product_csv)
    reviews = pd.read_csv(review_csv)
    review_features = build_review_features(reviews)
    product_aggregates = build_product_aggregates(reviews, products)

    review_export = export_dataframe(review_features, processed_dir / "review_features.parquet")
    product_export = export_dataframe(product_aggregates, processed_dir / "product_aggregates.parquet")
    summary = {
        "review_features_rows": len(review_features),
        "product_aggregates_rows": len(product_aggregates),
        "review_export": review_export,
        "product_export": product_export,
    }
    write_json(processed_dir / "feature_manifest.json", summary)
    _log_event(
        logger,
        "export_result",
        "metadata_features",
        review_features_rows=len(review_features),
        product_aggregates_rows=len(product_aggregates),
        review_export_path=review_export["preferred_path"],
        product_export_path=product_export["preferred_path"],
    )
    _log_event(
        logger,
        "stage_finished",
        "metadata_features",
        review_features_rows=len(review_features),
        product_aggregates_rows=len(product_aggregates),
    )
    if logger is not None:
        logger.close()
    return {
        "review_features": review_features,
        "product_aggregates": product_aggregates,
        "summary": summary,
        "log_path": str(logger.log_path) if logger and logger.log_path else None,
        "event_log_path": str(logger.event_log_path) if logger and logger.event_log_path else None,
    }


class _NullContext:
    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False


def _null_context() -> _NullContext:
    return _NullContext()
