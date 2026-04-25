# Shopee Reviews Scraper Pipeline

## Scope

- Project = Jupyter-first public-only Shopee review research pipeline.
- Main run order from notebooks:
  1. `notebooks/00_scope_ethics.ipynb`
  2. `notebooks/01_seed_products.ipynb`
  3. `notebooks/02_product_snapshot.ipynb`
  4. `notebooks/03_review_harvest.ipynb`
  5. `notebooks/04_clean_validate.ipynb`
  6. `notebooks/05_metadata_features.ipynb`
- Real orchestration code lives in `src/pipeline.py`.
- `main.py` not used for workflow. Only prints `Hello from nlp!`.

## Global Setup

- Every notebook sets `PROJECT_ROOT`, adds `src/` to `sys.path`, imports pipeline fns.
- `ensure_project_layout(PROJECT_ROOT)` creates:
  - `data/raw/html/`
  - `data/interim/`
  - `data/processed/`
  - `logs/`
  - `notebooks/`
- `load_config(config.yaml)` loads JSON-in-YAML payload, merges with `DEFAULT_CONFIG`.

## Config + Guardrails

- Config file: `config.yaml`
- Default / current key controls:
  - categories: 6 categories
  - `target_products_per_category`: `9`
  - `max_reviews_per_product`: `100`
  - request delay: `4` to `9` sec
  - cooldown every `10` requests for `60` to `120` sec
  - `max_requests_per_run`: `150`
  - `timeout_s`: `30`
  - browser fallback enabled
  - public only, single thread only
- Hard stop signals from `inspect_block_condition(...)`:
  - status `403`
  - status `429`
  - empty payload
  - final URL redirected to login/signin
  - visible/title text contains:
    - `captcha`
    - `verify you are human`
    - `login`
    - `sign in`
    - `forbidden`
    - `access denied`

## Stage 00: Scope And Ethics

- Notebook: `notebooks/00_scope_ethics.ipynb`
- Purpose:
  - create project dirs
  - create `config.yaml` if missing
  - load config
  - state guardrails
- No scrape here.

## Stage 01: Seed Products

- Notebook: `notebooks/01_seed_products.ipynb`
- Core fn: `build_seed_template(config)`
- Input:
  - `categories`
  - `target_products_per_category`
- Output file:
  - `data/interim/seed_products.csv`
- Generated columns:
  - `product_url`
  - `category_quota`
  - `chosen_reason`
  - `seed_date`
- User manually fills public product URLs. No mass search scrape. No auto discovery in code.
- `_load_seed_products(...)` later enforces:
  - file must exist
  - `product_url` column must exist
  - blank URLs removed
  - at least 1 valid URL required

## Stage 02: Product Snapshot

- Notebook: `notebooks/02_product_snapshot.ipynb`
- Core fn: `snapshot_seed_products(project_root, limit=None, sleep=True)`
- Usual dry run in notebook:
  - `limit=3`
  - `sleep=True`

### Flow

1. Load seeds from `data/interim/seed_products.csv`.
2. Optionally trim with `limit`.
3. Start logger for stage `product_snapshot`.
4. Create `SafeCrawler(config, sleep=sleep)`.
5. For each seeded product URL:
   - fetch public HTML with `crawler.fetch(url)`
   - inspect block / stop conditions
   - save raw HTML to `data/raw/html/product_snapshot_{idx}.html`
   - append request row to `logs/crawl_manifest.jsonl`
   - if blocked, stop whole stage
   - else parse metadata with `parse_product_snapshot(...)`
   - append parsed row
6. Build dataframe from parsed rows.
7. Write CSV:
   - `data/interim/product_snapshots.csv`
8. Write summary JSON:
   - `logs/product_snapshot_summary.json`
9. Write logs:
   - `logs/product_snapshot.log`
   - `logs/product_snapshot_events.jsonl`

### Product Parser Exact Behavior

- Product ID from URL query `itemid` or `product_id`; else last digits in path; else SHA-256 hash prefix.
- Parser sources:
  - meta tags
  - JSON-LD blocks
  - JS-assigned JSON blobs like `__INITIAL_STATE__`, `__NEXT_DATA__`, `__STATE__`
  - page `<title>`
- Output fields include:
  - `product_id`
  - `product_url`
  - `scrape_time`
  - `title`
  - `category_breadcrumb`
  - `price_display`
  - `discount_display`
  - `sold_count_display`
  - `rating_avg_display`
  - `rating_count_display`
  - `shop_name`
  - `shop_url`
  - `shop_location`
  - `follower_count_display`
  - `stock_display`
  - `variant_summary`
  - `product_description`
  - `required_field_completeness`
  - `response_hash`
  - `parse_status`
- `parse_status = ok` if required field completeness `>= 0.5`, else `partial`.

## Stage 03: Review Harvest

- Notebook: `notebooks/03_review_harvest.ipynb`
- Core fn:
  - `harvest_reviews(project_root, limit_products=None, sleep=True, max_pages_per_product=5, verbose=False)`
- Notebook dry run uses:
  - `limit_products=3`
  - `sleep=False`
  - `max_pages_per_product=3`
  - `verbose=True`

### Base Flow

1. Load seeds from `data/interim/seed_products.csv`.
2. Optionally trim with `limit_products`.
3. Start logger for stage `review_harvest`.
4. Create `SafeCrawler`.
5. For each product URL:
   - fetch first page HTML
   - inspect stop conditions
   - append request row to `logs/crawl_manifest.jsonl`
   - save first page HTML to `data/raw/html/review_page_{product}_{page}.html`
   - if blocked, stop whole stage
   - parse first page embedded reviews with `parse_reviews_from_html(...)`
   - detect if page is shell page with `detect_shell_page(...)`
6. If shell page signal strong and browser fallback enabled:
   - run `fetch_reviews_with_browser_fallback(...)`
   - if browser returns rows, use browser rows and continue next product
   - else fall back to pure-HTML review link discovery path
7. Discover candidate review pages with `discover_review_links(...)`.
8. For each page URL:
   - page 1 reuses first fetch result
   - page 2+ fetched with `crawler.fetch(...)`
   - page 2+ checked again for stop conditions
   - page 2+ raw HTML saved to `data/raw/html/review_page_{product}_{page}.html`
   - parse reviews with `parse_reviews_from_html(...)`
   - add parsed rows
   - stop current product if 3 consecutive empty parses
   - stop current product if `max_reviews_per_product` reached
9. After all products or hard stop:
   - write `data/interim/reviews_raw.csv` if any rows
   - write `logs/review_harvest_summary.json`
   - write stage logs:
     - `logs/review_harvest.log`
     - `logs/review_harvest_events.jsonl`

### Shell Page Detection

- `detect_shell_page(...)` flags `shell_page_no_embedded_reviews` when enough signals stack:
  - shell app markers like `id="main"` or `window.__ASSETS__`
  - visible text too short
  - no parsed review rows
  - no review candidates in JSON blobs
  - weak product metadata signals
- Threshold: `signals >= 4`.

### Browser Fallback Exact Behavior

- Entry fn: `fetch_reviews_with_browser_fallback(...)`
- Launches subprocess:
  - `sys.executable src/browser_runner.py`
- Request payload includes:
  - `product_url`
  - browser config
  - `max_pages`
- Browser runner uses Playwright sync API:
  - launch Chromium
  - set user agent + locale `id-ID`
  - startup probe `about:blank`
  - open product page
  - click likely review tabs if found:
    - `Penilaian Produk`
    - `Ulasan`
    - `Ratings`
    - links containing `rating` or `review`
  - scroll page repeatedly
  - capture XHR/fetch JSON payloads from response handler
  - parse payload reviews with `parse_reviews_from_payload(...)`
  - always save rendered HTML artifact
  - if no payload rows, try DOM parse with `parse_reviews_from_rendered_html(...)`
- Browser artifacts saved under `data/raw/html/`:
  - rendered HTML
  - payload JSON
  - diagnostic JSON on failure
- Browser runtime failures cached. If startup/runtime unavailable once, later products skip browser attempt and log warning.
- Browser error classes include:
  - `playwright_import_error`
  - `playwright_startup_failed`
  - `playwright_event_loop_conflict`
  - `playwright_permission_denied`
  - `playwright_browser_missing`
  - `playwright_navigation_timeout`
  - `playwright_process_failed`
  - `playwright_process_output_invalid`

### Review Parser Exact Behavior

- `parse_reviews_from_html(...)`:
  - extracts JSON blobs from HTML
  - recursively finds review-like dicts
  - requires review text plus review context keys
  - normalizes each review into row schema
  - dedupes by hash of:
    - `product_id`
    - `review_page`
    - `review_text`
    - `star_rating`
    - `review_time_display`
- Review row fields:
  - `product_id`
  - `review_page`
  - `scrape_time`
  - `review_text`
  - `star_rating`
  - `review_time_display`
  - `variant_text`
  - `media_flag`
  - `seller_reply_flag`
  - `reviewer_name_masked`
  - `helpful_count_display`
  - `purchase_variant`
  - `image_count`
  - `video_flag`
  - `response_hash`
  - `source_url`

## Stage 04: Clean And Validate

- Notebook: `notebooks/04_clean_validate.ipynb`
- Core fn: `clean_validate_artifacts(project_root)`
- Required inputs:
  - `data/interim/product_snapshots.csv`
  - `data/interim/reviews_raw.csv`
- If missing, raises `FileNotFoundError`.

### Flow

1. Load product snapshots CSV.
2. Load raw reviews CSV.
3. Clean products with `clean_products_df(...)`.
4. Clean reviews with `clean_reviews_df(...)`.
5. Export products with `export_dataframe(...)`.
6. Export reviews with `export_dataframe(...)`.
7. Write processed manifest JSON.
8. Write logs.

### Product Cleaning

- Lowercase + normalize whitespace for:
  - `title`
  - `category_breadcrumb`
  - `shop_name`
  - `product_description`
  - `variant_summary`
- Strip `product_url`, `product_id`
- Normalize numeric strings for rating/sold fields
- Drop duplicates on:
  - `product_id`
  - `product_url`

### Review Cleaning

- Lowercase + normalize whitespace for:
  - `review_text`
  - `variant_text`
  - `purchase_variant`
  - `review_time_display`
- Mask `reviewer_name_masked`
- Coerce `star_rating` numeric
- Fill/cast booleans:
  - `media_flag`
  - `seller_reply_flag`
- Create `normalized_review_text`
- Drop duplicates on available subset of:
  - `product_id`
  - `normalized_review_text`
  - `review_time_display`
  - `star_rating`

### Clean Outputs

- Preferred parquet outputs:
  - `data/processed/products.parquet`
  - `data/processed/reviews.parquet`
- CSV fallback always written:
  - `data/processed/products.csv`
  - `data/processed/reviews.csv`
- Manifest:
  - `data/processed/crawl_manifest.json`
- Logs:
  - `logs/clean_validate.log`
  - `logs/clean_validate_events.jsonl`

## Stage 05: Metadata Features

- Notebook: `notebooks/05_metadata_features.ipynb`
- Core fn: `build_metadata_feature_artifacts(project_root)`
- Required inputs:
  - `data/processed/products.csv`
  - `data/processed/reviews.csv`
- Uses CSV fallback, not parquet.

### Flow

1. Load cleaned product CSV.
2. Load cleaned review CSV.
3. Build review-level features with `build_review_features(...)`.
4. Build product-level aggregates with `build_product_aggregates(...)`.
5. Export both tables via `export_dataframe(...)`.
6. Write feature manifest JSON.
7. Write logs.

### Review-Level Features

- `normalized_review_text`
- `text_length`
- `emoji_count`
- `repeated_char_ratio`
- `exclamation_count`
- `variant_present_flag`
- `extreme_rating_flag`
- `short_review_flag`
- `relative_review_order`

### Product-Level Aggregates

- Per-product aggregate metrics:
  - `review_count`
  - `duplicate_ratio`
  - `extreme_rating_ratio`
  - `short_review_ratio`
  - `near_template_ratio`
  - `review_density_proxy`
- Metadata joins / derived bands:
  - `category_breadcrumb`
  - `rating_avg_display`
  - `rating_count_display`
  - `price_band`
  - `sold_count_band`
- `price_band` bins:
  - `budget`
  - `mid`
  - `upper_mid`
  - `premium`
- `sold_count_band` bins:
  - `low`
  - `medium`
  - `high`
  - `very_high`

### Feature Outputs

- Preferred parquet outputs:
  - `data/processed/review_features.parquet`
  - `data/processed/product_aggregates.parquet`
- CSV fallback always written:
  - `data/processed/review_features.csv`
  - `data/processed/product_aggregates.csv`
- Manifest:
  - `data/processed/feature_manifest.json`
- Logs:
  - `logs/metadata_features.log`
  - `logs/metadata_features_events.jsonl`

## Logging Pipeline

- Logging builder: `build_workflow_logger(...)`
- Per stage can write:
  - console logs
  - `.log` file
  - `_events.jsonl` structured log
  - tqdm progress bars
- Common event types:
  - `stage_started`
  - `fetch_result`
  - `parse_result`
  - `stop_condition`
  - `export_result`
  - `stage_finished`
  - stage-specific warnings / events

## Request Behavior

- HTTP client = stdlib `urllib.request`
- User agent set from config
- `Accept-Language` = `id-ID,id;q=0.9,en;q=0.8`
- Single-thread only
- No proxy rotation
- No login automation
- No captcha solving
- No hidden/private endpoint probing
- Delay inserted before each request except first.
- Hard cap: `max_requests_per_run`

## End State

- Final research-ready outputs live in `data/processed/`.
- Full audit trail lives in:
  - raw HTML / browser artifacts under `data/raw/html/`
  - interim tables under `data/interim/`
  - manifests + logs under `logs/` and `data/processed/`
- Pipeline target = small reproducible public review corpus + metadata features for later suspiciousness / NLP analysis.
