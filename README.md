# Safe Shopee Review Research Pipeline

Jupyter-first scaffold for collecting a small, public-only Shopee review corpus with metadata, then cleaning it into research-ready tables for later NLP work.

## What this project does

- keeps collection limited to public product/review pages
- uses single-thread, low-rate fetching with hard stop rules
- records crawl manifests for reproducibility
- builds product snapshots, review tables, cleaned tables, and metadata features
- ships notebook entrypoints plus reusable Python helpers

## Layout

- `config.yaml`: crawl configuration stored as JSON-compatible YAML
- `notebooks/`: `00` through `05` workflow notebooks
- `src/`: reusable helpers
- `tests/`: lightweight unit tests on parsing, cleaning, and feature logic
- `data/`: raw, interim, processed artifacts created by notebooks
- `logs/`: crawl logs and manifest rows

## Quick start

1. Use the bundled Python or your project Python.
2. Install dependencies from `requirements.txt`.
3. Open notebooks in order:
   - `notebooks/00_scope_ethics.ipynb`
   - `notebooks/01_seed_products.ipynb`
   - `notebooks/02_product_snapshot.ipynb`
   - `notebooks/03_review_harvest.ipynb`
   - `notebooks/04_clean_validate.ipynb`
   - `notebooks/05_metadata_features.ipynb`

## Safety notes

- No login automation.
- No proxy rotation.
- No captcha solving.
- No hidden/private endpoint probing.
- Stop immediately on `403`, `429`, captcha text, forced login, or repeated empty payloads.

## Dependency note

Core code is mostly stdlib + `pandas`, but parquet export needs either `pyarrow` or `fastparquet`. The cleaning notebook writes CSV fallbacks if parquet support is missing and reports that in its summary.
