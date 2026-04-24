# AGENT.md

## Project

Shopee review research pipeline.

Goal:
- collect only public Shopee product/review pages
- keep crawl low-rate, single-thread, and stop-fast
- turn raw HTML into cleaned tables and feature tables
- support notebook-first research work

## Design

Core code lives in `src/` as flat modules:
- `config.py`: default config, load/save config
- `cleaning.py`: text cleanup, identifier masking, dataframe cleaning, export helpers
- `features.py`: review features and product aggregates
- `parser.py`: HTML snapshot parsing and review extraction
- `pipeline.py`: end-to-end crawl, clean, validate, feature workflow
- `safe_http.py`: HTTP fetch, block detection, file/text/json helpers

Important:
- `src/http.py` was renamed to `src/safe_http.py`
- reason: `http.py` shadows Python stdlib `http`

Notebook imports now use top-level modules from `src/`:
- `from config import ...`
- `from pipeline import ...`
- `from parser import ...`
- `from safe_http import ...`

## Workflow

Notebook sequence:
1. `00_scope_ethics.ipynb`
   - set root paths
   - load config
   - create project layout
2. `01_seed_products.ipynb`
   - build seed template
   - fill public product URLs manually
3. `02_product_snapshot.ipynb`
   - fetch product pages
   - write raw HTML and product snapshots
4. `03_review_harvest.ipynb`
   - discover review pages
   - fetch and parse review rows
5. `04_clean_validate.ipynb`
   - clean raw products and reviews
   - write parquet + CSV fallback
6. `05_metadata_features.ipynb`
   - build review features
   - build product aggregates

Pipeline outputs:
- `data/raw/`: raw HTML
- `data/interim/`: raw CSV snapshots
- `data/processed/`: cleaned tables and feature tables
- `logs/`: crawl manifests and summaries

## Safety Rules

- public-only collection
- no login automation
- no proxy rotation
- no captcha solving
- stop on `403`, `429`, login, captcha, forbidden, access denied
- keep request rate low

## Tests

Tests live in `tests/`.
They cover:
- cleaning
- feature logic
- block detection
- parser behavior

## Editing Rules

- keep ASCII unless file already needs unicode
