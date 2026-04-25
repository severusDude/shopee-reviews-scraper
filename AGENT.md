# AGENT.md

**READ PROGRAM_PIPELINE.md first for understanding the program pipeline**

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
- `workflow_logging.py`: shared text logging, JSONL events, and notebook/terminal progress bars

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
   - show live `tqdm` progress in notebook runs
4. `03_review_harvest.ipynb`
   - discover review pages
   - fetch and parse review rows
   - show live `tqdm` progress in notebook runs
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
- `logs/`: crawl manifests, summaries, human-readable `.log` files, and structured `*_events.jsonl` files

## Safety Rules

- public-only collection
- no login automation
- no proxy rotation
- no captcha solving
- stop on `403`, `429`, login, captcha, forbidden, access denied
- keep request rate low
- notebook-first runs should prefer config-driven progress/logging over ad hoc `print`

## Tests

Tests live in `tests/`.
They cover:
- cleaning
- feature logic
- block detection
- parser behavior

## Useful Commands

- `uv sync`: install or refresh project dependencies
- `uv run python -m unittest discover -s tests`: run the test suite
- `uv run jupyter lab`: open the notebook workflow locally
- `uv run python main.py`: run the project entrypoint
- `uv run python -c "from pipeline import snapshot_seed_products; print('ok')"`: quick import smoke test
- `uv run python -m ipykernel install --user --name shopee-reviews-scraper`: register the notebook kernel
- `uv add <package>`: add a new dependency to the project
- `uv lock`: refresh the lockfile after dependency changes

## Editing Rules

- keep ASCII unless file already needs unicode
