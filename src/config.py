from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any


DEFAULT_CONFIG: dict[str, Any] = {
    "categories": [
        "Elektronik",
        "Fashion",
        "Kesehatan",
        "Kecantikan",
        "Rumah Tangga",
        "Olahraga",
    ],
    "target_products_per_category": 9,
    "max_reviews_per_product": 100,
    "min_delay_s": 4,
    "max_delay_s": 9,
    "cooldown_every_n_requests": 10,
    "cooldown_min_s": 60,
    "cooldown_max_s": 120,
    "max_requests_per_run": 150,
    "timeout_s": 30,
    "browser_fallback_enabled": True,
    "browser_engine": "chromium",
    "browser_timeout_s": 30,
    "browser_startup_probe_timeout_s": 10,
    "browser_headless": True,
    "browser_diagnostics_verbose": True,
    "stop_on_status": [403, 429],
    "stop_on_keywords": [
        "captcha",
        "verify you are human",
        "login",
        "sign in",
        "forbidden",
        "access denied",
    ],
    "user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "public_only": True,
    "single_thread_only": True,
    "logging": {
        "enabled": True,
        "level": "INFO",
        "console": True,
        "file": True,
        "event_jsonl": True,
        "verbose_notebook_events": True,
    },
    "progress": {
        "enabled": True,
        "style": "notebook",
    },
}


def _merged_with_defaults(payload: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(DEFAULT_CONFIG)
    for key, value in payload.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key].update(value)
        else:
            merged[key] = value
    return merged


def load_config(path: str | Path) -> dict[str, Any]:
    path = Path(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    return _merged_with_defaults(payload)


def save_config(path: str | Path, config: dict[str, Any]) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8")
    return path
