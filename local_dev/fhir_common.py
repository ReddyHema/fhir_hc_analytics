"""
Shared FHIR ingestion / medallion-layer logic.

This module is the *validated reference implementation*. The Fabric notebooks
under ../fabric_workspace/ contain the same logic adapted to run inside a
Fabric Lakehouse notebook (using `notebookutils`/`mssparkutils` paths such as
"Files/..." and the attached default Lakehouse for Delta tables, instead of
the local filesystem paths used here).

Kept deliberately dependency-light (requests + pyspark only) so it can be
unit-tested and run end-to-end against the real public HAPI FHIR server.
"""
from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import requests


# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

@dataclass
class FhirConfig:
    fhir_base_url: str
    resources_in_order: list[str]
    lookback_days: int = 3
    page_size: int = 50
    max_pages_per_day: int = 200
    request_timeout_seconds: int = 30
    retry_max_attempts: int = 4
    retry_backoff_seconds: int = 2

    @staticmethod
    def from_json(path: str | Path) -> "FhirConfig":
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        return FhirConfig(
            fhir_base_url=cfg["fhir_base_url"],
            resources_in_order=cfg["resources_in_order"],
            lookback_days=cfg.get("lookback_days", 3),
            page_size=cfg.get("page_size", 50),
            max_pages_per_day=cfg.get("max_pages_per_day", 200),
            request_timeout_seconds=cfg.get("request_timeout_seconds", 30),
            retry_max_attempts=cfg.get("retry", {}).get("max_attempts", 4),
            retry_backoff_seconds=cfg.get("retry", {}).get("backoff_seconds", 2),
        )


# --------------------------------------------------------------------------
# API access: incremental window + pagination
# --------------------------------------------------------------------------

def build_search_url(base_url: str, resource_type: str, since: date, until: date, count: int) -> str:
    """Build a FHIR search URL for an incremental [since, until) _lastUpdated window."""
    return (
        f"{base_url}/{resource_type}"
        f"?_lastUpdated=ge{since.isoformat()}"
        f"&_lastUpdated=lt{until.isoformat()}"
        f"&_count={count}"
        f"&_sort=_lastUpdated"
        f"&_format=json"
    )


def fetch_json(url: str, cfg: FhirConfig) -> dict:
    """GET a FHIR URL with retry/backoff, returning parsed JSON."""
    last_err = None
    for attempt in range(1, cfg.retry_max_attempts + 1):
        try:
            resp = requests.get(url, timeout=cfg.request_timeout_seconds, headers={"Accept": "application/fhir+json"})
            resp.raise_for_status()
            return resp.json()
        except Exception as err:  # noqa: BLE001 - want broad retry
            last_err = err
            if attempt < cfg.retry_max_attempts:
                time.sleep(cfg.retry_backoff_seconds * attempt)
    raise RuntimeError(f"Failed to fetch {url} after {cfg.retry_max_attempts} attempts: {last_err}")


def next_link(bundle: dict) -> str | None:
    for link in bundle.get("link", []):
        if link.get("relation") == "next":
            return link.get("url")
    return None


def fetch_all_pages(base_url: str, resource_type: str, since: date, until: date, cfg: FhirConfig) -> Iterable[tuple[int, str, dict]]:
    """
    Yields (page_number, page_url_called, bundle_json) for every page in the
    incremental [since, until) window, following the Bundle's "next" link.
    """
    url = build_search_url(base_url, resource_type, since, until, cfg.page_size)
    page_number = 1
    while url and page_number <= cfg.max_pages_per_day:
        bundle = fetch_json(url, cfg)
        yield page_number, url, bundle
        url = next_link(bundle)
        page_number += 1


# --------------------------------------------------------------------------
# RAW layer: store API responses verbatim, bucketed by date
# --------------------------------------------------------------------------

def raw_write_page(raw_root: Path, resource_type: str, extraction_date: date, batch_id: str,
                    page_number: int, bundle: dict) -> Path:
    out_dir = raw_root / resource_type / extraction_date.isoformat() / batch_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"page_{page_number:04d}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(bundle, f)
    return out_path


def raw_write_manifest(raw_root: Path, resource_type: str, extraction_date: date, batch_id: str,
                        api_url_or_params: str, extraction_timestamp: str, page_count: int,
                        record_count: int) -> Path:
    out_dir = raw_root / resource_type / extraction_date.isoformat() / batch_id
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "resource_type": resource_type,
        "extraction_date": extraction_date.isoformat(),
        "batch_id": batch_id,
        "api_url_or_params": api_url_or_params,
        "extraction_timestamp": extraction_timestamp,
        "page_count": page_count,
        "record_count": record_count,
    }
    out_path = out_dir / "_manifest.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    return out_path


def new_batch_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:8]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def incremental_window(control_last_date: date | None, lookback_days: int, run_date: date) -> list[date]:
    """
    Decide which extraction_date values to (re)process:
      - first run (no watermark): last `lookback_days` days up to (not including) run_date
      - subsequent runs: every day from the day after the watermark up to (not including) run_date
    Returns a list of dates in ascending order (so API calls happen oldest -> newest).
    """
    if control_last_date is None:
        start = run_date - timedelta(days=lookback_days)
    else:
        start = control_last_date + timedelta(days=1)
    days = []
    d = start
    while d < run_date:
        days.append(d)
        d += timedelta(days=1)
    return days


# --------------------------------------------------------------------------
# Business-key / change-hash helpers used by BRONZE + SILVER (SCD2)
# --------------------------------------------------------------------------

# Columns considered "business attributes" per resource type for SCD2 change
# detection (i.e. excludes technical/metadata columns).
SCD2_BUSINESS_COLUMNS = {
    "Patient": ["name_text", "gender", "birth_date", "deceased", "address_text", "marital_status", "managing_organization"],
    "Encounter": ["status", "class_code", "type_text", "subject_reference", "period_start", "period_end", "service_provider"],
    "Observation": ["status", "category_text", "code_text", "subject_reference", "encounter_reference",
                    "effective_datetime", "value_quantity", "value_unit", "value_string"],
    "Condition": ["clinical_status", "verification_status", "code_text", "subject_reference",
                  "encounter_reference", "onset_datetime", "recorded_date"],
}


def row_hash(values: Iterable[object]) -> str:
    """Stable sha256 hash over an ordered list of business-attribute values."""
    joined = "||".join("" if v is None else str(v) for v in values)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()
