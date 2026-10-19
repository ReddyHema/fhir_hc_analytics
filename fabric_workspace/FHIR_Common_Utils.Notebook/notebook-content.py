# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "00000000-0000-0000-0000-000000000100",
# META       "default_lakehouse_name": "fhir_lakehouse",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## FHIR_Common_Utils
# Shared helpers used by every other notebook in this pipeline (imported via `%run FHIR_Common_Utils`).
#
# Responsibilities:
# - Build incremental FHIR search URLs and page through `Bundle.link[rel=next]`.
# - Read/write the `etl_control` watermark table (per-resource last extracted date).
# - Write to the `etl_metadata_log` table (api_url_or_params, extraction_timestamp, load_timestamp, status).
# - Provide the schema-tolerant field-extraction helpers used by the Silver notebook.
#
# This logic mirrors `local_dev/fhir_common.py` and `local_dev/run_local_pipeline.py` in the Git repo,
# which were validated end-to-end against the live https://hapi.fhir.org/baseR4 server.

# CELL ********

import hashlib
import json
import time
import uuid
from datetime import date, datetime, timedelta, timezone

import requests
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********

# Config is loaded from a copy of config/pipeline_config.json uploaded to the
# Lakehouse Files area (Files/config/pipeline_config.json). Fabric notebooks
# can read/write the attached default Lakehouse's Files through this local
# OS-mounted path.
LAKEHOUSE_ROOT = "/lakehouse/default"
CONFIG_PATH = f"{LAKEHOUSE_ROOT}/Files/config/pipeline_config.json"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    _cfg = json.load(f)

FHIR_BASE_URL = _cfg["fhir_base_url"]
RESOURCES_IN_ORDER = _cfg["resources_in_order"]
LOOKBACK_DAYS = _cfg.get("lookback_days", 3)
PAGE_SIZE = _cfg.get("page_size", 50)
MAX_PAGES_PER_DAY = _cfg.get("max_pages_per_day", 200)
REQUEST_TIMEOUT_SECONDS = _cfg.get("request_timeout_seconds", 30)
RETRY_MAX_ATTEMPTS = _cfg.get("retry", {}).get("max_attempts", 4)
RETRY_BACKOFF_SECONDS = _cfg.get("retry", {}).get("backoff_seconds", 2)

RAW_ROOT = "Files/raw"
# Relative (Fabric-resolves-against-default-lakehouse) path, used with
# mssparkutils.fs.put()/spark.read() so writes and reads go through the same
# OneLake access layer (see raw_write_page/raw_write_manifest below).

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********

# --------------------------------------------------------------------------
# API access: incremental window + pagination
# --------------------------------------------------------------------------

def build_search_url(resource_type: str, since: date, until: date, count: int = PAGE_SIZE) -> str:
    """FHIR search URL for an incremental [since, until) _lastUpdated window, sorted for determinism."""
    return (
        f"{FHIR_BASE_URL}/{resource_type}"
        f"?_lastUpdated=ge{since.isoformat()}"
        f"&_lastUpdated=lt{until.isoformat()}"
        f"&_count={count}"
        f"&_sort=_lastUpdated"
        f"&_format=json"
    )


def fetch_json(url: str) -> dict:
    """GET a FHIR URL with retry/backoff, returning parsed JSON."""
    last_err = None
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS, headers={"Accept": "application/fhir+json"})
            resp.raise_for_status()
            return resp.json()
        except Exception as err:  # noqa: BLE001 - want broad retry
            last_err = err
            if attempt < RETRY_MAX_ATTEMPTS:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    raise RuntimeError(f"Failed to fetch {url} after {RETRY_MAX_ATTEMPTS} attempts: {last_err}")


def next_link(bundle: dict):
    for link in bundle.get("link", []):
        if link.get("relation") == "next":
            return link.get("url")
    return None


def fetch_all_pages(resource_type: str, since: date, until: date):
    """Yields (page_number, page_url_called, bundle_json), following Bundle 'next' links."""
    url = build_search_url(resource_type, since, until)
    page_number = 1
    while url and page_number <= MAX_PAGES_PER_DAY:
        bundle = fetch_json(url)
        yield page_number, url, bundle
        url = next_link(bundle)
        page_number += 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********

# --------------------------------------------------------------------------
# RAW layer: store API responses verbatim, bucketed by date
# --------------------------------------------------------------------------

def new_batch_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:8]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def raw_write_page(resource_type: str, extraction_date: date, batch_id: str, page_number: int, bundle: dict) -> str:
    rel_dir = f"{RAW_ROOT}/{resource_type}/{extraction_date.isoformat()}/{batch_id}"
    rel_path = f"{rel_dir}/page_{page_number:04d}.json"
    # Written via mssparkutils.fs (not plain open()) so the file goes through the
    # same OneLake access path that spark.read() uses moments later in this same
    # notebook run. Writing through the local /lakehouse/default mount instead can
    # race with Spark's read and intermittently 400/404 on a "getStatus" HEAD call.
    mssparkutils.fs.mkdirs(rel_dir)
    mssparkutils.fs.put(rel_path, json.dumps(bundle), True)
    return rel_path


def raw_write_manifest(resource_type: str, extraction_date: date, batch_id: str, api_url_or_params: str,
                        extraction_timestamp: str, page_count: int, record_count: int) -> str:
    rel_dir = f"{RAW_ROOT}/{resource_type}/{extraction_date.isoformat()}/{batch_id}"
    rel_path = f"{rel_dir}/_manifest.json"
    manifest = {
        "resource_type": resource_type,
        "extraction_date": extraction_date.isoformat(),
        "batch_id": batch_id,
        "api_url_or_params": api_url_or_params,
        "extraction_timestamp": extraction_timestamp,
        "page_count": page_count,
        "record_count": record_count,
    }
    mssparkutils.fs.mkdirs(rel_dir)
    mssparkutils.fs.put(rel_path, json.dumps(manifest, indent=2), True)
    return rel_path


def incremental_window(control_last_date, lookback_days: int, run_date: date) -> list:
    """
    - first run (no watermark): last `lookback_days` days up to (not including) run_date.
    - subsequent runs: every day from the day after the watermark up to (not including) run_date.
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********

# --------------------------------------------------------------------------
# Control (watermark) + metadata-log managed Delta tables
# --------------------------------------------------------------------------

def table_exists(name: str) -> bool:
    return spark.catalog.tableExists(name)


def read_watermark(resource_type: str):
    if not table_exists("etl_control"):
        return None
    row = (
        spark.table("etl_control")
        .filter(F.col("resource_type") == resource_type)
        .orderBy(F.col("last_extraction_date").desc())
        .first()
    )
    return row["last_extraction_date"] if row else None


def upsert_watermark(resource_type: str, last_extraction_date: date) -> None:
    new_row = spark.createDataFrame(
        [(resource_type, last_extraction_date, utc_now_iso())],
        schema="resource_type string, last_extraction_date date, updated_at string",
    )
    if table_exists("etl_control"):
        from delta.tables import DeltaTable

        dt = DeltaTable.forName(spark, "etl_control")
        (
            dt.alias("t")
            .merge(new_row.alias("s"), "t.resource_type = s.resource_type")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        new_row.write.format("delta").mode("overwrite").saveAsTable("etl_control")


def log_metadata(resource_type: str, layer: str, api_url_or_params: str, records_processed: int,
                  status: str, run_id: str) -> None:
    row = spark.createDataFrame(
        [(run_id, resource_type, layer, api_url_or_params, utc_now_iso(), utc_now_iso(), records_processed, status)],
        schema=("run_id string, resource_type string, layer string, api_url_or_params string, "
                "api_called_at string, data_saved_at string, records_processed long, status string"),
    )
    mode = "append" if table_exists("etl_metadata_log") else "overwrite"
    row.write.format("delta").mode(mode).saveAsTable("etl_metadata_log")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********

# --------------------------------------------------------------------------
# Schema-tolerant FHIR field extraction (used by the Silver notebook)
# --------------------------------------------------------------------------
# FHIR elements are optional: a given day's batch may entirely lack a nested
# field (e.g. Condition.clinicalStatus.text), in which case Spark's inferred
# schema won't contain that struct field and a direct `F.col("a.b")` raises
# FIELD_NOT_FOUND. These helpers go through to_json()+get_json_object(),
# which safely returns NULL instead of failing when a field/column is absent.

def scalar_field(df: DataFrame, name: str, cast_to=StringType()):
    if name not in df.columns:
        return F.lit(None).cast(cast_to)
    return F.col(name).cast(cast_to)


def nested_field(df: DataFrame, top_field: str, json_path: str, cast_to=StringType()):
    if top_field not in df.columns:
        return F.lit(None).cast(cast_to)
    return F.get_json_object(F.to_json(F.col(top_field)), json_path).cast(cast_to)


def codeable_concept_text(df: DataFrame, top_field: str):
    """Best-effort human-readable label for a FHIR CodeableConcept field."""
    if top_field not in df.columns:
        return F.lit(None).cast(StringType())
    json_col = F.to_json(F.col(top_field))
    return F.coalesce(
        F.get_json_object(json_col, "$.text"),
        F.get_json_object(json_col, "$.coding[0].display"),
        F.get_json_object(json_col, "$.coding[0].code"),
    )


def reference_field(df: DataFrame, top_field: str):
    return nested_field(df, top_field, "$.reference")


def patient_name(df: DataFrame):
    if "name" not in df.columns:
        return F.lit(None).cast(StringType())
    json_col = F.to_json(F.col("name"))
    return F.concat_ws(" ", F.get_json_object(json_col, "$[0].given[0]"), F.get_json_object(json_col, "$[0].family"))


def patient_address(df: DataFrame):
    if "address" not in df.columns:
        return F.lit(None).cast(StringType())
    json_col = F.to_json(F.col("address"))
    return F.concat_ws(
        ", ",
        F.get_json_object(json_col, "$[0].line[0]"),
        F.get_json_object(json_col, "$[0].city"),
        F.get_json_object(json_col, "$[0].state"),
        F.get_json_object(json_col, "$[0].postalCode"),
    )


# Business attributes used for SCD2 change-hash computation per resource type
# (excludes technical/metadata columns).
SCD2_BUSINESS_COLUMNS = {
    "Patient": ["name_text", "gender", "birth_date", "deceased", "address_text", "marital_status", "managing_organization"],
    "Encounter": ["status", "class_code", "type_text", "subject_reference", "period_start", "period_end", "service_provider"],
    "Observation": ["status", "category_text", "code_text", "subject_reference", "encounter_reference",
                    "effective_datetime", "value_quantity", "value_unit", "value_string"],
    "Condition": ["clinical_status", "verification_status", "code_text", "subject_reference",
                  "encounter_reference", "onset_datetime", "recorded_date"],
}

print("FHIR_Common_Utils loaded: FHIR_BASE_URL=", FHIR_BASE_URL, "resources=", RESOURCES_IN_ORDER)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
