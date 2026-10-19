# Architecture

## Overview

```
                 ┌───────────────────────────────────────────┐
                 │   https://hapi.fhir.org/baseR4 (HAPI FHIR) │
                 └───────────────────────┬────────────────────┘
                                          │ GET /<Resource>?_lastUpdated=ge..&lt..&_count=50&_sort=_lastUpdated
                                          │ follow Bundle.link[rel=next] until exhausted
                                          ▼
┌──────────────────────────────── RAW LAYER (Files) ─────────────────────────────────┐
│ Files/raw/<ResourceType>/<extraction_date=YYYY-MM-DD>/<batch_id>/page_0001.json ... │
│ Files/raw/<ResourceType>/<extraction_date>/<batch_id>/_manifest.json                │
│   { resource_type, extraction_date, batch_id, api_url_or_params,                    │
│     extraction_timestamp, page_count, record_count }                               │
└───────────────────────────────────┬─────────────────────────────────────────────────┘
                                     │ spark.read.json + explode(Bundle.entry)
                                     ▼
┌──────────────────── BRONZE LAYER (managed Delta tables) ───────────────────────────┐
│ bronze_patient / bronze_encounter / bronze_observation / bronze_condition          │
│   one row per FHIR resource (flattened from Bundle.entry[].resource)               │
│   + extraction_timestamp, api_url_or_params, extraction_date, batch_id, run_id     │
│   append-only, mergeSchema=true (tolerates FHIR's variable/optional elements)      │
└───────────────────────────────────┬─────────────────────────────────────────────────┘
                                     │ clean, type, dedupe, SCD2 merge
                                     ▼
┌──────────────────── SILVER LAYER (managed Delta tables) ───────────────────────────┐
│ silver_patient / silver_encounter / silver_observation / silver_condition          │
│   business_key, typed business columns, row_hash,                                  │
│   effective_start_ts, effective_end_ts, is_current   <-- SCD Type 2                │
└───────────────────────────────────┬─────────────────────────────────────────────────┘
                                     │ is_current = true only
                                     ▼
┌──────────────────── GOLD LAYER (tables + views) ────────────────────────────────────┐
│ gold_dim_patient, gold_fact_encounter, gold_fact_observation, gold_fact_condition    │
│ gold_vw_patient_summary, gold_vw_encounter_detail,                                   │
│ gold_vw_condition_summary, gold_vw_observation_trends                               │
└───────────────────────────────────┬─────────────────────────────────────────────────┘
                                     ▼
                              Power BI (Direct Lake / Import)
```

## Control tables (cross-cutting)

| Table | Purpose |
|---|---|
| `etl_control` | Watermark per `resource_type`: `last_extraction_date`, `updated_at`. Drives incremental pickup so re-runs on the same day are no-ops and each new day is only ever extracted once. |
| `etl_metadata_log` | Append-only audit log: `run_id`, `resource_type`, `layer`, `api_url_or_params`, `api_called_at`, `data_saved_at`, `records_processed`, `status`. Answers "when was each API called" and "when was data saved" for every layer transition (raw+bronze, silver_scd2, gold). |

## Raw layer design

- **Bucketed by date**: `Files/raw/<ResourceType>/<extraction_date>/...` so every day's pull is isolated, replayable, and auditable independently.
- **Verbatim storage**: each page of the FHIR `Bundle` response is written byte-for-byte as returned by the API (no transformation), satisfying "store JSON response as-is".
- **Pagination**: `_count=50` plus following `Bundle.link[relation="next"]` (HAPI's opaque `_getpages` continuation token) until no `next` link remains or `max_pages_per_day` safety cap is hit.
- **Incremental window**: `_lastUpdated=ge<day>&_lastUpdated=lt<day+1>` with `_sort=_lastUpdated`, computed per day across the configured lookback (default 3 days) on first run, or from `etl_control.last_extraction_date + 1 day` onward on subsequent runs.

## Bronze layer design

- Reads back the raw JSON for a batch with `spark.read.option("multiLine", True).json(...)`, explodes `Bundle.entry`, and selects `entry.resource.*` — i.e. one row per FHIR resource instance.
- Adds metadata columns: `extraction_timestamp`, `api_url_or_params`, `extraction_date`, `batch_id`, `run_id`.
- Appends with `mergeSchema=true` because FHIR resources are schema-flexible (optional elements vary batch to batch); Delta's schema evolution keeps the bronze table queryable as a superset schema over time.

## Silver layer design

- Selects a stable, typed subset of business columns per resource type (see [data_model.md](./data_model.md)).
- Uses schema-tolerant extraction (`to_json` + `get_json_object`) for nested/optional FHIR elements (e.g. `CodeableConcept.text`, `Reference.reference`) so a batch missing an optional sub-field never breaks the job — a real failure mode hit and fixed during local validation (see root [README.md](../README.md)).
- Deduplicates multiple extractions of the same `(business_key, meta_last_updated)`.
- Maintains **SCD Type 2** history — see [data_model.md](./data_model.md) for the exact merge algorithm.

## Gold layer design

- `gold_dim_patient` / `gold_fact_encounter` / `gold_fact_observation` / `gold_fact_condition`: physical Delta tables built from **current** (`is_current = true`) Silver rows only — a simple star schema, good for Power BI Direct Lake.
- `gold_vw_*`: SQL views layered on top of the physical Gold tables for report-ready, pre-joined/pre-aggregated shapes (patient 360, encounter detail, condition summary, observation trend). See [sql/gold_ddl.sql](../sql/gold_ddl.sql).

## Orchestration

See [orchestration.md](./orchestration.md).
