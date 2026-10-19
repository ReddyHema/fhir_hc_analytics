# Data Model

## Bronze (append-only, raw-shaped)

One managed Delta table per resource: `bronze_patient`, `bronze_encounter`, `bronze_observation`, `bronze_condition`.
Columns = the full flattened FHIR resource JSON (schema inferred/evolved by Spark) **plus**:

| Column | Description |
|---|---|
| `extraction_timestamp` | UTC ISO timestamp when this batch's API calls were made. |
| `api_url_or_params` | The last FHIR search URL called for this batch (includes `_lastUpdated` window + pagination params). |
| `extraction_date` | The `_lastUpdated` day this batch represents (raw layer partition key). |
| `batch_id` | Unique id correlating this row back to its `Files/raw/.../<batch_id>/` folder. |
| `run_id` | Correlates this row to a specific pipeline run (joins to `etl_metadata_log`). |

## Silver (cleaned, deduped, SCD Type 2)

One managed Delta table per resource: `silver_patient`, `silver_encounter`, `silver_observation`, `silver_condition`.

Common technical columns on every Silver table:

| Column | Description |
|---|---|
| `business_key` | FHIR resource `id` (natural key). |
| `row_hash` | `sha2(business columns, 256)` — used to detect a real content change vs. a no-op re-extraction. |
| `effective_start_ts` | When this version became current. |
| `effective_end_ts` | When this version was superseded (`NULL` while current). |
| `is_current` | `true` for exactly one row per `business_key` at any time. |
| `extraction_timestamp`, `api_url_or_params`, `extraction_date`, `batch_id`, `run_id` | Carried through from Bronze for lineage. |

Business columns per resource:

| Resource | Business columns |
|---|---|
| **Patient** | `name_text`, `gender`, `birth_date`, `deceased`, `address_text`, `marital_status`, `managing_organization` |
| **Encounter** | `status`, `class_code`, `type_text`, `subject_reference`, `period_start`, `period_end`, `service_provider` |
| **Observation** | `status`, `category_text`, `code_text`, `subject_reference`, `encounter_reference`, `effective_datetime`, `value_quantity`, `value_unit`, `value_string` |
| **Condition** | `clinical_status`, `verification_status`, `code_text`, `subject_reference`, `encounter_reference`, `onset_datetime`, `recorded_date` |

### SCD Type 2 merge algorithm (implemented in `02_Silver_SCD2_Transformation`)

1. Compute `row_hash` over the business columns for the latest incoming version of each `business_key` (one row per key, picking the newest `meta_last_updated`).
2. **First load** for a resource: insert all rows with `is_current=true`, `effective_start_ts=now`, `effective_end_ts=NULL`.
3. **Subsequent loads** — a two-step Delta `MERGE`:
   - **Close out changed rows**: `MERGE ... WHEN MATCHED AND target.is_current=true AND target.row_hash <> source.row_hash THEN UPDATE SET is_current=false, effective_end_ts=now`.
   - **Insert new/changed versions**: any `business_key` not present among current rows (brand new), or whose incoming `row_hash` differs from the (now-closed) current row, is inserted as a new row with `is_current=true`, `effective_start_ts=now`, `effective_end_ts=NULL`.
   - Unchanged keys produce **no** new row (verified by [local_dev/test_scd2_behavior.py](../local_dev/test_scd2_behavior.py)).

This guarantees: exactly one current row per `business_key` at all times, and a full, queryable history of every change (`SELECT * FROM silver_patient WHERE business_key = '...' ORDER BY effective_start_ts`).

## Gold (star schema + reporting views)

| Table/View | Type | Grain | Notes |
|---|---|---|---|
| `gold_dim_patient` | Delta table | 1 row / patient | From `silver_patient` current rows. `birth_date` is cast to `DATE`. |
| `gold_fact_encounter` | Delta table | 1 row / encounter | `patient_id` derived from `subject_reference` via regex. `period_start`/`period_end` cast to `TIMESTAMP`. |
| `gold_fact_observation` | Delta table | 1 row / observation | `patient_id`/`encounter_id` derived from references. `effective_datetime` cast to `TIMESTAMP`, `value_quantity` cast to `DOUBLE`. |
| `gold_fact_condition` | Delta table | 1 row / condition | `patient_id`/`encounter_id` derived from references. `onset_datetime`/`recorded_date` cast to `TIMESTAMP`. |
| `gold_vw_patient_summary` | View | 1 row / patient | Patient + encounter/condition/observation counts. |
| `gold_vw_encounter_detail` | View | 1 row / encounter | Encounter joined to patient name for readability. |
| `gold_vw_condition_summary` | View | 1 row / condition | Condition joined to patient name. |
| `gold_vw_observation_trends` | View | 1 row / (code, day) | Daily observation count + average value, for trend charts. |

> **Why cast here and not earlier?** Silver keeps every business column as `STRING`
> so the SCD2 `row_hash` is computed consistently regardless of how a date happens
> to be formatted in a given API response. Gold is the first layer where real types
> matter — and it must cast to genuine Spark `DATE`/`TIMESTAMP`/`DOUBLE` types (not
> just relabel them in a downstream tool), because **Direct Lake** semantic models
> in Power BI read the Delta/Parquet files directly and require the column's
> physical storage type to already match; they cannot reinterpret a stored
> `STRING` column as a date/number the way Import-mode or DAX type coercion can.

See [sql/gold_ddl.sql](../sql/gold_ddl.sql) for the reference DDL.

## Table Relationships (keys & lineage)

```
RAW (Files, JSON, no schema)
   Files/raw/<ResourceType>/<extraction_date>/<batch_id>/page_NNNN.json
   Files/raw/<ResourceType>/<extraction_date>/<batch_id>/_manifest.json
        │  batch_id, extraction_date, api_url_or_params  (carried forward as columns)
        ▼
BRONZE                                            SCD2 key = FHIR "id"
┌─────────────────┐  ┌───────────────────┐  ┌─────────────────────┐  ┌────────────────────┐
│ bronze_patient  │  │ bronze_encounter  │  │ bronze_observation  │  │ bronze_condition   │
│ id (PK, FHIR)   │  │ id (PK, FHIR)     │  │ id (PK, FHIR)       │  │ id (PK, FHIR)      │
│ ...resource cols│  │ subject.reference │  │ subject.reference   │  │ subject.reference  │
│ batch_id/run_id │  │  -> Patient/<id>  │  │  -> Patient/<id>    │  │  -> Patient/<id>   │
└────────┬────────┘  │ (FK, logical,     │  │ encounter.reference │  │ encounter.reference│
         │           │  not enforced)    │  │  -> Encounter/<id>  │  │  -> Encounter/<id> │
         │           └─────────┬─────────┘  │ (FK, logical)       │  │ (FK, logical)      │
         │                     │            └──────────┬──────────┘  └──────────┬─────────┘
         ▼                     ▼                       ▼                       ▼
SILVER (SCD2; business_key = Bronze "id"; exactly 1 row per business_key WHERE is_current=true)
┌─────────────────┐  ┌───────────────────┐  ┌─────────────────────┐  ┌────────────────────┐
│ silver_patient  │  │ silver_encounter  │  │ silver_observation  │  │ silver_condition   │
│ business_key PK │  │ business_key PK   │  │ business_key PK     │  │ business_key PK    │
│ row_hash        │  │ subject_reference │  │ subject_reference   │  │ subject_reference  │
│ is_current      │  │ (FK -> Patient)   │  │ encounter_reference │  │ encounter_reference│
│ effective_*_ts  │  │                   │  │ (FK -> Encounter)   │  │ (FK -> Encounter)  │
└────────┬────────┘  └─────────┬─────────┘  └──────────┬──────────┘  └──────────┬─────────┘
         │ is_current=true only (same filter on every Silver table feeding Gold)
         ▼                     ▼                       ▼                       ▼
GOLD (physical Delta tables; FK columns are regex-extracted from the *_reference strings above)
┌───────────────────┐
│ gold_dim_patient   │◄───────────────┐───────────────────────┐────────────────────────┐
│ patient_id   (PK)  │                │                       │                        │
└────────────────────┘                │                       │                        │
                              ┌────────┴──────────┐  ┌─────────┴───────────┐  ┌─────────┴──────────┐
                              │ gold_fact_encounter│  │ gold_fact_observation│  │ gold_fact_condition │
                              │ encounter_id  (PK) │◄─┤ encounter_id   (FK) │  │ encounter_id   (FK) │
                              │ patient_id    (FK) │  │ patient_id     (FK) │  │ patient_id     (FK) │
                              └────────────────────┘  └──────────────────────┘  └─────────────────────┘
```

**Cross-cutting (not resource-specific):**

| Table | Key | Relationship |
|---|---|---|
| `etl_control` | PK `resource_type` | One watermark row per FHIR resource; read/written by every `01_Raw_Bronze_Ingestion` run. |
| `etl_metadata_log` | append-only, `run_id` + `resource_type` + `layer` | Every Raw+Bronze, Silver, and Gold step appends one row here — the full cross-layer audit trail; no single-table PK, intentionally append-only for history. |

**Why `patient_id`/`encounter_id` foreign keys aren't DB-enforced constraints:** Delta Lake
(like most lakehouse formats) doesn't support declarative foreign-key constraints. The
relationships above are *logical* — built once in Gold by regex-extracting the id out of
each FHIR `Reference` string (e.g. `"Patient/abc123"` → `"abc123"`) — and are declared as
real relationships inside the Power BI semantic model (1:* from `gold_dim_patient` to each
fact table), which is where they're actually enforced/used for filtering and aggregation.
