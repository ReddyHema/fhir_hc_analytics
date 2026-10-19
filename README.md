# FHIR API Data Ingestion & Analytics — Medallion Lakehouse (Microsoft Fabric)

This project implements the assignment end to end against the public HAPI FHIR
sandbox (`https://hapi.fhir.org/baseR4`), targeting **Microsoft Fabric**
(Lakehouse + Notebooks + Data Pipeline). All PySpark/Delta logic was first
validated locally (see [local_dev/](./local_dev)) against the live API before
being adapted into Fabric notebook format, so the transformation logic is
proven to work, not just theoretical.

## Resources ingested
`Patient → Encounter → Observation → Condition` (strict order, enforced by the
orchestration pipeline).

## Architecture

```
FHIR API (hapi.fhir.org/baseR4)
        │  incremental window (_lastUpdated), paginated (_count + "next" link)
        ▼
RAW LAYER   Files/raw/<resource>/<extraction_date>/<batch_id>/page_NNNN.json  (verbatim API response)
        │   + Files/raw/<resource>/<extraction_date>/<batch_id>/_manifest.json (api_url, extraction_timestamp, page/record counts)
        ▼
BRONZE LAYER  Tables/bronze_<resource>  (Delta, Bundle entries flattened, metadata columns added, append-only)
        ▼
SILVER LAYER  Tables/silver_<resource>  (Delta, cleaned/deduped, typed, SCD Type 2 history)
        ▼
GOLD LAYER    Tables/gold_dim_*, Tables/gold_fact_*, Tables/gold_vw_*  (Delta tables/views for Power BI)
```

See [docs/architecture.md](./docs/architecture.md) for full layer-by-layer design,
[docs/data_model.md](./docs/data_model.md) for schemas + the SCD2 design,
[docs/orchestration.md](./docs/orchestration.md) for the pipeline design, and
[docs/powerbi_report.md](./docs/powerbi_report.md) for the 1-page Power BI report spec.

## Repository layout

| Path | Purpose |
|---|---|
| [fabric_workspace/](./fabric_workspace) | Fabric Git-integration-ready items: notebooks (`*.Notebook`) and the data pipeline (`*.DataPipeline`). Import these directly into a Fabric workspace connected to Git. |
| [config/pipeline_config.json](./config/pipeline_config.json) | Central config: API base URL, resource order, lookback days, page size. Every notebook reads from this file — no hardcoded URLs, resource names, or date windows anywhere in the code. |
| [local_dev/](./local_dev) | Local PySpark+Delta reproduction used to validate every transformation against the **live** FHIR API before porting to Fabric. Includes unit tests and a full local pipeline run. |
| [sql/gold_ddl.sql](./sql/gold_ddl.sql) | Reference DDL for the Gold star schema (for documentation / Power BI modeling). |
| [docs/](./docs) | Architecture, data model (including the full table-relationship/key diagram), orchestration, and Power BI documentation. |
| [powerbi/](./powerbi) | Exported `.pbix` (and a PDF/PNG snapshot) of the built 1-page Power BI report — see [powerbi/README.md](./powerbi/README.md) for export steps. |

## Why this is modular / reusable, not hardcoded

- **Config-driven**: the FHIR base URL, the 4 resource names/order, lookback window,
  page size, and retry settings all come from [config/pipeline_config.json](./config/pipeline_config.json),
  loaded once by `FHIR_Common_Utils` and reused by every other notebook. Changing the
  target FHIR server, adding a 5th resource, or widening the lookback window is a
  config edit, not a code edit.
- **One notebook per layer, parameterized by resource**: `01_Raw_Bronze_Ingestion` and
  `02_Silver_SCD2_Transformation` each take a single `resourceType` parameter and
  contain **zero** resource-specific control flow — the pipeline's `ForEach` activity
  drives all 4 resources through the exact same code path. The only per-resource
  pieces are small declarative lookup dictionaries (`SILVER_SELECT_EXPR`,
  `SCD2_BUSINESS_COLUMNS` in `FHIR_Common_Utils`) that describe *what columns* each
  FHIR resource type has — an unavoidable, intentional mapping (FHIR resources
  genuinely have different fields), not hardcoded business logic.
- **Shared utilities, no copy-paste**: all API/pagination/raw-write/watermark/
  metadata-log/SCD2-helper functions live once in `FHIR_Common_Utils` and are
  imported via `%run` by every other notebook.
- **No hardcoded dates or credentials**: the incremental window is always computed
  at runtime from `etl_control`'s watermark + `LOOKBACK_DAYS`, never a literal date.

## Submission checklist (ZIP contents)

Before zipping this folder for submission, make sure:

- [ ] `fabric_workspace/` contains the 4 notebooks + pipeline exactly as run in your workspace (already here).
- [ ] `powerbi/FHIR_Patient_Analytics.pbix` and a PDF/PNG snapshot are added — see [powerbi/README.md](./powerbi/README.md).
- [ ] `docs/` is up to date (architecture, data model + table relationships, orchestration, Power BI spec).
- [ ] `local_dev/` validation scripts are present (proof the logic was tested against the live API).
- [ ] No local run artifacts (`local_dev/lakehouse_local*/`, `*.log`, `__pycache__/`) are included — see [.gitignore](./.gitignore).

Then zip the **entire `fhir-lakehouse-assignment` folder** (e.g. right-click → Send to →
Compressed (zipped) folder on Windows, or `Compress-Archive` in PowerShell) and submit
the resulting `.zip`.

## How to deploy to Microsoft Fabric

1. Create a Fabric workspace (60-day trial) and a **Lakehouse** named `fhir_lakehouse`.
2. Connect the workspace to this Git repository (Workspace settings → Git integration),
   pointing at the `fabric_workspace/` folder, **or** manually create each item:
   - Create notebooks `FHIR_Common_Utils`, `01_Raw_Bronze_Ingestion`,
     `02_Silver_SCD2_Transformation`, `03_Gold_Views` and paste in the contents of
     the corresponding `notebook-content.py` files.
   - Attach `fhir_lakehouse` as the **default lakehouse** to each notebook.
   - Create a Data Pipeline named `FHIR_Ingestion_Pipeline` and recreate the
     activities described in `fabric_workspace/FHIR_Ingestion_Pipeline.DataPipeline/pipeline-content.json`
     (ForEach over resources → Raw/Bronze → ForEach → Silver SCD2 → Gold), or import the JSON directly
     via **Edit pipeline → Code view** (Fabric Data Pipelines support pasting pipeline JSON).
3. Run the pipeline. It will backfill the configured lookback window (default 3 days)
   on first run, and incrementally pick up only new days on subsequent daily runs
   (tracked in the `etl_control` watermark table).
4. Point a Power BI report (Direct Lake or Import) at the Gold tables — see
   [docs/powerbi_report.md](./docs/powerbi_report.md).

## How it was validated locally

Because this environment has no live Fabric/Databricks workspace, the exact
same business logic (API pagination, raw storage, bronze flattening, SCD2
merge, gold modeling) was executed locally with `pyspark` + `delta-spark`
against the **real** public FHIR API:

```powershell
cd local_dev
pip install -r requirements.txt
python run_local_pipeline.py
pytest -q
```

This produces a local `local_dev/lakehouse_local/` folder with the same
Raw/Bronze/Silver/Gold structure Fabric would produce in OneLake, and confirms
record counts, SCD2 versioning, and dedup logic are correct end-to-end.
