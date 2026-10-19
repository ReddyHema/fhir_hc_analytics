# Orchestration

## Pipeline: `FHIR_Ingestion_Pipeline` (Fabric Data Pipeline)

Defined in [fabric_workspace/FHIR_Ingestion_Pipeline.DataPipeline/pipeline-content.json](../fabric_workspace/FHIR_Ingestion_Pipeline.DataPipeline/pipeline-content.json).

```
ForEach_Raw_Bronze_Ingestion (isSequential=true, items=[Patient,Encounter,Observation,Condition])
   └── Notebook_Raw_Bronze_Ingestion  (01_Raw_Bronze_Ingestion, resourceType=@item())
        │
        ▼  (on Succeeded)
ForEach_Silver_SCD2_Transformation (isSequential=true, items=[Patient,Encounter,Observation,Condition])
   └── Notebook_Silver_SCD2_Transformation  (02_Silver_SCD2_Transformation, resourceType=@item())
        │
        ▼  (on Succeeded)
Notebook_Gold_Views  (03_Gold_Views, no parameters)
```

- **Order is enforced** by `isSequential: true` on each `ForEach` iterating the pipeline
  parameter `resourcesInOrder = ["Patient", "Encounter", "Observation", "Condition"]` —
  Patient is always ingested (and transformed) before Encounter, before Observation,
  before Condition, matching the assignment's required call order.
- **Transformation** happens inside each Notebook activity (Bronze flattening in step 1,
  SCD2 cleaning/deduping in step 2, star-schema + views in step 3).
- **Metadata logging** happens inside the notebooks themselves (`etl_metadata_log`,
  `etl_control`), so it is captured regardless of whether the pipeline is run from the
  UI, a schedule, or triggered on demand — not bolted on as a separate pipeline activity.
- **Data loading** is the Delta `saveAsTable`/`MERGE` calls inside each notebook.

## Importing into Fabric

1. Create the 4 notebooks and 1 data pipeline described in the repo root
   [README.md](../README.md) deployment steps.
2. After creating `01_Raw_Bronze_Ingestion`, `02_Silver_SCD2_Transformation`, and
   `03_Gold_Views` in your workspace, open `FHIR_Ingestion_Pipeline` in **Edit pipeline →
   Code view**, paste in `pipeline-content.json`, and replace the placeholder
   `notebookId` GUIDs (`00000000-...-0002/0003/0004`) with your workspace's real notebook
   IDs — or simply reselect each Notebook activity's target notebook from the dropdown in
   the pipeline canvas, which rewrites the ID automatically.
3. If you instead connect the whole `fabric_workspace/` folder via **Git integration**,
   Fabric resolves cross-item references by `logicalId` (see each item's `.platform`
   file) automatically on sync — no manual ID editing needed.

## Scheduling

Add a schedule trigger (e.g. daily at a fixed time) to `FHIR_Ingestion_Pipeline` in the
Fabric UI. Because of the `etl_control` watermark, a daily schedule naturally produces
incremental loads: the first run backfills `lookback_days` (default 3) days, and every
subsequent run only pulls the one new day since the last successful watermark — with
no duplicate extraction and no gaps.
