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

# ## 01_Raw_Bronze_Ingestion
# Parameterized by `resourceType`. Invoked once per FHIR resource by the
# `FHIR_Ingestion_Pipeline` Data Pipeline, **sequentially**, in the order
# Patient -> Encounter -> Observation -> Condition.
#
# 1. **RAW**: incrementally fetch `_lastUpdated` windows (paginated via the
#    Bundle `next` link) and store each API page verbatim as JSON under
#    `Files/raw/<resource>/<date>/<batch_id>/page_NNNN.json`, plus a
#    `_manifest.json` capturing `api_url_or_params` + `extraction_timestamp`.
# 2. **BRONZE**: read the raw JSON back, explode `Bundle.entry`, flatten to
#    one row per FHIR resource, and append to the managed Delta table
#    `bronze_<resource>` with ingestion metadata columns.
# 3. Advances the `etl_control` watermark and writes to `etl_metadata_log`.

# PARAMETERS CELL ********

# Overridden by the pipeline's Notebook activity `baseParameters`.
resourceType = "Patient"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********

%run FHIR_Common_Utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********

import json
from datetime import datetime, timezone

from pyspark.sql import functions as F

assert resourceType in RESOURCES_IN_ORDER, f"Unknown resourceType: {resourceType}"

run_id = new_batch_id()
run_date = datetime.now(timezone.utc).date()

watermark = read_watermark(resourceType)
days_to_process = incremental_window(watermark, LOOKBACK_DAYS, run_date)

print(f"[{resourceType}] watermark={watermark} run_date={run_date} days_to_process={[d.isoformat() for d in days_to_process]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********

total_records = 0
bronze_frames = []

for day in days_to_process:
    until = day.fromordinal(day.toordinal() + 1)
    batch_id = new_batch_id()
    extraction_timestamp = utc_now_iso()
    page_count = 0
    record_count = 0
    last_url = None

    for page_number, url, bundle in fetch_all_pages(resourceType, day, until):
        raw_write_page(resourceType, day, batch_id, page_number, bundle)
        page_count = page_number
        record_count += len(bundle.get("entry", []))
        last_url = url

    raw_write_manifest(resourceType, day, batch_id, last_url or "", extraction_timestamp, page_count, record_count)
    total_records += record_count
    print(f"  {day}: {page_count} page(s), {record_count} record(s)")

    batch_dir = f"{RAW_ROOT}/{resourceType}/{day.isoformat()}/{batch_id}"
    page_files = [f"{batch_dir}/page_{n:04d}.json" for n in range(1, page_count + 1)]
    if not page_files:
        continue

    raw_df = spark.read.option("multiLine", True).json(page_files)
    if "entry" not in raw_df.columns:
        continue

    entries = raw_df.select(F.explode("entry").alias("entry")).select("entry.resource.*")
    entries = (
        entries.withColumn("extraction_timestamp", F.lit(extraction_timestamp))
        .withColumn("api_url_or_params", F.lit(last_url))
        .withColumn("extraction_date", F.lit(day.isoformat()))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("run_id", F.lit(run_id))
    )
    bronze_frames.append(entries)
    upsert_watermark(resourceType, day)

if bronze_frames:
    combined = bronze_frames[0]
    for extra in bronze_frames[1:]:
        combined = combined.unionByName(extra, allowMissingColumns=True)
    (
        combined.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(f"bronze_{resourceType.lower()}")
    )

log_metadata(resourceType, "raw+bronze", FHIR_BASE_URL, total_records, "SUCCESS", run_id)
print(f"[{resourceType}] total records ingested this run: {total_records}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********

mssparkutils.notebook.exit(json.dumps({
    "resourceType": resourceType,
    "runId": run_id,
    "daysProcessed": [d.isoformat() for d in days_to_process],
    "recordsIngested": total_records,
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
