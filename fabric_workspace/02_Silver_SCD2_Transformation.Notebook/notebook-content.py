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

# ## 02_Silver_SCD2_Transformation
# Parameterized by `resourceType`. Reads `bronze_<resource>`, cleans/dedupes,
# and maintains history with **Slowly Changing Dimension Type 2**:
#
# - `business_key` = FHIR resource `id`.
# - `row_hash` = sha256 over the resource's business attributes (see
#   `SCD2_BUSINESS_COLUMNS` in `FHIR_Common_Utils`), used to detect real
#   content changes vs. a no-op re-extraction.
# - `is_current` / `effective_start_ts` / `effective_end_ts` track history:
#   when an existing current row's hash changes, it is closed out
#   (`is_current=false`, `effective_end_ts=now`) and a new current row is
#   inserted (`is_current=true`, `effective_end_ts=NULL`).
#
# This two-step Delta MERGE pattern (close-then-insert) and the schema was
# validated locally (`local_dev/test_scd2_behavior.py`) with a synthetic
# before/after change, confirming exactly 1 historical + 1 current row result.

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

from delta.tables import DeltaTable
from pyspark.sql import Window, functions as F
from pyspark.sql.types import StringType

assert resourceType in RESOURCES_IN_ORDER, f"Unknown resourceType: {resourceType}"
run_id = new_batch_id()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********

def _silver_select_patient(df: DataFrame):
    name_text_expr = patient_name(df)
    return df.select(
        scalar_field(df, "id").alias("business_key"),
        name_text_expr.alias("name_text"),
        scalar_field(df, "gender").alias("gender"),
        scalar_field(df, "birthDate").alias("birth_date"),
        F.coalesce(scalar_field(df, "deceasedBoolean"), scalar_field(df, "deceasedDateTime")).alias("deceased"),
        patient_address(df).alias("address_text"),
        codeable_concept_text(df, "maritalStatus").alias("marital_status"),
        reference_field(df, "managingOrganization").alias("managing_organization"),
        is_valid_name_expr(name_text_expr).alias("is_name_valid"),
        nested_field(df, "meta", "$.lastUpdated").alias("meta_last_updated"),
        "extraction_timestamp", "api_url_or_params", "extraction_date", "batch_id", "run_id",
    )


SILVER_SELECT_EXPR = {
    "Patient": _silver_select_patient,
    "Encounter": lambda df: df.select(
        scalar_field(df, "id").alias("business_key"),
        scalar_field(df, "status").alias("status"),
        nested_field(df, "class", "$.code").alias("class_code"),
        codeable_concept_text(df, "type").alias("type_text"),
        reference_field(df, "subject").alias("subject_reference"),
        nested_field(df, "period", "$.start").alias("period_start"),
        nested_field(df, "period", "$.end").alias("period_end"),
        reference_field(df, "serviceProvider").alias("service_provider"),
        nested_field(df, "meta", "$.lastUpdated").alias("meta_last_updated"),
        "extraction_timestamp", "api_url_or_params", "extraction_date", "batch_id", "run_id",
    ),
    "Observation": lambda df: df.select(
        scalar_field(df, "id").alias("business_key"),
        scalar_field(df, "status").alias("status"),
        codeable_concept_text(df, "category").alias("category_text"),
        codeable_concept_text(df, "code").alias("code_text"),
        reference_field(df, "subject").alias("subject_reference"),
        reference_field(df, "encounter").alias("encounter_reference"),
        scalar_field(df, "effectiveDateTime").alias("effective_datetime"),
        nested_field(df, "valueQuantity", "$.value").alias("value_quantity"),
        nested_field(df, "valueQuantity", "$.unit").alias("value_unit"),
        scalar_field(df, "valueString").alias("value_string"),
        nested_field(df, "meta", "$.lastUpdated").alias("meta_last_updated"),
        "extraction_timestamp", "api_url_or_params", "extraction_date", "batch_id", "run_id",
    ),
    "Condition": lambda df: df.select(
        scalar_field(df, "id").alias("business_key"),
        codeable_concept_text(df, "clinicalStatus").alias("clinical_status"),
        codeable_concept_text(df, "verificationStatus").alias("verification_status"),
        codeable_concept_text(df, "code").alias("code_text"),
        reference_field(df, "subject").alias("subject_reference"),
        reference_field(df, "encounter").alias("encounter_reference"),
        scalar_field(df, "onsetDateTime").alias("onset_datetime"),
        scalar_field(df, "recordedDate").alias("recorded_date"),
        nested_field(df, "meta", "$.lastUpdated").alias("meta_last_updated"),
        "extraction_timestamp", "api_url_or_params", "extraction_date", "batch_id", "run_id",
    ),
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********

bronze_table = f"bronze_{resourceType.lower()}"
silver_table = f"silver_{resourceType.lower()}"

if not table_exists(bronze_table):
    print(f"[{resourceType}] no bronze data yet, skipping silver")
    inserted_count = 0
else:
    bronze_df = spark.table(bronze_table)
    selected = SILVER_SELECT_EXPR[resourceType](bronze_df)

    # Dedupe: keep the latest-extracted record per (business_key, meta_last_updated).
    w = Window.partitionBy("business_key", "meta_last_updated").orderBy(F.col("extraction_timestamp").desc())
    deduped = selected.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    business_cols = SCD2_BUSINESS_COLUMNS[resourceType]
    source = deduped.withColumn(
        "row_hash",
        F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast(StringType()), F.lit("")) for c in business_cols]), 256),
    )

    now_ts = utc_now_iso()
    # One row per business_key representing the newest incoming version.
    w_latest = Window.partitionBy("business_key").orderBy(F.col("meta_last_updated").desc())
    incoming = source.withColumn("_rn", F.row_number().over(w_latest)).filter(F.col("_rn") == 1).drop("_rn")

    if not table_exists(silver_table):
        initial = (
            incoming.withColumn("effective_start_ts", F.lit(now_ts))
            .withColumn("effective_end_ts", F.lit(None).cast(StringType()))
            .withColumn("is_current", F.lit(True))
        )
        initial.write.format("delta").mode("overwrite").saveAsTable(silver_table)
        inserted_count = initial.count()
    else:
        target = DeltaTable.forName(spark, silver_table)

        # Step 1: close out current rows whose hash changed.
        (
            target.alias("t")
            .merge(incoming.alias("s"), "t.business_key = s.business_key AND t.is_current = true")
            .whenMatchedUpdate(
                condition="t.row_hash <> s.row_hash",
                set={"is_current": F.lit(False), "effective_end_ts": F.lit(now_ts)},
            )
            .execute()
        )

        # Step 2: insert brand-new keys and new versions of changed keys.
        current_df = spark.table(silver_table).filter(F.col("is_current") == True)  # noqa: E712
        to_insert = incoming.join(
            current_df.select("business_key", "row_hash"), on="business_key", how="left_anti"
        ).unionByName(
            incoming.alias("i")
            .join(current_df.alias("c"), on="business_key", how="inner")
            .filter(F.col("i.row_hash") != F.col("c.row_hash"))
            .select("i.*")
        )
        to_insert = (
            to_insert.withColumn("effective_start_ts", F.lit(now_ts))
            .withColumn("effective_end_ts", F.lit(None).cast(StringType()))
            .withColumn("is_current", F.lit(True))
        )
        inserted_count = to_insert.count()
        if inserted_count > 0:
            to_insert.write.format("delta").mode("append").saveAsTable(silver_table)

    log_metadata(resourceType, "silver_scd2", "bronze->silver", inserted_count, "SUCCESS", run_id)

print(f"[{resourceType}] silver rows inserted/changed: {inserted_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********

mssparkutils.notebook.exit(json.dumps({"resourceType": resourceType, "runId": run_id, "silverRowsChanged": inserted_count}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
