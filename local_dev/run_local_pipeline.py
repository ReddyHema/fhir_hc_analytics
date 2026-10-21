"""
End-to-end local reproduction of the Medallion pipeline, run with real
PySpark + Delta Lake against the live public HAPI FHIR server. This proves
the raw/bronze/silver(SCD2)/gold logic is functionally correct before it is
ported into Fabric notebook syntax.

Usage:
    cd local_dev
    pip install -r requirements.txt
    python run_local_pipeline.py
"""
from __future__ import annotations

import json
import shutil
from datetime import date, datetime, timezone
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession, Window, functions as F
from pyspark.sql.types import StringType

from fhir_common import (
    FhirConfig,
    SCD2_BUSINESS_COLUMNS,
    fetch_all_pages,
    incremental_window,
    new_batch_id,
    raw_write_manifest,
    raw_write_page,
    utc_now_iso,
)

HERE = Path(__file__).parent
LAKEHOUSE = HERE / "lakehouse_local"
RAW_ROOT = LAKEHOUSE / "Files" / "raw"
TABLES_ROOT = LAKEHOUSE / "Tables"
CONFIG_PATH = HERE.parent / "config" / "pipeline_config.json"


def delta_exists(path: str | Path) -> bool:
    """True only if `path` is an actual initialized Delta table (has a _delta_log)."""
    return (Path(path) / "_delta_log").exists()


def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("fhir-medallion-local")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# --------------------------------------------------------------------------
# Control / metadata log tables
# --------------------------------------------------------------------------

def read_watermark(spark: SparkSession, resource_type: str) -> date | None:
    path = str(TABLES_ROOT / "etl_control")
    if not delta_exists(path):
        return None
    df = spark.read.format("delta").load(path).filter(F.col("resource_type") == resource_type)
    row = df.orderBy(F.col("last_extraction_date").desc()).first()
    return row["last_extraction_date"] if row else None


def upsert_watermark(spark: SparkSession, resource_type: str, last_extraction_date: date) -> None:
    path = str(TABLES_ROOT / "etl_control")
    new_row = spark.createDataFrame(
        [(resource_type, last_extraction_date, utc_now_iso())],
        schema="resource_type string, last_extraction_date date, updated_at string",
    )
    if delta_exists(path):
        from delta.tables import DeltaTable

        dt = DeltaTable.forPath(spark, path)
        (
            dt.alias("t")
            .merge(new_row.alias("s"), "t.resource_type = s.resource_type")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        new_row.write.format("delta").mode("overwrite").save(path)


def log_metadata(spark: SparkSession, resource_type: str, layer: str, api_url_or_params: str,
                  records_processed: int, status: str, run_id: str) -> None:
    path = str(TABLES_ROOT / "etl_metadata_log")
    row = spark.createDataFrame(
        [(run_id, resource_type, layer, api_url_or_params, utc_now_iso(), utc_now_iso(), records_processed, status)],
        schema=("run_id string, resource_type string, layer string, api_url_or_params string, "
                "api_called_at string, data_saved_at string, records_processed long, status string"),
    )
    if delta_exists(path):
        row.write.format("delta").mode("append").save(path)
    else:
        row.write.format("delta").mode("overwrite").save(path)


# --------------------------------------------------------------------------
# RAW -> BRONZE
# --------------------------------------------------------------------------

def ingest_raw_and_bronze(spark: SparkSession, cfg: FhirConfig, resource_type: str, run_id: str) -> int:
    run_date = datetime.now(timezone.utc).date()
    watermark = read_watermark(spark, resource_type)
    days = incremental_window(watermark, cfg.lookback_days, run_date)
    if not days:
        print(f"[{resource_type}] up to date, no new days to extract (watermark={watermark})")
        return 0

    print(f"[{resource_type}] extracting days: {[d.isoformat() for d in days]}")
    total_records = 0
    bronze_frames: list[DataFrame] = []

    for day in days:
        until = day.__class__.fromordinal(day.toordinal() + 1)
        batch_id = new_batch_id()
        extraction_timestamp = utc_now_iso()
        page_count = 0
        record_count = 0
        last_url = None

        for page_number, url, bundle in fetch_all_pages(cfg.fhir_base_url, resource_type, day, until, cfg):
            raw_write_page(RAW_ROOT, resource_type, day, batch_id, page_number, bundle)
            page_count = page_number
            record_count += len(bundle.get("entry", []))
            last_url = url

        raw_write_manifest(RAW_ROOT, resource_type, day, batch_id, last_url or "", extraction_timestamp,
                            page_count, record_count)
        total_records += record_count
        print(f"  {day}: {page_count} page(s), {record_count} record(s)")

        # BRONZE: read back everything just written for this batch and flatten.
        batch_dir = RAW_ROOT / resource_type / day.isoformat() / batch_id
        page_files = sorted(str(p) for p in batch_dir.glob("page_*.json"))
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

        upsert_watermark(spark, resource_type, day)

    if bronze_frames:
        combined = bronze_frames[0]
        for extra in bronze_frames[1:]:
            combined = combined.unionByName(extra, allowMissingColumns=True)
        bronze_path = str(TABLES_ROOT / f"bronze_{resource_type.lower()}")
        combined.write.format("delta").mode("append").option("mergeSchema", "true").save(bronze_path)

    log_metadata(spark, resource_type, "raw+bronze", cfg.fhir_base_url, total_records, "SUCCESS", run_id)
    return total_records


# --------------------------------------------------------------------------
# BRONZE -> SILVER (clean, dedupe, SCD2)
# --------------------------------------------------------------------------
#
# FHIR resources are schema-flexible: optional elements (e.g. CodeableConcept
# "text", a Patient's "address", a Condition's "clinicalStatus.text") may be
# entirely absent from a given day's extracted batch, in which case Spark's
# inferred schema won't even contain that struct field and a direct
# `F.col("a.b")` access raises FIELD_NOT_FOUND at analysis time. The helpers
# below extract optional nested fields via to_json()+get_json_object(), which
# safely returns NULL instead of failing when a field/column is missing.

def _scalar(df: DataFrame, name: str, cast_to=StringType()):
    if name not in df.columns:
        return F.lit(None).cast(cast_to)
    return F.col(name).cast(cast_to)


def _nested(df: DataFrame, top_field: str, json_path: str, cast_to=StringType()):
    if top_field not in df.columns:
        return F.lit(None).cast(cast_to)
    return F.get_json_object(F.to_json(F.col(top_field)), json_path).cast(cast_to)


def _codeable_concept_text(df: DataFrame, top_field: str):
    """Best-effort human-readable label for a FHIR CodeableConcept field."""
    if top_field not in df.columns:
        return F.lit(None).cast(StringType())
    json_col = F.to_json(F.col(top_field))
    return F.coalesce(
        F.get_json_object(json_col, "$.text"),
        F.get_json_object(json_col, "$.coding[0].display"),
        F.get_json_object(json_col, "$.coding[0].code"),
    )


def _reference(df: DataFrame, top_field: str):
    return _nested(df, top_field, "$.reference")


def _patient_name(df: DataFrame):
    if "name" not in df.columns:
        return F.lit(None).cast(StringType())
    json_col = F.to_json(F.col("name"))
    return F.concat_ws(
        " ",
        F.get_json_object(json_col, "$[0].given[0]"),
        F.get_json_object(json_col, "$[0].family"),
    )


def _patient_address(df: DataFrame):
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


def _is_valid_name_expr(name_col):
    """Mirrors is_valid_name_expr in FHIR_Common_Utils (see that notebook for the
    full rationale): flags template placeholders / garbled test-sandbox names
    without deleting the row."""
    letter_chars = F.length(F.regexp_replace(name_col, "[^\\p{L}\\s]", ""))
    total_chars = F.greatest(F.length(name_col), F.lit(1))
    letter_ratio = letter_chars / total_chars
    return (
        name_col.isNotNull()
        & (F.trim(name_col) != "")
        & (~name_col.rlike("\\{\\{|\\}\\}"))
        & (letter_ratio >= F.lit(0.6))
    )


def _silver_select_patient(df: DataFrame):
    name_text_expr = _patient_name(df)
    return df.select(
        _scalar(df, "id").alias("business_key"),
        name_text_expr.alias("name_text"),
        _scalar(df, "gender").alias("gender"),
        _scalar(df, "birthDate").alias("birth_date"),
        F.coalesce(_scalar(df, "deceasedBoolean"), _scalar(df, "deceasedDateTime")).alias("deceased"),
        _patient_address(df).alias("address_text"),
        _codeable_concept_text(df, "maritalStatus").alias("marital_status"),
        _reference(df, "managingOrganization").alias("managing_organization"),
        _is_valid_name_expr(name_text_expr).alias("is_name_valid"),
        _nested(df, "meta", "$.lastUpdated").alias("meta_last_updated"),
        "extraction_timestamp", "api_url_or_params", "extraction_date", "batch_id", "run_id",
    )


SILVER_SELECT_EXPR = {
    "Patient": _silver_select_patient,
    "Encounter": lambda df: df.select(
        _scalar(df, "id").alias("business_key"),
        _scalar(df, "status").alias("status"),
        _nested(df, "class", "$.code").alias("class_code"),
        _codeable_concept_text(df, "type").alias("type_text"),
        _reference(df, "subject").alias("subject_reference"),
        _nested(df, "period", "$.start").alias("period_start"),
        _nested(df, "period", "$.end").alias("period_end"),
        _reference(df, "serviceProvider").alias("service_provider"),
        _nested(df, "meta", "$.lastUpdated").alias("meta_last_updated"),
        "extraction_timestamp", "api_url_or_params", "extraction_date", "batch_id", "run_id",
    ),
    "Observation": lambda df: df.select(
        _scalar(df, "id").alias("business_key"),
        _scalar(df, "status").alias("status"),
        _codeable_concept_text(df, "category").alias("category_text"),
        _codeable_concept_text(df, "code").alias("code_text"),
        _reference(df, "subject").alias("subject_reference"),
        _reference(df, "encounter").alias("encounter_reference"),
        _scalar(df, "effectiveDateTime").alias("effective_datetime"),
        _nested(df, "valueQuantity", "$.value").alias("value_quantity"),
        _nested(df, "valueQuantity", "$.unit").alias("value_unit"),
        _scalar(df, "valueString").alias("value_string"),
        _nested(df, "meta", "$.lastUpdated").alias("meta_last_updated"),
        "extraction_timestamp", "api_url_or_params", "extraction_date", "batch_id", "run_id",
    ),
    "Condition": lambda df: df.select(
        _scalar(df, "id").alias("business_key"),
        _codeable_concept_text(df, "clinicalStatus").alias("clinical_status"),
        _codeable_concept_text(df, "verificationStatus").alias("verification_status"),
        _codeable_concept_text(df, "code").alias("code_text"),
        _reference(df, "subject").alias("subject_reference"),
        _reference(df, "encounter").alias("encounter_reference"),
        _scalar(df, "onsetDateTime").alias("onset_datetime"),
        _scalar(df, "recordedDate").alias("recorded_date"),
        _nested(df, "meta", "$.lastUpdated").alias("meta_last_updated"),
        "extraction_timestamp", "api_url_or_params", "extraction_date", "batch_id", "run_id",
    ),
}


def transform_silver_scd2(spark: SparkSession, resource_type: str, run_id: str) -> int:
    bronze_path = str(TABLES_ROOT / f"bronze_{resource_type.lower()}")
    if not delta_exists(bronze_path):
        print(f"[{resource_type}] no bronze data yet, skipping silver")
        return 0

    bronze_df = spark.read.format("delta").load(bronze_path)
    selected = SILVER_SELECT_EXPR[resource_type](bronze_df)

    # Dedupe: keep the latest-extracted record per (business_key, meta_last_updated).
    w = Window.partitionBy("business_key", "meta_last_updated").orderBy(F.col("extraction_timestamp").desc())
    deduped = (
        selected.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    business_cols = SCD2_BUSINESS_COLUMNS[resource_type]
    source = deduped.withColumn("row_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast(StringType()), F.lit("")) for c in business_cols]), 256))

    silver_path = str(TABLES_ROOT / f"silver_{resource_type.lower()}")
    now_ts = utc_now_iso()

    if not delta_exists(silver_path):
        initial = (
            source.withColumn("effective_start_ts", F.lit(now_ts))
            .withColumn("effective_end_ts", F.lit(None).cast(StringType()))
            .withColumn("is_current", F.lit(True))
        )
        # Keep only one row per business_key for the very first load (latest by meta_last_updated).
        w2 = Window.partitionBy("business_key").orderBy(F.col("meta_last_updated").desc())
        initial = initial.withColumn("_rn", F.row_number().over(w2)).filter(F.col("_rn") == 1).drop("_rn")
        initial.write.format("delta").mode("overwrite").save(silver_path)
        count = initial.count()
        log_metadata(spark, resource_type, "silver_scd2", "initial_load", count, "SUCCESS", run_id)
        return count

    from delta.tables import DeltaTable

    target = DeltaTable.forPath(spark, silver_path)
    # One row per business_key representing the newest incoming version.
    w3 = Window.partitionBy("business_key").orderBy(F.col("meta_last_updated").desc())
    incoming = source.withColumn("_rn", F.row_number().over(w3)).filter(F.col("_rn") == 1).drop("_rn")

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
    current_df = spark.read.format("delta").load(silver_path).filter(F.col("is_current") == True)  # noqa: E712
    to_insert = incoming.join(
        current_df.select("business_key", "row_hash"), on="business_key", how="left_anti"
    ).unionByName(
        incoming.alias("i").join(
            current_df.alias("c"), on="business_key", how="inner"
        ).filter(F.col("i.row_hash") != F.col("c.row_hash")).select("i.*")
    )
    to_insert = (
        to_insert.withColumn("effective_start_ts", F.lit(now_ts))
        .withColumn("effective_end_ts", F.lit(None).cast(StringType()))
        .withColumn("is_current", F.lit(True))
    )
    inserted_count = to_insert.count()
    if inserted_count > 0:
        to_insert.write.format("delta").mode("append").save(silver_path)

    log_metadata(spark, resource_type, "silver_scd2", "incremental_merge", inserted_count, "SUCCESS", run_id)
    return inserted_count


# --------------------------------------------------------------------------
# SILVER -> GOLD
# --------------------------------------------------------------------------

def build_gold(spark: SparkSession) -> None:
    def silver(name: str) -> DataFrame | None:
        path = str(TABLES_ROOT / f"silver_{name}")
        if not delta_exists(path):
            return None
        return spark.read.format("delta").load(path).filter(F.col("is_current") == True)  # noqa: E712

    patient = silver("patient")
    encounter = silver("encounter")
    observation = silver("observation")
    condition = silver("condition")

    if patient is not None:
        patient_for_gold = patient
        if "is_name_valid" in patient.columns:
            patient_for_gold = patient.filter(F.col("is_name_valid") == True)  # noqa: E712
        dim_patient = patient_for_gold.select(
            F.col("business_key").alias("patient_id"), "name_text", "gender",
            F.to_date("birth_date").alias("birth_date"),
            "deceased", "address_text", "marital_status",
        )
        dim_patient.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(TABLES_ROOT / "gold_dim_patient"))

    if encounter is not None:
        fact_encounter = encounter.select(
            F.col("business_key").alias("encounter_id"),
            F.regexp_extract("subject_reference", r"Patient/(.+)", 1).alias("patient_id"),
            "status", "class_code", "type_text",
            F.to_timestamp("period_start").alias("period_start"),
            F.to_timestamp("period_end").alias("period_end"),
            "service_provider",
        )
        fact_encounter.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(TABLES_ROOT / "gold_fact_encounter"))

    if observation is not None:
        fact_observation = observation.select(
            F.col("business_key").alias("observation_id"),
            F.regexp_extract("subject_reference", r"Patient/(.+)", 1).alias("patient_id"),
            F.regexp_extract("encounter_reference", r"Encounter/(.+)", 1).alias("encounter_id"),
            "status", "category_text", "code_text",
            F.to_timestamp("effective_datetime").alias("effective_datetime"),
            F.col("value_quantity").cast("double").alias("value_quantity"),
            "value_unit", "value_string",
        )
        fact_observation.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(TABLES_ROOT / "gold_fact_observation"))

    if condition is not None:
        fact_condition = condition.select(
            F.col("business_key").alias("condition_id"),
            F.regexp_extract("subject_reference", r"Patient/(.+)", 1).alias("patient_id"),
            F.regexp_extract("encounter_reference", r"Encounter/(.+)", 1).alias("encounter_id"),
            "clinical_status", "verification_status", "code_text",
            F.to_timestamp("onset_datetime").alias("onset_datetime"),
            F.to_timestamp("recorded_date").alias("recorded_date"),
        )
        fact_condition.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(TABLES_ROOT / "gold_fact_condition"))

    if patient is not None and encounter is not None:
        enc_counts = encounter.withColumn(
            "patient_id", F.regexp_extract("subject_reference", r"Patient/(.+)", 1)
        ).groupBy("patient_id").agg(F.count("*").alias("encounter_count"))
        cond_counts = None
        if condition is not None:
            cond_counts = condition.withColumn(
                "patient_id", F.regexp_extract("subject_reference", r"Patient/(.+)", 1)
            ).groupBy("patient_id").agg(F.count("*").alias("condition_count"))
        obs_counts = None
        if observation is not None:
            obs_counts = observation.withColumn(
                "patient_id", F.regexp_extract("subject_reference", r"Patient/(.+)", 1)
            ).groupBy("patient_id").agg(F.count("*").alias("observation_count"))

        summary = patient_for_gold.select(F.col("business_key").alias("patient_id"), "name_text", "gender", "birth_date")
        summary = summary.join(enc_counts, "patient_id", "left")
        if cond_counts is not None:
            summary = summary.join(cond_counts, "patient_id", "left")
        if obs_counts is not None:
            summary = summary.join(obs_counts, "patient_id", "left")
        summary = summary.na.fill(0, subset=[c for c in ["encounter_count", "condition_count", "observation_count"] if c in summary.columns])
        summary.write.format("delta").mode("overwrite").save(str(TABLES_ROOT / "gold_vw_patient_summary"))


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main():
    cfg = FhirConfig.from_json(CONFIG_PATH)
    run_id = new_batch_id()
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"=== RUN {run_id} ===")
    print("--- RAW + BRONZE (order enforced: Patient -> Encounter -> Observation -> Condition) ---")
    for resource_type in cfg.resources_in_order:
        ingest_raw_and_bronze(spark, cfg, resource_type, run_id)

    print("--- SILVER (clean, dedupe, SCD2) ---")
    for resource_type in cfg.resources_in_order:
        n = transform_silver_scd2(spark, resource_type, run_id)
        print(f"[{resource_type}] silver rows inserted/changed: {n}")

    print("--- GOLD ---")
    build_gold(spark)

    print("--- Summary ---")
    for name in ["bronze_patient", "silver_patient", "gold_dim_patient", "gold_vw_patient_summary"]:
        path = TABLES_ROOT / name
        if delta_exists(path):
            cnt = spark.read.format("delta").load(str(path)).count()
            print(f"{name}: {cnt} rows")

    spark.read.format("delta").load(str(TABLES_ROOT / "etl_metadata_log")).orderBy("api_called_at").show(
        truncate=False
    )
    spark.stop()


if __name__ == "__main__":
    main()
