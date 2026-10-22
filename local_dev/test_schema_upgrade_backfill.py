"""
Reproduces the exact failure you hit in Fabric: a silver_patient table that
already existed (built before `is_name_valid` was added to the Patient
select), then a second run with the upgraded select that adds the column.

Verifies:
  1. The append no longer throws DELTA_METADATA_MISMATCH (mergeSchema fix).
  2. Pre-existing, UNCHANGED patient rows (the vast majority in a real
     schema-upgrade scenario) get is_name_valid correctly backfilled instead
     of silently staying NULL forever -- which would otherwise make a Gold
     filter on "is_name_valid = true" drop them too, since SQL/Spark filters
     treat NULL as neither true nor false.
"""
import os
import shutil
import sys
from pathlib import Path

os.environ.setdefault("HADOOP_HOME", "C:\\hadoop")
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession, functions as F  # noqa: E402

sys.path.insert(0, str(Path(__file__).parent))

TEST_ROOT = Path(__file__).parent / "lakehouse_local_schema_upgrade_test"


def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("schema-upgrade-test")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main():
    if TEST_ROOT.exists():
        shutil.rmtree(TEST_ROOT)

    import run_local_pipeline as rlp

    rlp.TABLES_ROOT = TEST_ROOT / "Tables"
    rlp.RAW_ROOT = TEST_ROOT / "Files" / "raw"

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    bronze_path = str(rlp.TABLES_ROOT / "bronze_patient")

    # Two patients: one with a real name, one with sandbox-junk test data,
    # loaded in a first run BEFORE is_name_valid existed (simulating your
    # original pipeline run).
    day1 = [
        {
            "id": "real-patient-1", "name": [{"given": ["Lucia"], "family": "Valenzuela"}],
            "meta": {"lastUpdated": "2026-10-13T00:00:00Z"},
            "extraction_timestamp": "2026-10-13T01:00:00Z", "api_url_or_params": "test",
            "extraction_date": "2026-10-13", "batch_id": "batch1", "run_id": "run1",
        },
        {
            "id": "junk-patient-1", "name": [{"given": ["{{testGivenName}}"], "family": "Test"}],
            "meta": {"lastUpdated": "2026-10-13T00:00:00Z"},
            "extraction_timestamp": "2026-10-13T01:00:00Z", "api_url_or_params": "test",
            "extraction_date": "2026-10-13", "batch_id": "batch1", "run_id": "run1",
        },
    ]
    spark.createDataFrame(day1).write.format("delta").mode("append").option("mergeSchema", "true").save(bronze_path)

    # Simulate the OLD (pre-is_name_valid) silver select/write directly,
    # bypassing the current (already-patched) transform_silver_scd2, so the
    # resulting silver_patient table genuinely lacks the column -- exactly
    # your situation before this fix.
    bronze_df = spark.read.format("delta").load(bronze_path)
    old_silver = bronze_df.select(
        F.col("id").alias("business_key"),
        F.concat_ws(" ", F.get_json_object(F.to_json(F.col("name")), "$[0].given[0]"),
                    F.get_json_object(F.to_json(F.col("name")), "$[0].family")).alias("name_text"),
        F.get_json_object(F.to_json(F.col("meta")), "$.lastUpdated").alias("meta_last_updated"),
        "extraction_timestamp", "api_url_or_params", "extraction_date", "batch_id", "run_id",
    ).withColumn("row_hash", F.sha2(F.col("name_text"), 256)) \
     .withColumn("effective_start_ts", F.lit("2026-10-13T01:00:00Z")) \
     .withColumn("effective_end_ts", F.lit(None).cast("string")) \
     .withColumn("is_current", F.lit(True))
    silver_path = str(rlp.TABLES_ROOT / "silver_patient")
    old_silver.write.format("delta").mode("overwrite").save(silver_path)

    before_cols = spark.read.format("delta").load(silver_path).schema.fieldNames()
    assert "is_name_valid" not in before_cols, "test setup should NOT have is_name_valid yet"
    print("PASS: simulated pre-existing silver_patient table WITHOUT is_name_valid column")

    # Now run the CURRENT (patched) transform_silver_scd2, as if the pipeline
    # notebooks had just been upgraded with the new Patient select + backfill.
    inserted = rlp.transform_silver_scd2(spark, "Patient", "run2")
    print(f"rows inserted/changed on upgrade run: {inserted}")

    result = spark.read.format("delta").load(silver_path).collect()
    by_id = {r["business_key"]: r for r in result}

    assert "is_name_valid" in spark.read.format("delta").load(silver_path).schema.fieldNames(), \
        "is_name_valid column should now exist after mergeSchema append"
    print("PASS: append with mergeSchema succeeded, is_name_valid column now present")

    null_count = sum(1 for r in result if r["is_name_valid"] is None)
    assert null_count == 0, f"expected 0 rows with NULL is_name_valid after backfill, got {null_count}"
    print("PASS: no rows left with NULL is_name_valid after backfill")

    assert by_id["real-patient-1"]["is_name_valid"] is True, "real patient should be flagged valid"
    assert by_id["junk-patient-1"]["is_name_valid"] is False, "sandbox junk patient should be flagged invalid"
    print("PASS: pre-existing UNCHANGED rows correctly backfilled (real=True, junk=False)")

    spark.stop()
    shutil.rmtree(TEST_ROOT, ignore_errors=True)
    print("\nALL SCHEMA-UPGRADE BACKFILL ASSERTIONS PASSED")


if __name__ == "__main__":
    main()
