"""
Focused test proving SCD Type 2 behavior: when a Patient's business
attributes change between two daily loads, the Silver table must keep the
old version (is_current=false, effective_end_ts set) and add a new current
version (is_current=true), rather than overwriting history.

This complements run_local_pipeline.py (which exercises the real API) by
isolating just the bronze->silver SCD2 merge logic with synthetic,
fully-controlled input so the assertions are deterministic.
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
from run_local_pipeline import transform_silver_scd2  # noqa: E402

TEST_ROOT = Path(__file__).parent / "lakehouse_local_scd2_test"


def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("scd2-unit-test")
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

    day1 = [{
        "id": "test-patient-1", "gender": "male", "birthDate": "1990-01-01",
        "meta": {"lastUpdated": "2026-10-13T00:00:00Z"},
        "extraction_timestamp": "2026-10-13T01:00:00Z", "api_url_or_params": "test",
        "extraction_date": "2026-10-13", "batch_id": "batch1", "run_id": "run1",
    }]
    spark.createDataFrame(day1).write.format("delta").mode("append").option("mergeSchema", "true").save(bronze_path)

    n1 = transform_silver_scd2(spark, "Patient", "run1")
    assert n1 == 1, f"expected 1 row on initial load, got {n1}"

    silver_path = str(rlp.TABLES_ROOT / "silver_patient")
    rows = spark.read.format("delta").load(silver_path).collect()
    assert len(rows) == 1
    assert rows[0]["gender"] == "male"
    assert rows[0]["is_current"] is True
    assert rows[0]["effective_end_ts"] is None
    print("PASS: initial load creates one current row")

    # Day 2: same patient, gender changed male -> female (simulates a real-world correction).
    day2 = [{
        "id": "test-patient-1", "gender": "female", "birthDate": "1990-01-01",
        "meta": {"lastUpdated": "2026-10-14T00:00:00Z"},
        "extraction_timestamp": "2026-10-14T01:00:00Z", "api_url_or_params": "test",
        "extraction_date": "2026-10-14", "batch_id": "batch2", "run_id": "run2",
    }]
    spark.createDataFrame(day2).write.format("delta").mode("append").option("mergeSchema", "true").save(bronze_path)

    n2 = transform_silver_scd2(spark, "Patient", "run2")
    assert n2 == 1, f"expected 1 new/changed row on second load, got {n2}"

    rows2 = spark.read.format("delta").load(silver_path).orderBy("effective_start_ts").collect()
    assert len(rows2) == 2, f"expected 2 historical rows after a change, got {len(rows2)}"

    old_row = [r for r in rows2 if r["is_current"] is False]
    new_row = [r for r in rows2 if r["is_current"] is True]
    assert len(old_row) == 1 and len(new_row) == 1
    assert old_row[0]["gender"] == "male"
    assert old_row[0]["effective_end_ts"] is not None, "closed-out row must have an effective_end_ts"
    assert new_row[0]["gender"] == "female"
    assert new_row[0]["effective_end_ts"] is None
    print("PASS: changed attribute closes old version (is_current=false) and inserts new current version")

    # Day 3: no change -> must NOT create a 3rd row.
    day3 = [{
        "id": "test-patient-1", "gender": "female", "birthDate": "1990-01-01",
        "meta": {"lastUpdated": "2026-10-15T00:00:00Z"},
        "extraction_timestamp": "2026-10-15T01:00:00Z", "api_url_or_params": "test",
        "extraction_date": "2026-10-15", "batch_id": "batch3", "run_id": "run3",
    }]
    spark.createDataFrame(day3).write.format("delta").mode("append").option("mergeSchema", "true").save(bronze_path)
    n3 = transform_silver_scd2(spark, "Patient", "run3")
    assert n3 == 0, f"expected 0 new rows when nothing changed, got {n3}"
    rows3 = spark.read.format("delta").load(silver_path).collect()
    assert len(rows3) == 2, f"expected still only 2 historical rows, got {len(rows3)}"
    print("PASS: unchanged attributes do not create spurious new versions")

    spark.stop()
    shutil.rmtree(TEST_ROOT, ignore_errors=True)
    print("\nALL SCD2 ASSERTIONS PASSED")


if __name__ == "__main__":
    main()
