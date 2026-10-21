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

# ## 03_Gold_Views
# Builds the Gold layer from the **current** (`is_current = true`) rows of
# each Silver SCD2 table: a small star schema (`gold_dim_patient` +
# `gold_fact_encounter` / `gold_fact_observation` / `gold_fact_condition`)
# plus reporting-ready SQL views on top, optimized for Power BI
# (see `docs/powerbi_report.md` and `sql/gold_ddl.sql` in the Git repo).
# No `resourceType` parameter: this notebook always rebuilds Gold from the
# latest Silver state, after all four Silver notebooks have completed.

# CELL ********

%run FHIR_Common_Utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********

from pyspark.sql import functions as F

run_id = new_batch_id()


def current_silver(name: str):
    table = f"silver_{name}"
    if not table_exists(table):
        return None
    return spark.table(table).filter(F.col("is_current") == True)  # noqa: E712


patient = current_silver("patient")
encounter = current_silver("encounter")
observation = current_silver("observation")
condition = current_silver("condition")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********

# --- gold_dim_patient ---
if patient is not None:
    # Filter out patients whose extracted name failed the data-quality check (see
    # is_valid_name_expr in FHIR_Common_Utils): hapi.fhir.org is a public,
    # unauthenticated test sandbox, so some Patient records contain unresolved API
    # testing template placeholders or garbled encoding-test strings instead of real
    # names. Gold is the reporting-optimized layer, so it's the right place to
    # exclude these; Bronze/Silver keep them for full lineage/audit fidelity.
    patient_for_gold = patient
    if "is_name_valid" in patient.columns:
        patient_for_gold = patient.filter(F.col("is_name_valid") == True)  # noqa: E712

    dim_patient = patient_for_gold.select(
        F.col("business_key").alias("patient_id"), "name_text", "gender",
        F.to_date("birth_date").alias("birth_date"),
        "deceased", "address_text", "marital_status",
    )
    # overwriteSchema=true: birth_date is changing from STRING to DATE, which Delta
    # otherwise rejects as an incompatible schema change on overwrite.
    dim_patient.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_dim_patient")

# --- gold_fact_encounter ---
if encounter is not None:
    fact_encounter = encounter.select(
        F.col("business_key").alias("encounter_id"),
        F.regexp_extract("subject_reference", r"Patient/(.+)", 1).alias("patient_id"),
        "status", "class_code", "type_text",
        F.to_timestamp("period_start").alias("period_start"),
        F.to_timestamp("period_end").alias("period_end"),
        "service_provider",
    )
    fact_encounter.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_fact_encounter")

# --- gold_fact_observation ---
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
    fact_observation.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_fact_observation")

# --- gold_fact_condition ---
if condition is not None:
    fact_condition = condition.select(
        F.col("business_key").alias("condition_id"),
        F.regexp_extract("subject_reference", r"Patient/(.+)", 1).alias("patient_id"),
        F.regexp_extract("encounter_reference", r"Encounter/(.+)", 1).alias("encounter_id"),
        "clinical_status", "verification_status", "code_text",
        F.to_timestamp("onset_datetime").alias("onset_datetime"),
        F.to_timestamp("recorded_date").alias("recorded_date"),
    )
    fact_condition.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_fact_condition")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********

# --- Reporting views (see sql/gold_ddl.sql for the reference DDL) ---
if table_exists("gold_dim_patient"):
    joins = []
    select_extra = []
    for fact_table, count_col, key_col in [
        ("gold_fact_encounter", "encounter_count", "e"),
        ("gold_fact_condition", "condition_count", "c"),
        ("gold_fact_observation", "observation_count", "o"),
    ]:
        if table_exists(fact_table):
            joins.append(
                f"LEFT JOIN (SELECT patient_id, COUNT(*) AS {count_col} FROM {fact_table} GROUP BY patient_id) {key_col} "
                f"ON p.patient_id = {key_col}.patient_id"
            )
            select_extra.append(f"COALESCE({key_col}.{count_col}, 0) AS {count_col}")
        else:
            select_extra.append(f"CAST(0 AS BIGINT) AS {count_col}")

    spark.sql(f"""
        CREATE OR REPLACE VIEW gold_vw_patient_summary AS
        SELECT
            p.patient_id, p.name_text, p.gender, p.birth_date,
            {', '.join(select_extra)}
        FROM gold_dim_patient p
        {' '.join(joins)}
    """)

if table_exists("gold_fact_encounter") and table_exists("gold_dim_patient"):
    spark.sql("""
        CREATE OR REPLACE VIEW gold_vw_encounter_detail AS
        SELECT e.encounter_id, p.name_text AS patient_name, e.status, e.class_code, e.type_text,
               e.period_start, e.period_end, e.service_provider
        FROM gold_fact_encounter e
        LEFT JOIN gold_dim_patient p ON e.patient_id = p.patient_id
    """)

if table_exists("gold_fact_condition") and table_exists("gold_dim_patient"):
    spark.sql("""
        CREATE OR REPLACE VIEW gold_vw_condition_summary AS
        SELECT c.condition_id, p.name_text AS patient_name, c.code_text, c.clinical_status,
               c.verification_status, c.onset_datetime, c.recorded_date
        FROM gold_fact_condition c
        LEFT JOIN gold_dim_patient p ON c.patient_id = p.patient_id
    """)

if table_exists("gold_fact_observation"):
    spark.sql("""
        CREATE OR REPLACE VIEW gold_vw_observation_trends AS
        SELECT code_text, CAST(effective_datetime AS DATE) AS observation_date,
               COUNT(*) AS observation_count,
               AVG(CAST(value_quantity AS DOUBLE)) AS avg_value_quantity
        FROM gold_fact_observation
        GROUP BY code_text, CAST(effective_datetime AS DATE)
    """)

log_metadata("ALL", "gold", "silver->gold", 0, "SUCCESS", run_id)
print("Gold layer rebuilt.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
