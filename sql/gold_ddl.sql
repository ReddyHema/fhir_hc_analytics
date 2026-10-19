-- Reference DDL for the Gold layer star schema + reporting views.
-- The 03_Gold_Views notebook builds these programmatically with PySpark/Delta;
-- this file documents the equivalent SQL shape for reference / Power BI modeling,
-- and can be run as-is against the Lakehouse SQL analytics endpoint or in a
-- Spark SQL cell if you prefer a SQL-first workflow instead.

-- ==========================================================================
-- Dimension
-- ==========================================================================
CREATE TABLE IF NOT EXISTS gold_dim_patient (
    patient_id            STRING,
    name_text             STRING,
    gender                STRING,
    birth_date            DATE,
    deceased              STRING,
    address_text          STRING,
    marital_status        STRING
) USING DELTA;

-- ==========================================================================
-- Facts
-- ==========================================================================
CREATE TABLE IF NOT EXISTS gold_fact_encounter (
    encounter_id    STRING,
    patient_id      STRING,
    status          STRING,
    class_code      STRING,
    type_text       STRING,
    period_start    TIMESTAMP,
    period_end      TIMESTAMP,
    service_provider STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS gold_fact_observation (
    observation_id      STRING,
    patient_id           STRING,
    encounter_id         STRING,
    status                STRING,
    category_text         STRING,
    code_text             STRING,
    effective_datetime    TIMESTAMP,
    value_quantity        DOUBLE,
    value_unit            STRING,
    value_string          STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS gold_fact_condition (
    condition_id          STRING,
    patient_id            STRING,
    encounter_id          STRING,
    clinical_status       STRING,
    verification_status   STRING,
    code_text             STRING,
    onset_datetime        TIMESTAMP,
    recorded_date         TIMESTAMP
) USING DELTA;

-- ==========================================================================
-- Reporting views
-- ==========================================================================
CREATE OR REPLACE VIEW gold_vw_patient_summary AS
SELECT
    p.patient_id, p.name_text, p.gender, p.birth_date,
    COALESCE(e.encounter_count, 0)   AS encounter_count,
    COALESCE(c.condition_count, 0)   AS condition_count,
    COALESCE(o.observation_count, 0) AS observation_count
FROM gold_dim_patient p
LEFT JOIN (SELECT patient_id, COUNT(*) AS encounter_count   FROM gold_fact_encounter   GROUP BY patient_id) e ON p.patient_id = e.patient_id
LEFT JOIN (SELECT patient_id, COUNT(*) AS condition_count   FROM gold_fact_condition   GROUP BY patient_id) c ON p.patient_id = c.patient_id
LEFT JOIN (SELECT patient_id, COUNT(*) AS observation_count FROM gold_fact_observation GROUP BY patient_id) o ON p.patient_id = o.patient_id;

CREATE OR REPLACE VIEW gold_vw_encounter_detail AS
SELECT e.encounter_id, p.name_text AS patient_name, e.status, e.class_code, e.type_text,
       e.period_start, e.period_end, e.service_provider
FROM gold_fact_encounter e
LEFT JOIN gold_dim_patient p ON e.patient_id = p.patient_id;

CREATE OR REPLACE VIEW gold_vw_condition_summary AS
SELECT c.condition_id, p.name_text AS patient_name, c.code_text, c.clinical_status,
       c.verification_status, c.onset_datetime, c.recorded_date
FROM gold_fact_condition c
LEFT JOIN gold_dim_patient p ON c.patient_id = p.patient_id;

CREATE OR REPLACE VIEW gold_vw_observation_trends AS
SELECT code_text, CAST(effective_datetime AS DATE) AS observation_date,
       COUNT(*) AS observation_count,
       AVG(CAST(value_quantity AS DOUBLE)) AS avg_value_quantity
FROM gold_fact_observation
GROUP BY code_text, CAST(effective_datetime AS DATE);
