# Power BI Report — "FHIR Patient Analytics" (1-page)

**Data source**: the Gold layer (`gold_dim_patient`, `gold_fact_encounter`,
`gold_fact_observation`, `gold_fact_condition`, and the `gold_vw_*` views) via the
Fabric Lakehouse's **SQL analytics endpoint**, connected with **Direct Lake** mode
(Power BI Desktop → Get Data → OneLake catalog → `fhir_lakehouse`) so the report always
reflects the latest Gold refresh with no separate import/refresh schedule needed.
Import mode against the same endpoint is a valid fallback.

## Data model (relationships)

```
gold_dim_patient (patient_id) 1 ──* gold_fact_encounter   (patient_id)
gold_dim_patient (patient_id) 1 ──* gold_fact_observation (patient_id)
gold_dim_patient (patient_id) 1 ──* gold_fact_condition   (patient_id)
gold_fact_encounter (encounter_id) 1 ──* gold_fact_observation (encounter_id)   [optional]
gold_fact_encounter (encounter_id) 1 ──* gold_fact_condition   (encounter_id)   [optional]
```

## Page layout: "Patient 360 Overview"

| Zone | Visual | Fields | Purpose |
|---|---|---|---|
| Top-left KPI cards (x4) | Card | `COUNT(patient_id)`, `COUNT(encounter_id)`, `COUNT(condition_id)`, `COUNT(observation_id)` | At-a-glance volumes across the Gold layer. |
| Top-right | Slicer (dropdown) | `gold_dim_patient[gender]`, date slicer on `gold_fact_encounter[period_start]` | Lets the reviewer filter the whole page by gender / encounter date range. |
| Middle-left | Clustered bar chart | Axis: `gold_fact_condition[code_text]`, Value: count of `condition_id` | Top conditions by frequency (most common diagnoses). |
| Middle-right | Line chart | Axis: `gold_vw_observation_trends[observation_date]`, Value: `avg_value_quantity`, Legend: `code_text` | Observation value trend over the ingested window (e.g. vitals/labs trending). |
| Bottom-left | Donut chart | `gold_fact_encounter[class_code]`, count of `encounter_id` | Encounter mix (ambulatory / inpatient / emergency, etc). |
| Bottom-right | Table | `gold_vw_patient_summary`: `name_text`, `gender`, `birth_date`, `encounter_count`, `condition_count`, `observation_count` | Drillable patient-level detail/summary grid. |

## Suggested DAX measures

```DAX
Total Patients      = DISTINCTCOUNT(gold_dim_patient[patient_id])
Total Encounters    = COUNTROWS(gold_fact_encounter)
Total Conditions    = COUNTROWS(gold_fact_condition)
Total Observations  = COUNTROWS(gold_fact_observation)
Avg Encounters / Patient =
    DIVIDE([Total Encounters], [Total Patients])
```

## Build steps

1. In Power BI Desktop: **Get data → OneLake catalog** → select the `fhir_lakehouse`
   Lakehouse → choose Direct Lake → pick the 4 Gold tables + the `gold_vw_*` views.
2. Create the relationships listed above (Power BI usually autodetects the `patient_id`
   relationships; verify cardinality is 1:*).
3. Add the DAX measures above to `gold_dim_patient` (or a dedicated measures table).
4. Lay out the single page per the table above; apply the gender/date slicers to all
   visuals (Format → Edit interactions, or just leave default "Filter all").
5. Publish to the same Fabric workspace so the report lives alongside the Lakehouse and
   pipeline for a complete, self-contained deliverable.

> A `.pbix` binary isn't included in this repository (it requires Power BI Desktop and a
> live Direct Lake connection to the deployed Lakehouse to build/open); this document is
> the complete, ready-to-follow spec for recreating the 1-page report in ~10 minutes once
> the Gold layer has been populated by a pipeline run.
