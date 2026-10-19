# Power BI Report — export instructions

This folder is where the **built** Power BI report (`FHIR Patient Analytics`) should be
exported to before packaging the final submission ZIP. The report itself was authored
directly in the Fabric web UI against the `FHIR_HC_Semantic_Model` (Direct Lake), per
[docs/powerbi_report.md](../docs/powerbi_report.md).

## How to export the .pbix from Fabric

1. Open the `FHIR Patient Analytics` report in your Fabric workspace.
2. **File** menu (top-left) → **Download this file** (or **Export → Download a copy**,
   depending on your Fabric version).
3. Save the downloaded `.pbix` into this folder, e.g.:
   ```
   powerbi/FHIR_Patient_Analytics.pbix
   ```

> Note: because the report is connected **live** via Direct Lake, the downloaded `.pbix`
> is a "thin" file — it contains the report's visuals/layout, but reopening it outside
> this Fabric workspace (e.g. on a machine without access to your tenant) will prompt to
> reconnect to the semantic model rather than show data offline. This is expected and is
> still the correct, standard artifact to submit alongside the rest of the project.

## Also include a static snapshot (recommended)

Since a grader may not have access to your Fabric tenant to open the live `.pbix`, also
export a PDF or PNG snapshot of the finished report page so the visuals are reviewable
without any live connection:

1. In the report, **File → Export → Export to PDF** (or use **File → Print → Save as PDF**).
2. Save it here as `powerbi/FHIR_Patient_Analytics.pdf`.

## Checklist before zipping the project

- [ ] `FHIR_Patient_Analytics.pbix` placed in this folder
- [ ] `FHIR_Patient_Analytics.pdf` (or `.png` screenshot) placed in this folder
