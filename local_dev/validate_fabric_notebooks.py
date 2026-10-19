"""
Validates the ACTUAL Fabric notebook source files under
../fabric_workspace/*.Notebook/notebook-content.py by extracting their code
cells and executing them against a local Spark session with a managed
(Hive-style) catalog, which mimics how Fabric Lakehouse `saveAsTable` /
`spark.catalog.tableExists` / `spark.table()` behave.

This is a test-only harness: it does NOT modify the committed notebook
files. It only rewrites the single hardcoded Fabric mount path
"/lakehouse/default" to a local temp directory, and stubs `mssparkutils`,
purely in-memory for this test process.

Usage:
    cd local_dev
    python validate_fabric_notebooks.py
"""
import os
import re
import shutil
import sys
from pathlib import Path

os.environ.setdefault("HADOOP_HOME", "C:\\hadoop")
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

HERE = Path(__file__).parent
FABRIC_ROOT = HERE.parent / "fabric_workspace"
LOCAL_LAKEHOUSE = HERE / "lakehouse_local_fabric_validation"
WAREHOUSE_DIR = LOCAL_LAKEHOUSE / "spark-warehouse"


class _FakeNotebookExit(Exception):
    def __init__(self, payload):
        super().__init__(payload)
        self.payload = payload


class _FakeNotebook:
    @staticmethod
    def exit(payload):
        raise _FakeNotebookExit(payload)


class _FakeFs:
    """Emulates mssparkutils.fs against the local filesystem (relative to the
    process's current working directory, which main() chdir's into LOCAL_LAKEHOUSE
    so that "Files/raw/..." resolves the same way for both this stub and Spark's
    own relative-path reads, mirroring how Fabric resolves relative paths against
    the attached default Lakehouse for both APIs)."""

    @staticmethod
    def mkdirs(path):
        Path(path).mkdir(parents=True, exist_ok=True)
        return True

    @staticmethod
    def put(path, content, overwrite=False):
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        if p.exists() and not overwrite:
            return False
        p.write_text(content, encoding="utf-8")
        return True


class _FakeMssparkutils:
    notebook = _FakeNotebook()
    fs = _FakeFs()


def extract_code_cells(notebook_path: Path) -> str:
    """Extract CELL/PARAMETERS CELL bodies from Fabric's git-export notebook format."""
    lines = notebook_path.read_text(encoding="utf-8").splitlines()
    code_blocks = []
    current = None
    for line in lines:
        if re.match(r"^# (CELL|PARAMETERS CELL) \*+$", line):
            current = []
            code_blocks.append(current)
        elif re.match(r"^# (METADATA|MARKDOWN) \*+$", line):
            current = None
        elif current is not None:
            if line.startswith("# META"):
                continue
            current.append(line)
    return code_blocks


def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("fabric-notebook-validation")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(WAREHOUSE_DIR))
        .config("spark.sql.shuffle.partitions", "4")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def run_notebook(name: str, exec_globals: dict, param_overrides: dict | None = None) -> dict:
    path = FABRIC_ROOT / f"{name}.Notebook" / "notebook-content.py"
    blocks = extract_code_cells(path)
    if param_overrides:
        # Mimic how Fabric/papermill actually injects pipeline parameters: as a new
        # cell appended right after the "parameters" cell (blocks[0]), not by
        # pre-seeding globals (which the parameters cell's own defaults would overwrite).
        override_lines = [f"{k} = {v!r}" for k, v in param_overrides.items()]
        blocks = [blocks[0], override_lines] + blocks[1:]
    code = "\n".join("\n".join(block) for block in blocks)
    # Substitute as a forward-slash path so it's safe to embed inside a Python string literal
    # (a raw Windows path would contain backslash sequences like \U that Python mis-parses as escapes).
    local_lakehouse_posix = str(LOCAL_LAKEHOUSE).replace("\\", "/")
    code = code.replace("/lakehouse/default", local_lakehouse_posix)
    # %run is an IPython/notebook magic, not valid standalone Python; this harness
    # instead runs FHIR_Common_Utils first and threads its globals through explicitly.
    code = "\n".join(line for line in code.splitlines() if not line.strip().startswith("%run"))
    try:
        exec(compile(code, str(path), "exec"), exec_globals)  # noqa: S102
    except _FakeNotebookExit as exit_exc:
        print(f"[{name}] notebook.exit() payload: {exit_exc.payload}")
        return exec_globals
    return exec_globals


def main():
    if LOCAL_LAKEHOUSE.exists():
        shutil.rmtree(LOCAL_LAKEHOUSE)
    (LOCAL_LAKEHOUSE / "Files" / "config").mkdir(parents=True, exist_ok=True)
    shutil.copy(HERE.parent / "config" / "pipeline_config.json", LOCAL_LAKEHOUSE / "Files" / "config" / "pipeline_config.json")
    # Speed up the validation run: only pull 1 day instead of the default lookback.
    cfg_path = LOCAL_LAKEHOUSE / "Files" / "config" / "pipeline_config.json"
    cfg_text = cfg_path.read_text(encoding="utf-8").replace('"lookback_days": 3', '"lookback_days": 1')
    cfg_path.write_text(cfg_text, encoding="utf-8")

    # The notebooks now use Fabric-relative paths (e.g. "Files/raw/...") for raw
    # read/write, which Fabric resolves against the attached default Lakehouse.
    # Emulate that here by chdir'ing into the local lakehouse root so both our
    # _FakeFs stub and Spark's own relative-path resolution agree on location.
    os.chdir(LOCAL_LAKEHOUSE)

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    shared_globals = {"spark": spark, "mssparkutils": _FakeMssparkutils()}

    print("=== Running FHIR_Common_Utils ===")
    run_notebook("FHIR_Common_Utils", shared_globals)

    for resource in ["Patient", "Encounter"]:  # subset is enough to prove the pattern works
        print(f"=== Running 01_Raw_Bronze_Ingestion (resourceType={resource}) ===")
        run_notebook("01_Raw_Bronze_Ingestion", dict(shared_globals), param_overrides={"resourceType": resource})

        print(f"=== Running 02_Silver_SCD2_Transformation (resourceType={resource}) ===")
        run_notebook("02_Silver_SCD2_Transformation", dict(shared_globals), param_overrides={"resourceType": resource})

    print("=== Running 03_Gold_Views ===")
    run_notebook("03_Gold_Views", dict(shared_globals))

    print("\n=== Verification ===")
    for table in ["bronze_patient", "silver_patient", "gold_dim_patient", "gold_vw_patient_summary",
                  "bronze_encounter", "silver_encounter", "gold_fact_encounter", "gold_vw_encounter_detail"]:
        assert spark.catalog.tableExists(table), f"expected table/view {table} to exist"
        cnt = spark.table(table).count()
        print(f"{table}: {cnt} rows")
        assert cnt > 0, f"expected {table} to have rows"

    print("\nALL FABRIC NOTEBOOK VALIDATIONS PASSED")
    spark.stop()
    os.chdir(HERE)
    shutil.rmtree(LOCAL_LAKEHOUSE, ignore_errors=True)


if __name__ == "__main__":
    main()
