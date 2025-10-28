# setup-environment/config/setup_project.py
from __future__ import annotations
from src.utils import ROOT, VENV, PY, REQS
from pathlib import Path
import os, sys, subprocess
from src.settings import settings
from src.utils.logging_config import logger, log_query_event
from src.utils.constants import DATASETS
import yaml

GROUND_PATH = ROOT / "data" / ".ground_truth"
SUBMISSIONS_PATH = ROOT / "data" / ".output"

TEST_DB_INSTANCE = ROOT / "src" / "db" / "duckdb_db_graphdb.py" #test connection script path
QUERIES = ROOT / "src" / "db" / "run_queries_to_json"
EVAL  = ROOT / "src" / "utils" / "galois_eval"
CONFIG_PATH = ROOT / "config" / "config.yaml"

def load_config(path: Path) -> dict:
    if path.exists():
        with open(path, "r") as f:
            return yaml.safe_load(f)
    return {}

def get_dataset_selection() -> list[str]:
    #  Priority to the command line
    if len(sys.argv) > 1:
        args = [arg.upper() for arg in sys.argv[1:]]
        logger.info(f"Parametro da linea di comando: {args}")
    else:
        #  Otherwise use the parameter from the .yaml file
        config = load_config(CONFIG_PATH)
        raw = config.get("database", {}).get("run", "ALL")
        args = [s.strip().upper() for s in raw.split(",")] if isinstance(raw, str) else ["ALL"]
        logger.info(f"Parameters from YAML file: {args}")

    # Parameters validation
    if "ALL" in args:
        return DATASETS
    valid = [d for d in args if d in DATASETS]
    invalid = [d for d in args if d not in DATASETS]
    if invalid:
        logger.warning(f"Dataset '{args}' not valid. The available datasets are: {DATASETS}")
    return valid if valid else DATASETS  # fallback with every dataset if any is valid

def run(cmd: list[str | Path], *, check: bool = True) -> None: #helper to run a command
    printable = " ".join(map(str, cmd)) #convert command to string for printing
    logger.info(f"\n Running: {printable}")
    subprocess.run([str(c) for c in cmd], check=check) #execute command

def main() -> None:

    logger.info(f"Model: {settings.model.name}")
    logger.info(f"Logging level: {settings.logging.level}")
    logger.info(f"Output directory: {settings.io.outputs_dir}")
    logger.info(f"setup_project.py starting at ROOT = {ROOT}")

#---- requirements and environment

    selected_datasets = get_dataset_selection()

    #  Run the connection test
    if TEST_DB_INSTANCE.exists():
        module_name = ".".join(TEST_DB_INSTANCE.relative_to(ROOT).with_suffix("").parts)
        for dataset in selected_datasets:
            logger.info(f"\n Testing connection and building database for dataset: {dataset}")
            run([str(PY), "-m", str(module_name), dataset])
    else:
        logger.warning(f" Test file not found: {TEST_DB_INSTANCE}")

    logger.info("\n Setup completed.")

    #  Run the queries foreach dataset and report the results in JSON format
    logger.info("\nRunning queries to generate JSON files...")

    # Execute queries for each dataset
    for dataset in selected_datasets:
        logger.info(f"\n▶ Running queries for dataset: {dataset}")
        run([str(PY), "-m", "src.db.run_queries_to_json", dataset])

    # Evaluation for each dataset
    for dataset in selected_datasets:
        logger.info(f"▶ Running evaluation for dataset: {dataset}")
        run([
            PY,
            "-m",
            "src.utils.galois_eval",
            "--ground", GROUND_PATH,
            "--submissions", SUBMISSIONS_PATH,
            "--datasets", dataset
        ], check=True)


if __name__ == "__main__":
    main()


