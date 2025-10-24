# setup-environment/config/setup_project.py
from __future__ import annotations
from src.utils import ROOT, VENV, PY, REQS
from pathlib import Path
import os, sys, subprocess

GROUND_PATH = ROOT / "data" / ".ground_truth"
SUBMISSIONS_PATH = ROOT / "data" / ".output"

TEST_DB_INSTANCE = ROOT / "src" / "db" / "duckdb_db_graphdb.py" #test connection script path
QUERIES = ROOT / "src" / "db" / "run_queries_to_json"
EVAL  = ROOT / "src" / "utils" / "galois_eval"

def run(cmd: list[str | Path], *, check: bool = True) -> None: #helper to run a command
    printable = " ".join(map(str, cmd)) #convert command to string for printing
    print(f"\n Running: {printable}")
    subprocess.run([str(c) for c in cmd], check=check) #execute command

def main() -> None:
    print(f" setup_project.py starting at ROOT = {ROOT}")

    # Create venv if missing
    if not VENV.exists():
        print(" Creating virtual environment...")
        run([sys.executable, "-m", "venv", str(VENV)])
    else:
        print(" Virtual environment already exists")

    # Install dependencies
    if REQS.exists():
        print("\n Installing dependencies from requirements.txt...")

    # Always use 'python -m pip' 
        run([str(PY), "-m", "pip", "install", "--upgrade", "pip"], check=False)
        run([str(PY), "-m", "pip", "install", "-r", str(REQS)])
    else:
        print(f"  requirements.txt not found at {REQS}; skipping installs")

    #  Run the connection test
    if TEST_DB_INSTANCE.exists():
        print("\n Testing connection and build database instances...")
        run([str(PY), str(TEST_DB_INSTANCE)])
    else:
        print(f" Test file not found: {TEST_DB_INSTANCE}")

    print("\n Setup completed.")

    #  Run the queries foreach dataset and report the results in JSON format
    datasets = ["FLIGHT-2", "FLIGHT-4", "FORTUNE", "GEO", "MOVIES", "PREMIER", "PRESIDENTS", "WORLD"]

    print("\nRunning queries to generate JSON files...")

    # Execute queries for each dataset
    for dataset in datasets:
        print(f"\n▶ Running queries for dataset: {dataset}")
        run([str(PY), "-m", "src.db.run_queries_to_json", dataset])

    # Evaluation for each dataset
    for dataset in datasets:
        print(f"▶ Running evaluation for dataset: {dataset}")
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


