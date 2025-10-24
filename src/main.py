from src.utils import PY, SUBMISSIONS_PATH, GROUND_PATH
from src.db import run_queries_to_json
import os, sys, subprocess

def main():
    datasets = ["FLIGHT-2", "FLIGHT-4", "FORTUNE", "GEO", "MOVIES", "PREMIER", "PRESIDENTS", "WORLD"]

    for dataset in datasets:
        print(f"\n=== Processing dataset: {dataset} ===")
        run_queries_to_json.run_queries_to_json(dataset)
        
    for dataset in datasets:
        print(f"▶ Running evaluation for dataset: {dataset}")
        subprocess.run([
            PY,
            "-m",
            "src.utils.galois_eval",
            "--ground", GROUND_PATH,
            "--submissions", SUBMISSIONS_PATH,
            "--datasets", dataset
        ], check=True)
        
if __name__ == "__main__":
    main()
