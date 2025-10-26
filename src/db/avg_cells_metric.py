import os
import json
from ..utils.constants import GROUND_PATH

GROUND_DIR = GROUND_PATH

print(f"{'Dataset':15s} | {'# queries':10s} | {'Avg. expected cells'}")
print("-" * 50)

for dataset in sorted(os.listdir(GROUND_DIR)):
    dataset_path = GROUND_DIR / dataset
    if not dataset_path.is_dir():
        continue

    json_files = list(dataset_path.glob("*.json"))
    num_queries = len(json_files)
    total_cells = 0

    for jf in json_files:
        try:
            data = json.load(jf.open("r", encoding="utf-8"))
            if isinstance(data, list) and data:
                n_rows = len(data)
                n_cols = len(data[0]) if isinstance(data[0], dict) else 0
                total_cells += n_rows * n_cols
                print(f"Total number of cells:{total_cells}")
        except Exception as e:
            print(f" Error reading {jf}: {e}")

    avg_cells = total_cells / num_queries if num_queries > 0 else 0
    print(f"{dataset:15s} | {num_queries:10d} | {avg_cells:17.1f}")
