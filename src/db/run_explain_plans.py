"""
Batch-generate EXPLAIN + EXPLAIN ANALYZE outputs for each SQL statement
in queries_*.sql under setup-environment/data/<dataset>/.

Usage:
  (.venv) python setup-environment/config/run_explain_plans.py world
  (.venv) python setup-environment/config/run_explain_plans.py all
"""

from pathlib import Path
import sys

# Make the repo root importable so we can import galois_core_system
HERE = Path(__file__).resolve()
ROOT = HERE.parents[2]  # .../<repo-root>
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from . import duckdb_explain as dx
from .duckdb_explain import save_both


DATA_ROOT =  ROOT / "data"
RESULTS_ROOT = ROOT / "results" 


def find_datasets() -> list[Path]:
    return sorted([p for p in DATA_ROOT.iterdir() if p.is_dir()])


def load_statements(sql_file: Path) -> list[str]:
    """
    Ensures each statement ends with a semicolon for clearer logs (optional).
    """
    text = sql_file.read_text(encoding="utf-8")
    stmts = [s.strip() for s in text.split(";") if s.strip()]
    return [s if s.endswith(";") else s + ";" for s in stmts]


def process_dataset(dataset_dir: Path) -> int:
    dataset = dataset_dir.name 
    db_path = dataset_dir / f"{dataset}.duckdb" 
    if not db_path.exists():
        print(f"  ✗ No DB file: {db_path}  (run duckdb_db_graphdb.py first)")
        return 0

    out_dir = RESULTS_ROOT / dataset

    sql_files = sorted(dataset_dir.glob("queries_*.sql"))
    if not sql_files:
        print(f"  (no queries_*.sql in {dataset_dir})")
        return 0

    total = 0
    for sql_path in sql_files:
        stmts = load_statements(sql_path)
        stem = sql_path.stem  # e.g., queries_world
        print(f"  {sql_path.name} → {len(stmts)} statement(s)")
        for i, sql in enumerate(stmts, 1):
            base = out_dir / f"{stem}__q{i}"
            try:
                # Writes 4 files:
                #   <base>__explain.json / .txt
                #   <base>__analyze.json / .txt
                j1, t1, j2, t2 = save_both(db_path, sql, base)
                print(f"    ✓ {base.name}  (EXPLAIN + ANALYZE)")
                total += 1
            except Exception as e:
                print(f"    ✗ {base.name} -> {e}")
    return total


def main():
    if len(sys.argv) < 2:
        print("Usage: python setup-environment/config/run_explain_plans.py <dataset>|all")
        sys.exit(1)

    arg = sys.argv[1].lower()
    if arg == "all":
        datasets = find_datasets()
        if not datasets:
            print(f"No datasets found in {DATA_ROOT}")
            sys.exit(1)
    else:
        ds = (DATA_ROOT / arg)
        if not ds.exists():
            print(f"Dataset not found: {ds}")
            sys.exit(1)
        datasets = [ds]

    grand_total = 0
    for ds in datasets:
        print(f"\n=== DATASET: {ds.name} ===")
        grand_total += process_dataset(ds)

    print(f"\nDone. Wrote plans for {grand_total} statement(s).")
    print(f"Results → {RESULTS_ROOT}")


if __name__ == "__main__":
    main()