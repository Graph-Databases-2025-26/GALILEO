"""
Batch-generate EXPLAIN + EXPLAIN ANALYZE outputs for each SQL statement
in queries_*.sql 

Usage:
  (.venv) python -m src.db.run_explain_plans world
  (.venv) python -m src.db.run_explain_plans all
"""

from pathlib import Path
import sys
import time

HERE = Path(__file__).resolve()
ROOT = HERE.parents[2]  # repo-root
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from . import duckdb_explain as dx
from .duckdb_explain import save_both

from src.utils.logging_config import logger, log_query_event



DATA_ROOT =  ROOT / "data"
RESULTS_ROOT = ROOT / "results" 


def find_datasets() -> list[Path]:
    return sorted([p for p in DATA_ROOT.iterdir() if p.is_dir()]) # dataset dirs


def load_statements(sql_file: Path) -> list[str]:
    """
    Ensures each statement ends with a semicolon for clearer logs 
    """
    text = sql_file.read_text(encoding="utf-8") 
    stmts = [s.strip() for s in text.split(";") if s.strip()] 
    return [s if s.endswith(";") else s + ";" for s in stmts]


def process_dataset(dataset_dir: Path) -> int:
    dataset = dataset_dir.name
    db_path = dataset_dir / f"{dataset.lower()}.duckdb"
    if not db_path.exists():
        logger.warning(f"No DB file: {db_path}  (run duckdb_db_graphdb.py first)")
        return 0

    out_dir = RESULTS_ROOT / dataset

    sql_files = sorted(dataset_dir.glob("queries_*.sql"))
    if not sql_files:
        logger.info(f"No queries_*.sql in {dataset_dir}")
        return 0

    dataset_start = time.time()  
    total = 0
    for sql_path in sql_files:
        stmts = load_statements(sql_path)
        stem = sql_path.stem  # e.g., queries_world
        logger.info(f"{sql_path.name} → {len(stmts)} statement(s)")
        for i, sql in enumerate(stmts, 1):
            base = out_dir / f"{stem}__q{i}"
            try:
                # Writes 4 files:
                #   <base>__explain.json / .txt
                #   <base>__analyze.json / .txt
                j1, t1, j2, t2 = save_both(db_path, sql, base)
                logger.info(f"✓ {base.name}  (EXPLAIN + ANALYZE)")
                total += 1
            except Exception as e:
                logger.error(f"✗ {base.name} -> {e}")
    elapsed_ms  = (time.time() - dataset_start) * 1000.0  
    log_query_event("dataset_completed", dataset=dataset, statements=total, latency_ms=f"{elapsed_ms:.1f}") 
    return total



def main():
    if len(sys.argv) < 2:
        logger.error("Usage: python -m src.db.run_explain_plans.py <dataset>|all")
        sys.exit(1)

    arg = sys.argv[1].upper()
    if arg == "all" or arg == "ALL":
        datasets = find_datasets()
        if not datasets:
            logger.error(f"No datasets found in {DATA_ROOT}")
            sys.exit(1)
    else:
        ds = (DATA_ROOT / arg)
        if not ds.exists():
            logger.error(f"Dataset not found: {ds}")
            sys.exit(1)
        datasets = [ds]

    logger.info("Starting EXPLAIN plan generation...")
    run_start = time.time() 
    grand_total = 0
    for ds in datasets:
        logger.info(f"=== DATASET: {ds.name} ===")
        grand_total += process_dataset(ds)

    total_ms = (time.time() - run_start) * 1000.0  
    logger.info(f"Done. Wrote plans for {grand_total} statement(s). total_latency_ms={total_ms:.1f}")
    logger.info(f"Results → {RESULTS_ROOT}")


if __name__ == "__main__":
    main()