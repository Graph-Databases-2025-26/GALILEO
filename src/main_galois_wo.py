import argparse
import json

from src.config import Config_Loader
from src.utils import LOG, log_init
from src.galois_wo.galois_executor import GaloisExecutor


def parse_args() -> argparse.Namespace:
    """
    Minimal CLI to execute the Galois-WO Table-Scan.

    Usage examples:

      python -m src.main_galois_wo MOVIES --sql "SELECT * FROM movie WHERE year > 2000"
      python -m src.main_galois_wo WORLD --sql "SELECT * FROM city WHERE population > 1000000"
      python -m src.main_galois_wo PREMIER --sql "SELECT * FROM matches WHERE season = '2019/2020'"
    """
    parser = argparse.ArgumentParser(
        description="Run Galois-WO Table-Scan on a single SQL query."
    )

    parser.add_argument(
        "dataset",
        help="Dataset name (e.g. WORLD, GEO, MOVIES, PRESIDENTS, ...).",
    )

    parser.add_argument(
        "--sql",
        required=True,
        help="SQL query to execute with the Galois-WO Table-Scan.",
    )

    parser.add_argument(
        "--max-iter",
        type=int,
        default=None,
        help="Maximum number of Table-Scan iterations (override config.execution.max_retries).",
    )

    return parser.parse_args()


def main() -> None:
    # 1. CLI
    args = parse_args()

    # 2. Config + logging
    config_loader = Config_Loader()
    config = config_loader.get_config()

    log_init()
    LOG.info("=== Galois-WO Table-Scan ===")
    LOG.info(f"Dataset: {args.dataset}")
    LOG.info(f"SQL: {args.sql}")

    # 3. inizializatione GaloisExecutor
    executor = GaloisExecutor(config=config, dataset=args.dataset)

    # 4. executione Table-Scan
    rows = executor.table_scan(sql_query=args.sql, max_iter=args.max_iter)

    LOG.info(f"Collected {len(rows)} rows from Table-Scan.")

    # 5. stamp results
    print(json.dumps(rows, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
