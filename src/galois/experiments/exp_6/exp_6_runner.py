import argparse
import json
import time
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Optional
import httpx
from src.config import Config_Loader
from src.db import load_queries_from_folder
from src.galois.galois import Galois
from src.utils import DATA_DIR, IK_DATASETS, LOG


# --- Helpers ---

def with_retries(fn, *, tries=4, base_sleep=2.0, what="call"):
    last = None
    for t in range(tries):
        try:
            return fn()
        except (
                httpx.RemoteProtocolError,
                httpx.ReadTimeout,
                httpx.ConnectTimeout,
                httpx.ConnectError,
        ) as e:
            last = e
            sleep_s = base_sleep * (2 ** t)
            LOG.warning(
                f"[EXP6] {what} failed ({type(e).__name__}): {e}. "
                f"Retry {t + 1}/{tries} in {sleep_s:.1f}s"
            )
            time.sleep(sleep_s)
    raise last


def extract_exp6_stats_from_debug(debug_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extracts statistics by reading the correct structure returned by galois.py.
    The data is inside the 'tables' list in the debug dictionary.
    """
    # Retrieve the list of scanned tables (usually 1, but supports JOIN)
    tables = debug_info.get("tables", [])

    # Sum tokens from all involved tables
    input_tokens = sum(t.get("input_tokens_total_all_iters", 0) for t in tables)

    # Take the maximum number of iterations across the tables (query depth)
    n_iters = max((t.get("n_iters", 0) for t in tables), default=0)

    # Heuristic fallback: if watsonx returns 0 tokens, try to estimate (optional)
    return {
        "input_tokens_total_all_iters": input_tokens,
        "n_iters": n_iters,
        "ge_max_iters": n_iters >= 10  # Assume 10 as the standard threshold from the paper
    }


def summarize_exp6(per_query_stats: List[Dict[str, Any]], max_iter: int) -> Dict[str, Any]:
    """Aggregate results per dataset."""
    if not per_query_stats:
        return _empty_summary(0)

    # Filter only valid runs (where tokens are numbers)
    valid = [
        x for x in per_query_stats
        if isinstance(x.get("input_tokens_total_all_iters"), (int, float))
    ]

    if not valid:
        return _empty_summary(len(per_query_stats))

    avg_tokens = mean(x["input_tokens_total_all_iters"] for x in valid)

    # Compute average iterations only on entries that have the data
    iters_vals = [x["n_iters"] for x in valid if x.get("n_iters") is not None]
    avg_iters = mean(iters_vals) if iters_vals else 0

    n_ge = sum(1 for x in valid if x.get("n_iters", 0) >= max_iter)

    return {
        "n_queries": len(per_query_stats),
        "n_success": len(valid),
        "avg_input_tokens_all_iters": avg_tokens,
        "avg_iters_per_query": avg_iters,
        "n_queries_ge_max_iter": n_ge,
    }


def _empty_summary(n_queries):
    return {
        "n_queries": n_queries, "n_success": 0,
        "avg_input_tokens_all_iters": 0.0, "avg_iters_per_query": 0.0,
        "n_queries_ge_max_iter": 0
    }

# --- Core Logic ---

def run_exp6_dataset(dataset_name: str, provider: str, max_iter: int, only_ids: Optional[set] = None):
    config = Config_Loader().get_config()
    dataset_path = DATA_DIR / dataset_name
    queries = load_queries_from_folder(dataset_path)

    stats_empty = []
    stats_a = []
    stats_f = []

    LOG.info(f"[EXP6] Starting dataset {dataset_name} ({len(queries)} queries)")

    for i, (_, sql_query) in enumerate(queries):
        # Optional filter for ID
        if only_ids and (i + 1) not in only_ids:
            continue

        LOG.info(f"--- Query {i + 1}/{len(queries)} ---")

        # Initialize Galois forcing the physical strategy "table" (exception for Exp 6)
        g = Galois(
            config=config,
            dataset=dataset_name,
            sql_query=sql_query,
            physical_strategy="table"
        )

        # -----------------------------------------------------------
        #  GALOIS Ø (No-Push) - Manual to force Table-Scan
        # -----------------------------------------------------------
        try:
            # We don't use g.run_no_push() because it would force KEY-SCAN.
            # Manually build an empty plan.
            plan_empty = g.build_execution_plan(conditions_to_push=[])
            # Inject max_iter into the plan if supported by the executor
            plan_empty["max_iter"] = max_iter

            # Execute by calling execute_variant with debug=True to obtain tokens
            # execute_variant returns (results, stats, debug_info)
            _, _, debug_empty = g.execute_variant(plan_empty, "GALOIS_WO (No-Push)", debug=True)
            stats_empty.append(extract_exp6_stats_from_debug(debug_empty))

        except Exception as e:
            LOG.error(f"Error GALOIS_EMPTY query {i}: {e}")
            stats_empty.append({})

        # -----------------------------------------------------------
        #  GALOIS A (Push-All) - Use native method
        # -----------------------------------------------------------
        try:
            # g.run_push_all internally forces Table-Scan, so it's safe.
            _, _, debug_a = g.run_push_all(debug=True)
            stats_a.append(extract_exp6_stats_from_debug(debug_a))
        except Exception as e:
            LOG.error(f"Error GALOIS_A query {i}: {e}")
            stats_a.append({})

        # -----------------------------------------------------------
        #  GALOIS F (Smart) - Use native method
        # -----------------------------------------------------------
        try:
            # g.run_push_confident uses the instance strategy (which is "table"), so it's safe.
            # Internally includes the call to the Estimator.
            _, _, debug_f = g.run_push_confident(debug=True)
            stats_f.append(extract_exp6_stats_from_debug(debug_f))
        except Exception as e:
            LOG.error(f"Error GALOIS_F query {i}: {e}")
            stats_f.append({})

    return {
        "galois_empty": summarize_exp6(stats_empty, max_iter),
        "galois_a": summarize_exp6(stats_a, max_iter),
        "galois_f": summarize_exp6(stats_f, max_iter),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default=None)
    ap.add_argument("--provider", default="watsonx")
    ap.add_argument("--max-iter", type=int, default=10)
    ap.add_argument(
        "--only-query-ids",
        default=None,
        help="Comma-separated query indices (1-based), e.g. '1,7,19'"
    )

    args = ap.parse_args()

    # If no dataset is specified, use all IK datasets (Exp-1 set)
    datasets = [args.dataset] if args.dataset else IK_DATASETS

    only_ids = None
    if args.only_query_ids:
        only_ids = {int(x.strip()) for x in args.only_query_ids.split(",") if x.strip()}

    summary: Dict[str, Any] = {}
    for ds in datasets:
        try:
            summary[ds] = run_exp6_dataset(ds, args.provider, args.max_iter, only_ids)
        except Exception as e:
            LOG.error(f"Failed dataset {ds}: {e}")

    out_path = Path(__file__).parent / f"exp6_summary_maxiter{args.max_iter}.json"
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    LOG.info(f"[EXP6] Summary written to {out_path}")


if __name__ == "__main__":
    main()