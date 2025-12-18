import argparse
import json
import time
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Optional

import httpx

from src.config import Config_Loader
from src.db import load_queries_from_folder
from src.galois.galois import Galois, save_galois_results
from src.galois.galois_estimator import ConfidenceEstimator
from src.galois.galois_prompts import (
    system_prompt_galois_confidence,
    human_prompt_galois_confidence,
)
from src.utils import DATA_DIR, IK_DATASETS, LOG



# helpers for retrying network calls with exponential backoff
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
                f"Retry {t+1}/{tries} in {sleep_s:.1f}s"
            )
            time.sleep(sleep_s)
    raise last



# metric extraction from debug info

def extract_exp6_stats_from_debug(debug: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns:
      - input_tokens_total: float  (proxy: sum of per-table scan tokens)
      - n_iters: Optional[int]     (if present somewhere, otherwise None)
    """

    tables = debug.get("tables", []) if isinstance(debug, dict) else []
    input_tokens_total = 0.0
    iters: List[int] = []

    for t in tables:
        if not isinstance(t, dict):
            continue
        
        if t.get("input_tokens_total_all_iters") is not None:
            try:
                input_tokens_total += float(t["input_tokens_total_all_iters"])
            except Exception:
                pass
        else:
            # fallback to old proxy if needed
            try:
                input_tokens_total += float(t.get("tokens", 0) or 0)
            except Exception:
              pass
            
        if isinstance(t.get("n_iters"), int):
            iters.append(t["n_iters"])

    return {
        "input_tokens_total": input_tokens_total,
        "n_iters": (max(iters) if iters else None),
    }


def summarize_exp6(per_query_stats: List[Dict[str, Any]], max_iter: int) -> Dict[str, Any]:
    if not per_query_stats:
        return {
            "n_queries": 0,
            "n_success": 0,
            "avg_input_tokens_all_iters": 0.0,
            "avg_iters_per_query": None,
            "n_queries_ge_max_iter": 0,
        }

    # Only successful stats (skip failures where token total is None)
    valid = [
        x for x in per_query_stats
        if isinstance(x.get("input_tokens_total"), (int, float))
    ]

    if not valid:
        return {
            "n_queries": len(per_query_stats),
            "n_success": 0,
            "avg_input_tokens_all_iters": 0.0,
            "avg_iters_per_query": None,
            "n_queries_ge_max_iter": 0,
        }

    avg_tokens = mean(x["input_tokens_total"] for x in valid)

    iters = [x["n_iters"] for x in valid if isinstance(x.get("n_iters"), int)]
    avg_iters = mean(iters) if iters else None
    n_ge = sum(
        1 for x in valid
        if isinstance(x.get("n_iters"), int) and x["n_iters"] >= max_iter
    )

    return {
        "n_queries": len(per_query_stats),  # attempted
        "n_success": len(valid),            # successful parsed
        "avg_input_tokens_all_iters": avg_tokens,
        "avg_iters_per_query": avg_iters,
        "n_queries_ge_max_iter": n_ge,
    }



def run_exp6_dataset(dataset: str, provider: str, max_iter: int, only_ids: Optional[set[int]] = None) -> Dict[str, Any]:
    config = Config_Loader().get_config()
    dataset_path = DATA_DIR / dataset
    queries = load_queries_from_folder(dataset_path)

    out_empty: List[Dict[str, Any]] = []
    out_a: List[Dict[str, Any]] = []
    out_f: List[Dict[str, Any]] = []

    stats_empty: List[Dict[str, Any]] = []
    stats_a: List[Dict[str, Any]] = []
    stats_f: List[Dict[str, Any]] = []

    for q_idx, (_, sql_query) in enumerate(queries, start=1):
        if only_ids is not None and q_idx not in only_ids:
            continue
        query_id = f"query{q_idx}"
        LOG.info(f"[EXP6] {dataset} – {query_id}")

        # Build base Galois object 
        g = Galois(config, dataset, sql_query, physical_strategy="table")

        all_conds = g.parsed_sql.get("where_conditions", []) or []

        # Confidence-based filter 
        confident_conds: List[str] = []
        if all_conds:
            estimator = ConfidenceEstimator(
                g.llm_wrapper,
                g.dataset,
                system_prompt_galois_confidence(),
                human_prompt_galois_confidence("CONDITION"),
            )
            # Wrap estimator call with retries (network robustness)
            confident_conds = with_retries(
                lambda: estimator.estimate_confidence_conditions(
                    g.parsed_sql["from_table"], all_conds
                ),
                what=f"confidence-estimator {dataset}/{query_id}",
            ) or []

        variants = [
            # Paper Exp-6: Galois ∅ = Table-Scan without push-down. 
            ("EXP6_GALOIS_EMPTY_TABLE", [], out_empty, stats_empty),
            # Galois A = push all conditions
            ("EXP6_GALOIS_A_TABLE", all_conds, out_a, stats_a),
            # Galois F = push only confident conditions
            ("EXP6_GALOIS_F_TABLE", confident_conds, out_f, stats_f),
        ]

        for variant_name, conds, out_list, stat_list in variants:
            try:
                plan = g.build_execution_plan(conditions_to_push=conds)

                plan["max_iter"] = max_iter  # set max_iter in plan

                # execute_variant(...) with debug=True to get detailed stats
                results, base_stats, debug = g.execute_variant(plan, variant_name, debug=True)

                exp6_stats = extract_exp6_stats_from_debug(debug)

                # Record per-query output
                out_list.append({
                    "query_id": str(q_idx),
                    "result_set": results,
                    "tokens": base_stats.get("total_tokens", 0),
                    "time": base_stats.get("total_time", 0.0),
                    "exp6": {
                        "input_tokens_total": exp6_stats["input_tokens_total"],
                        "n_iters": exp6_stats["n_iters"],          
                        "max_iter": max_iter,
                    },
                    "debug": debug,  
                })
                stat_list.append({"input_tokens_total": exp6_stats["input_tokens_total"],"n_iters": exp6_stats["n_iters"],
                                  })


            except Exception as e:
                LOG.exception("[EXP6] {} query{} {} failed: {}", dataset, q_idx, variant_name, str(e))
                
                out_list.append({
                    "query_id": str(q_idx),
                    "result_set": None,
                    "tokens": None,
                    "time": None,
                    "exp6": {"input_tokens_total": None, "n_iters": None, "max_iter": max_iter},
                    "error": str(e),
                    })
                stat_list.append({"input_tokens_total": None, "n_iters": None})
                
                continue

    # Save per-query 
    save_galois_results(out_empty, "EXP6_GALOIS_EMPTY_TABLE", provider, dataset)
    save_galois_results(out_a, "EXP6_GALOIS_A_TABLE", provider, dataset)
    save_galois_results(out_f, "EXP6_GALOIS_F_TABLE", provider, dataset)

    return {
        "galois_empty": summarize_exp6(stats_empty, max_iter),
        "galois_a": summarize_exp6(stats_a, max_iter),
        "galois_f": summarize_exp6(stats_f, max_iter),
        "notes": {
            "iters_available": any(isinstance(x.get("n_iters"), int) for x in (stats_empty + stats_a + stats_f)),
            "iters_note": (
                "n_iters is not exposed by current execute_variant(debug=True). "
                "To match paper Table 9 exactly, propagate iteration counts from the scan executor into debug_info."
            ),
        }
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

    datasets = [args.dataset] if args.dataset else IK_DATASETS

    only_ids = None
    if args.only_query_ids:
        only_ids = {int(x.strip()) for x in args.only_query_ids.split(",") if x.strip()}

    summary: Dict[str, Any] = {}
    for ds in datasets:
        summary[ds] = run_exp6_dataset(ds, args.provider, args.max_iter, only_ids=only_ids,)

    out_path = Path(__file__).parent / f"exp6_summary_maxiter{args.max_iter}.json"
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    LOG.info(f"[EXP6] Summary written to {out_path}")


if __name__ == "__main__":
    main()

