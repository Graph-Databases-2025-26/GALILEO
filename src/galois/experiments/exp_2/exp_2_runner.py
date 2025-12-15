import argparse
import json
import time
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Optional, Tuple

import httpx

from src.config import Config_Loader
from src.db import load_queries_from_folder
from src.galois.galois import Galois, save_galois_results
from src.galois.galois_estimator import ConfidenceEstimator
from src.galois.galois_prompts import (
    human_prompt_galois_confidence,
    system_prompt_galois_confidence,
)
from src.utils import DATA_DIR, GROUND_PATH, LOG
from src.utils.tutor.galois_eval import (
    cardinality,
    f1_cell_exact,
    tuple_constraint,
)

# -----------------------------
# Robustness helpers
# -----------------------------

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
                f"[EXP2] {what} failed ({type(e).__name__}): {e}. "
                f"Retry {t+1}/{tries} in {sleep_s:.1f}s"
            )
            time.sleep(sleep_s)
    raise last


# -----------------------------
# Helpers
# -----------------------------

def resolve_existing_dir(base: Path, candidates: List[str]) -> str:
    for c in candidates:
        if (base / c).exists():
            return c
    return candidates[0]


def _norm(v):
    return "" if v is None else str(v)


def load_ground_truth(
    gt_base: Path, dataset_dir: str, query_id: str
) -> Tuple[List[str], List[List[Any]]]:
    p = gt_base / dataset_dir / f"{query_id}.json"
    if not p.exists():
        raise FileNotFoundError(f"Missing ground truth file: {p}")

    data = json.loads(p.read_text(encoding="utf-8"))

    if isinstance(data, list) and (len(data) == 0 or isinstance(data[0], dict)):
        if not data:
            return [], []
        cols = list(data[0].keys())
        rows = [[_norm(row.get(c)) for c in cols] for row in data]
        return cols, rows

    if isinstance(data, dict) and "columns" in data and "rows" in data:
        cols = data["columns"]
        rows = [[_norm(v) for v in row] for row in data["rows"]]
        return cols, rows

    raise ValueError(f"Unsupported GT format in {p}")


def results_to_cols_rows(
    results: List[Dict[str, Any]],
    gt_cols: Optional[List[str]] = None,
) -> Tuple[List[str], List[List[Any]]]:
    if not results:
        return (gt_cols or []), []

    if gt_cols:
        cols = list(gt_cols)
        rows = [[_norm(r.get(c)) for c in cols] for r in results]
        return cols, rows

    cols = list(results[0].keys())
    rows = [[_norm(r.get(c)) for c in cols] for r in results]
    return cols, rows


def compute_metrics(gt_cols, gt_rows, pred_cols, pred_rows) -> Dict[str, float]:
    f1 = f1_cell_exact(gt_cols, gt_rows, pred_cols, pred_rows)
    card = cardinality(gt_rows, pred_rows)
    tcon = tuple_constraint(gt_rows, pred_rows)
    avg = mean([f1, card, tcon])
    return {
        "f1": f1,
        "cardinality": card,
        "tuple_constraint": tcon,
        "avg": avg,
    }


def approx_equal(a: float, b: float, eps: float = 1e-6) -> bool:
    return abs(a - b) <= eps


@dataclass
class VariantResult:
    query_id: str
    metrics: Dict[str, float]
    total_tokens: int
    total_time: float


# -----------------------------
# Core logic
# -----------------------------

def compute_galois_f_pushdown_conditions(
    config, dataset_db: str, sql_query: str
) -> List[str]:
    g = Galois(config, dataset_db, sql_query, physical_strategy="table")
    all_conditions = g.parsed_sql.get("where_conditions", []) or []

    estimator = ConfidenceEstimator(
        g.llm_wrapper,
        g.dataset,
        system_prompt_galois_confidence(),
        human_prompt_galois_confidence("CONDITION"),
    )

    if not all_conditions:
        return []

    confident = estimator.estimate_confidence_conditions(
        g.parsed_sql["from_table"], all_conditions
    )

    if len(confident) == 0:
        return []
    if len(confident) == 1:
        return confident
    return all_conditions


def run_fixed_pushdown(
    config,
    dataset_db: str,
    sql_query: str,
    conditions_to_push: List[str],
    physical_strategy: str,
):
    g = Galois(config, dataset_db, sql_query, physical_strategy=physical_strategy)
    plan = g.build_execution_plan(conditions_to_push=conditions_to_push)

    def _run():
        return g.execute_variant(
            plan, f"EXP2_FIXED_PUSHDOWN_{physical_strategy.upper()}"
        )

    return with_retries(_run, what=f"execute_variant {physical_strategy}")


# -----------------------------
# Experiment 2
# -----------------------------

def run_exp2(
    dataset_db: str,
    dataset_gt: str,
    provider: str,
    limit: Optional[int],
    only: str,
) -> Path:
    config = Config_Loader().get_config()

    queries = load_queries_from_folder(DATA_DIR / dataset_db)
    if limit:
        queries = queries[:limit]

    phys_table_out, phys_key_out, phys_auto_out = [], [], []
    phys_table_eval, phys_key_eval, phys_auto_eval = [], [], []

    log_nopush_out, log_a_out, log_f_out = [], [], []
    log_nopush_eval, log_a_eval, log_f_eval = [], [], []

    n_auto_correct = 0
    n_auto_total = 0
    n_log_total = 0

    for i, (_, sql_query) in enumerate(queries):
        idx = i + 1
        gt_id = f"query{idx}"
        save_id = str(idx)

        LOG.info(f"[EXP2] Running {gt_id}")

        try:
            gt_cols, gt_rows = load_ground_truth(
                GROUND_PATH, dataset_gt, gt_id
            )

            conditions_f = compute_galois_f_pushdown_conditions(
                config, dataset_db, sql_query
            )

            # ---------- Physical ----------
            if only in ("physical", "all"):
                r_t, s_t = run_fixed_pushdown(
                    config, dataset_db, sql_query, conditions_f, "table"
                )
                pcols, prows = results_to_cols_rows(r_t, gt_cols)
                m_t = compute_metrics(gt_cols, gt_rows, pcols, prows)

                phys_table_out.append({
                    "query_id": save_id,
                    "result_set": {"columns": pcols, "rows": prows},
                    "total_tokens": s_t["total_tokens"],
                    "total_time": s_t["total_time"],
                })
                phys_table_eval.append(
                    VariantResult(gt_id, m_t, s_t["total_tokens"], s_t["total_time"])
                )

                r_k, s_k = run_fixed_pushdown(
                    config, dataset_db, sql_query, conditions_f, "key"
                )
                pcols, prows = results_to_cols_rows(r_k, gt_cols)
                m_k = compute_metrics(gt_cols, gt_rows, pcols, prows)

                phys_key_out.append({
                    "query_id": save_id,
                    "result_set": {"columns": pcols, "rows": prows},
                    "total_tokens": s_k["total_tokens"],
                    "total_time": s_k["total_time"],
                })
                phys_key_eval.append(
                    VariantResult(gt_id, m_k, s_k["total_tokens"], s_k["total_time"])
                )

                r_a, s_a = run_fixed_pushdown(
                    config, dataset_db, sql_query, conditions_f, "auto"
                )
                pcols, prows = results_to_cols_rows(r_a, gt_cols)
                m_a = compute_metrics(gt_cols, gt_rows, pcols, prows)

                phys_auto_out.append({
                    "query_id": save_id,
                    "result_set": {"columns": pcols, "rows": prows},
                    "total_tokens": s_a["total_tokens"],
                    "total_time": s_a["total_time"],
                })
                phys_auto_eval.append(
                    VariantResult(gt_id, m_a, s_a["total_tokens"], s_a["total_time"])
                )

                best = max(m_t["avg"], m_k["avg"])
                n_auto_total += 1
                if approx_equal(m_a["avg"], best):
                    n_auto_correct += 1

            # ---------- Logical ----------
            if only in ("logical", "all"):
                g_tmp = Galois(config, dataset_db, sql_query, "table")
                all_conds = g_tmp.parsed_sql.get("where_conditions", []) or []
                if len(all_conds) < 2:
                    continue

                n_log_total += 1

                for label, conds, out, ev in [
                    ("nopush", [], log_nopush_out, log_nopush_eval),
                    ("a", all_conds, log_a_out, log_a_eval),
                    ("f", conditions_f, log_f_out, log_f_eval),
                ]:
                    r, s = run_fixed_pushdown(
                        config, dataset_db, sql_query, conds, "table"
                    )
                    pcols, prows = results_to_cols_rows(r, gt_cols)
                    m = compute_metrics(gt_cols, gt_rows, pcols, prows)

                    out.append({
                        "query_id": save_id,
                        "result_set": {"columns": pcols, "rows": prows},
                        "total_tokens": s["total_tokens"],
                        "total_time": s["total_time"],
                    })
                    ev.append(
                        VariantResult(gt_id, m, s["total_tokens"], s["total_time"])
                    )

        except Exception as e:
            LOG.error(f"[EXP2] Skipping {gt_id}: {type(e).__name__}: {e}")
            continue

    summary = {
        "dataset_db": dataset_db,
        "dataset_ground_truth": dataset_gt,
        "provider": provider,
        "only": only,
        "physical": None,
        "logical": None,
    }

    if only in ("physical", "all"):
        save_galois_results(phys_table_out, "EXP2_PHYS_TABLE", provider, dataset_db)
        save_galois_results(phys_key_out, "EXP2_PHYS_KEY", provider, dataset_db)
        save_galois_results(phys_auto_out, "EXP2_PHYS_AUTO", provider, dataset_db)

        summary["physical"] = {
            "n_queries": n_auto_total,
            "auto_accuracy": n_auto_correct / n_auto_total if n_auto_total else 0.0,
            "avg_score_mean": {
                "table": mean(v.metrics["avg"] for v in phys_table_eval),
                "key": mean(v.metrics["avg"] for v in phys_key_eval),
                "auto": mean(v.metrics["avg"] for v in phys_auto_eval),
            },
            "tokens_m_total": {
                "table": sum(v.total_tokens for v in phys_table_eval) / 1e6,
                "key": sum(v.total_tokens for v in phys_key_eval) / 1e6,
                "auto": sum(v.total_tokens for v in phys_auto_eval) / 1e6,
            },
        }

    if only in ("logical", "all"):
        save_galois_results(log_nopush_out, "EXP2_LOG_NOPUSH_TABLE", provider, dataset_db)
        save_galois_results(log_a_out, "EXP2_LOG_GALOIS_A_TABLE", provider, dataset_db)
        save_galois_results(log_f_out, "EXP2_LOG_GALOIS_F_TABLE", provider, dataset_db)

        summary["logical"] = {
            "n_queries": n_log_total,
            "avg_score_mean": {
                "no_push": mean(v.metrics["avg"] for v in log_nopush_eval),
                "galois_a": mean(v.metrics["avg"] for v in log_a_eval),
                "galois_f": mean(v.metrics["avg"] for v in log_f_eval),
            },
            "tokens_m_total": {
                "no_push": sum(v.total_tokens for v in log_nopush_eval) / 1e6,
                "galois_a": sum(v.total_tokens for v in log_a_eval) / 1e6,
                "galois_f": sum(v.total_tokens for v in log_f_eval) / 1e6,
            },
        }

    out_dir = Path(__file__).parent
    summary_path = out_dir / "exp2_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    LOG.info(f"[EXP2] Summary saved to {summary_path}")
    return summary_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", default=None)
    parser.add_argument("--ground-dataset", default=None)
    parser.add_argument("--provider", default="watsonx")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--only", choices=["physical", "logical", "all"], default="all")
    args = parser.parse_args()

    dataset_db = args.dataset or resolve_existing_dir(DATA_DIR, ["geo", "GEO"])
    dataset_gt = args.ground_dataset or resolve_existing_dir(GROUND_PATH, ["GEO", "geo"])

    run_exp2(
        dataset_db=dataset_db,
        dataset_gt=dataset_gt,
        provider=args.provider,
        limit=args.limit,
        only=args.only,
    )


if __name__ == "__main__":
    main()
