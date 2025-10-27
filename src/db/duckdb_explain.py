"""
duckdb_explain.py
Utilities to export DuckDB execution plans for a SQL query.

- get_explain()           -> {"format":"text", "plan_text": "..."}
- get_explain_analyze()   -> {"format":"text", "plan_text": "..."}

- save_explain_pair()     -> writes <base>__explain.json and <base>__analyze.json
"""

from __future__ import annotations
from pathlib import Path
from typing import Dict
import json
import time
import duckdb
import os

from src.utils.logging_config import logger, log_query_event


def _ensure_db(db_path: Path | str) -> Path:
    p = Path(db_path)
    if not p.exists():
        logger.warning(f"Database file not found: {p}")
        raise FileNotFoundError(f"Database file not found: {p}")
    return p

def _run_explain_text(db_path: Path | str, sql: str, analyze: bool) -> str:
    """
    Run EXPLAIN (or EXPLAIN ANALYZE) and return the ASCII plan as one string.
    """
    db = _ensure_db(db_path)
    con = duckdb.connect(str(db), read_only=True)
    try:
        stmt = f"EXPLAIN {'ANALYZE ' if analyze else ''}{sql}"
        rows = con.execute(stmt).fetchall()  # list[tuple[str, ...]]
        plan_lines = [" ".join(str(c) for c in row if c is not None) for row in rows]
        return "\n".join(plan_lines)
    finally:
        con.close()

def get_explain_text(db_path: Path | str, sql: str) -> Dict[str, str]:
    start = time.time()  
    result = {"format": "text", "plan_text": _run_explain_text(db_path, sql, analyze=False)}
    elapsed = (time.time() - start) * 1000.0  
    logger.info(f"EXPLAIN completed latency_ms={elapsed:.1f}")  
    return result

def get_explain_analyze_text(db_path: Path | str, sql: str) -> Dict[str, str]:
    start = time.time()  
    result = {"format": "text", "plan_text": _run_explain_text(db_path, sql, analyze=True)}
    elapsed = (time.time() - start) * 1000.0  
    logger.info(f"EXPLAIN ANALYZE completed latency_ms={elapsed:.1f}")  
    return result


def save_text_plan(plan: Dict[str, str], out_json: Path | str, out_txt: Path | str | None = None) -> Path:
    """
    Write the plan dict to JSON and TXT.
    - JSON → results/explain_result_json/<dataset>/
    - TXT  → results/explain_result/<dataset>/
    """
    out_json = Path(out_json)
    out_txt = Path(out_txt) if out_txt else None

    # normalize + clean headers
    raw = plan.get("plan_text", "")
    norm = raw.replace("\r\n", "\n").replace("\r", "\n").lstrip()
    clean_lines = []
    for line in norm.splitlines():
        ls = line.strip().lower()
        if ls.startswith("analyzed_plan"):
            continue
        if ls.startswith("physical_plan"):
            continue
        if ls.startswith("explain analyze") or ls.startswith("explain "):
            continue
        clean_lines.append(line)
    clean_text = "\n".join(clean_lines).strip() + "\n"

    # derive dataset name from the original out_json path (without creating it)
    dataset = out_json.parent.name  # e.g. 'flight-2'

    # JSON → results/explain_result_json/<dataset>/
    json_dir = Path("results") / "explain_result_json" / dataset
    json_dir.mkdir(parents=True, exist_ok=True)
    json_path = json_dir / out_json.name
    json_path.write_text(json.dumps(plan, indent=2), encoding="utf-8")

    # TXT → results/explain_result/<dataset>/
    if out_txt:
        txt_dir = Path("results") / "explain_result" / dataset
        txt_dir.mkdir(parents=True, exist_ok=True)
        txt_path = txt_dir / out_txt.name
        with open(txt_path, "w", encoding="utf-8", newline="\n") as f:
            f.write(clean_text)
        logger.info(f"Saved TXT plan → {txt_path}")  # NEW
    else:
        txt_path = None

    logger.info(f"Saved JSON plan → {json_path}")  # NEW
    return json_path



# -------- Compatibility aliases (tests/teammates may import these) --------

def get_explain(db_path: Path | str, sql: str):
    return get_explain_text(db_path, sql)

def get_explain_analyze(db_path: Path | str, sql: str):
    return get_explain_analyze_text(db_path, sql)

def save_explain(db_path: Path | str, sql: str, out_path: Path | str):
    plan = get_explain_text(db_path, sql)
    return save_text_plan(plan, out_path)

def save_analyze(db_path: Path | str, sql: str, out_path: Path | str):
    plan = get_explain_analyze_text(db_path, sql)
    return save_text_plan(plan, out_path)

def save_explain_pair(db_path: Path | str, sql: str, out_base: Path | str):
    """
    Write TWO JSON files:
      <out_base>__explain.json
      <out_base>__analyze.json
    Returns (path_explain_json, path_analyze_json).
    """
    base = Path(out_base)
    p_explain = base.with_name(base.name + "__explain.json")
    p_analyze = base.with_name(base.name + "__analyze.json")
    save_explain(db_path, sql, p_explain)
    save_analyze(db_path, sql, p_analyze)
    return p_explain, p_analyze

def save_both(db_path: Path | str, sql: str, out_base: Path | str):
    """
    Write BOTH formats for both modes:
      <out_base>__explain.json / .txt
      <out_base>__analyze.json / .txt
    Returns (j_explain, t_explain, j_analyze, t_analyze).
    """
    base = Path(out_base)

    # EXPLAIN (plan only)
    plan = get_explain(db_path, sql)
    json_explain = base.with_name(base.name + "__explain.json")
    txt_explain  = base.with_name(base.name + "__explain.txt")
    save_text_plan(plan, json_explain, txt_explain)

    # EXPLAIN ANALYZE (plan + timings)
    plan_an = get_explain_analyze(db_path, sql)
    json_analyze = base.with_name(base.name + "__analyze.json")
    txt_analyze  = base.with_name(base.name + "__analyze.txt")
    save_text_plan(plan_an, json_analyze, txt_analyze)

    return json_explain, txt_explain, json_analyze, txt_analyze
