"""
Utilities to export DuckDB execution plans for a SQL query.

- get_explain()           -> {"format":"text", "plan_text": "..."}
- get_explain_analyze()   -> {"format":"text", "plan_text": "..."}

- save_explain_pair()     -> writes  <base>__analyze.json
"""

from __future__ import annotations
from pathlib import Path
from typing import Dict
import json
import time
import duckdb
import os

from src.utils.logging_config import logger, log_query_event

# cleanly exporting JSON data
def _write_json_pretty(p: Path, obj) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8") 

# clean up explain ASCII text to keep only the actual execution plan tree
def _clean_explain_text(raw: str) -> str:
    norm = (raw or "").replace("\r\n", "\n").replace("\r", "\n").lstrip()
    keep = []
    for line in norm.splitlines():
        ls = line.strip().lower()
        if ls.startswith("analyzed_plan"):   continue
        if ls.startswith("physical_plan"):   continue
        if ls.startswith("explain analyze"): continue
        if ls.startswith("explain "):        continue
        keep.append(line)
    return ("\n".join(keep)).strip() + "\n"


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

def _run_explain_json_try(db_path: Path | str, sql: str, analyze: bool) -> dict | None:
    """
    Try EXPLAIN (FORMAT json) / EXPLAIN ANALYZE (FORMAT json).
    Returns dict on success, or None if not supported / empty.
    """
    db = _ensure_db(db_path)
    con = duckdb.connect(str(db), read_only=True)
    try:
        stmt = f"EXPLAIN {'ANALYZE ' if analyze else ''}(FORMAT json) {sql}"
        row = con.execute(stmt).fetchone()
        if not row:
            return None
        payload = row[0]
        if not payload:
            return None
        try:
            return json.loads(payload) if isinstance(payload, str) else payload
        except Exception:
            return None
    finally:
        con.close()

def _profile_to_json_file(db_path: Path | str, sql: str, out_json_path: Path) -> dict:
    """
    Executes the query and writes JSON to out_json_path.
    Returns the parsed JSON dict.
    """
    db = _ensure_db(db_path)
    out_json_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db), read_only=True)
    try:
        con.execute("PRAGMA enable_profiling='json';")
        con.execute(f"PRAGMA profiling_output='{out_json_path.as_posix()}';")
        # Optional: richer details
        con.execute("PRAGMA profiling_mode='detailed';")
        # This **executes** the query:
        con.execute(sql).fetchall()
    finally:
        con.close()

    # Read what DuckDB wrote
    text = out_json_path.read_text(encoding="utf-8")
    return json.loads(text)


def get_explain_text(db_path: Path | str, sql: str) -> Dict[str, str]:
    """
    Run DuckDB’s EXPLAIN command on a query, measure how long it took, and return the plan text
    """
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

def _run_explain_json_try(db_path: Path | str, sql: str, analyze: bool) -> dict | None:
    """
    Try EXPLAIN (FORMAT json)/(ANALYZE). Return dict on success, or None
    if unsupported or any error occurs (so caller can use a fallback).
    """
    db = _ensure_db(db_path)
    con = duckdb.connect(str(db), read_only=True)
    try:
        stmt = f"EXPLAIN {'ANALYZE ' if analyze else ''}(FORMAT json) {sql}"
        try:
            row = con.execute(stmt).fetchone()
        except Exception as e:
            # DuckDB version likely doesn't support FORMAT json
            logger.debug(f"FORMAT json not supported or failed: {e}")
            return None
        if not row:
            return None
        payload = row[0]
        if not payload:
            return None
        try:
            return json.loads(payload) if isinstance(payload, str) else payload
        except Exception as e:
            logger.debug(f"Failed to parse EXPLAIN JSON payload: {e}")
            return None
    finally:
        con.close()


def get_explain_json(db_path: Path | str, sql: str) -> dict:
    start = time.time()
    plan_obj = _run_explain_json(db_path, sql, analyze=False)
    elapsed = (time.time() - start) * 1000.0
    logger.info(f"EXPLAIN (json) completed latency_ms={elapsed:.1f}")
    return {"format": "json", "plan": plan_obj}

def get_explain_analyze_json(db_path: Path | str, sql: str) -> dict:
    start = time.time()
    plan_obj = _run_explain_json(db_path, sql, analyze=True)
    elapsed = (time.time() - start) * 1000.0
    logger.info(f"EXPLAIN ANALYZE (json) completed latency_ms={elapsed:.1f}")
    return {"format": "json", "plan": plan_obj}


def save_text_plan(plan: Dict[str, str],out_json: Path | str | None,out_txt: Path | str | None = None) -> Path | None:
    """
    Write the *text* plan dict to TXT (cleaned),
    and optionally write a small JSON provenance (if out_json is provided).

    - TXT  → results/explain_result/<dataset>/
    - JSON → results/explain_result_json/<dataset>/   (only if out_json is not None)
    """
    # Safely coerce paths
    out_json = Path(out_json) if out_json else None
    out_txt  = Path(out_txt) if out_txt else None

    # Clean text for TXT
    clean_text = _clean_explain_text(plan.get("plan_text", ""))

    # Figure out dataset from whichever path we actually have
    base_path = out_json or out_txt
    dataset = base_path.parent.name if base_path else "UNKNOWN"

    json_path: Path | None = None

    # TXT (human-friendly)
    if out_txt:
        txt_dir = Path("results") / "explain_result" / dataset
        txt_dir.mkdir(parents=True, exist_ok=True)
        txt_path = txt_dir / out_txt.name
        txt_path.write_text(clean_text, encoding="utf-8")
        logger.info(f"Saved TXT plan → {txt_path}")

    # JSON (text provenance) 
    if out_json:
        json_dir = Path("results") / "explain_result_json" / dataset
        json_dir.mkdir(parents=True, exist_ok=True)
        json_path = json_dir / out_json.name
        json_path.write_text(json.dumps(plan, indent=2), encoding="utf-8")
        logger.info(f"Saved JSON (text provenance) → {json_path}")

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
    Writes:
      results/explain_result/<DATASET>/<base>__explain.txt   (clean text)
      results/explain_result/<DATASET>/<base>__analyze.txt   (clean text)
      results/explain_result_json/<DATASET>/<base>__analyze.json  (structured ONLY)

    Returns (txt_explain, txt_analyze, json_analyze) as Paths.
    """
    base = Path(out_base)
    dataset = base.parent.name

    # --- TXT only  ---
    plan_text = get_explain_text(db_path, sql)          # {"format":"text","plan_text":"..."}
    txt_explain = base.with_name(base.name + "__explain.txt")
    save_text_plan(plan_text, out_json=None, out_txt=txt_explain)

    plan_an_text = get_explain_analyze_text(db_path, sql)
    txt_analyze = base.with_name(base.name + "__analyze.txt")
    save_text_plan(plan_an_text, out_json=None, out_txt=txt_analyze)

    # --- Structured ANALYZE JSON only ---
    json_dir = Path("results") / "explain_result_json" / dataset
    json_dir.mkdir(parents=True, exist_ok=True)
    json_analyze = json_dir / (base.name + "__analyze.json")

    an_obj = _run_explain_json_try(db_path, sql, analyze=True)
    if an_obj is None:
        # fallback: profiling writes & then we read it back
        an_obj = _profile_to_json_file(db_path, sql, json_analyze)
        logger.info(f"Saved ANALYZE profiling JSON → {json_analyze}")
    else:
        json_analyze.write_text(json.dumps(an_obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        logger.info(f"Saved STRUCTURED ANALYZE JSON → {json_analyze}")

    return txt_explain, txt_analyze, json_analyze