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
import duckdb

def _ensure_db(db_path: Path | str) -> Path:
    p = Path(db_path)
    if not p.exists():
        raise FileNotFoundError(f"Database file not found: {p}")
    return p

def _run_explain_text(db_path: Path | str, sql: str, analyze: bool) -> str:
    """
    Run EXPLAIN (or EXPLAIN ANALYZE) and return the ASCII plan as one string.
    DuckDB 1.4.x returns EXPLAIN as multiple rows and (in some builds) multiple columns.
    We join ALL columns in each row, then join all rows.
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
    return {"format": "text", "plan_text": _run_explain_text(db_path, sql, analyze=False)}

def get_explain_analyze_text(db_path: Path | str, sql: str) -> Dict[str, str]:
    return {"format": "text_analyze", "plan_text": _run_explain_text(db_path, sql, analyze=True)}

import os
import json
from pathlib import Path
from typing import Dict


def save_text_plan(plan: Dict[str, str], out_json: Path | str, out_txt: Path | str | None = None) -> Path:
    """
    Write the plan dict to JSON.
    If out_txt is provided, also write a clean .txt with a header line and
    a blank line before the box, with normalized Unix newlines.
    """
    # 1) JSON
    out_json = Path(out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(plan, indent=2), encoding="utf-8")

    if out_txt:
        out_txt = Path(out_txt)
        out_txt.parent.mkdir(parents=True, exist_ok=True)

        # 2) Normalize, pick a header, and force a blank line
        raw = plan.get("plan_text", "")

        # normalize all Windows newlines to \n
        norm = raw.replace("\r\n", "\n").replace("\r", "\n").lstrip()

        # pick a simple header
        lower = norm.lower()
        if "analyze" in lower:
            header = "analyzed_plan"
        else:
            header = "physical_plan"

        # header + blank line + plan
        content = header + "\n\n" + norm

        # enforce unix newlines in the written file (so VS Code doesn't merge lines)
        with open(out_txt, "w", encoding="utf-8", newline="\n") as f:
            f.write(content)

    return out_json


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
