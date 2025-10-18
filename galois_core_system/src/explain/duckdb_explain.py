"""
duckdb_explain.py
Utilities to export DuckDB execution plans for a SQL query.

- get_explain()           -> {"format":"text", "plan_text": "..."}
- get_explain_analyze()   -> {"format":"text", "plan_text": "..."}

- save_explain_pair()     -> writes <base>__explain.json and <base>__analyze.json
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

import duckdb


def _ensure_db(path: Path) -> Path:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Database file not found: {p}")
    return p


def _run_explain(con: duckdb.DuckDBPyConnection, sql: str, analyze: bool) -> Dict:
    """
    Runs EXPLAIN (or EXPLAIN ANALYZE) and returns a small JSON object.
    We target DuckDB 1.4.x: JSON output is not available, so we always
    return a text plan wrapped as JSON.
    """
    stmt = f"EXPLAIN {'ANALYZE ' if analyze else ''}{sql}"
    # DuckDB returns the plan as multiple rows (one line per row)
    rows = con.execute(stmt).fetchall()
    plan_text = "\n".join(r[0] for r in rows) if rows else ""

    return {
        "format": "text_analyze" if analyze else "text",
        "plan_text": plan_text,
    }


def get_explain(db_path: Path | str, sql: str) -> Dict:
    """EXPLAIN (no runtime) -> JSON object with plan text."""
    db = _ensure_db(Path(db_path))
    con = duckdb.connect(str(db), read_only=True)
    try:
        return _run_explain(con, sql, analyze=False)
    finally:
        con.close()


def get_explain_analyze(db_path: Path | str, sql: str) -> Dict:
    """EXPLAIN ANALYZE (with profile) -> JSON object with plan text."""
    db = _ensure_db(Path(db_path))
    con = duckdb.connect(str(db), read_only=True)
    try:
        return _run_explain(con, sql, analyze=True)
    finally:
        con.close()


def save_json(obj: Dict, out_path: Path | str) -> Path:
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)
    return out


def save_explain_pair(db_path: Path | str, sql: str, out_base: Path | str) -> tuple[Path, Path]:
    """
    Convenience: writes TWO files side-by-side:

      <out_base>__explain.json
      <out_base>__analyze.json
    """
    base = Path(out_base)
    p1 = base.with_name(base.name + "__explain.json")
    p2 = base.with_name(base.name + "__analyze.json")

    plan = get_explain(db_path, sql)
    save_json(plan, p1)

    plan_an = get_explain_analyze(db_path, sql)
    save_json(plan_an, p2)

    return p1, p2
