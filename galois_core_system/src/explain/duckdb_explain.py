# galois_core_system/src/explain/duckdb_explain.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import duckdb


def get_explain_json(db_path: str | Path, sql: str) -> Dict[str, Any]:
    """
    Try EXPLAIN JSON first (new DuckDB); on error fallback to plain EXPLAIN text.
    Always return a JSON-friendly dict.
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database file not found: {db_path}")

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        # Preferred path (DuckDB >= ~1.6)
        try:
            row = con.execute(f"EXPLAIN JSON {sql}").fetchone()
            print("[explain] used: EXPLAIN JSON")
            return json.loads(row[0])  # row[0] is a JSON string
        except Exception as e:
            # Fallback for older DuckDB (e.g., 1.4.x)
            print(f"[explain] JSON failed: {e!r} -> falling back to plain EXPLAIN")
            rows = con.execute(f"EXPLAIN {sql}").fetchall()
            plan_text = "\n".join(str(r[0]) for r in rows) if rows else ""
            return {"format": "text", "plan_text": plan_text}
    finally:
        con.close()


def save_explain_json(db_path: str | Path, sql: str, out_file: str | Path) -> Path:
    plan = get_explain_json(db_path, sql)
    out_file = Path(out_file)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(plan, indent=2), encoding="utf-8")
    return out_file
