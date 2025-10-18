from pathlib import Path
from galois_core_system.src.explain.duckdb_explain import (
    get_explain,
    get_explain_analyze,
    save_explain_pair,
)

ROOT = Path(__file__).resolve().parent
DB   = ROOT / "setup-environment" / "data" / "world" / "world.duckdb"

# NOTE: correct column is 'country_code_3_letters' in your world dataset
SQL  = "SELECT id, name FROM target.city WHERE country_code_3_letters='ITA' ORDER BY id LIMIT 3"

# 1) Use them individually
plan = get_explain(DB, SQL)
print("EXPLAIN keys:", list(plan.keys()))

plan_an = get_explain_analyze(DB, SQL)
print("ANALYZE keys:", list(plan_an.keys()))

# 2) Or save both at once
OUT_BASE = ROOT / "galois_core_system" / "results" / "plan_world_ita"
p_explain, p_analyze = save_explain_pair(DB, SQL, OUT_BASE)
print("Wrote:", p_explain)
print("Wrote:", p_analyze)
