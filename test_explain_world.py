# test_explain_world.py
from pathlib import Path
from galois_core_system.src.explain.duckdb_explain import get_explain_json, save_explain_json

ROOT = Path(__file__).resolve().parent
DB   = ROOT / "setup-environment" / "data" / "world" / "world.duckdb"
SQL  = """
SELECT id, name 
FROM target.city 
WHERE country_code_3_letters='ITA' 
ORDER BY id 
LIMIT 3
"""

plan = get_explain_json(DB, SQL)
print("Top-level keys:", list(plan.keys()))

OUT = ROOT / "galois_core_system" / "results" / "plan_world_ita.json"
OUT.parent.mkdir(parents=True, exist_ok=True)
save_explain_json(DB, SQL, OUT)
print("Wrote:", OUT)

