import duckdb

DB = "setup-environment/data/world/world.duckdb"
SQL = """
SELECT id, name
FROM target.city
WHERE country_code_3_letters='ITA'
ORDER BY id
LIMIT 3
"""

con = duckdb.connect(DB, read_only=True)

# EXPLAIN (logical/physical plan)
rows = con.execute(f"EXPLAIN {SQL}").fetchall()
print("EXPLAIN plan:\n")
for row in rows:
    # join everything in the row tuple into one string
    print(" ".join(str(c) for c in row if c is not None))

print("\n--- EXPLAIN ANALYZE ---")

# EXPLAIN ANALYZE (execution plan + timings)
rows = con.execute(f"EXPLAIN ANALYZE {SQL}").fetchall()
for row in rows:
    print(" ".join(str(c) for c in row if c is not None))

con.close()

