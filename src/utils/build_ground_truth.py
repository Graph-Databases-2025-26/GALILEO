#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build ground-truth by executing queries over CSVs using DuckDB.
- Creates all tables under a chosen SCHEMA (default: "target") so queries like target.table work.
- Adds a few compatibility aliases (e.g., fortune1000_2024 -> fortune_2024).

Requires: duckdb, pandas

Folder structure expected (as in data.zip):
  data/
    <dataset>[/subdataset]/
      *.csv
      queries_*.sql

Outputs:
  ground/<DATASET>/query{i}.json
"""
import argparse, json, os, re, sys
from pathlib import Path
import duckdb, pandas as pd

def read_sql_statements(path: Path):
    text = path.read_text(encoding="utf-8")
    # Strip simple -- comments
    lines = []
    for line in text.splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    text = "\n".join(lines)
    # Split by semicolons that terminate statements
    # Keep robust split: newlines + ; + optional trailing comment
    parts = [p.strip() for p in re.split(r';\s*(?:--.*)?\n', text) if p.strip()]
    stmts = []
    for p in parts:
        for q in [s.strip() for s in p.split(';') if s.strip()]:
            if q:
                stmts.append(q)
    # Deduplicate preserving order
    out, seen = [], set()
    for s in stmts:
        if s not in seen:
            seen.add(s); out.append(s)
    return out

def find_dataset_dirs(data_root: Path):
    # Accept both data_root/ and data_root/data/
    root = data_root / "data" if (data_root / "data").exists() else data_root
    out = {}
    for ds in sorted(root.iterdir()):
        if not ds.is_dir():
            continue
        if ds.name == "flight":
            for sub in sorted(ds.iterdir()):
                if sub.is_dir():
                    out[f"flight-{sub.name.split('-')[-1]}"] = sub
        else:
            out[ds.name] = ds
    return out

def load_csvs_into_duckdb(con: duckdb.DuckDBPyConnection, ds_dir: Path, schema: str):
    con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    for csv_path in sorted(ds_dir.glob("*.csv")):
        table = csv_path.stem
        if table == "movies":
            # keep schema variable consistent everywhere
            con.execute(f'''
                CREATE SCHEMA IF NOT EXISTS "{schema}";
                CREATE OR REPLACE TABLE "{schema}".movies(
                primarytitle VARCHAR,
                originaltitle VARCHAR,
                startyear BIGINT,
                endyear BIGINT,
                runtimeminutes BIGINT,
                genres VARCHAR,
                director VARCHAR,
                birthyear BIGINT,
                deathyear BIGINT
                );
            ''')
            # use a parameter for the path and let DuckDB handle quoting
            con.execute(
                f'''COPY "{schema}".movies FROM ? 
                    (FORMAT CSV, HEADER TRUE, DELIMITER ',', QUOTE '"', ESCAPE '"', NULL 'null');''',
                [str(csv_path)]  # or csv_path.as_posix()
            )
        else:
            con.execute(
                f'CREATE OR REPLACE TABLE "{schema}"."{table}" AS '
                'SELECT * FROM read_csv_auto(?, header=True)',
                [str(csv_path)]
            )

def add_compat_aliases(con: duckdb.DuckDBPyConnection, schema: str, dataset_name: str):
    # Aliases needed because some SQLs reference slightly different table names than CSV file names.
    aliases = []
    if dataset_name == "fortune":
        # CSV is fortune1000_2024, SQL uses fortune_2024
        aliases.append(("fortune_2024", "fortune1000_2024"))
    # You can extend here if more mismatches appear.

    for alias, real in aliases:
        # Create view alias only if real table exists
        exists = con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema=? AND table_name=?",
            [schema, real]
        ).fetchone()
        if exists:
            con.execute(f'CREATE OR REPLACE VIEW "{schema}"."{alias}" AS SELECT * FROM "{schema}"."{real}"')

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", type=Path, required=True)
    ap.add_argument("--ground-root", type=Path, required=True)
    ap.add_argument("--datasets", nargs="*", default=None)
    ap.add_argument("--schema-name", default="target", help='DuckDB schema to load tables into (default: "target")')
    args = ap.parse_args()

    ds_dirs = find_dataset_dirs(args.data_root)
    if args.datasets:
        ds_dirs = {k:v for k,v in ds_dirs.items() if k in args.datasets}

    for ds_name, ds_dir in ds_dirs.items():
        qfiles = list(ds_dir.glob("queries_*.sql"))
        if not qfiles:
            print(f"[WARN] No queries_*.sql in {ds_dir}; skipping")
            continue
        qpath = sorted(qfiles)[0]
        sqls = read_sql_statements(qpath)
        if not sqls:
            print(f"[WARN] No SQL statements in {qpath}")
            continue

        out_dir = args.ground_root / ds_name.upper()
        out_dir.mkdir(parents=True, exist_ok=True)

        con = duckdb.connect()
        # Load CSVs into the target schema
        schema = args.schema_name
        load_csvs_into_duckdb(con, ds_dir, schema)
        # Add dataset-specific aliases
        add_compat_aliases(con, schema, ds_name)

        for i, sql in enumerate(sqls, start=1):
            try:
                df = con.execute(sql).df()
                rows = df.to_dict(orient="records")
            except Exception as e:
                print(f"[ERROR] {ds_name} q{i}: {e}\n")
                rows = []
            (out_dir / f"query{i}.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[OK] {ds_name} query{i}: {len(rows)} rows")
        con.close()

if __name__ == "__main__":
    try:
        main()
    except duckdb.IOException as e:
        print("DuckDB I/O error:", e, file=sys.stderr)
        sys.exit(1)
