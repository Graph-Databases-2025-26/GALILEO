#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Batch exporter: runs py-galois over datasets and saves JSON results + meta
- Discovers queries_*.sql per dataset
- Calls the FastAPI endpoint /query of the running py-galois server
- Produces submissions/<STUDENT>/<DATASET>/query{i}.json + .meta.json
"""
import argparse, os, time, json, re
from pathlib import Path
import requests

def read_sql_statements(path: Path):
    text = path.read_text(encoding="utf-8")
    # Remove simple -- line comments
    lines = []
    for line in text.splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    text = "\n".join(lines)
    # Split on semicolons that end a statement
    parts = [p.strip() for p in re.split(r';\s*(?:--.*)?\n', text) if p.strip()]
    # Some files may not end with semicolon/newline; also split by ';' fallback
    stmts = []
    for p in parts:
        for q in [s.strip() for s in p.split(';') if s.strip()]:
            if q:
                stmts.append(q)
    # Deduplicate preserving order
    out = []
    seen = set()
    for s in stmts:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out

def find_dataset_dirs(data_root: Path):
    # structure is data_root/data/<dataset>[/subdataset]
    root = data_root / "data" if (data_root / "data").exists() else data_root
    out = {}
    for ds in sorted(root.iterdir()):
        if not ds.is_dir():
            continue
        if ds.name == "flight":
            # split in flight-2 and flight-4
            for sub in sorted(ds.iterdir()):
                if sub.is_dir():
                    out[f"flight-{sub.name.split('-')[-1]}"] = sub
        else:
            out[ds.name] = ds
    return out

def schema_path_for(dataset_name: str, schemas_root: Path):
    # flight-2/4 are named flight_flight-2.json etc
    if dataset_name.startswith("flight-"):
        fname = f"flight_{dataset_name}.json"
    else:
        fname = f"{dataset_name}.json"
    p = schemas_root / fname
    if not p.exists():
        raise FileNotFoundError(f"Schema not found for {dataset_name}: {p}")
    return str(p.resolve())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", type=Path, required=True)
    ap.add_argument("--schemas-root", type=Path, required=True)
    ap.add_argument("--submissions-root", type=Path, required=True)
    ap.add_argument("--student", required=True, help="Student/system name for output folder")
    ap.add_argument("--datasets", nargs="*", default=None, help="If set, only these datasets (e.g., geo world flight-2 flight-4)")
    ap.add_argument("--api-url", default="http://127.0.0.1:8000/query", help="py-galois /query endpoint")
    ap.add_argument("--provider", default=os.getenv("GALOIS_PROVIDER","openai"))
    ap.add_argument("--tau", type=float, default=float(os.getenv("GALOIS_TAU", "0.6")))
    ap.add_argument("--max-iter", type=int, default=int(os.getenv("GALOIS_MAX_ITER","10")))
    args = ap.parse_args()

    ds_dirs = find_dataset_dirs(args.data_root)
    if args.datasets:
        ds_dirs = {k:v for k,v in ds_dirs.items() if k in args.datasets}

    base_out = args.submissions_root / args.student
    for ds_name, ds_dir in ds_dirs.items():
        # find queries file
        qfiles = list(ds_dir.glob("queries_*.sql"))
        if not qfiles:
            print(f"[WARN] No queries_*.sql in {ds_dir}; skipping")
            continue
        qpath = sorted(qfiles)[0]
        sqls = read_sql_statements(qpath)
        if not sqls:
            print(f"[WARN] No SQL statements in {qpath}")
            continue

        schema_path = schema_path_for(ds_name, args.schemas_root)
        ds_out = base_out / ds_name.upper()
        ds_out.mkdir(parents=True, exist_ok=True)

        for i, sql in enumerate(sqls, start=1):
            payload = {
                "schema_path": schema_path,
                "provider": args.provider,
                "tau": args.tau,
                "max_iter": args.max_iter,
                "sql": sql,
            }
            t0 = time.time()
            try:
                r = requests.post(args.api_url, json=payload, timeout=300)
                r.raise_for_status()
                obj = r.json()
            except Exception as e:
                print(f"[ERROR] {ds_name} q{i}: {e}")
                obj = {"rows": [], "tokens": 0, "error": str(e)}
            dt = time.time() - t0
            rows = obj.get("rows", [])
            tokens = obj.get("tokens", 0)

            # save
            (ds_out / f"query{i}.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
            meta = {"tokens": tokens, "time_sec": round(dt, 3)}
            (ds_out / f"query{i}.meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[OK] {ds_name} query{i}: rows={len(rows)} tokens={tokens} time={dt:.2f}s")

if __name__ == "__main__":
    main()
