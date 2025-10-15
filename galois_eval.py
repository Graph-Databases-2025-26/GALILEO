#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GALOIS metrics evaluator (Java-faithful) — per DATASET only

Submission JSON format (per-query file):
- SIMPLE format: the JSON file is the result rows directly (e.g. a list of dicts).
  Example:
    [
      {"originaltitle": "The Adventures of Tom Sawyer"},
      {"originaltitle": "The Last of the Mohicans"}
    ]

- FULL format: an object with three fields:
    {
      "result_set": <same shapes accepted as SIMPLE (list of dicts/lists, or {"columns","rows"}, {"data"}, {"tuples"})>,
      "time": <seconds as number or string>,
      "tokens": <integer as number or string>
    }

Rules for tokens/time aggregation:
- For each dataset, we sum `tokens` across its queries and average `time` across its queries.
- BUT if ANY query file in that dataset lacks `tokens` (or `time`), the dataset's `#Tokens`
  (or `Avg Time (s)`) cell is left blank.
- Same policy for the ALL row across selected datasets: if ANY dataset is blank for a field,
  the ALL cell is blank for that field.

Implements the logic of the Java classes:
- CellNormalizer
- CellPrecision/CellRecall/CellF1Score  (exact set match after normalization)
- CellSimilarityPrecision/Recall/F1Score (edit distance + ±10% numeric tolerance)
- TupleCardinality (min(|E|,|R|)/max(|E|,|R|))
- TupleConstraint (multiset equality of normalized full tuples; counts must match)
- TupleSimilarityConstraint (tuple-level fuzzy version using the same edit-distance rule)

Layout:
  ground_root/
    MOVIES/  Q1.csv|json, Q2...
  submissions_root/
    MOVIES/  Q1.csv|json, Q2...         <-- each file is ONE query (simple or full JSON; CSV also allowed for rows)

Usage:
  python galois_eval.py --ground ground/ --submissions submissions/ --datasets MOVIES
Options:
  --cell-metric exact|similarity        (default: exact)
  --tuple-metric constraint|similarity  (default: constraint)
  --format table|csv|json|tex           (default: table)
  --overall                             (print a single ALL row across selected datasets)
  --latex-*                             (for --format=tex)
"""

from __future__ import annotations
import argparse, csv, json, math, re, sys
from pathlib import Path
from typing import Any, Dict, List, Tuple, Set, Optional
from collections import Counter

# ---------------- I/O ----------------

def _read_csv(path: Path) -> Tuple[List[str], List[List[Any]]]:
    rows = list(csv.reader(path.open("r", encoding="utf-8-sig", newline="")))
    if not rows: return [], []
    cols = [c.strip() for c in rows[0]]
    data = [r + [""]*(len(cols)-len(r)) for r in rows[1:]]
    data = [r[:len(cols)] for r in data]
    return cols, data

def _parse_table_from_obj(obj: Any) -> Tuple[List[str], List[List[Any]]]:
    """
    Accepts the same shapes as before:
      - list[dict]  -> columns = union of keys (sorted); rows = values per column
      - list[list]  -> columns = c0..cN; rows padded
      - dict with {"columns","rows"} -> respected
      - dict with {"tuples": list[dict]} -> union of keys (sorted)
      - dict with {"data": list[dict|list]} -> union or c0..cN
    """
    if isinstance(obj, dict):
        if "columns" in obj and "rows" in obj:
            cols = [str(c) for c in obj["columns"]]
            rows: List[List[Any]] = []
            for row in obj["rows"]:
                if isinstance(row, dict):
                    rows.append([row.get(c, "") for c in cols])
                else:
                    rows.append([row[i] if i < len(row) else "" for i in range(len(cols))])
            return cols, rows
        if "tuples" in obj and isinstance(obj["tuples"], list):
            cols = sorted({k for t in obj["tuples"] for k in (t.keys() if isinstance(t, dict) else [])})
            rows = [[t.get(c, "") for c in cols] for t in obj["tuples"] if isinstance(t, dict)]
            return cols, rows
        if "data" in obj and isinstance(obj["data"], list):
            arr = obj["data"]
            if arr and isinstance(arr[0], dict):
                cols = sorted({k for t in arr for k in t.keys()})
                rows = [[t.get(c, "") for c in cols] for t in arr]
                return cols, rows
            if arr and isinstance(arr[0], list):
                maxw = max(len(r) for r in arr)
                cols = [f"c{i}" for i in range(maxw)]
                rows = [r + [""]*(maxw-len(r)) for r in arr]
                return cols, rows
        # not a recognized dict -> empty
        return [], []
    if isinstance(obj, list):
        if not obj: return [], []
        if isinstance(obj[0], dict):
            cols = sorted({k for t in obj for k in t.keys()})
            rows = [[t.get(c, "") for c in cols] for t in obj]
            return cols, rows
        if isinstance(obj[0], list):
            maxw = max(len(r) for r in obj)
            cols = [f"c{i}" for i in range(maxw)]
            rows = [r + [""]*(maxw-len(r)) for r in obj]
            return cols, rows
    return [], []

def _read_json_table_only(path: Path) -> Tuple[List[str], List[List[Any]]]:
    obj = json.load(path.open("r", encoding="utf-8"))
    # SIMPLE format or any legacy shapes
    if isinstance(obj, dict) and "result_set" in obj:
        # If someone calls this on a FULL JSON, use its result_set and ignore metrics here
        return _parse_table_from_obj(obj["result_set"])
    return _parse_table_from_obj(obj)

def read_table_file(path: Path) -> Tuple[List[str], List[List[Any]]]:
    s = path.suffix.lower()
    if s == ".csv":  return _read_csv(path)
    if s == ".json": return _read_json_table_only(path)
    raise ValueError(f"Unsupported file type: {path}")

# --- Submission reader (per-query) that also extracts tokens/time when FULL format is used ---

def _to_float(x: Any) -> Optional[float]:
    if x is None: return None
    if isinstance(x, (int, float)): return float(x)
    if isinstance(x, str):
        xs = x.strip()
        if not xs: return None
        try:
            return float(xs)
        except Exception:
            return None
    return None

def read_submission_file(path: Optional[Path]) -> Tuple[List[str], List[List[Any]], Optional[float], Optional[float]]:
    """
    Returns (cols, rows, tokens, time_seconds).

    - CSV or SIMPLE JSON -> tokens=None, time=None
    - FULL JSON -> tokens extracted from 'tokens', time from 'time'
    """
    if not path or not path.exists():
        return [], [], None, None
    s = path.suffix.lower()
    if s == ".csv":
        cols, rows = _read_csv(path)
        return cols, rows, None, None
    if s == ".json":
        obj = json.load(path.open("r", encoding="utf-8"))
        if isinstance(obj, dict) and "result_set" in obj:
            cols, rows = _parse_table_from_obj(obj["result_set"])
            tokens = _to_float(obj.get("tokens"))
            time_s = _to_float(obj.get("time"))
            return cols, rows, tokens, time_s
        # SIMPLE format
        cols, rows = _parse_table_from_obj(obj)
        return cols, rows, None, None
    raise ValueError(f"Unsupported file type: {path}")

# -------------- Java CellNormalizer --------------

_million = re.compile(r'(\d+(?:\.\d+)?)\s*(million|m)\b', re.I)
_billion = re.compile(r'(\d+(?:\.\d+)?)\s*(billion|b)\b', re.I)
_thousand = re.compile(r'(\d+(?:\.\d+)?)\s*(thousand|k)\b', re.I)
_numeric = re.compile(r'^-?\d+(\.\d+)?$')

def normalize_cell_java(cell_as_string: str) -> str:
    # Mirrors the Java semantics, operating on strings
    s = str(cell_as_string)
    s2 = s.replace(",", "")
    m = _million.search(s2)
    if m:  return f"{float(m.group(1))*1_000_000:.0f}"
    m = _billion.search(s2)
    if m:  return f"{float(m.group(1))*1_000_000_000:.0f}"
    m = _thousand.search(s2)
    if m:  return f"{float(m.group(1))*1_000:.0f}"
    s3 = s2.strip()
    if _numeric.match(s3):
        v = float(s3)
        return f"{v:.2f}" if "." in s3 else f"{v:.0f}"
    return s.replace("\n", "").strip().lower()

def norm(v: Any) -> str:
    return normalize_cell_java(str(v))

# -------------- EditDistance (Java logic) --------------

def edit_distance(a: str, b: str) -> int:
    if a == b: return 0
    la, lb = len(a), len(b)
    if la == 0: return lb
    if lb == 0: return la
    if la > lb:
        a, b = b, a
        la, lb = lb, la
    prev = list(range(la+1))
    for j in range(1, lb+1):
        cj = [j] + [0]*la
        bj = b[j-1]
        for i in range(1, la+1):
            cj[i] = min(prev[i] + 1, cj[i-1] + 1, prev[i-1] + (0 if a[i-1]==bj else 1))
        prev = cj
    return prev[la]

def cells_similar(expected: str, result: str, threshold_frac: float = 0.1) -> bool:
    try:
        ev = float(expected.replace(",", "."))
        rv = float(result.replace(",", "."))
        variation = abs((ev - rv) / ev) if ev != 0 else (abs(rv) <= 0.1)
        return variation <= 0.1
    except Exception:
        pass
    thr = int(math.floor(len(expected) * threshold_frac))
    return edit_distance(expected, result) <= thr

# -------------- Cell-level Metrics --------------

def cells_set(cols: List[str], rows: List[List[Any]]) -> Set[str]:
    out: Set[str] = set()
    for r in rows:
        for v in r:
            out.add(norm(v))
    return out

def f1_cell_exact(gt_cols, gt_rows, pr_cols, pr_rows) -> float:
    gt = cells_set(gt_cols, gt_rows)
    pr = cells_set(pr_cols, pr_rows)
    if not gt and not pr: return 1.0
    if not gt or not pr:  return 0.0
    inter = gt & pr
    prec = len(inter)/len(pr)
    rec  = len(inter)/len(gt)
    return 0.0 if (prec+rec)==0 else 2*prec*rec/(prec+rec)

def f1_cell_similarity(gt_cols, gt_rows, pr_cols, pr_rows) -> float:
    gt = list(cells_set(gt_cols, gt_rows))
    pr = list(cells_set(pr_cols, pr_rows))
    if not gt and not pr: return 1.0
    if not gt or not pr:  return 0.0
    p_count = 0
    for rc in pr:
        if any(cells_similar(ec, rc) for ec in gt):
            p_count += 1
    precision = p_count / len(pr)
    r_count = 0
    for ec in gt:
        if any(cells_similar(ec, rc) for rc in pr):
            r_count += 1
    recall = r_count / len(gt)
    return 0.0 if (precision+recall)==0 else 2*precision*recall/(precision+recall)

# -------------- Tuple metrics --------------

def cardinality(gt_rows, pr_rows) -> float:
    se, sa = len(gt_rows), len(pr_rows)
    if se==0 and sa==0: return 1.0
    if se==0 or  sa==0: return 0.0
    return min(se, sa) / max(se, sa)

def _normalize_rows_full(rows: List[List[Any]]) -> List[List[str]]:
    return [[norm(v) for v in r] for r in rows]

def _sort_by_second_cell(norm_rows: List[List[str]]) -> List[List[str]]:
    return sorted(norm_rows, key=lambda r: (r[1] if len(r) > 1 else ""))

def tuple_constraint(gt_rows, pr_rows) -> float:
    gt_nr = _sort_by_second_cell(_normalize_rows_full(gt_rows))
    pr_nr = _sort_by_second_cell(_normalize_rows_full(pr_rows))
    gt_count = Counter(tuple(r) for r in gt_nr)
    pr_count = Counter(tuple(r) for r in pr_nr)
    if not gt_count and not pr_count: return 1.0
    if not gt_count:                  return 0.0
    good = 0
    for row, c in gt_count.items():
        good += 1 if pr_count.get(row, None) == c else 0
    return good / len(gt_count)

def tuple_similarity_constraint(gt_rows, pr_rows) -> float:
    gt_nr = _sort_by_second_cell(_normalize_rows_full(gt_rows))
    pr_nr = _sort_by_second_cell(_normalize_rows_full(pr_rows))
    gt_count = Counter(tuple(r) for r in gt_nr)
    pr_count = Counter(tuple(r) for r in pr_nr)

    if not gt_count and not pr_count: return 1.0
    if not gt_count:                  return 0.0

    def rows_similar(a: Tuple[str, ...], b: Tuple[str, ...]) -> bool:
        L = min(len(a), len(b))
        if L == 0: return len(a) == len(b)
        for i in range(L):
            if not cells_similar(a[i], b[i]):
                return False
        return len(a) == len(b)

    satisfied = 0
    for erow, ecnt in gt_count.items():
        found_match = False
        for prow, pcnt in pr_count.items():
            if pcnt != ecnt:
                continue
            if rows_similar(erow, prow):
                found_match = True
                break
        if found_match:
            satisfied += 1
    return satisfied / len(gt_count)

# -------------- Aggregation & CLI --------------

def fmt(x: float) -> str: return f"{x:.3f}"

def fmt_int(x: Optional[float]) -> str:
    if x is None: return ""
    try:
        if abs(x - round(x)) < 1e-9:
            return str(int(round(x)))
        return f"{x:.0f}"
    except Exception:
        return ""

def print_table(rows: List[List[str]], headers: List[str]) -> None:
    widths = [len(h) for h in headers]
    for r in rows:
        for i, c in enumerate(r):
            widths[i] = max(widths[i], len(c))
    def line(): print("+" + "+".join("-"*(w+2) for w in widths) + "+")
    def row(cells): print("| " + " | ".join(c.ljust(widths[i]) for i,c in enumerate(cells)) + " |")
    line(); row(headers); line()
    for r in rows: row(r)
    line()

def eval_dataset(gt_dir: Path, sub_dir: Path,
                 cell_mode: str, tuple_mode: str) -> Tuple[Dict[str, float], Optional[float], Optional[float]]:
    """
    Returns:
      - metrics dict: F1, Card, TCon, AVG, n
      - tokens_sum or None (None if any query missing tokens)
      - avg_time or None (None if any query missing time)
    """
    gt_files = {p.stem: p for p in gt_dir.glob("*") if p.suffix.lower() in (".csv",".json")}
    scores: List[Tuple[float,float,float,float]] = []

    tokens_sum: float = 0.0
    all_have_tokens = True
    time_values: List[float] = []
    all_have_time = True

    for qid, gt_path in sorted(gt_files.items()):
        pr_path = sub_dir / (qid + gt_path.suffix)
        if not pr_path.exists():
            alt = sub_dir / (qid + (".json" if gt_path.suffix.lower()==".csv" else ".csv"))
            pr_path = alt if alt.exists() else None

        gt_cols, gt_rows = read_table_file(gt_path)
        pr_cols, pr_rows, q_tokens, q_time = read_submission_file(pr_path)

        # cell metric
        if cell_mode == "similarity":
            f1 = f1_cell_similarity(gt_cols, gt_rows, pr_cols, pr_rows)
        else:
            f1 = f1_cell_exact(gt_cols, gt_rows, pr_cols, pr_rows)

        # cardinality
        card = cardinality(gt_rows, pr_rows)

        # tuple metric
        if tuple_mode == "similarity":
            tcon = tuple_similarity_constraint(gt_rows, pr_rows)
        else:
            tcon = tuple_constraint(gt_rows, pr_rows)

        avg = (f1 + card + tcon) / 3.0
        scores.append((f1, card, tcon, avg))

        # metrics presence checks (strict: all queries must provide)
        if q_tokens is None:
            all_have_tokens = False
        else:
            tokens_sum += q_tokens

        if q_time is None:
            all_have_time = False
        else:
            time_values.append(q_time)

    n = len(scores)
    if n == 0:
        metrics = dict(F1=0.0, Card=0.0, TCon=0.0, AVG=0.0, n=0)
    else:
        metrics = dict(
            F1 = sum(s[0] for s in scores)/n,
            Card = sum(s[1] for s in scores)/n,
            TCon = sum(s[2] for s in scores)/n,
            AVG = sum(s[3] for s in scores)/n,
            n = n
        )

    dataset_tokens = (tokens_sum if (n > 0 and all_have_tokens) else None)
    dataset_avg_time = ((sum(time_values)/len(time_values)) if (n > 0 and all_have_time) else None)

    return metrics, dataset_tokens, dataset_avg_time

def find_dataset_dirs(root: Path, allow: Optional[Set[str]]) -> Dict[str, Path]:
    out: Dict[str, Path] = {}
    for p in root.iterdir():
        if p.is_dir():
            name = p.name
            if allow and name not in allow: continue
            out[name] = p
    return out

# -------- LaTeX output --------

def _latex_escape(s: str) -> str:
    rep = {
        '&': r'\&', '%': r'\%', '$': r'\$', '#': r'\#',
        '_': r'\_', '{': r'\{', '}': r'\}', '~': r'\textasciitilde{}',
        '^': r'\textasciicircum{}', '\\': r'\textbackslash{}',
    }
    return ''.join(rep.get(ch, ch) for ch in s)

def print_latex_table(rows: List[List[str]], headers: List[str],
                      caption: Optional[str] = None,
                      label: Optional[str] = None,
                      booktabs: bool = False) -> None:
    align = "l" + "r" * (len(headers) - 1)
    esc = _latex_escape
    out = []

    wrap = bool(caption or label)
    if wrap:
        out.append(r"\begin{table}[!ht]")
        out.append(r"\centering")

    out.append(rf"\begin{{tabular}}{{{align}}}")

    if booktabs:
        out.append(r"\toprule")
        out.append(" & ".join(esc(h) for h in headers) + r" \\")
        out.append(r"\midrule")
    else:
        out.append(r"\hline")
        out.append(" & ".join(esc(h) for h in headers) + r" \\")
        out.append(r"\hline")

    for r in rows:
        out.append(" & ".join(esc(c) for c in r) + r" \\")

    if booktabs:
        out.append(r"\bottomrule")
    else:
        out.append(r"\hline")

    out.append(r"\end{tabular}")

    if caption:
        out.append(rf"\caption{{{esc(caption)}}}")
    if label:
        out.append(rf"\label{{{esc(label)}}}")

    if wrap:
        out.append(r"\end{table}")

    print("\n".join(out))

def main():
    ap = argparse.ArgumentParser(description="GALOIS metrics (Java-faithful), per dataset, with per-query FULL/SIMPLE JSON input for tokens/time.")
    ap.add_argument("--ground", required=True, type=Path)
    ap.add_argument("--submissions", required=True, type=Path)
    ap.add_argument("--datasets", nargs="*", default=None, help="Datasets to evaluate (default: all)")
    ap.add_argument("--cell-metric", choices=["exact","similarity"], default="exact",
                    help="Cell F1: exact (CellF1Score) or similarity (CellSimilarityF1Score)")
    ap.add_argument("--tuple-metric", choices=["constraint","similarity"], default="constraint",
                    help="Tuple metric: TupleConstraint or TupleSimilarityConstraint")
    ap.add_argument("--format", choices=["table","csv","json","tex"], default="table")
    ap.add_argument("--latex-caption", default=None)
    ap.add_argument("--latex-label", default=None)
    ap.add_argument("--latex-booktabs", action="store_true")
    ap.add_argument("--overall", action="store_true", help="Aggregate across all selected datasets and print a single ALL row (Exp-1 style)")
    args = ap.parse_args()

    allow = set(args.datasets) if args.datasets else None
    gt_dsets  = find_dataset_dirs(args.ground, allow)
    sub_dsets = find_dataset_dirs(args.submissions, allow)
    if not gt_dsets:
        print("No datasets found under --ground", file=sys.stderr); sys.exit(2)

    headers = ["Dataset","F1-Cell","Cardinality","Tuple Constr.","AVG-Score","#Queries","#Tokens","Avg Time (s)"]

    rows: List[List[str]] = []
    jout: Dict[str, Dict[str, Any]] = {}

    tot_q = 0
    sF1 = sCard = sTCon = sAVG = 0.0

    # For ALL-row strict blanking logic:
    all_datasets_have_tokens = True
    all_datasets_have_time   = True
    overall_tokens_sum: float = 0.0
    overall_time_weighted_sum: float = 0.0  # weight by #queries
    overall_time_weight_n: int = 0

    for dname, gt_dir in sorted(gt_dsets.items()):
        sub_dir = sub_dsets.get(dname)
        if not sub_dir:
            m = dict(F1=0.0, Card=0.0, TCon=0.0, AVG=0.0, n=0)
            d_tokens = None
            d_avg_time = None
        else:
            m, d_tokens, d_avg_time = eval_dataset(gt_dir, sub_dir, args.cell_metric, args.tuple_metric)

        # accumulate core metrics (weighted by #queries)
        tot_q += m["n"]
        sF1   += m["F1"]  * m["n"]
        sCard += m["Card"]* m["n"]
        sTCon += m["TCon"]* m["n"]
        sAVG  += m["AVG"] * m["n"]

        # ALL-row strict logic
        if d_tokens is None:
            all_datasets_have_tokens = False
        else:
            overall_tokens_sum += d_tokens
        if d_avg_time is None or m["n"] == 0:
            all_datasets_have_time = False
        else:
            overall_time_weighted_sum += d_avg_time * m["n"]
            overall_time_weight_n     += m["n"]

        # per-dataset output (unless --overall)
        if not args.overall:
            jout[dname] = {
                "F1-Cell": m["F1"], "Cardinality": m["Card"], "Tuple-Constraint": m["TCon"],
                "AVG-Score": m["AVG"], "#Queries": m["n"],
                "#Tokens": (d_tokens if d_tokens is not None else None),
                "Avg-Time": (d_avg_time if d_avg_time is not None else None),
            }
            rows.append([
                dname,
                fmt(m["F1"]), fmt(m["Card"]), fmt(m["TCon"]), fmt(m["AVG"]), str(m["n"]),
                fmt_int(d_tokens), (fmt(d_avg_time) if d_avg_time is not None else "")
            ])

    # ---- OUTPUT ----
    if args.overall:
        if tot_q == 0:
            overall = dict(F1=0.0, Card=0.0, TCon=0.0, AVG=0.0, n=0)
        else:
            overall = dict(F1=sF1/tot_q, Card=sCard/tot_q, TCon=sTCon/tot_q, AVG=sAVG/tot_q, n=tot_q)

        overall_tokens = (overall_tokens_sum if all_datasets_have_tokens and tot_q>0 else None)
        overall_avg_time = ((overall_time_weighted_sum/overall_time_weight_n)
                            if all_datasets_have_time and overall_time_weight_n>0 else None)

        if args.format == "json":
            print(json.dumps({
                "ALL": {
                    "F1-Cell": overall["F1"], "Cardinality": overall["Card"],
                    "Tuple-Constraint": overall["TCon"], "AVG-Score": overall["AVG"],
                    "#Queries": overall["n"],
                    "#Tokens": overall_tokens,
                    "Avg-Time": overall_avg_time
                }
            }, indent=2))
            return
        elif args.format == "csv":
            w = csv.writer(sys.stdout); 
            w.writerow(headers)
            w.writerow([
                "ALL", fmt(overall["F1"]), fmt(overall["Card"]), fmt(overall["TCon"]),
                fmt(overall["AVG"]), str(overall["n"]),
                fmt_int(overall_tokens), (fmt(overall_avg_time) if overall_avg_time is not None else "")
            ])
            return
        elif args.format == "tex":
            print_latex_table(
                [[
                    "ALL", fmt(overall["F1"]), fmt(overall["Card"]), fmt(overall["TCon"]),
                    fmt(overall["AVG"]), str(overall["n"]),
                    fmt_int(overall_tokens), (fmt(overall_avg_time) if overall_avg_time is not None else "")
                ]],
                headers, caption=args.latex_caption, label=args.latex_label, booktabs=args.latex_booktabs
            )
            return
        else:
            print_table([[
                "ALL", fmt(overall["F1"]), fmt(overall["Card"]), fmt(overall["TCon"]),
                fmt(overall["AVG"]), str(overall["n"]),
                fmt_int(overall_tokens), (fmt(overall_avg_time) if overall_avg_time is not None else "")
            ]], headers)
            return

    else:
        if args.format == "json":
            print(json.dumps(jout, indent=2))
        elif args.format == "csv":
            w = csv.writer(sys.stdout); w.writerow(headers)
            for r in rows: w.writerow(r)
        elif args.format == "tex":
            print_latex_table(rows, headers, caption=args.latex_caption, label=args.latex_label, booktabs=args.latex_booktabs)
        else:
            print_table(rows, headers)

if __name__ == "__main__":
    main()
