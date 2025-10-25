#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GALOIS metrics evaluator (Java-faithful) — optimized

Key speedups:
- LRU caches for normalization, edit distance, and cell-similarity
- Faster F1-cell(sililarity): numeric matching via binary search; string matching via bucketed candidates
- Faster tuple similarity: bucket by (row length, multiplicity) + cached per-cell similarity
- Optional multiprocessing across datasets: --jobs N
- Optional fast JSON via orjson (if installed)

CLI is compatible with the original, with one addition:
  --jobs N     (default 1) run datasets in parallel
"""
from __future__ import annotations
import argparse, csv, json, math, re, sys
from pathlib import Path
from typing import Any, Dict, List, Tuple, Set, Optional, Iterable
from collections import Counter, defaultdict
from functools import lru_cache
from bisect import bisect_left, bisect_right
from concurrent.futures import ProcessPoolExecutor, as_completed


def _eval_query_once(args):
    # args: (qid, gt_path_str, sub_dir_str_or_none, gt_suffix, cell_mode, tuple_mode)
    qid, gt_path_str, sub_dir_str, gt_sfx, cell_mode, tuple_mode = args
    gt_path = Path(gt_path_str)
    sub_dir = (Path(sub_dir_str) if sub_dir_str else None)

    pr_path = (sub_dir / (qid + gt_sfx)) if sub_dir else None
    if pr_path and not pr_path.exists():
        alt = sub_dir / (qid + (".json" if gt_sfx.lower()==".csv" else ".csv"))
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
    return (f1, card, tcon, avg, q_tokens, q_time)

# -------- Optional fast JSON loader --------
def _json_load_fast(path: Path) -> Any:
    try:
        import orjson  # type: ignore
        return orjson.loads(path.read_bytes())
    except Exception:
        return json.load(path.open("r", encoding="utf-8"))

# ---------------- I/O ----------------
def _read_csv(path: Path) -> Tuple[List[str], List[List[Any]]]:
    rows = list(csv.reader(path.open("r", encoding="utf-8-sig", newline="")))
    if not rows: return [], []
    cols = [c.strip() for c in rows[0]]
    W = len(cols)
    out = []
    for r in rows[1:]:
        if len(r) < W: r = r + [""]*(W-len(r))
        out.append(r[:W])
    return cols, out

def _parse_table_from_obj(obj: Any) -> Tuple[List[str], List[List[Any]]]:
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
    obj = _json_load_fast(path)
    if isinstance(obj, dict) and "result_set" in obj:
        return _parse_table_from_obj(obj["result_set"])
    return _parse_table_from_obj(obj)

def read_table_file(path: Path) -> Tuple[List[str], List[List[Any]]]:
    s = path.suffix.lower()
    if s == ".csv":  return _read_csv(path)
    if s == ".json": return _read_json_table_only(path)
    raise ValueError(f"Unsupported file type: {path}")

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
    if not path or not path.exists():
        return [], [], None, None
    s = path.suffix.lower()
    if s == ".csv":
        cols, rows = _read_csv(path); return cols, rows, None, None
    if s == ".json":
        obj = _json_load_fast(path)
        if isinstance(obj, dict) and "result_set" in obj:
            cols, rows = _parse_table_from_obj(obj["result_set"])
            tokens = _to_float(obj.get("tokens"))
            time_s = _to_float(obj.get("time"))
            return cols, rows, tokens, time_s
        cols, rows = _parse_table_from_obj(obj)
        return cols, rows, None, None
    raise ValueError(f"Unsupported file type: {path}")

# -------------- Normalization (Java-like) --------------
_million = re.compile(r'(\d+(?:\.\d+)?)\s*(million|m)\b', re.I)
_billion = re.compile(r'(\d+(?:\.\d+)?)\s*(billion|b)\b', re.I)
_thousand = re.compile(r'(\d+(?:\.\d+)?)\s*(thousand|k)\b', re.I)
_numeric = re.compile(r'^-?\d+(\.\d+)?$')

@lru_cache(maxsize=200_000)
def _normalize_string(cell_as_string: str) -> str:
    s2 = cell_as_string.replace(",", "")
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
    return cell_as_string.replace("\n", "").strip().lower()

def norm(v: Any) -> str:
    return _normalize_string(str(v))

# -------------- Edit distance + similarity (cached) --------------
try:
    # optional speedup if installed
    from Levenshtein import distance as _lev_distance  # type: ignore
    def _edit_distance(a: str, b: str) -> int:
        return _lev_distance(a, b)
except Exception:
    @lru_cache(maxsize=200_000)
    def _edit_distance(a: str, b: str) -> int:
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
    
try:
    from rapidfuzz.distance import Levenshtein as RFLev  # pip install rapidfuzz
    def _lev_leq_k(a: str, b: str, k: int) -> bool:
        if abs(len(a) - len(b)) > k:
            return False
        # score_cutoff ensures we don't compute distance > k
        d = RFLev.distance(a, b, score_cutoff=k+1)
        return d <= k
except Exception:
    # Fallback: compute full distance (possibly accelerated by python-Levenshtein if present)
    def _lev_leq_k(a: str, b: str, k: int) -> bool:
        if abs(len(a) - len(b)) > k:
            return False
        return _edit_distance(a, b) <= k

@lru_cache(maxsize=500_000)
def _cells_similar_default(a: str, b: str) -> bool:
    # numeric branch (±10%)
    try:
        ev = float(a.replace(",", "."))
        rv = float(b.replace(",", "."))
        if ev == 0.0:
            return abs(rv) <= 0.1
        return abs((ev - rv) / ev) <= 0.1
    except Exception:
        pass

    # string branch: exact 10% edit-distance rule with safe length pre-check + early-exit DP
    k = int(math.floor(len(a) * 0.10))
    if k < 0:
        return False
    if abs(len(a) - len(b)) > k:
        return False
    return _lev_leq_k(a, b, k)


def cells_similar(expected: str, result: str, threshold_frac: float = 0.1) -> bool:
    if threshold_frac == 0.1:
        return _cells_similar_default(expected, result)
    # rarely used path; uncached to avoid cache bloat
    try:
        ev = float(expected.replace(",", "."))
        rv = float(result.replace(",", "."))
        if ev == 0.0:
            return abs(rv) <= 0.1
        return abs((ev - rv) / ev) <= threshold_frac
    except Exception:
        pass
    thr = int(math.floor(len(expected) * threshold_frac))
    return _edit_distance(expected, result) <= thr

# -------------- Cell-level metrics --------------
def cells_set(cols: List[str], rows: List[List[Any]]) -> Set[str]:
    # Set of normalized string values across all cells
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
    if gt == pr: return 1.0
    inter = len(gt & pr)
    prec = inter/len(pr)
    rec  = inter/len(gt)
    return 0.0 if (prec+rec)==0 else 2*prec*rec/(prec+rec)

def _partition_numeric(strings: Iterable[str]) -> Tuple[List[float], List[str]]:
    nums: List[float] = []
    txts: List[str] = []
    for s in strings:
        try:
            nums.append(float(s.replace(",", ".")))
        except Exception:
            txts.append(s)
    return nums, txts

def _count_numeric_matches(pr_nums_sorted: List[float], gt_nums_sorted: List[float]) -> Tuple[int,int]:
    # Count how many pr numbers have a gt within ±10% (precision count),
    # and how many gt numbers have a pr within ±10% (recall count).
    def has_within(sorted_vals: List[float], x: float) -> bool:
        if x == 0.0:
            # accept |y| <= 0.1
            lo, hi = -0.1, 0.1
        else:
            lo = x/1.1
            hi = x*1.1
        L = bisect_left(sorted_vals, lo)
        R = bisect_right(sorted_vals, hi)
        return R > L

    p_count = sum(1 for x in pr_nums_sorted if has_within(gt_nums_sorted, x))
    r_count = sum(1 for x in gt_nums_sorted if has_within(pr_nums_sorted, x))
    return p_count, r_count

def _build_string_buckets(values: Iterable[str]) -> Dict[Tuple[str,int], List[str]]:
    # bucket by (first_char_or_empty, length_bin)
    buckets: Dict[Tuple[str,int], List[str]] = defaultdict(list)
    for s in values:
        first = s[0] if s else ""
        buckets[(first, len(s)//2)].append(s)
    return buckets

def f1_cell_similarity(gt_cols, gt_rows, pr_cols, pr_rows) -> float:
    # Java-faithful semantics (same as your original), but fast via cached _cells_similar_default
    gt = list(cells_set(gt_cols, gt_rows))
    pr = list(cells_set(pr_cols, pr_rows))
    if not gt and not pr: return 1.0
    if not gt or not pr:  return 0.0

    # precision: for each predicted cell, does ANY expected cell match (±10% numeric or edit distance threshold)?
    p_count = 0
    for rc in pr:
        if any(_cells_similar_default(ec, rc) for ec in gt):
            p_count += 1
    precision = p_count / len(pr)

    # recall: for each expected cell, does ANY predicted cell match?
    r_count = 0
    for ec in gt:
        if any(_cells_similar_default(ec, rc) for rc in pr):
            r_count += 1
    recall = r_count / len(gt)

    return 0.0 if (precision + recall) == 0 else 2 * precision * recall / (precision + recall)


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
    gt_nr = _normalize_rows_full(gt_rows)
    pr_nr = _normalize_rows_full(pr_rows)

    # multiplicity counters on tuples (order-sensitive)
    gt_count = Counter(tuple(r) for r in gt_nr)
    pr_count = Counter(tuple(r) for r in pr_nr)
    if not gt_count and not pr_count: return 1.0
    if not gt_count:                  return 0.0

    @lru_cache(maxsize=200_000)
    def rows_similar_cached(a: Tuple[str, ...], b: Tuple[str, ...]) -> bool:
        if len(a) != len(b): return False
        for i in range(len(a)):
            ai, bi = a[i], b[i]
            if ai == bi:
                continue
            if not _cells_similar_default(ai, bi):
                return False
        return True

    # bucket by (len(row), multiplicity)
    buckets: Dict[Tuple[int,int], List[Tuple[str,...]]] = defaultdict(list)
    for prow, pcnt in pr_count.items():
        buckets[(len(prow), pcnt)].append(prow)

    satisfied = 0
    for erow, ecnt in gt_count.items():
        candidates = buckets.get((len(erow), ecnt), [])
        found = False
        for prow in candidates:
            if rows_similar_cached(tuple(erow), tuple(prow)):
                found = True
                break
        if found:
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

def _eval_dataset_once(args):
    # args: (dname, gt_dir, sub_dir, cell_mode, tuple_mode, jobs_queries)
    dname, gt_dir, sub_dir, cell_mode, tuple_mode, jobs_queries = args

    if not sub_dir:
        m = dict(F1=0.0, Card=0.0, TCon=0.0, AVG=0.0, n=0)
        return dname, m, None, None

    gt_files = {p.stem: p for p in gt_dir.glob("*") if p.suffix.lower() in (".csv",".json")}
    tasks = []
    for qid, gt_path in sorted(gt_files.items()):
        tasks.append((qid, str(gt_path), str(sub_dir), gt_path.suffix, cell_mode, tuple_mode))

    results = []
    if jobs_queries > 1 and len(tasks) > 1:
        with ProcessPoolExecutor(max_workers=jobs_queries) as ex:
            futs = [ex.submit(_eval_query_once, t) for t in tasks]
            for f in as_completed(futs):
                results.append(f.result())
    else:
        for t in tasks:
            results.append(_eval_query_once(t))

    n = len(results)
    if n == 0:
        metrics = dict(F1=0.0, Card=0.0, TCon=0.0, AVG=0.0, n=0)
        return dname, metrics, None, None

    sF1 = sum(x[0] for x in results)
    sCard = sum(x[1] for x in results)
    sTCon = sum(x[2] for x in results)
    sAVG = sum(x[3] for x in results)

    # tokens/time strict policy (only if ALL queries provide it)
    tokens_list = [x[4] for x in results]
    time_list   = [x[5] for x in results]

    all_have_tokens = all(t is not None for t in tokens_list)
    all_have_time   = all(t is not None for t in time_list)

    d_tokens = (sum(tokens_list) if all_have_tokens else None)
    d_avg_time = ((sum(time_list)/len(time_list)) if all_have_time else None)

    metrics = dict(
        F1   = sF1/n,
        Card = sCard/n,
        TCon = sTCon/n,
        AVG  = sAVG/n,
        n    = n
    )
    return dname, metrics, d_tokens, d_avg_time


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
    ap = argparse.ArgumentParser(description="GALOIS metrics (Java-faithful), per dataset, optimized.")
    ap.add_argument("--ground", required=True, type=Path)
    ap.add_argument("--submissions", required=True, type=Path)
    ap.add_argument("--datasets", nargs="*", default=None, help="Datasets to evaluate (default: all)")
    ap.add_argument("--cell-metric", choices=["exact","similarity"], default="exact",
                    help="Cell F1: exact or similarity")
    ap.add_argument("--tuple-metric", choices=["constraint","similarity"], default="constraint",
                    help="Tuple metric")
    ap.add_argument("--format", choices=["table","csv","json","tex"], default="table")
    ap.add_argument("--latex-caption", default=None)
    ap.add_argument("--latex-label", default=None)
    ap.add_argument("--latex-booktabs", action="store_true")
    ap.add_argument("--overall", action="store_true", help="Aggregate across all selected datasets and print a single ALL row")
    ap.add_argument("--jobs", type=int, default=6, help="Parallelize across datasets (processes)")
    ap.add_argument("--jobs-queries", type=int, default=6, help="Parallelize queries within each dataset")
    args = ap.parse_args()

    allow = set(args.datasets) if args.datasets else None
    gt_dsets  = find_dataset_dirs(args.ground, allow)
    sub_dsets = find_dataset_dirs(args.submissions, allow)
    if not gt_dsets:
        print("No datasets found under --ground", file=sys.stderr); sys.exit(2)

    headers = ["Dataset","F1-Cell","Cardinality","Tuple Constr.","AVG-Score","#Queries","#Tokens","Avg Time (s)"]
    rows: List[List[str]] = []
    jout: Dict[str, Dict[str, Any]] = {}

    # Evaluate (optionally in parallel) one dataset at a time
    tasks = []
    for dname, gt_dir in sorted(gt_dsets.items()):
        sub_dir = sub_dsets.get(dname)
        tasks.append((dname, gt_dir, sub_dir, args.cell_metric, args.tuple_metric, args.jobs_queries))


    results = []
    if args.jobs > 1 and len(tasks) > 1:
        with ProcessPoolExecutor(max_workers=args.jobs) as ex:
            fut2name = {ex.submit(_eval_dataset_once, t): t[0] for t in tasks}
            for fut in as_completed(fut2name):
                results.append(fut.result())
    else:
        for t in tasks:
            results.append(_eval_dataset_once(t))

    # Accumulate
    tot_q = 0
    sF1 = sCard = sTCon = sAVG = 0.0
    all_datasets_have_tokens = True
    all_datasets_have_time   = True
    overall_tokens_sum: float = 0.0
    overall_time_weighted_sum: float = 0.0
    overall_time_weight_n: int = 0

    for dname, m, d_tokens, d_avg_time in sorted(results, key=lambda x: x[0]):
        tot_q += m["n"]
        sF1   += m["F1"]  * m["n"]
        sCard += m["Card"]* m["n"]
        sTCon += m["TCon"]* m["n"]
        sAVG  += m["AVG"] * m["n"]

        if d_tokens is None:
            all_datasets_have_tokens = False
        else:
            overall_tokens_sum += d_tokens

        if d_avg_time is None or m["n"] == 0:
            all_datasets_have_time = False
        else:
            overall_time_weighted_sum += d_avg_time * m["n"]
            overall_time_weight_n     += m["n"]

        if not args.overall:
            jout[dname] = {
                "F1-Cell": m["F1"], "Cardinality": m["Card"], "Tuple-Constraint": m["TCon"],
                "AVG-Score": m["AVG"], "#Queries": m["n"],
                "#Tokens": (d_tokens if d_tokens is not None else None),
                "Avg-Time": (d_avg_time if d_avg_time is not None else None),
            }
            rows.append([
                dname, fmt(m["F1"]), fmt(m["Card"]), fmt(m["TCon"]), fmt(m["AVG"]), str(m["n"]),
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
            }, indent=2)); return
        elif args.format == "csv":
            w = csv.writer(sys.stdout); 
            w.writerow(headers)
            w.writerow([
                "ALL", fmt(overall["F1"]), fmt(overall["Card"]), fmt(overall["TCon"]),
                fmt(overall["AVG"]), str(overall["n"]),
                fmt_int(overall_tokens), (fmt(overall_avg_time) if overall_avg_time is not None else "")
            ]); return
        elif args.format == "tex":
            print_latex_table(
                [[
                    "ALL", fmt(overall["F1"]), fmt(overall["Card"]), fmt(overall["TCon"]),
                    fmt(overall["AVG"]), str(overall["n"]),
                    fmt_int(overall_tokens), (fmt(overall_avg_time) if overall_avg_time is not None else "")
                ]],
                headers, caption=args.latex_caption, label=args.latex_label, booktabs=args.latex_booktabs
            ); return
        else:
            print_table([[
                "ALL", fmt(overall["F1"]), fmt(overall["Card"]), fmt(overall["TCon"]),
                fmt(overall["AVG"]), str(overall["n"]),
                fmt_int(overall_tokens), (fmt(overall_avg_time) if overall_avg_time is not None else "")
            ]], headers); return
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
