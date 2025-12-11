import json
import re
import statistics
import subprocess
from collections import defaultdict
from pathlib import Path
from src.utils import PY, DATA_DIR, GROUND_PATH, IK_DATASETS
from src.config import Config_Loader
from src.db import load_queries_from_folder
from src.galois.galois import Galois, save_galois_results
import matplotlib.pyplot as plt
from src.utils import LOG
from src.utils.sql_query_parser import parse_sql
from src.utils.tutor.galois_eval import f1_cell_exact, cardinality, tuple_constraint


# ... import config loader ...


def classify_query_complexity(sql_query: str) -> str:
    """
    Classifies a SQL query into one of the categories defined in the paper (Exp-5).
    """
    try:
        parsed = parse_sql(sql_query)
    except:
        return "UNKNOWN"

    #  JOIN
    if len(parsed.get('joins', [])) > 0:
        return "JOIN"

    #  AGGREGATE (AGGR)
    cols = parsed.get('select_columns', [])
    # Check for aggregation keywords in select columns
    is_aggr = any(kw in str(col).upper() for col in cols for kw in ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN'])
    if is_aggr:
        return "AGGR"

    #  GROUP BY / ORDER BY (G/O)
    if parsed.get('group_by_columns') or parsed.get('order_by_clauses'):
        return "G/O"

    #  DISTINCT (DIST)
    # Check original query string for DISTINCT
    if "DISTINCT" in parsed.get('original_query', '').upper():
        return "DIST"

    #  SP2 vs SP2> (Selection Projection)
    num_conditions = len(parsed.get('where_conditions', []))
    if num_conditions > 2:
        return "SP2>"
    else:
        return "SP2"

def save_summary_for_plot(results_by_category):
    """
    Salva i punteggi medi per categoria in un file JSON
    che può essere letto direttamente dal Jupyter Notebook.
    """
    ordered_cats = ["SP2", "SP2>", "DIST", "AGGR", "G/O", "JOIN"]
    summary_data = {}

    for cat in ordered_cats:
        scores = results_by_category.get(cat, [])
        if scores:
            # Calcola la media
            avg = statistics.mean(scores)
            summary_data[cat] = avg
        else:
            # Se non ci sono query per questa categoria, metti 0
            summary_data[cat] = 0.0

    # Salva nella root del progetto
    output_file = Path(__file__).resolve().parent / "exp5_summary.json"
    try:
        with open(output_file, "w") as f:
            json.dump(summary_data, f, indent=4)
        print(f"\n[AUTO] Dati per il grafico salvati in: {output_file.absolute()}")
    except Exception as e:
        LOG.error(f"Impossibile salvare i dati di riepilogo: {e}")



def print_summary_table(results_by_category):
    print("\n" + "=" * 60)
    print(f"   RESULTS SUMMARY - EXP 5 (Dataset: ALL DATASETS)")
    print("=" * 50)
    print(f"{'COMPLEXITY':<12} | {'AVG SCORE':<12} | {'COUNT':<12}")
    print("-" * 46)

    # Order categories by complexity logic
    ordered_cats = ["SP2", "SP2>", "DIST", "AGGR", "G/O", "JOIN"]

    # Add any unexpected categories found at runtime
    found_cats = set(results_by_category.keys())
    for c in found_cats:
        if c not in ordered_cats:
            ordered_cats.append(c)

    for cat in ordered_cats:
        scores = results_by_category.get(cat, [])
        if scores:
            avg = statistics.mean(scores)
            count = len(scores)
            print(f"{cat:<12} | {avg:<12.4f} | {count:<12}")
        else:
            print(f"{cat:<12} | {'0.0000':<12} | {'0':<12}")
    print("=" * 60)


class Exp5Runner:
    def __init__(self, dataset_name: str, shared_results_dict: dict):
        self.config = Config_Loader().get_config()
        self.dataset_name = dataset_name.upper()

        # Dictionary to accumulate scores
        self.results_by_category = shared_results_dict

    def get_ground_truth_from_json(self, query_id):
        """
        Reads the Ground Truth from the JSON file provided by the professor.
        Expects files like: data/GEO/query1.json
        Expected format: List[Dict], e.g.: [{"col1": val1}, {"col1": val2}]
        """
        # Build path: data/GEO/query1.json
        json_path = GROUND_PATH / self.dataset_name / f"{query_id}.json"

        if not json_path.exists():
            LOG.warning(f"Ground Truth file missing: {json_path}")
            return [], []

        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if not data:
                return [], []

            # 1. Extract columns (keys of the first dict)
            # Assume all dicts have the same keys
            cols = list(data[0].keys())

            # 2. Extract rows and convert EVERYTHING to string
            # (galois_eval expects lists of strings to perform comparisons)
            clean_rows = []
            for entry in data:
                row = []
                for col in cols:
                    val = entry.get(col)
                    # Handle None -> empty string, Other -> string
                    row.append(str(val) if val is not None else "")
                clean_rows.append(row)

            return cols, clean_rows

        except Exception as e:
            LOG.error(f"Error reading GT JSON {json_path}: {e}")
            return [], []

    def run(self):
        #log_init()
        print(f"==================================================")
        print(f"   STARTING EXP 5: COMPLEXITY ANALYSIS ({self.dataset_name})")
        print(f"   Using JSON Ground Truth from: {GROUND_PATH}")
        print(f"==================================================")

        # 1. Load SQL queries
        dataset_path = DATA_DIR / self.dataset_name
        queries = load_queries_from_folder(dataset_path)

        dataset_detailed_json = []

        for i, (source_file, raw_sql) in enumerate(queries):
            sql_clean = re.sub(r'--query\d+', '', raw_sql)
            sql = sql_clean.strip()
            # filename is like `query1.sql`, stem is `query1`
            query_id = f"query{i+1}"
            #LOG.debug(f"Processing query {query_id} (from file: {source_file})")
            #LOG.debug(f"SQL Content: {sql}")
            LOG.debug(f"Running query {i+1}")

            # 2. CLASSIFICATION (Join, Aggregates, etc.)
            category = classify_query_complexity(sql)
            print(f"\n>> Running [{self.dataset_name}] {query_id} | Type: [{category}]")

            try:
                # 3. EXECUTE GALOIS F (Push Confident)
                # physical_strategy="auto" enables automatic choice Table vs Key scan
                galois = Galois(self.config, self.dataset_name, sql, physical_strategy="auto")

                # Execute the logic
                predicted_dicts, stats = galois.run_push_confident()

                # Convert Galois output (List[Dict]) -> List[List[str]] for the evaluator
                if predicted_dicts:
                    pred_cols = list(predicted_dicts[0].keys())
                    pred_rows = [[str(row.get(c, "")) for c in pred_cols] for row in predicted_dicts]
                else:
                    pred_cols = []
                    pred_rows = []

                # 4. FETCH GROUND TRUTH (From JSON)
                gt_cols, gt_rows = self.get_ground_truth_from_json(query_id)

                # 5. COMPUTE METRICS (Using professor's functions)
                # Note: f1_cell_exact internally handles string normalization
                f1 = f1_cell_exact(gt_cols, gt_rows, pred_cols, pred_rows)
                card = cardinality(gt_rows, pred_rows)
                tcon = tuple_constraint(gt_rows, pred_rows)

                # Compute AVG-SCORE
                avg_score = (f1 + card + tcon) / 3.0

                # Accumulate by category
                self.results_by_category[category].append(avg_score)

                print(f"   [Score: {avg_score:.4f}] F1: {f1:.2f} | Card: {card:.2f} | TCon: {tcon:.2f}")

                # Accumulate data for the output file
                dataset_detailed_json.append({
                    "query_id": str(i+1),
                    "complexity": category,
                    "result_set": {"columns": pred_cols, "rows": pred_rows},
                    "tokens": stats.get('total_tokens', 0),
                    "time": stats.get('total_time', 0)
                })

            except Exception as e:
                LOG.error(f"Failed query {query_id}: {e}", exc_info=True)
                # Penalize failed queries with score 0
                self.results_by_category[category].append(0.0)

        #  Save detailed results to disk
        try:
            # "EXP5" will be the variant name in the output folder
            save_path = save_galois_results(dataset_detailed_json, "EXP5", "openai", self.dataset_name)
            print(f"\nDetailed JSON results saved to: {save_path}")
        except Exception as e:
            LOG.error(f"Could not save JSON results: {e}")




if __name__ == "__main__":
    # 1. Creiamo il dizionario accumulatore GLOBALE
    global_results = defaultdict(list)

    # 2. Iteriamo sui dataset
    for dataset in IK_DATASETS:
        # Passiamo l'accumulatore globale al runner
        runner = Exp5Runner(dataset_name=dataset, shared_results_dict=global_results)
        runner.run()

    # 3. Solo alla fine di TUTTI i dataset, stampiamo e salviamo il riepilogo totale
    print("\n--- ELABORAZIONE TERMINATA SU TUTTI I DATASET ---")
    print_summary_table(global_results)
    save_summary_for_plot(global_results)