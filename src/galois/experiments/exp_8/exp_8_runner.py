import sys
import json
import re
import statistics
from pathlib import Path
from collections import defaultdict

sys.path.append(str(Path(__file__).parent))

from src.config import Config_Loader
from src.galois.galois import Galois, save_galois_results
from src.utils.logging_config import log_init, LOG
from src.utils import load_queries_from_folder, GROUND_PATH
from src.utils.tutor.galois_eval import f1_cell_exact, cardinality, tuple_constraint


class Exp8Runner:
    def __init__(self):
        self.config = Config_Loader().get_config()
        # GLOBAL accumulators (to compute the "ALL" average as in the paper)
        self.global_scores = {
            "GALOIS_A": [],
            "GALOIS_F": [],
            "OPTIMAL": []
        }
        self.final_plot_data= []

    def get_ground_truth(self, dataset_name, query_id):
        json_path = GROUND_PATH / dataset_name / f"{query_id}.json"
        if not json_path.exists(): return [], []
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if not data: return [], []
            cols = list(data[0].keys())
            clean_rows = [[str(entry.get(c, "") if entry.get(c) is not None else "") for c in cols] for entry in data]
            return cols, clean_rows
        except:
            return [], []

    def calculate_score(self, pred_data, gt_cols, gt_rows):
        if not pred_data: return 1.0 if not gt_rows else 0.0
        pred_cols = list(pred_data[0].keys())
        pred_rows = [[str(row.get(c, "")) for c in pred_cols] for row in pred_data]
        f1 = f1_cell_exact(gt_cols, gt_rows, pred_cols, pred_rows)
        card = cardinality(gt_rows, pred_rows)
        tcon = tuple_constraint(gt_rows, pred_rows)
        return (f1 + card + tcon) / 3.0

    def run_dataset(self, dataset_name):
        dataset_name = dataset_name.upper()
        dataset_path = Path(f"data/{dataset_name}")

        if not dataset_path.exists():
            print(f"Skipping {dataset_name} (Not found)")
            return

        print(f"\n" + "=" * 60)
        print(f"   PROCESSING DATASET: {dataset_name}")
        print(f"=" * 60)

        queries = load_queries_from_folder(dataset_path)

        # Score locali per questo dataset
        local_scores_f = []
        local_scores_opt = []
        local_scores_a = []
        #list for saving the details in JSON in disk
        dataset_detailed_json = []

        for i, item in enumerate(queries):
            sql = re.sub(r'--query\d+', '', item).strip()
            # Unique global ID for the report (e.g., GEO_query1)
            query_id_short = f"query{i + 1}"

            gt_cols, gt_rows = self.get_ground_truth(dataset_name, query_id_short)
            if not gt_rows and not gt_cols:
                print("   Skipping (GT not found)")
                continue

            # --- BRUTE FORCE (Optimal) ---
            try:
                g_ta = Galois(self.config, dataset_name, sql, physical_strategy="table")
                res_ta, _ = g_ta.run_push_all()
                score_a = self.calculate_score(res_ta, gt_cols, gt_rows)
            except:
                score_a = 0.0

            try:
                g_kn = Galois(self.config, dataset_name, sql, physical_strategy="key")
                res_kn, _ = g_kn.run_no_push()
                score_wo = self.calculate_score(res_kn, gt_cols, gt_rows)
            except:
                score_wo = 0.0

            # --- GALOIS F (Automatic) ---
            try:
                g_auto = Galois(self.config, dataset_name, sql, physical_strategy="auto")
                res_auto, _ = g_auto.run_push_confident()
                score_f = self.calculate_score(res_auto, gt_cols, gt_rows)
            except:
                score_f = 0.0

            # Compute Optimal
            score_opt = max(score_wo, score_a, score_f)

            #accumulate the local data
            local_scores_a.append(score_a)
            local_scores_f.append(score_f)
            local_scores_opt.append(score_opt)

            # Accumulate data
            self.global_scores["GALOIS_A"].append(score_a)
            self.global_scores["GALOIS_F"].append(score_f)
            self.global_scores["OPTIMAL"].append(score_opt)


            print(f"   [F: {score_f:.2f}] [OPT: {score_opt:.2f}] (Gap: {score_opt - score_f:.2f})")

            #add data for disk saving
            dataset_detailed_json.append({
                "query_id": str(i + 1),  # Importante per save_galois_results
                "dataset": dataset_name,
                "scores": {
                    "galois_a": score_a,
                    "galois_wo": score_wo,
                    "galois_f": score_f,
                    "optimal": score_opt
                },
                "optimality_gap": score_opt - score_f
            })

            if dataset_detailed_json:
                try:
                    save_path = save_galois_results(dataset_detailed_json, "EXP8", "openai", dataset_name)
                    print(f"Detailed results saved to: {save_path}")
                except Exception as e:
                    LOG.error(f"Could not save detailed JSON: {e}")



        # Local summary
        if local_scores_f:
            avg_a = statistics.mean(local_scores_a)
            avg_f = statistics.mean(local_scores_f)
            avg_opt = statistics.mean(local_scores_opt)
            print(f"--- {dataset_name} SUMMARY: F_Avg={avg_f:.4f} | OPT_Avg={avg_opt:.4f} ---")

            #add to the list for the plot
            self.final_plot_data.append({
                "dataset": dataset_name,
                "galois_a": avg_a,
                "galois_f": avg_f,
                "optimal": avg_opt
            })

    def run_all(self):
        log_init()

        # SELECT THE DATASETS TO RUN HERE
        # Tip: Start with GEO, WORLD, MOVIES
        datasets = ["GEO", "WORLD", "MOVIES"]
        # datasets = ["GEO"] # If you want to run only GEO

        for ds in datasets:
            self.run_dataset(ds)

        self.print_global_summary()

        # Save everything for plotting
        output_dir = Path(__file__).resolve().parent
        plot_file = output_dir / "exp8_all_data.json"
        with open(plot_file, "w") as f:
            json.dump(self.final_plot_data, f, indent=4)
            print(f"\n[PLOT DATA] Aggregated data for plotting saved to: {plot_file}")

    def print_global_summary(self):
        print("\n" + "=" * 50)
        print(f"   FINAL REPORT - EXP 8 (ALL DATASETS)")
        print("=" * 50)

        if not self.global_scores["GALOIS_F"]:
            print("No results collected.")
            return

        avg_a = statistics.mean(self.global_scores["GALOIS_A"])
        avg_f = statistics.mean(self.global_scores["GALOIS_F"])
        avg_opt = statistics.mean(self.global_scores["OPTIMAL"])

        print(f"Total Queries: {len(self.global_scores['OPTIMAL'])}")
        print("-" * 30)
        print(f"GALOIS A (Baseline): {avg_a:.4f}")
        print(f"GALOIS F (Ours):     {avg_f:.4f}")
        print(f"OPTIMAL (Oracle):    {avg_opt:.4f}")
        print("-" * 30)
        print(f"Global Optimality Gap: {avg_opt - avg_f:.4f}")
        print("=" * 50)


if __name__ == "__main__":
    runner = Exp8Runner()
    runner.run_all()