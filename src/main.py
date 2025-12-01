from src.galois.galois import Galois, save_galois_results
from src.utils import PY, SUBMISSIONS_PATH, GROUND_PATH, DATA_DIR, IK_DATASETS, MC_DATASETS, BASELINE_OUTPUT
from src.utils import get_dataset_selection, load_queries_from_folder
from src.utils import LOG, log_init

from src.llm import execute_baseline_sql, llm_interaction_nl_baseline
from src.db import load_nl_queries_from_txt
from src.config import Config_Loader
from src.utils.logging_config import LOG

from pathlib import Path
import subprocess, argparse

from src.utils.constants import SUBMISSIONS_PATH_GALOIS


# from src.llm.baseline_nl import llm_interaction_second_version
# from src.llm.baseline_sql_gemini import run_sql_baseline_gemini
# from src.db.run_queries_to_json import load_nl_queries_from_txt

# CLI parsing
def parse_args():
     """
     Command-line interface for the project.

     Examples:
       python -m src.main WORLD --mode sql --provider gemini
       python -m src.main GEO MOVIES --mode both
       python -m src.main --mode nl   (datasets from config.database.run)
     """
     parser = argparse.ArgumentParser(
         description="Run NL/SQL/PALIMPZEST baselines over one or more datasets."
     )
     parser.add_argument(
         "datasets",
         nargs="*",
         help="Dataset names, e.g. GEO MOVIES WORLD. If omitted, uses config.database.run.",
     )
     parser.add_argument(
         "--mode",
         choices=["nl", "sql", "both","pzsql", "pznl"],
         default="sql",
         help="Which baseline(s) to run.",
     )
     parser.add_argument(
         "--galois",
         choices=["wo", "s", "a", "f", "all"],
         default=None,
         help="Run a specific GALOIS variant (wo=Without Opt, s=Selective, a=All, f=Full) or 'all'.",
     )
     parser.add_argument(
         "--provider",
         choices=["gemini", "watsonx"],
         default=None,
         help="Override LLM provider. If omitted, uses config.llm.provider.",
     )
     return parser.parse_args()

# # Watsonx SQL baseline (your old logic moved into a helper)
# def run_sql_baseline_watsonx(config, datasets):
#     """
#     This is basically your old main(), restricted to:
#       - load SQL queries from each dataset
#       - run execute_IK_baseline_sql_query / execute_MC_baseline_sql_query
#       - save baselines to JSON
#     """
#     LOG.info("Running SQL baseline with Watsonx")

#     for dataset in datasets:
#         dataset = dataset.upper()
#         dataset_path = DATA_DIR / dataset

#         LOG.info(f"=== Processing dataset (Watsonx SQL): {dataset} ===")

#         queries = load_queries_from_folder(dataset_path)
#         LOG.info(f"Loaded {len(queries)} queries for dataset {dataset}")

#         if dataset in IK_DATASETS:
#             sql_baseline = execute_IK_baseline_sql_query(config, queries)
#         elif dataset in MC_DATASETS:
#             sql_baseline = execute_MC_baseline_sql_query(config, dataset, queries)
#         else:
#             LOG.warning(f"Dataset {dataset} not in IK_DATASETS or MC_DATASETS; skipping.")
#             continue

#         save_baseline_to_json(dataset, sql_baseline)

# # NL baseline 
# def run_nl_baseline(config, datasets):
#     """
#     NL baseline:
#       - for each dataset, load NL queries (from .txt)
#       - call llm_interaction_second_version(dataset, duckdb_path, prompt)
#     """
#     LOG.info("Running NL baseline")

#     for d in datasets:
#         dataset_name = d.upper()
#         if dataset_name not in IK_DATASETS and dataset_name not in MC_DATASETS:
#             LOG.warning(f"[NL] Dataset {dataset_name} not recognized; skipping.")
#             continue

#         dataset_path = DATA_DIR / dataset_name
#         duckdb_path = dataset_path / f"{dataset_name.lower()}.duckdb"

#         if not dataset_path.is_dir():
#             LOG.warning(f"[NL] Dataset path not found: {dataset_path}; skipping.")
#             continue

#         LOG.info(f"[NL] Processing dataset: {dataset_name}")

#         nl_queries = load_nl_queries_from_txt(dataset_path)
#         LOG.info(f"[NL] Loaded {len(nl_queries)} prompts for dataset {dataset_name}")

#         for prompt in nl_queries:
#             llm_interaction_second_version(dataset_name, str(duckdb_path), prompt)

# # Local Gemini SQL baseline 
# def run_sql_baseline_gemini_wrapper(datasets):
#     """
#     Simple wrapper around run_sql_baseline_gemini(datasets),
#     so the main() code has a symmetrical layout.
#     """
#     LOG.info("Running SQL baseline with Gemini")
#     run_sql_baseline_gemini(datasets)

def main():
    
    config = Config_Loader().get_config()
    args = parse_args()
    #log_init()

    if args.provider:
        #if args.mode in ("pzsql", "pznl") and args.provider == "gemini":
            #LOG.error("You cannot run Palimpzest with Gemini; these modes require WatsonX.")
            #return
        config.llm_provider = args.provider.lower()

    datasets = []
    if args.datasets:
        # Se l'utente ha specificato dataset nel comando, usa quelli
        requested = [d.upper() for d in args.datasets]
        if "ALL" in requested:
            # 1. 'ALL' rilevato via CLI: decidi il set in base alla modalità
            LOG.info(f"'ALL' detected via CLI. Loading datasets based on run mode: {args.mode}")

            # Le modalità che caricano MC_DATASETS
            if args.mode in ["PZSQL", "PZNL", "pzsql", "pznl"]:
                datasets = MC_DATASETS
                LOG.info("PZSQL/PZNL mode detected. Loading MC_DATASETS.")
            # Le modalità che caricano IK_DATASETS (fallback, es. NL, SQL, o altre)
            elif args.mode in ["NL", "SQL", "sql", "nl"] or args.galois.lower() in ["wo", "s", "a", "f", "all"]:
                datasets = IK_DATASETS
                LOG.info("NL/SQL mode or galois framework detected. Loading IK_DATASETS.")
        else:
            # 2. Lista specifica di dataset via CLI
            datasets = requested
            LOG.info(f"Using specific datasets requested via CLI: {datasets}")
    else:
        # Altrimenti usa la logica di fallback del config/tools
        yaml_selection = get_dataset_selection(config.database.run)
        datasets = [d.upper() for d in yaml_selection]

    #track which types of baselines were run (handling "both" mode)
    run_types = set()

    for dataset in datasets:
        dataset_path = DATA_DIR / dataset
        LOG.info(f"=== Processing dataset: {dataset} ===")
        #nl_queries = load_nl_queries_from_txt(dataset_path)
        queries = load_queries_from_folder(dataset_path)

        # ------------------------------------------
        # NL SQL BASELINES BLOCK
        # ------------------------------------------
        if args.mode:
            if args.mode =="sql":
                LOG.info(f"Loaded {len(queries)} queries for dataset {dataset} in {args.mode} mode")
                if dataset in IK_DATASETS:
                    execute_baseline_sql(config, dataset, queries, args.mode.upper())
                    run_types.add("SQL")

            elif args.mode == "nl":
                nl_queries = load_nl_queries_from_txt(dataset_path)
                LOG.info(f"Loaded {len(nl_queries)} queries for dataset {dataset} in {args.mode} mode")
                if dataset in IK_DATASETS:
                    llm_interaction_nl_baseline(config, dataset, nl_queries, args.mode.upper())
                    run_types.add("NL")

            elif args.mode == "pznl":
                LOG.info(f"Loaded {len(nl_queries)} queries for dataset {dataset} in {args.mode} mode")
                if dataset in MC_DATASETS:
                    llm_interaction_nl_baseline(config, dataset, nl_queries, args.mode.upper())
                    run_types.add("PZNL")

            elif args.mode == "pzsql":
                LOG.info(f"Loaded {len(queries)} queries for dataset {dataset} in {args.mode} mode")
                if dataset in MC_DATASETS:
                    llm_interaction_nl_baseline(config, dataset, nl_queries, args.mode.upper())
                    run_types.add("PZSQL")

            elif args.mode == "both":
                LOG.info(f"Mode both: deciding pipelines for dataset {dataset}")
                # IK datasets -> run SQL + NL
                if dataset in IK_DATASETS:
                    LOG.info(f"[BOTH] Running SQL and NL baselines for IK dataset {dataset}")
                    execute_baseline_sql(config, dataset, queries, "SQL")
                    run_types.add("SQL")
                    llm_interaction_nl_baseline(config, dataset, nl_queries, "NL")
                    run_types.add("NL")
                # MC datasets -> run PZSQL + PZNL
                elif dataset in MC_DATASETS:
                    LOG.info(f"[BOTH] Running PZSQL and PZNL baselines for MC dataset {dataset}")
                    llm_interaction_nl_baseline(config, dataset, queries, "PZSQL")
                    run_types.add("PZSQL")
                    llm_interaction_nl_baseline(config, dataset, nl_queries, "PZNL")
                    run_types.add("PZNL")
                else:
                    LOG.warning(f"[BOTH] Dataset")
        

        # ------------------------------------------
        # GALOIS BLOCK
        # ------------------------------------------
        if args.galois:
            # Determine which variants to run
            variants_input = args.galois.lower()
            variants_to_run = []
            if variants_input == "all":
                variants_to_run = ["WO", "S", "A", "F"]
            else:
                variants_to_run = [variants_input.upper()]

            LOG.info(f"Initializing GaloisPlanner for {dataset}...")

            for variant in variants_to_run:
                LOG.info(f">>> Running GALOIS_{variant} on {dataset}")
                results_for_eval = []

                for i, q_sql in enumerate(queries):
                    # Instantiate the Planner once per dataset
                    planner = Galois(config, dataset, q_sql)
                    rows = []
                    try:
                        # Execute the specific variant method based on CLI arg
                        if variant == "WO":
                            # Without Optimization: No pushdown, Key-Scan
                            rows = planner.run_no_push()

                        elif variant == "S":
                            # Selective: Push selective conditions (LLM estimated), Table-Scan
                            rows = planner.run_push_selective()

                        elif variant == "A":
                            # All: Push all conditions, Table-Scan
                            rows = planner.run_push_all()

                        elif variant == "F":
                            # Full/Confident: Push confident conditions, Dynamic Scan (Table/Key)
                            rows = planner.run_push_confident()

                        else:
                            LOG.error(f"Unknown variant {variant}")
                            continue

                        results_for_eval.append({
                            "query_id": i+1,
                            "sql": q_sql,
                            "result_set": rows
                        })
                        # Minimal visual feedback
                        LOG.debug(f"Query {i+1}: {len(rows)} rows returned.")

                    except Exception as e:
                        LOG.error(f"Failed query {i+1} in mode {variant}: {e}")
                        results_for_eval.append({
                            "query_id": i+1, "sql": q_sql, "result_set": [], "error": str(e)
                        })

                # Save the results
                save_galois_results(
                    results_for_eval,
                    variant,
                    config.llm_provider.upper(),
                    dataset
                )
                run_types.add(f"GALOIS_{variant}")


    # ------------------------------------------
    # EVALUATION BLOCK
    # ------------------------------------------
    if not run_types:
        LOG.warning("No baselines were executed; skipping evaluation.")
        return

    LOG.info("\n" + "=" * 30)
    LOG.info(" STARTING EVALUATION ")
    LOG.info("=" * 30)


    for run_type in sorted(run_types):
        submissions_path= ""
        if args.galois:
            submissions_path = SUBMISSIONS_PATH_GALOIS / run_type.upper()
        elif args.mode:
            submissions_path = BASELINE_OUTPUT[run_type][config.llm_provider.upper()]

        LOG.info(f"Evaluating submissions for baseline type {run_type}: {submissions_path}")
        subprocess.run([
            PY,
            "-m",
            "src.utils.tutor.galois_eval",
            "--ground", GROUND_PATH,
            "--submissions", submissions_path,
            "--datasets", *datasets,
            "--cell-metric similarity",
            "--tuple-metric constraint",
            "--format table"
        ], check=True)

        
if __name__ == "__main__":
    main()
