from src.galois.demo_process_single_query import debug
from src.galois.galois import Galois, save_galois_results
from src.utils import PY, SUBMISSIONS_PATH, GROUND_PATH, DATA_DIR, IK_DATASETS, MC_DATASETS, BASELINE_OUTPUT
from src.utils import get_dataset_selection, load_queries_from_folder
from src.utils import LOG, log_init

from src.llm import execute_baseline_sql, llm_interaction_nl_baseline
from src.db import load_nl_queries_from_txt
from src.config import Config_Loader

from pathlib import Path
import subprocess, argparse

from src.utils.constants import SUBMISSIONS_PATH_GALOIS


def parse_args():
     """
     Command-line interface for the project.
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
         default=None,
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
         choices=["gemini", "watsonx", "open_router"],
         default=None,
         help="Override LLM provider. If omitted, uses config.llm.provider.",
     )
     parser.add_argument(
         "--debug",
         action="store_true",
         default=None,
         help="Debug the system with a dummy query",
     )
     return parser.parse_args()


def main():
    
    config = Config_Loader().get_config()
    log_init()
    
    args = parse_args()

    if args.debug:
        debug()
        return

    if args.provider:
        config.llm_provider = args.provider.lower()

    datasets = []
    if args.datasets:
        requested = [d.upper() for d in args.datasets]
        if "ALL" in requested:
            LOG.info(f"INIT | [MAIN] 'ALL' detected. Mode: {args.mode}")

            if args.mode in ["PZSQL", "PZNL", "pzsql", "pznl"]:
                datasets = MC_DATASETS
                LOG.info("INIT | [MAIN] Target: MC_DATASETS (Palimpzest)")
            elif args.mode in ["NL", "SQL", "sql", "nl"]:
                datasets = IK_DATASETS
                LOG.info("INIT | [MAIN] Target: IK_DATASETS (Standard)")
            elif args.galois:
                datasets = IK_DATASETS
                LOG.info("INIT | [MAIN] Target: IK_DATASETS (Galois Default)")
        else:
            datasets = requested
            LOG.info(f"INIT | [MAIN] Target Specific: {datasets}")
    else:
        yaml_selection = get_dataset_selection(config.database.run)
        datasets = [d.upper() for d in yaml_selection]

    run_types = set()

    for dataset in datasets:
        dataset_path = DATA_DIR / dataset
        LOG.info(f"INIT | [MAIN] Processing Dataset: {dataset}")
        
        queries = load_queries_from_folder(dataset_path)

        # ------------------------------------------
        # NL SQL BASELINES BLOCK
        # ------------------------------------------
        """
        if args.mode:
            mode_upper = args.mode.upper()
            
            if args.mode =="sql":
                LOG.info(f"DATA | [SQL ] Loaded {len(queries)} queries for {dataset}")
                if dataset in IK_DATASETS:
                    execute_baseline_sql(config, dataset, queries, mode_upper)
                    run_types.add("SQL")

            elif args.mode == "nl":
                nl_queries = load_nl_queries_from_txt(dataset_path)
                LOG.info(f"DATA | [NL  ] Loaded {len(nl_queries)} queries for {dataset}")
                if dataset in IK_DATASETS:
                    llm_interaction_nl_baseline(config, dataset, nl_queries, mode_upper)
                    run_types.add("NL")

            elif args.mode == "pznl":
                nl_queries = load_nl_queries_from_txt(dataset_path)
                LOG.info(f"DATA | [PZNL] Loaded {len(nl_queries)} queries for {dataset}")
                if dataset in MC_DATASETS:
                    llm_interaction_nl_baseline(config, dataset, nl_queries, mode_upper)
                    run_types.add("PZNL")

            elif args.mode == "pzsql":
                LOG.info(f"DATA | [PZSQL] Loaded {len(queries)} queries for {dataset}")
                if dataset in MC_DATASETS:
                    llm_interaction_nl_baseline(config, dataset, nl_queries, mode_upper)
                    run_types.add("PZSQL")

            elif args.mode == "both":
                LOG.info(f"PLAN | [MAIN] Mode 'both': configuring pipelines for {dataset}")
                if dataset in IK_DATASETS:
                    LOG.info(f"EXEC | [BOTH] Running SQL + NL on {dataset}")
                    execute_baseline_sql(config, dataset, queries, "SQL")
                    run_types.add("SQL")
                    
                    nl_queries = load_nl_queries_from_txt(dataset_path)
                    llm_interaction_nl_baseline(config, dataset, nl_queries, "NL")
                    run_types.add("NL")
                elif dataset in MC_DATASETS:
                    LOG.info(f"EXEC | [BOTH] Running PZSQL + PZNL on {dataset}")
                    llm_interaction_nl_baseline(config, dataset, queries, "PZSQL")
                    run_types.add("PZSQL")
                    
                    nl_queries = load_nl_queries_from_txt(dataset_path)
                    llm_interaction_nl_baseline(config, dataset, nl_queries, "PZNL")
                    run_types.add("PZNL")
                else:
                    LOG.warning(f"WARN | [BOTH] Dataset {dataset} not supported for combined run.")

        """
        # ------------------------------------------
        # GALOIS BLOCK
        # ------------------------------------------
        if args.galois:
            variants_input = args.galois.lower()
            variants_to_run = ["WO", "S", "A", "F"] if variants_input == "all" else [variants_input.upper()]

            LOG.info(f"INIT | [GALOIS] Initializing Planner for {dataset}")

            for variant in variants_to_run:
                LOG.info(f"EXEC | [GALOIS] Running Variant: GALOIS_{variant} on {dataset}")
                results_for_eval = []

                for i, q_sql in enumerate(queries):
                    planner = Galois(config, dataset, q_sql)
                    rows = []
                    stats={}
                    try:
                        if variant == "WO":
                            rows, stats = planner.run_no_push()
                        elif variant == "S":
                            rows, stats = planner.run_push_selective()
                        elif variant == "A":
                            rows, stats = planner.run_push_all()
                        elif variant == "F":
                            rows, stats = planner.run_push_confident()
                        else:
                            LOG.error(f"ERR  | [GALOIS] Unknown variant {variant}")
                            continue

                        results_for_eval.append({
                            "query_id": i+1,
                            "sql": q_sql,
                            "result_set": rows,
                            "time": stats.get("total_time", 0),
                            "tokens": stats.get("total_tokens", 0)
                        })
                        
                        # Minimal visual feedback aligned
                        LOG.debug(f"RSLT | [GALOIS] Query {i + 1}: {len(rows)} rows | Time: {stats.get('total_time', 0):.2f}s | Tokens: {stats.get('total_tokens', 0)}")
                        
                    except Exception as e:
                        LOG.error(f"ERR  | [GALOIS] Query {i+1} Failed: {e}")
                        results_for_eval.append({
                            "query_id": i+1, "sql": q_sql, "result_set": [], "error": str(e)
                        })

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
        LOG.warning("WARN | [MAIN] No baselines executed. Skipping evaluation.")
        return

    LOG.info("EVAL | [MAIN] Starting Evaluation Phase")

    for run_type in sorted(run_types):
        submissions_path= ""
        if args.galois:
            submissions_path = SUBMISSIONS_PATH_GALOIS / run_type.upper()
        elif args.mode:
            submissions_path = BASELINE_OUTPUT[run_type][config.llm_provider.upper()]

        LOG.info(f"EVAL | [MAIN] Assessing: {run_type}")
        LOG.info(f"     | Path: {submissions_path}")
        
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