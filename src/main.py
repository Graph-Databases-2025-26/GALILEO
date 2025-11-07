from src.utils import PY, SUBMISSIONS_PATH, GROUND_PATH, DATA_DIR, IK_DATASETS, MC_DATASETS
from src.utils import get_dataset_selection
from src.utils import save_baseline_to_json
from src.utils import LOG, log_init 
from src.llm import execute_IK_baseline_sql_query, execute_MC_baseline_sql_query
from src.db import run_queries_to_json, db_creation
from config import Config_Loader
from pathlib import Path
import subprocess, re
from src.utils import (
    PY,
    SUBMISSIONS_PATH,
    GROUND_PATH,
    DATA_DIR,
    IK_DATASETS,
    MC_DATASETS,
    get_dataset_selection,
    save_baseline_to_json,
    LOG,
    log_init,
)


from src.llm import (
    execute_IK_baseline_sql_query,
    execute_MC_baseline_sql_query,
)


from src.llm.baseline_nl import llm_interaction_second_version
from src.llm.baseline_sql_gemini import run_sql_baseline_gemini
from src.db.run_queries_to_json import load_nl_queries_from_txt

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
        description="Run NL/SQL baselines over one or more datasets."
    )
    parser.add_argument(
        "datasets",
        nargs="*",
        help="Dataset names, e.g. GEO MOVIES WORLD. If omitted, uses config.database.run.",
    )
    parser.add_argument(
        "--mode",
        choices=["nl", "sql", "both"],
        default="both",
        help="Which baseline(s) to run.",
    )
    parser.add_argument(
        "--provider",
        choices=["gemini", "watsonx"],
        default=None,
        help="Override LLM provider. If omitted, uses config.llm.provider.",
    )
    return parser.parse_args()

# Watsonx SQL baseline (your old logic moved into a helper)
def run_sql_baseline_watsonx(config, datasets):
    """
    This is basically your old main(), restricted to:
      - load SQL queries from each dataset
      - run execute_IK_baseline_sql_query / execute_MC_baseline_sql_query
      - save baselines to JSON
    """
    LOG.info("Running SQL baseline with Watsonx")

    for dataset in datasets:
        dataset = dataset.upper()
        dataset_path = DATA_DIR / dataset

        LOG.info(f"=== Processing dataset (Watsonx SQL): {dataset} ===")

        queries = load_queries_from_folder(dataset_path)
        LOG.info(f"Loaded {len(queries)} queries for dataset {dataset}")

        if dataset in IK_DATASETS:
            sql_baseline = execute_IK_baseline_sql_query(config, queries)
        elif dataset in MC_DATASETS:
            sql_baseline = execute_MC_baseline_sql_query(config, dataset, queries)
        else:
            LOG.warning(f"Dataset {dataset} not in IK_DATASETS or MC_DATASETS; skipping.")
            continue

        save_baseline_to_json(dataset, sql_baseline)

# NL baseline 
def run_nl_baseline(config, datasets):
    """
    NL baseline:
      - for each dataset, load NL queries (from .txt)
      - call llm_interaction_second_version(dataset, duckdb_path, prompt)
    """
    LOG.info("Running NL baseline")

    for d in datasets:
        dataset_name = d.upper()
        if dataset_name not in IK_DATASETS and dataset_name not in MC_DATASETS:
            LOG.warning(f"[NL] Dataset {dataset_name} not recognized; skipping.")
            continue

        dataset_path = DATA_DIR / dataset_name
        duckdb_path = dataset_path / f"{dataset_name.lower()}.duckdb"

        if not dataset_path.is_dir():
            LOG.warning(f"[NL] Dataset path not found: {dataset_path}; skipping.")
            continue

        LOG.info(f"[NL] Processing dataset: {dataset_name}")

        nl_queries = load_nl_queries_from_txt(dataset_path)
        LOG.info(f"[NL] Loaded {len(nl_queries)} prompts for dataset {dataset_name}")

        for prompt in nl_queries:
            llm_interaction_second_version(dataset_name, str(duckdb_path), prompt)

# Local Gemini SQL baseline 
def run_sql_baseline_gemini_wrapper(datasets):
    """
    Simple wrapper around run_sql_baseline_gemini(datasets),
    so the main() code has a symmetrical layout.
    """
    LOG.info("Running SQL baseline with Gemini")
    run_sql_baseline_gemini(datasets)


# Helper: load SQL queries      
def load_queries_from_folder(data_folder: Path) -> list[str]:
    
    sql_files = list(data_folder.glob("queries_*.sql"))    
    file_path = sql_files[0] 
    
    f_content = ""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            f_content = f.read().strip()
            
    except Exception as e:
        LOG.error(f"Error reading file {file_path}: {e}")
        return {}
    
    qry_regx = re.compile(r'--query(?:\d+)\s*(.*?)(?=--query|\Z)', re.DOTALL | re.IGNORECASE)    
    matches = qry_regx.findall(f_content)

    queries = []
    for qry in matches:
        queries.append(qry.strip())
        
    return queries

def main():
    
    config = Config_Loader().get_config()
    log_init()
    
    LOG.info(f"Watsonx_Model = {config.watsonx.model}")

    datasets = get_dataset_selection(config.database.run)

    for dataset in datasets:
        
        dataset_path = DATA_DIR / dataset
        LOG.info(f"=== Processing dataset: {dataset} ===")
        
        queries = load_queries_from_folder(dataset_path)
        LOG.info(f"Loaded {len(queries)} queries for dataset {dataset}")
        
        if dataset in IK_DATASETS:
            sql_baseline = execute_IK_baseline_sql_query(config, queries)
        elif dataset in MC_DATASETS:
            sql_baseline = execute_MC_baseline_sql_query(config, dataset, queries)
        
        save_baseline_to_json(dataset, sql_baseline)
        
        ##db_creation(dataset)
        ##run_queries_to_json.run_queries_to_json(dataset)

    
    ##subprocess.run([
        # PY,
        # "-m",
        # "src.utils.galois_eval",
        # "--ground", GROUND_PATH,
        # "--submissions", SUBMISSIONS_PATH,
        # "--datasets", *datasets,
        # "--cell-metric similarity",
        # "--tuple-metric constraint",
        # "--format table" 
    ##], check=True)
        
if __name__ == "__main__":
    main()
