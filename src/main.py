from src.utils import PY, ROOT, SUBMISSIONS_PATH, GROUND_PATH, DATASETS
from src.utils import LOG, log_init 
from src.db import run_queries_to_json, db_creation
from config import Config_Loader
import os, sys, subprocess

def get_dataset_selection(database_run: str) -> list[str]:
    #  Priority to the command line
    if len(sys.argv) > 1:
        args = [arg.upper() for arg in sys.argv[1:]]
        LOG.info(f"Command line args: {args}")
    else:
        #  Otherwise use the parameter from the .yaml file
        if database_run is None:
            database_run = "ALL"
        else:
            args = [s.strip().upper() for s in database_run.split(",")]

        LOG.info(f"Parameters from YAML file: {args}")

    # Parameters validation
    if "ALL" in args:
        return DATASETS
    valid = [d for d in args if d in DATASETS]
    invalid = [d for d in args if d not in DATASETS]
    if invalid:
        LOG.warning(f"Dataset '{args}' not valid. The available datasets are: {DATASETS}")
    return valid if valid else DATASETS  # fallback with every dataset if any is valid



def main():
    config = Config_Loader().get_config()
    log_init()
    
    LOG.info(f"Logging level: {config.logging.level}")
    LOG.info(f"Output directory: {config.io.outputs_dir}")
    LOG.info(f"setup_project.py starting at ROOT = {ROOT}")


    datasets = get_dataset_selection(config.database.run)

    for dataset in datasets:
        
        LOG.info(f"=== Processing dataset: {dataset} ===")
        db_creation(dataset)
        run_queries_to_json.run_queries_to_json(dataset)
    
    subprocess.run([
        PY,
        "-m",
        "src.utils.galois_eval",
        "--ground", GROUND_PATH,
        "--submissions", SUBMISSIONS_PATH,
        "--datasets", *datasets,
        "--cell-metric similarity",
        "--tuple-metric constraint",
        "--format table" 
    ], check=True)
        
if __name__ == "__main__":
    main()
