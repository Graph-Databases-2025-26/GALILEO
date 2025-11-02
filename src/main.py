from src.utils import PY, ROOT, SUBMISSIONS_PATH, GROUND_PATH, DATASETS
from src.utils import LOG, log_init 
from src.db import run_queries_to_json, db_creation
from config import Config_Loader
from utils.dataset_selection import get_dataset_selection
import subprocess

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
