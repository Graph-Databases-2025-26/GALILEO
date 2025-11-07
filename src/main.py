from src.utils import PY, SUBMISSIONS_PATH, GROUND_PATH, DATA_DIR, IK_DATASETS, MC_DATASETS
from src.utils import get_dataset_selection
from src.utils import save_baseline_to_json
from src.utils import LOG, log_init 
from src.llm import execute_IK_baseline_sql_query, execute_MC_baseline_sql_query
from src.db import run_queries_to_json, db_creation
from config import Config_Loader
from pathlib import Path
import subprocess, re

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
    
        
if __name__ == "__main__":
    main()
