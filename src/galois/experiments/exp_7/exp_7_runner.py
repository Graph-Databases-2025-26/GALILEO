import json
import numpy as np
from pathlib import Path
import os

# Import da src
from src.config import Config_Loader
from src.galois.galois import Galois
from src.utils.logging_config import log_init, LOG
from src.utils.main_tools import load_queries_from_folder
from src.utils.tutor.galois_eval import read_table_file, f1_cell_exact, cardinality, tuple_constraint

# --- CONFIGURAZIONE PATH ---
OUTPUT_BASE_DIR = Path("data/.galois_output/GALOIS_EXP_7")

def calculate_avg_score(gt_path: Path, pred_rows: list) -> float:
    """Calcola l'AVG Score confrontando le predizioni con la Ground Truth."""
    if not gt_path.exists():
        LOG.warning(f"GT missing: {gt_path}")
        return 0.0
    
    try:
        gt_cols, gt_rows = read_table_file(gt_path)
    except Exception:
        return 0.0
    
    if not pred_rows:
        pr_cols, pr_rows = [], []
    else:
        pr_cols = list(pred_rows[0].keys())
        pr_rows = [[row.get(c, "") for c in pr_cols] for row in pred_rows]
        
    try:
        f1 = f1_cell_exact(gt_cols, gt_rows, pr_cols, pr_rows)
        card = cardinality(gt_rows, pr_rows)
        tcon = tuple_constraint(gt_rows, pr_rows)
        return (f1 + card + tcon) / 3.0
    except Exception:
        return 0.0

def get_model_name_from_config(config) -> str:
    """
    Estrae il nome del modello in base al provider configurato in AppConfig.
    """
    provider = config.llm_provider
    
    if provider == "watsonx":
        return config.watsonx.model
    elif provider == "gemini":
        return config.gemini.model
    elif provider == "open_router":
        return config.open_router.model
    
    return f"Unknown_Model_{provider}"

def run_exp_7_single_model():
    config = Config_Loader().get_config()
    log_init()

    # 1. Identifica il modello corrente usando la logica corretta
    raw_model_name = get_model_name_from_config(config)
    safe_model_name = raw_model_name.replace("/", "_").replace(":", "_")
    
    # 2. Crea cartella di output specifica per questo modello
    model_output_dir = OUTPUT_BASE_DIR / safe_model_name
    model_output_dir.mkdir(parents=True, exist_ok=True)
    
    LOG.info(f"--- STARTING EXP-7 CALIBRATION FOR: {raw_model_name} ---")
    LOG.info(f"Saving results to: {model_output_dir}")

    # 3. Setup Dataset
    DATASET_NAME = "GEO"
    DATA_FOLDER = Path("data") / DATASET_NAME 
    GT_DIR = Path("data/.ground_truth") / DATASET_NAME

    if not DATA_FOLDER.exists() or not GT_DIR.exists():
        LOG.error(f"Data or GT folders missing.")
        return

    queries = load_queries_from_folder(DATA_FOLDER)
    if not queries:
        LOG.error("No queries found.")
        return

    # 4. Soglie da testare
    thresholds = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.0]
    results_map = {} 

    for tau in thresholds:
        print(f"\n>>> Running Calibration with Tau = {tau}")
        query_scores = []
        
        for idx, sql_text in enumerate(queries):
            query_id = idx + 1
            try:
                gt_file = GT_DIR / f"query{query_id}.json"
                
                galois = Galois(
                    config=config,
                    dataset=DATASET_NAME,
                    sql_query=sql_text,
                    physical_strategy="auto",
                    confidence_threshold=tau
                )
                
                results, stats = galois.run_push_confident()
                score = calculate_avg_score(gt_file, results)
                query_scores.append(score)
                
            except Exception as e:
                LOG.error(f"Error Q{query_id} at tau {tau}: {e}")
                query_scores.append(0.0)

        avg_score = np.mean(query_scores) if query_scores else 0.0
        results_map[tau] = avg_score
        print(f"--> AVG Score: {avg_score:.4f}")

    # 5. Salva i risultati JSON
    output_file = model_output_dir / "results.json"
    
    final_data = {
        "model_name": raw_model_name,
        "dataset": DATASET_NAME,
        "results": results_map,  # {threshold: score}
        "best_tau": max(results_map, key=results_map.get) if results_map else 0.0,
        "best_score": max(results_map.values()) if results_map else 0.0
    }

    with open(output_file, 'w') as f:
        json.dump(final_data, f, indent=4)
        
    LOG.info(f"Calibration completed. Results saved to: {output_file}")
    print(f"\nSaved results for {raw_model_name} in {output_file}")

if __name__ == "__main__":
    run_exp_7_single_model()