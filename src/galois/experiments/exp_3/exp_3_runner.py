import json
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from typing import List, Tuple

# Importiamo costanti e funzioni di supporto dal progetto
from src.utils.tutor.galois_eval import cells_set, _cells_similar_default, read_table_file
from src.utils.constants import SUBMISSIONS_PATH_GALOIS, GROUND_PATH, IK_DATASETS
from src.utils.logging_config import LOG, log_init

def calculate_precision_recall(gt_rows, gt_cols, pr_rows, pr_cols):
    """
    Calcola Precision e Recall separatamente usando la logica di galois_eval
    (F1-Cell metric split into P/R).
    """
    gt = list(cells_set(gt_cols, gt_rows))
    pr = list(cells_set(pr_cols, pr_rows))
    
    if not gt and not pr: return 1.0, 1.0
    if not gt: return 0.0, 0.0 # Precision 0 (nothing to find)
    if not pr: return 1.0, 0.0 # Precision 1 (empty result but safe), Recall 0

    # Precision: quante celle predette esistono nella GT?
    p_count = 0
    for rc in pr:
        if any(_cells_similar_default(ec, rc) for ec in gt):
            p_count += 1
    precision = p_count / len(pr)

    # Recall: quante celle della GT sono state trovate?
    r_count = 0
    for ec in gt:
        if any(_cells_similar_default(ec, rc) for rc in pr):
            r_count += 1
    recall = r_count / len(gt)
    
    return precision, recall

def run_experiment_3():
    log_init()
    
    # Thresholds come da Figura 7 (con aggiunta del 0.95 che avevi nel tuo codice)
    thresholds = [0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.995]
    
    # Struttura per accumulare i risultati: threshold -> lists of scores
    results_map = {t: {"precisions": [], "recalls": []} for t in thresholds}

    base_pred_path = SUBMISSIONS_PATH_GALOIS / "GALOIS_F" / "meta-llama" / "llama-3-3-70b"
    
    print(f"--- STARTING EXPERIMENT 3 (Replicating Fig. 7) ---")
    print(f"Predictions Path: {base_pred_path}")
    print(f"Ground Truth Path: {GROUND_PATH}")
    print(f"Datasets considered (IK): {IK_DATASETS}")

    total_queries_processed = 0

    # Iteriamo sui dataset definiti come Internal Knowledge (IK) nel paper
    for dataset_name in IK_DATASETS:
        dataset_name_upper = dataset_name.upper()
        
        pred_dir = base_pred_path / dataset_name_upper
        gt_dir = GROUND_PATH / dataset_name_upper

        if not pred_dir.exists():
            print(f"[WARNING] Prediction directory not found for {dataset_name}: {pred_dir}")
            continue
        
        if not gt_dir.exists():
            print(f"[WARNING] Ground Truth directory not found for {dataset_name}: {gt_dir}")
            continue

        print(f"Processing dataset: {dataset_name}...")
        
        query_files = list(pred_dir.glob("*.json"))
        
        for json_file in query_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except Exception as e:
                print(f"Error reading {json_file}: {e}")
                continue
            
            if "result_set" in data:
                rows = data["result_set"]
                logprobs = data.get("logprobs", [])
            else:
                print(f"Skipping {json_file.name}: format not compatible")
                continue

            if not rows:
                 pass

            if len(logprobs) != len(rows):
                current_logprobs = [0.0] * len(rows)
            else:
                current_logprobs = logprobs

            gt_file = gt_dir / json_file.name
            if not gt_file.exists():
                gt_file = gt_dir / (json_file.stem + ".csv")
            
            if not gt_file.exists():
                continue
            
            gt_cols, gt_rows_list = read_table_file(gt_file)

            for t in thresholds:
                filtered_rows = []
                for row, lp in zip(rows, current_logprobs):
                    prob = np.exp(lp) if lp <= 0 else lp
                    if prob >= t:
                        filtered_rows.append(row)
                
                pr_cols = list(filtered_rows[0].keys()) if filtered_rows else []
                pr_rows_list = [[r.get(c, "") for c in pr_cols] for r in filtered_rows]

                p, r = calculate_precision_recall(gt_rows_list, gt_cols, pr_rows_list, pr_cols)
                
                results_map[t]["precisions"].append(p)
                results_map[t]["recalls"].append(r)

            total_queries_processed += 1

    print(f"Total queries processed: {total_queries_processed}")

    if total_queries_processed == 0:
        print("No queries processed. Check paths.")
        return

    # --- Aggregazione ---
    avg_precisions = []
    avg_recalls = []

    for t in thresholds:
        p_list = results_map[t]["precisions"]
        r_list = results_map[t]["recalls"]
        
        avg_p = np.mean(p_list) if p_list else 0.0
        avg_r = np.mean(r_list) if r_list else 0.0
        
        avg_precisions.append(avg_p)
        avg_recalls.append(avg_r)

    # --- PLOTTING STYLE FIGURE 7 ---
    
    # 1. Configurazione Font (Serif come nei paper LaTeX)
    plt.rcParams["font.family"] = "serif"
    plt.rcParams["font.size"] = 12
    
    # Creazione figura (aspect ratio simile al paper, largo e basso)
    fig, ax = plt.subplots(figsize=(8, 3.5))

    # 2. Gestione Asse X "Categorico"
    # Il paper non usa una scala lineare per X (0.9 e 0.99 sono distanti quanto 0.6 e 0.7).
    # Usiamo un range di interi per distanziare equamente i punti.
    x_indices = np.arange(len(thresholds))

    # 3. Colori e Stili (Copiati dalla Figura 7)
    # Precision: Linea azzurra, Marker cerchio blu pieno con bordo bianco/chiaro
    line_cyan = '#56B4E9'   # Azzurro chiaro per la linea
    marker_blue = '#0072B2' # Blu scuro per il riempimento marker

    # Recall: Linea arancione chiaro, Marker quadrato arancio scuro/rosso
    line_orange = '#FDBE85' # Arancione chiaro (albicocca) per la linea
    marker_red = '#D55E00'  # Rosso arancio per il riempimento marker

    # Plot Precision
    ax.plot(x_indices, avg_precisions, 
            color=line_cyan, 
            linewidth=2.5, 
            label='PRECISION',
            marker='o', 
            markersize=7, 
            markerfacecolor=marker_blue, 
            markeredgecolor=line_cyan, 
            markeredgewidth=1.5)

    # Plot Recall
    ax.plot(x_indices, avg_recalls, 
            color=line_orange, 
            linewidth=2.5, 
            label='RECALL',
            marker='s', 
            markersize=7, 
            markerfacecolor=marker_red, 
            markeredgecolor=line_orange, 
            markeredgewidth=1.5)

    # 4. Formattazione Assi
    # Asse X: mappiamo gli indici ai valori reali
    ax.set_xticks(x_indices)
    ax.set_xticklabels([str(t) for t in thresholds])
    ax.set_xlabel(r"$p$ threshold", fontsize=14, fontstyle='italic') # LaTeX-style 'p'

    # Asse Y: Solo griglia orizzontale tratteggiata
    ax.yaxis.grid(True, linestyle='--', color='gray', alpha=0.5, linewidth=1)
    ax.xaxis.grid(False) # Niente griglia verticale
    
    # Rimuoviamo il box superiore e destro per un look più pulito (opzionale, il paper ha il box chiuso)
    # ax.spines['top'].set_visible(False)
    # ax.spines['right'].set_visible(False)

    # Limiti Y: adattati ai dati ma simili al paper (0.55 - 0.70 circa)
    # Se i tuoi dati variano molto, commenta queste righe per l'autoscale
    # ax.set_ylim(0.50, 0.70) 

    # 5. Legenda
    # In basso a sinistra, bordo nero, 2 colonne
    legend = ax.legend(loc='lower left', ncol=2, frameon=True, edgecolor='black', fancybox=False, fontsize=10)
    legend.get_frame().set_linewidth(0.8)

    # Salvataggio
    output_img = 'figure_7_reproduction.png'
    plt.tight_layout()
    plt.savefig(output_img, dpi=300, bbox_inches='tight')
    print(f"Graph saved as {output_img}")

if __name__ == "__main__":
    run_experiment_3()