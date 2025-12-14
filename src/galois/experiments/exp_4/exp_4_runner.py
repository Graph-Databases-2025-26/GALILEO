import re

import pandas as pd
import duckdb
import json
import os
import glob
import sys
from src.utils import LOG, DATA_DIR
from src.db.run_queries_to_json import load_queries_from_folder

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '../../../../'))
EVAL_DIR = os.path.join(PROJECT_ROOT, 'src', 'utils', 'tutor')


if EVAL_DIR not in sys.path:
    sys.path.append(EVAL_DIR)

print(f"DEBUG: Cerco galois_eval in: {EVAL_DIR}")

sys.path.append(SCRIPT_DIR)

try:
    from galois_eval import (
        _parse_table_from_obj,
        f1_cell_similarity,
        cardinality,
        tuple_constraint
    )
except ImportError:
    print(f"ERRORE CRITICO: Non trovo 'galois_eval.py' in {SCRIPT_DIR}.")
    print("Assicurati di aver copiato il file galois_eval.py in questa cartella.")
    sys.exit(1)



DATA_DIR = os.path.join(PROJECT_ROOT, "data")
PRESIDENTS_DIR = os.path.join(DATA_DIR, "PRESIDENTS")
DB_PATH = os.path.join(PRESIDENTS_DIR, "presidents.duckdb")
QUERIES_PATH = os.path.join(PRESIDENTS_DIR, "queries_presidents.sql")

GALOIS_RESULTS_DIR = os.path.join(DATA_DIR, ".galois_output")
BASELINE_RESULTS_DIR = os.path.join(DATA_DIR, ".output")

def find_data_dir(start_path):
    current = start_path
    while True:
        possible_data = os.path.join(current, "data")
        if os.path.isdir(possible_data):
            return possible_data

        parent = os.path.dirname(current)
        if parent == current:  # Raggiunta la root del filesystem
            return None
        current = parent


DATA_DIR = find_data_dir(SCRIPT_DIR)
if not DATA_DIR:
    print("ATTENZIONE: Non sono riuscito a trovare la cartella 'data/' risalendo da questo script.")
    # Fallback: assume che lo script venga lanciato dalla root
    DATA_DIR = os.path.abspath("data")

# Percorsi output risultati
GALOIS_RESULTS_DIR = os.path.join(DATA_DIR, ".galois_output")
BASELINE_RESULTS_DIR = os.path.join(DATA_DIR, ".output")

print(f"--- CONFIGURAZIONE ---")
print(f"Script Dir: {SCRIPT_DIR}")
print(f"Data Dir found: {DATA_DIR}")
print(f"DB Path: {DB_PATH}")
print(f"----------------------")


# ---------------------------------------------------------
# 2. LOGICA DI CLASSIFICAZIONE DEL PAPER (Exp-4)
# ---------------------------------------------------------

def categorize_query_id(q_id):
    """
    Logica Esperimento 4:
    - Rarity: USA (Popular) vs Venezuela (Rare)
    - Temporality: Recent vs Past vs All-Time
    """
    # Venezuela (1-13)
    if 1 <= q_id <= 13:
        country = "Venezuela"
        if q_id in [8, 10, 13]: return country, "Recent"
        if q_id in [11, 12]: return country, "Past"
        return country, "All-Time"
    # USA (14-26)
    elif 14 <= q_id <= 26:
        country = "USA"
        if q_id in [18, 21, 24, 26]: return country, "Recent"
        if q_id in [23, 25]: return country, "Past"
        return country, "All-Time"
    return "Unknown", "Unknown"


def load_queries_map(file_path):
    q_map = {}
    idx = 1
    if not os.path.exists(file_path):
        print(f"WARNING: File query non trovato in {file_path}")
        return {}

    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read().strip()
        for q in content.split(";"):
            if q.strip():
                lines = q.strip().split('\n')
                clean_sql = '\n'.join([l for l in lines if not l.strip().startswith('--')]).strip()
                if clean_sql:
                    q_map[idx] = clean_sql
                    idx += 1
    return q_map


def discover_result_folders():
    """
    Trova le CARTELLE che contengono i file query*.json.
    Target specifico: Sottocartella 'PRESIDENTS'.
    """
    found_folders = []

    print("\n--- DISCOVERY (Target: PRESIDENTS) ---")

    # 1. Cerca GALOIS (.galois_output/<version>/PRESIDENTS/query*.json)
    if os.path.exists(GALOIS_RESULTS_DIR):
        for version in os.listdir(GALOIS_RESULTS_DIR):
            # Costruisce il path: data/.galois_output/GALOIS_F/PRESIDENTS
            if "EXP5" in version.upper():
                continue
            target_path = os.path.join(GALOIS_RESULTS_DIR, version, "PRESIDENTS")

            if os.path.isdir(target_path):
                # Conta i file json
                files = glob.glob(os.path.join(target_path, "query*.json"))
                if files:
                    print(f"  > Trovato GALOIS: {version} ({len(files)} files)")
                    found_folders.append((version.upper(), target_path))

    # 2. Cerca BASELINES (.output/<version>/watsonx/PRESIDENTS/query*.json)
    if os.path.exists(BASELINE_RESULTS_DIR):
        for version_folder in os.listdir(BASELINE_RESULTS_DIR):
            # Esclude cartelle di dataset che potrebbero essere nella root di .output
            if version_folder in ['PRESIDENTS', 'FLIGHT', 'GEO']:
                continue

            # Costruisce il path: data/.output/.nl_output/watsonx/PRESIDENTS
            target_path = os.path.join(BASELINE_RESULTS_DIR, version_folder, "watsonx", "PRESIDENTS")

            if os.path.isdir(target_path):
                files = glob.glob(os.path.join(target_path, "query*.json"))
                if files:
                    # Pulisce il nome (es. .nl_output -> NL)
                    method_name = version_folder.replace(".", "").replace("_output", "").upper()
                    print(f"  > Trovato BASELINE: {method_name} ({len(files)} files)")
                    found_folders.append((method_name, target_path))

    print("--------------------------------------\n")
    return found_folders


def normalize_data_keys(data_list):
    """
    Pulisce i nomi delle colonne per allinearli tra Ground Truth e Predizioni.
    - Rimuove prefissi (es. 'p.name' -> 'name', 'target.party' -> 'party')
    - Rende tutto minuscolo (es. 'NAME' -> 'name')
    """
    if not data_list:
        return []

    normalized = []
    for row in data_list:
        # Se la riga non è un dizionario (es. lista di valori), la lasciamo stare
        if not isinstance(row, dict):
            normalized.append(row)
            continue

        new_row = {}
        for k, v in row.items():
            # Prende solo l'ultima parte dopo il punto e converte in minuscolo
            clean_key = k.split('.')[-1].lower()
            new_row[clean_key] = v
        normalized.append(new_row)
    return normalized


def calculate_metrics_row(gt_data, pred_data, q_id, method_name):
    # 1. NORMALIZZAZIONE CHIAVI (Cruciale per evitare 0.0 su 'p.name' vs 'name')
    gt_clean = normalize_data_keys(gt_data)
    pred_clean = normalize_data_keys(pred_data)

    # Parsing
    gt_cols, gt_rows = _parse_table_from_obj(gt_clean)
    pr_cols, pr_rows = _parse_table_from_obj(pred_clean)

    # Calcolo Metriche
    f1 = f1_cell_similarity(gt_cols, gt_rows, pr_cols, pr_rows)
    card = cardinality(gt_rows, pr_rows)
    tcon = tuple_constraint(gt_rows, pr_rows)

    avg = (f1 + card + tcon) / 3.0

    # --- DEBUGGING PER I RISULTATI 0.0 ---
    # Se la Ground Truth ha dati ma il risultato è 0, stampiamo perché
    # (Lo limitiamo alle prime righe per non intasare la console)
    if avg == 0.0 and len(gt_rows) > 0 and len(pr_rows) > 0:
        # Stampiamo solo una volta per metodo per evitare spam
        if not hasattr(calculate_metrics_row, "logged_methods"):
            calculate_metrics_row.logged_methods = set()

        if method_name not in calculate_metrics_row.logged_methods:
            print(f"\n[DEBUG {method_name}] Query {q_id} -> Score 0.0!")
            print(f"   GT Keys (sample): {list(gt_clean[0].keys()) if gt_clean else 'Empty'}")
            print(f"   PRED Keys (sample): {list(pred_clean[0].keys()) if pred_clean else 'Empty'}")
            print(f"   GT Row[0]: {list(gt_rows[0]) if gt_rows else ''}")
            print(f"   PRED Row[0]: {list(pr_rows[0]) if pr_rows else ''}")
            calculate_metrics_row.logged_methods.add(method_name)

    return avg, card


def load_results_from_folder(folder_path):
    """
    CORREZIONE: Legge i file DENTRO la cartella, non la cartella stessa.
    """
    results_map = {}

    if not os.path.exists(folder_path):
        return results_map

    # Itera sui file nella cartella
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)

        # Ignora sottocartelle
        if os.path.isdir(file_path): continue

        # Filtra solo i json delle query
        if not filename.lower().endswith(".json") or not filename.lower().startswith("query"):
            continue

        # Estrae l'ID (query12.json -> 12)
        match = re.search(r'query(\d+)\.json', filename)
        if not match: continue
        q_id = int(match.group(1))

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            # Normalizza result set
            res = data.get("result_set", data.get("tuples", []))
            results_map[q_id] = res
        except Exception:
            pass  # Ignora file corrotti

    return results_map


# ---------------------------------------------------------
# 3. ESECUZIONE
# ---------------------------------------------------------
def main():
    print(f"--- AVVIO ANALISI EXPERIMENT 4 ---")

    if not os.path.exists(DB_PATH):
        print(f"ERRORE CRITICO: DB non trovato in {DB_PATH}")
        return

    con = duckdb.connect(DB_PATH)
    try:
        con.execute("SELECT 1 FROM target.world_presidents LIMIT 1")
    except Exception:
        print("ERRORE: Tabella 'world_presidents' vuota o inesistente. Esegui init_db.py")
        return

    sql_map = load_queries_map(QUERIES_PATH)
    print(f"Query caricate: {len(sql_map)}")

    experiments = discover_result_folders()
    if not experiments:
        print("NESSUNA CARTELLA RISULTATI TROVATA.")
        return

    all_metrics = []

    for method_name, folder_path in experiments:
        results_map = load_results_from_folder(folder_path)

        for q_id, sql in sql_map.items():
            if q_id not in results_map: continue

            clean_sql = sql.replace("target.", "")
            try:
                cursor = con.execute(clean_sql)
                col_names = [d[0] for d in cursor.description]
                gt_data = [dict(zip(col_names, row)) for row in cursor.fetchall()]
            except Exception:
                gt_data = []

            pred_data = results_map[q_id]

            # Passiamo ID e Metodo per il debug
            avg, card = calculate_metrics_row(gt_data, pred_data, q_id, method_name)

            country, time_cat = categorize_query_id(q_id)

            all_metrics.append({
                'Method': method_name,
                'Country': country,
                'Time': time_cat,
                'AVG_SCORE': avg,
                'Cardinality': card
            })

    if not all_metrics:
        print("Nessuna metrica calcolata.")
        return

    df = pd.DataFrame(all_metrics)

    print("\n" + "=" * 60)
    print("TABLE 6 REPLICATION: IMPACT OF RARITY (AVG-SCORE)")
    print("=" * 60)
    try:
        tbl6 = df.groupby(['Method', 'Country'])['AVG_SCORE'].mean().unstack(level=0)
        print(tbl6)
    except Exception:
        print("Errore Tabella 6")

    print("\n" + "=" * 60)
    print("TABLE 7 REPLICATION: IMPACT OF TEMPORALITY (AVG-SCORE)")
    print("=" * 60)
    try:
        tbl7 = df.groupby(['Method', 'Time'])['AVG_SCORE'].mean().unstack(level=0)
        sorter = ['Recent', 'Past', 'All-Time']
        available = [s for s in sorter if s in tbl7.index]
        tbl7 = tbl7.reindex(available)
        print(tbl7)
    except Exception:
        print("Errore Tabella 7")


if __name__ == "__main__":
    main()