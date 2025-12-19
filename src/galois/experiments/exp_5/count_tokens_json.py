import json
import os
import statistics
from collections import defaultdict
from pathlib import Path
from src.utils.constants import *
# ==========================================
# CONFIGURAZIONE AUTOMATICA PERCORSI
# ==========================================

# Cartella dove si trova questo script (Root del progetto)
SCRIPT_DIR = Path(__file__).resolve().parent

# Percorso dei risultati basato sulla tua struttura (data/.galois_output)
# Cerchiamo la cartella "data" nella root, poi ".galois_output"



def generate_summary():
    print(f"--- RICERCA DATI ---")
    print(f"Base: {SUBMISSIONS_PATH_GALOIS}")

    if not SUBMISSIONS_PATH_GALOIS.exists():
        print(f"[ERRORE] La cartella {SUBMISSIONS_PATH_GALOIS} non esiste.")
        print("Assicurati di eseguire lo script dalla root del progetto (dove c'è la cartella 'data').")
        return

    # Trova la cartella dell'esperimento 5 (es. GALOIS_EXP5 o GALOIS_EXP5_openai)
    # Prendiamo la prima che inizia con GALOIS_EXP5
    exp_dirs = sorted(list(SUBMISSIONS_PATH_GALOIS.glob("GALOIS_EXP5*")))

    if not exp_dirs:
        print("[ERRORE] Nessuna cartella che inizia con 'GALOIS_EXP5' trovata in output.")
        return

    # Usa la cartella più recente o la prima trovata
    target_dir = exp_dirs[0]
    print(f"Analisi cartella esperimento: {target_dir.name}")

    tokens_by_category = defaultdict(list)
    files_processed = 0

    # Scansiona ricorsivamente tutte le sottocartelle (FLIGHT, GEO, ecc.)
    for json_file in target_dir.rglob("query*.json"):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Verifica struttura: {"complexity": "...", "tokens": 123, ...}
            if "complexity" in data and "tokens" in data:
                cat = data["complexity"]
                tok = data["tokens"]

                if isinstance(tok, (int, float)):
                    tokens_by_category[cat].append(tok)
                    files_processed += 1
        except Exception:
            continue

    if files_processed == 0:
        print("[ERRORE] Nessun file JSON valido trovato nelle sottocartelle!")
        return

    print(f"File processati con successo: {files_processed}")

    # ==========================================
    # CALCOLO E SALVATAGGIO
    # ==========================================

    ordered_cats = ["SP2", "SP2>", "DIST", "AGGR", "G/O", "JOIN"]
    summary_data = {}

    print(f"\n{'CATEGORIA':<10} | {'AVG TOKENS':<10} | {'COUNT':<5}")
    print("-" * 35)

    for cat in ordered_cats:
        values = tokens_by_category.get(cat, [])
        if values:
            avg = statistics.mean(values)
            summary_data[cat] = avg
            print(f"{cat:<10} | {avg:<10.1f} | {len(values):<5}")
        else:
            summary_data[cat] = 0.0
            print(f"{cat:<10} | {0.0:<10.1f} | 0")

    # Salva il file JSON nella stessa cartella dello script
    output_file = SCRIPT_DIR / "exp5_tokens_summary.json"

    try:
        with open(output_file, "w") as f:
            json.dump(summary_data, f, indent=4)
        print(f"\n[OK] File di riepilogo creato: {output_file.name}")
        print(f"Percorso: {output_file}")
    except Exception as e:
        print(f"\n[ERRORE] Scrittura file fallita: {e}")


if __name__ == "__main__":
    generate_summary()