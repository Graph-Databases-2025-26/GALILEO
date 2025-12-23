import json
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

# --- CONFIGURAZIONE ---
INPUT_BASE_DIR = Path("data/.galois_output/GALOIS_EXP_7")
OUTPUT_IMAGE = "figure_10_comparison.png"

class ChartPlotter:
    @staticmethod
    def plot_figure_10(results_data: dict, output_filename):
        """
        Genera il grafico stile Figura 10 usando SOLO i dati forniti.
        """
        # Stile tipografico professionale
        plt.rcParams['font.family'] = 'serif'
        plt.rcParams['font.size'] = 12
        
        fig, ax = plt.subplots(figsize=(9, 5))
        
        # Palette colori distinti per le tue linee
        colors = ["#009E73", "#D55E00", "#0072B2", "#CC79A7", "#F0E442", "#56B4E9"]
        markers = ["D", "o", "s", "^", "v", "P"]
        
        idx = 0
        for model_name, data_points in results_data.items():
            try:
                # 1. Crea una mappa { float: string_key } per evitare KeyError
                # Questo gestisce il fatto che JSON ha chiavi stringa ("1.0") ma noi ordiniamo float (1.0)
                float_to_key_map = {float(k): k for k in data_points.keys()}
                
                # 2. Ordina le soglie decrescenti (1.0 -> 0.0)
                sorted_taus = sorted(float_to_key_map.keys(), reverse=True)
                
                # 3. Recupera gli score usando la chiave originale
                scores = [data_points[float_to_key_map[t]] for t in sorted_taus]
                
                if not scores:
                    continue

                # Assegna stile ciclico
                color = colors[idx % len(colors)]
                marker = markers[idx % len(markers)]
                idx += 1

                # Plot Linea
                ax.plot(sorted_taus, scores, 
                        label=model_name,
                        color=color,
                        linestyle="-",
                        linewidth=2.5,
                        alpha=0.9,
                        marker=marker,
                        markersize=8,
                        markeredgecolor='black',
                        markerfacecolor=color)

                # Plot Punto Ottimale (X Nera)
                best_idx = np.argmax(scores)
                ax.scatter([sorted_taus[best_idx]], [scores[best_idx]], 
                           marker='x', color='black', s=120, linewidth=2.5, zorder=10)
                           
            except Exception as e:
                print(f"Skipping model {model_name} due to data error: {e}")
                continue

        # Configurazione Assi (Stile Paper)
        ax.set_xlabel(r"$Table-Scan < \tau < Key-Scan$", fontsize=13, style='italic')
        ax.set_ylabel("AVG-SCORE", fontsize=13)
        
        # Inverti asse X: 1.0 (Table) a SX -> 0.0 (Key) a DX
        ax.set_xlim(1.05, -0.05) 
        
        # Ticks espliciti
        ax.set_xticks([1, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0])
        ax.set_xticklabels(["1", "0.9", "0.8", "0.7", "0.6", "0.5", "0.4", "0.3", "0.2", "0.1", "0"])
        
        # Griglia
        ax.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Legenda -> MODIFICATA QUI
        # Ho cambiato -0.25 in -0.35 per spostarla più in basso
        ax.legend(loc='lower center', bbox_to_anchor=(0.5, -0.35), ncol=3, frameon=True)
        
        # Aumenta leggermente il padding inferiore per far spazio alla legenda
        plt.subplots_adjust(bottom=0.25)
        plt.savefig(output_filename, dpi=300, bbox_inches="tight")
        print(f"Chart saved successfully as {output_filename}")

def main():
    if not INPUT_BASE_DIR.exists():
        print(f"Directory {INPUT_BASE_DIR} does not exist. Run 'run_exp_7.py' first.")
        return

    # 1. Raccogli i dati dai file JSON
    collected_data = {}
    
    # Cerca tutti i file results.json nelle sottocartelle
    result_files = list(INPUT_BASE_DIR.rglob("results.json"))
    
    if not result_files:
        print("No 'results.json' files found in output directory.")
        return
    
    print(f"Found {len(result_files)} experiment results.")
    
    for f in result_files:
        try:
            with open(f, 'r') as json_file:
                data = json.load(json_file)
                # Usa il nome modello salvato nel JSON, o il nome della cartella come fallback
                m_name = data.get("model_name", f.parent.name)
                m_results = data.get("results", {})
                
                if m_results:
                    collected_data[m_name] = m_results
                    print(f" - Loaded: {m_name}")
        except Exception as e:
            print(f"Error loading {f}: {e}")

    if not collected_data:
        print("No valid data found to plot.")
        return

    # 2. Genera Plot
    ChartPlotter.plot_figure_10(collected_data, OUTPUT_IMAGE)

if __name__ == "__main__":
    main()