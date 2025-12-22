import streamlit as st
import json
import pandas as pd
import sys
import os

# --- IMPORT FIX ---
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from src.config.loaders import Config_Loader
from src.utils.constants import debug_query
from src.utils import sql_query_parser
from src.galois.galois import Galois
# Importiamo LOG direttamente dal tuo config
from src.utils.logging_config import LOG

st.set_page_config(page_title="Galois Framework Demo", layout="wide")


# --- CUSTOM SINK PER LOGURU ---
class StreamlitSink:
    """
    Una classe che agisce come 'file-like object' per Loguru.
    Accumula i log in una lista invece che scriverli su disco/console.
    """

    def __init__(self):
        self.logs = []

    def write(self, message):
        # Loguru passa il messaggio come stringa (già formattata)
        self.logs.append(message)

    def get_content(self):
        return "".join(self.logs)


def run_galois_demo(variant_key):
    # 1. Creiamo il nostro "secchio" per i log
    sink = StreamlitSink()

    # 2. AGGANCIAMO LOGURU
    # Definiamo un filtro custom:
    # - "src": "DEBUG" -> Mostra tutto il debug del tuo codice
    # - "": "WARNING"  -> Per tutto il resto (httpx, ibm, etc), mostra solo errori gravi
    log_filter = {
        "src": "DEBUG",
        "": "WARNING"
    }

    # LOG.add restituisce un ID numerico che serve per rimuovere l'handler dopo
    handler_id = LOG.add(
        sink.write,
        format="{time:HH:mm:ss} | {level: <8} | {name}:{line} - {message}",
        level="DEBUG",  # Livello minimo globale (poi filtrato dal dizionario sopra)
        filter=log_filter,
        colorize=False  # Streamlit non supporta i colori ANSI nel blocco code standard
    )

    try:
        cfg = Config_Loader().get_config()
        dataset = "WORLD"
        #sql_query = debug_query().format()
        sql_query = """SELECT distinct t2.region
                        FROM target.country_language AS t1
                        JOIN target.country AS t2
                        ON t1.country_code_3_letters = t2.code_3_letters
                        WHERE t1.language = 'English'
                        OR t1.language = 'Dutch';"""

        g = Galois(
            config=cfg,
            dataset=dataset,
            sql_query=sql_query,
            physical_strategy="auto"
        )

        strategies = {
            "WO": g.run_no_push,
            "A": g.run_push_all,
            "S": g.run_push_selective,
            "F": g.run_push_confident,
        }

        # Esecuzione (con debug=True che attiva i log interni)
        results, stats, debug_info = strategies[variant_key](debug=True)

        # Recuperiamo il testo accumulato
        captured_logs = sink.get_content()

    finally:
        # 3. PULIZIA FONDAMENTALE
        # Rimuoviamo il sink per non avere log doppi/tripli se ripremi il bottone
        LOG.remove(handler_id)

    return sql_query, results, stats, debug_info, captured_logs


# --- INTERFACCIA STREAMLIT ---
st.title("🛡️ Galois Framework Interactive Demo")
st.markdown("Analysis of SQL query optimization on unstructured data via LLM.")

st.subheader("📝 SQL Query Input")
st.code(debug_query().format(), language="sql")

with st.sidebar:
    st.header("Configuration")
    st.info(f"Dataset: WORLD")
    variant = st.selectbox(
        "Execution Strategy",
        ["WO", "A", "S", "F"],
        format_func=lambda x: {
            "WO": "WO (Without Optimization)",
            "A": "A (Total Pushdown)",
            "S": "S (Selective LLM)",
            "F": "F (Full Confident - Hybrid)"
        }[x]
    )
    run_btn = st.button("Execute Query", type="primary")

if run_btn:
    with st.spinner(f"Executing strategy {variant}..."):
        try:
            sql_q, res, stats, d_info, logs = run_galois_demo(variant)

            # 1. LOG DI ESECUZIONE
            st.subheader("📟 Execution Logs (Backend Trace)")
            with st.expander("View Detailed Logs (Terminal Style)", expanded=True):
                if not logs.strip():
                    st.warning("⚠️ No logs captured. Verify that src modules are using 'LOG.debug()' or 'LOG.info()'")
                else:
                    # Usiamo language="bash" o "log" per dare un po' di colore ai timestamp/info
                    st.code(logs, language="bash")

            st.divider()

            # 2. Parsing e Stats
            c1, c2 = st.columns(2)
            with c1:
                st.subheader("🔍 Parser Output")
                st.json(sql_query_parser.parse_sql(sql_q))
            with c2:
                st.subheader("📊 Performance Stats")
                m1, m2, m3 = st.columns(3)
                m1.metric("Rows", len(res))
                m2.metric("Latency", f"{stats.get('total_time', 0):.2f}s")
                m3.metric("LLM Tokens", stats.get('total_tokens', 0))

            # 3. Logic Info
            with st.expander("🧠 Internal Optimizer Logic (JSON)", expanded=False):
                st.json(d_info)

            # 4. Results
            st.subheader("💾 Results Preview")
            if res:
                st.dataframe(pd.DataFrame(res).head(10), use_container_width=True)
            else:
                st.warning("No results found.")

        except Exception as e:
            st.error(f"Error during execution: {e}")
            st.exception(e)
else:
    st.info("Select a variant and press 'Execute'.")