import streamlit as st
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
from src.utils.logging_config import LOG

st.set_page_config(page_title="Galois Framework Demo", layout="wide")


# --- CUSTOM SINK FOR LOGURU ---
class StreamlitSink:
    """
    A class that acts like a file-like object for Loguru.
    It accumulates logs in a list instead of writing them to disk/console.
    """

    def __init__(self):
        self.logs = []

    def write(self, message):
        # Loguru passes the message as a (already formatted) string
        self.logs.append(message)

    def get_content(self):
        return "".join(self.logs)


def run_galois_demo(variant_key):
    # 1. Create our "bucket" for logs
    sink = StreamlitSink()

    # 2. ATTACH LOGURU
    # Define a custom filter:
    # - "src": "DEBUG" -> Show all debug from your code
    # - "": "WARNING"  -> For everything else (httpx, ibm, etc), show only serious errors
    log_filter = {
        "src": "DEBUG",
        "": "WARNING"
    }

    # LOG.add returns a numeric ID used to remove the handler later
    handler_id = LOG.add(
        sink.write,
        format="{time:HH:mm:ss} | {level: <8} | {name}:{line} - {message}",
        level="DEBUG",  # Global minimum level (then filtered by the dict above)
        filter=log_filter,
        colorize=False  # Streamlit does not support ANSI colors in the standard code block
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

        # Execution (with debug=True to enable internal logs)
        results, stats, debug_info = strategies[variant_key](debug=True)

        # Retrieve accumulated text
        captured_logs = sink.get_content()

    finally:
        # 3. ESSENTIAL CLEANUP
        # Remove the sink to avoid duplicate/triple logs if the button is pressed again
        LOG.remove(handler_id)

    return sql_query, results, stats, debug_info, captured_logs


# --- STREAMLIT INTERFACE ---
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

            # 1. EXECUTION LOGS
            st.subheader("📟 Execution Logs (Backend Trace)")
            with st.expander("View Detailed Logs (Terminal Style)", expanded=True):
                if not logs.strip():
                    st.warning("⚠️ No logs captured. Verify that src modules are using 'LOG.debug()' or 'LOG.info()'")
                else:
                    # Use language="bash" or "log" to give some color to timestamps/info
                    st.code(logs, language="bash")

            st.divider()

            # 2. Parsing and Stats
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