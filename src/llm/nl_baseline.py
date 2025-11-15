from time import time

from src.config import Config_Loader
from src.llm.baseline_tools import parse_llm_response, save_baseline_to_json, build_lcel_chain
from src.llm.llm_factory import get_llm_wrapper
from src.utils.invoke_with_backoff import invoke_with_backoff
from ..db.run_queries_to_json import load_queries_from_folder, load_nl_queries_from_txt
# from src.main import parse_args
from ..utils.constants import *
from ..utils.logging_config import logger, LOG
from ..utils.main_tools import get_dataset_selection

"""
    The goal of this script is to query the LLM model with interrogations in Natural Language
    receive the answer from the LLM model and stores it.
    For query the LLM will be used the sql_to_nl script for converting the query took from the queries_*.sql files of each dataset in a prompt in natural language.

"""

def llm_interaction_nl_baseline(config, database: str, prompts: list[str], b_type: str):

    logger.info(f"baseline: {b_type}")
    llm = get_llm_wrapper(config)
    chain = build_lcel_chain(llm, b_type)

    folder = os.path.join(DATA_DIR, database)

    queries = load_queries_from_folder(folder)
    assert len(prompts) == len(queries), (
        f"Mismatch: {len(prompts)} prompts vs {len(queries)} queries in {database}"
    )

    results = []
    for prompt, sql_query in zip(prompts, queries):

        if isinstance(sql_query, tuple):
            sql_query = sql_query[1]

        LOG.info(f"Executing baseline NL prompts: {prompt} with corresponding SQL query: {sql_query}")

        t_start = time()
        try:

            payload = {"database": database.lower(), "b_type": b_type, "query": sql_query, "prompt":prompt}
            raw_response = invoke_with_backoff(chain, payload)

            t_end = time()

            result = parse_llm_response(raw_response, t_end - t_start, llm)
            results.append(result)

        except Exception as e:
            LOG.error(f"LLM/Parsing Error: {e}. Failed to parse required JSON structure.")

    save_baseline_to_json(database, results, llm, b_type)

if __name__ == "__main__":
    config = Config_Loader().get_config()
    #args = parse_args()
    d_type=""
    datasets = get_dataset_selection(config.database.run)
    for d in datasets:

        d = d.upper()
        if d in IK_DATASETS:
            b_type = "NL"
        elif d in MC_DATASETS:
            b_type = "PZ"
        else:
            LOG.debug(f"You need to specify a valid dataset: for the NL &SQL baselines you need to specify one or more of these datasets: {IK_DATASETS}, and for testing the Palimpzest: {MC_DATASETS}")

        LOG.info(f"Checking folder: {d}")
        if d in DATASETS:
            dataset_path = os.path.join(DATA_DIR, d)
            print(dataset_path)
            duckdb_path = os.path.join(DATA_DIR, d, f"{d.lower()}.duckdb")
            if not os.path.isdir(dataset_path):
                continue
            LOG.info(f"Processing dataset: {d}")

            nl_queries = load_nl_queries_from_txt(dataset_path)
            llm_interaction_nl_baseline(config, d, nl_queries, b_type)
