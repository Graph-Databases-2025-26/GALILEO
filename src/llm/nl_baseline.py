from time import time
from dotenv import load_dotenv
from src.llm.baseline_tools import parse_llm_response, save_baseline_to_json, build_lcel_chain
from src.llm.llm_factory import get_llm_wrapper
#from src.main import parse_args
from ..utils.constants import *
from ..db.run_queries_to_json import load_queries_from_folder, load_nl_queries_from_txt
from config import Config_Loader
from ..utils.dataset_selection import get_dataset_selection
from ..utils.logging_config import logger, LOG
from src.utils.constants import DATA_DIR, DATASETS

#CONFIGURE THE API KEY
load_dotenv()
api_key = os.getenv("WATSONX_API_KEY", "").strip()

"""
    The goal of this script is to query the LLM model with interrogations in Natural Language
    receive the answer from the LLM model and stores it.
    For query the LLM will be used the sql_to_nl script for converting the query took from the queries_*.sql files of each dataset in a prompt in natural language.

"""

def llm_interaction_nl_baseline(config, database: str, prompts: list[str], b_type: str, d_type: str):

    logger.info(f"baseline: {b_type}, dataset type: {d_type}")
    llm = get_llm_wrapper(config)
    chain = build_lcel_chain(llm,b_type, d_type)

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

            raw_response = chain.invoke({"database": database.lower(), "b_type": b_type, "query": sql_query, "prompt":prompt, "BASELINE": d_type })

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
            d_type = "IK"
        elif d in MC_DATASETS:
            d_type = "MC"
        else:
            LOG.debug(f"You need to specify a valid dataset: for the NL &SQL baselines you need to specify one or more of these datasets: {IK_DATASETS}, and for testing the Palimpzest: {MC_DATASETS}")

        logger.info(f"Checking folder: {d}")
        if d in DATASETS:
            dataset_path = os.path.join(DATA_DIR, d)
            print(dataset_path)
            duckdb_path = os.path.join(DATA_DIR, d, f"{d.lower()}.duckdb")
            if not os.path.isdir(dataset_path):
                continue
            logger.info(f"Processing dataset: {d}")

            nl_queries = load_nl_queries_from_txt(dataset_path)
            llm_interaction_nl_baseline(config, d, nl_queries, "NL", d_type)
