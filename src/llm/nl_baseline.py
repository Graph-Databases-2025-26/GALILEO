import os
import json
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
    """
    Iterative NL baseline:
    - For each NL prompt (paired with its SQL query) we call the LLM up to max_iter times.
    - We aggregate result rows across iterations and remove duplicates.
    - Each subsequent iteration receives the already-returned rows in the prompt, so the LLM
      is asked to return only *new* tuples or an empty result_set if none are left.
    """

    logger.info(f"baseline: {b_type}")
    llm = get_llm_wrapper(config)
    chain = build_lcel_chain(llm, b_type)

# Max number of LLM iterations per prompt from config.yaml
    max_iter = config.galois_execution.max_iter

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

        all_rows = []        # merged rows across iterations
        seen_rows = set()    # hashable representation of rows to avoid duplicates
        total_time = 0.0
        total_tokens = 0

        # Keep a copy of the original natural-language prompt
        base_prompt = prompt

        for iteration in range(1, max_iter + 1):
            LOG.info(f"[NL baseline] Iteration {iteration}/{max_iter} for current prompt.")

            # Build iteration-specific prompt
            if iteration == 1:
                iteration_prompt = base_prompt
            else:
                if not all_rows:
                    LOG.info(
                        "No rows were returned in previous iterations; "
                        "stopping NL iterations for this prompt."
                    )
                    break

                already_rows_json = json.dumps(all_rows, ensure_ascii=False, indent=2)
                iteration_prompt = (
                    f"{base_prompt}\n\n"
                    "The rows below have ALREADY been returned in previous responses and "
                    "MUST NOT be repeated.\n"
                    "If there are additional correct rows, answer ONLY with those new rows "
                    "using the same JSON structure.\n"
                    "If there are no more rows, return an empty result_set.\n"
                    f"ALREADY RETURNED ROWS (JSON):\n{already_rows_json}\n"
                )

            payload = {
                "database": database.lower(),
                "b_type": b_type,
                "query": sql_query,
                "prompt": iteration_prompt,
            }

        t_start = time()
        try:
            raw_response = invoke_with_backoff(chain, payload)
        except Exception as e:
            LOG.error(f"LLM invocation error in NL baseline: {e}")
            break
        t_end = time()


        result = parse_llm_response(raw_response, t_end - t_start, llm)
        rows = result.get("result_set", [])

# If LLM returns empty result_set, nothing more to add
        if not rows:
            LOG.info(
                f"Empty result_set returned by LLM at iteration {iteration}; "
                "stopping NL iterations for this prompt."
            )
            break
        
        # Deduplicate vs all previously seen rows
        new_rows = []
        for row in rows:
            key = tuple(sorted(row.items()))
            if key not in seen_rows:
                seen_rows.add(key)
                all_rows.append(row)
                new_rows.append(row)

        LOG.info(
            f"NL iteration {iteration}: "
            f"{len(rows)} rows returned, {len(new_rows)} new rows after deduplication."
        )

        if not new_rows:
            LOG.info(
                "All rows in this NL iteration were duplicates; "
                "no new rows to add. Stopping iterations for this prompt."
            )
            break

        total_time += result.get("time", 0.0)
        total_tokens += result.get("tokens", 0)

        # Aggregated FULL_JSON result for this NL prompt / SQL query pair
        aggregated_result = {
            "result_set": all_rows,
            "time": round(total_time, 3),
            "tokens": total_tokens,
        }
        results.append(aggregated_result)


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
