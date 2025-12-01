import json
from time import time

from .baseline_tools import build_lcel_chain, parse_llm_response, save_baseline_to_json
from .llm_factory import get_llm_wrapper

from src.utils import LOG, ERR_LLM_PARSING_FAILURE
from src.config import AppConfig

from time import time

def execute_baseline_sql(config: AppConfig, database: str, queries: list[str], b_type: str):
    """
    Executes a series of SQL (or NL/PZ) queries against an LLM to generate a baseline result set and saves the results.

    Iterative baseline:
    - For each query, it repeatedly calls the LLM, asking for additional tuples.
    - It aggregates results across iterations, avoiding duplicates.
    - It stops when no new tuples are produced or when the configured max_iter is reached.

    Args:
        config: The application configuration object.
        database: The name of the database/dataset being queried.
        queries: A list of string queries (SQL or NL questions) to execute.
        b_type: The type of baseline run ("SQL", "NL", or "PZ").
    """
    
     # Read the maximum number of LLM iterations per query from the configuration
    max_iter = config.galois_execution.max_iter

    llm_model = get_llm_wrapper(config)   
       
    lcel_chain = build_lcel_chain(llm_model, b_type)   
       
    results = []
    for qry in queries:
        LOG.info(f"Executing baseline SQL query: {qry}")

        # Accumulators for the iterative execution of this single query
        all_rows = []        # merged rows across iterations
        seen_rows = set()    # hashable representation of rows to avoid duplicates
        total_time = 0.0     # total latency across all iterations
        total_tokens = 0     # total tokens across all iterations

        # Iterate from 1 to max_iter (inclusive) for clearer logging
        for iteration in range(1, max_iter + 1):
            LOG.info(f"[SQL baseline] Iteration {iteration}/{max_iter} for current query.")

            # Build the query payload for this iteration
            if iteration == 1:
                # First iteration: use the original SQL query as-is
                payload_query = qry
            else:
                # If nothing has been collected so far, nothing to extend
                if not all_rows:
                    LOG.info(
                        "No rows were returned in previous iterations; "
                        "stopping SQL iterations for this query."
                    )
                    break

                # Provide already returned rows to the LLM to avoid duplicates
                already_rows_json = json.dumps(all_rows, ensure_ascii=False, indent=2)
                payload_query = (
                    f"{qry}\n\n"
                    "-- The following rows have already been returned and MUST NOT be repeated.\n"
                    "-- If there are additional correct rows, return ONLY those new rows.\n"
                    "-- If there are no more rows to add, return an empty result_set.\n"
                    f"-- ALREADY RETURNED ROWS (JSON):\n{already_rows_json}\n"
                )

            # Call the LLM with the iteration-specific payload_query
            try:
                t_start = time()
                raw_response = lcel_chain.invoke(
                    {"database": database, "query": payload_query, "b_type": b_type}
                )
                t_end = time()
            except Exception as e:
                LOG.error(ERR_LLM_PARSING_FAILURE.format(e))
                break

            # Parse this iteration response
            result = parse_llm_response(raw_response, t_end - t_start, llm_model)
            rows = result.get("result_set", [])

            # If the LLM returns an empty result_set, no more tuples are available
            if not rows:
                LOG.info(
                    f"Empty result_set returned by LLM at iteration {iteration}; "
                    "stopping SQL iterations for this query."
                )
                break

            # Deduplicate rows across all iterations
            new_rows = []
            for row in rows:
                # Build a hashable key independent of column order
                key = tuple(sorted(row.items()))
                if key not in seen_rows:
                    seen_rows.add(key)
                    all_rows.append(row)
                    new_rows.append(row)

            LOG.info(
                f"SQL iteration {iteration}: "
                f"{len(rows)} rows returned, {len(new_rows)} new rows after deduplication."
            )

            # If this iteration only produced duplicates, nothing more to fetch
            if not new_rows:
                LOG.info(
                    "All rows in this SQL iteration were duplicates; "
                    "no new rows to add. Stopping iterations for this query."
                )
                break

            # Accumulate time and tokens across iterations
            total_time += result.get("time", 0.0)
            total_tokens += result.get("tokens", 0)

        # Build the aggregated FULL_JSON result for this query
        aggregated_result = {
            "result_set": all_rows,
            "time": round(total_time, 3),
            "tokens": total_tokens,
        }
        results.append(aggregated_result)

    # Persist results for all queries to JSON files
    save_baseline_to_json(database, results, llm_model, b_type)