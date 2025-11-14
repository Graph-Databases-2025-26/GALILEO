from .baseline_tools import build_lcel_chain, parse_llm_response, save_baseline_to_json
from .llm_factory import get_llm_wrapper

from src.utils import LOG, ERR_LLM_PARSING_FAILURE
from src.config import AppConfig

from time import time

def execute_baseline_sql(config: AppConfig, database: str, queries: list[str], b_type: str):
    """
    Executes a series of SQL (or NL/PZ) queries against an LLM to generate a baseline result set and saves the results.

    It initializes the LLM wrapper, builds the LCEL chain, iterates through  the queries, times the LLM invocation,
    and parses the response.

    Args:
        config: The application configuration object.
        database: The name of the database/dataset being queried.
        queries: A list of string queries (SQL or NL questions) to execute.
        b_type: The type of baseline run ("SQL", "NL", or "PZ").
    """
       
    llm_model = get_llm_wrapper(config)   
       
    lcel_chain = build_lcel_chain(llm_model, b_type)   
       
    results = []
    for qry in queries:
        
        LOG.info(f"Executing baseline SQL query: {qry}")
        
        t_start = time()
        try:

            raw_response= lcel_chain.invoke({"database": database, "query": qry, "b_type": b_type})

            t_end = time()
            
            result = parse_llm_response(raw_response, t_end - t_start, llm_model)
            results.append(result)
            
        except Exception as e:
            LOG.error(ERR_LLM_PARSING_FAILURE.format(e))
        
    save_baseline_to_json(database, results, llm_model, b_type)
  