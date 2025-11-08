from .baseline_tools import build_lcel_chain, parse_llm_response, save_baseline_to_json
from .llm_factory import get_llm_wrapper
from src.utils import LOG
from time import time

def execute_baseline_sql_query(config, database: str, queries: list[str], b_type: str, d_type: str):
       
    llm_model = get_llm_wrapper(config)   
       
    lcel_chain = build_lcel_chain(llm_model, b_type, d_type)   
       
    results = []
    for qry in queries:
        
        LOG.info(f"Executing baseline SQL query: {qry}")
        
        t_start = time()
        try:

            raw_response= lcel_chain.invoke({"database": database, "query": qry, "BASELINE": d_type})

            t_end = time()
            
            result = parse_llm_response(raw_response, t_end - t_start, llm_model)
            results.append(result)
            
        except Exception as e:
            LOG.error(f"LLM/Parsing Error: {e}. Failed to parse required JSON structure.")
        
    save_baseline_to_json(database, results, llm_model, b_type)
  