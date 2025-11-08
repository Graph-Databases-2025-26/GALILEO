from .baseline_tools import build_lcel_chain, parse_llm_response
from src.utils import LOG
from time import time

def execute_IK_baseline_sql_query(config, database, queries: list[str]) -> list[dict]:
    
    results = []
    for qry in queries:
        
        LOG.info(f"Executing baseline SQL query: {qry}")
        
        lcel_chain = build_lcel_chain(config, database, qry, "IK")
        
        t_start = time()
        try:

            raw_response= lcel_chain.invoke({"database" : database, "query" : qry, "BASELINE" : "IK"})

            t_end = time()
            
            result = parse_llm_response(raw_response, t_end - t_start)
            results.append(result)
            
        except Exception as e:
            LOG.error(f"LLM/Parsing Error: {e}. Failed to parse required JSON structure.")
        
    return results



def execute_MC_baseline_sql_query(config, database, queries: list[str]) -> list[dict]:
       
    results = []
    for qry in queries:
        
        LOG.info(f"Executing baseline SQL query: {qry}")
        
        lcel_chain = build_lcel_chain(config, database, qry, "MC")
        
        t_start = time()
        try:

            raw_response= lcel_chain.invoke({"database" : database, "query" : qry, "BASELINE" : "MC"})

            t_end = time()
            
            result = parse_llm_response(raw_response, t_end - t_start)
            results.append(result)
            
        except Exception as e:
            LOG.error(f"LLM/Parsing Error: {e}. Failed to parse required JSON structure.")
        
    return results

if __name__ == "__main__":
    execute_IK_baseline_sql_query(["SELECT call_sign FROM target.usa_airline_companies WHERE airline='jetblue airways';"])