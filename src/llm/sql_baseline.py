from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_community.utilities import SQLDatabase

from .watsonx_ai_connection import run_prompt_on_watsonx

from src.utils import LOG, DATA_DIR,SQL_IK_PROMPT,SQL_MC_PROMPT, WatsonxResponse
from src.utils import build_sql_prompt

from typing import List, Dict, Union, Any
from pydantic import BaseModel, Field


class Response(BaseModel):
   
    result_set: List[Dict[str, Union[str, int, float, Any]]] = Field( 
        default_factory=list,
        description="List of result records, each as a {column_name: value} dict where values can be mixed types."
    )




def execute_IK_baseline_sql_query(config, queries: list[str]) -> list[dict]:
    
    results = []
    for qry in queries:
        
        LOG.info(f"Executing baseline SQL query: {qry}")
        
        
        parser = PydanticOutputParser(pydantic_object=Response)

        format_instructions = parser.get_format_instructions()
        
        FULL_PROMPT = [
            SystemMessage(SQL_IK_PROMPT + format_instructions),
            HumanMessage(build_sql_prompt(qry))
        ]
    
        
        result = run_prompt_on_watsonx(parser, config, FULL_PROMPT)
        
        LOG.info(f"Received result: {result}")
        
        results.append(result)
        
    return results

def execute_MC_baseline_sql_query(config, database, queries: list[str]) -> list[dict]:
    
    results = []
    for qry in queries:
        
        LOG.info(f"Executing baseline SQL query: {qry}")
       
        parser = PydanticOutputParser(pydantic_object=Response)
        format_instructions = parser.get_format_instructions()
        
        dbs_path = DATA_DIR / database.upper() / f"{database}.duckdb"
        db = SQLDatabase.from_uri(f"duckdb:///{dbs_path}")  
        db_schema = db.get_table_info()
        
        LOG.info(f"Tipo di 'query' prima di db.run(): {type(qry)}")
        LOG.info(f"Contenuto di 'query' prima di db.run(): {qry}")
        
        raw_data = db.run(qry) 
      
        FULL_PROMPT = [
            SystemMessage(SQL_IK_PROMPT + SQL_MC_PROMPT.format(schema_info=db_schema, raw_data=raw_data) + format_instructions),
            HumanMessage(build_sql_prompt(qry))
        ]
        
        result = run_prompt_on_watsonx(parser, config, FULL_PROMPT)
        
        LOG.info(f"Received result: {result}")
        
        results.append(result)
        
    return results

if __name__ == "__main__":
    execute_IK_baseline_sql_query(["SELECT call_sign FROM target.usa_airline_companies WHERE airline='jetblue airways';"])