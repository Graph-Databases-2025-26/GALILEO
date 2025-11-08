from src.utils import LOG, DATA_DIR, SQL_IK_PROMPT, SQL_MC_PROMPT, SQL_HUMAN_PROMPT, WATSONX_OUTPUT, GEMINI_OUTPUT
from .llm_factory import get_model

from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableLambda
from langchain_community.utilities import SQLDatabase

from typing import List, Dict, Union, Any
from pydantic import BaseModel, Field
import json

class Response(BaseModel):
   
    result_set: List[Dict[str, Union[str, int, float, Any]]] = Field( 
        default_factory=list,
        description="List of result records, each as a {column_name: value} dict where values can be mixed types."
    )

def parse_llm_response(raw_response, time) -> dict:
    
    LOG.info("Parsing LLM Response ...")
    parser = PydanticOutputParser(pydantic_object=Response)
    
    watsonx_rsp = parser.parse(raw_response.content)
    LOG.debug(f"LLM Content Output: {watsonx_rsp}")
        
    tokens = raw_response.usage_metadata.get("output_tokens")
    LOG.debug(f"LLM Tokens Output: {tokens}")
    
    fullJ_structure ={
        "result_set": watsonx_rsp.result_set,
        "time": round(time, 3),
        "tokens": tokens
    }
    
    return fullJ_structure

def save_baseline_to_json(dataset: str, baseline: list[dict], llm_provider):
    
    if llm_provider == "gemini":
        bline_folder = GEMINI_OUTPUT / dataset 
    
    elif llm_provider == "watsonx":
        bline_folder = WATSONX_OUTPUT / dataset   
    
    bline_folder.mkdir(parents=True, exist_ok=True)    

    LOG.info(f"Saving dataset:{dataset} in JSON format")
    for i, result in enumerate(baseline):
        with open(bline_folder / f"query{i + 1}.json", "w") as f:
            json.dump(result, f, indent=4)


def get_db_context(input_d: dict) -> dict:
    
    database = input_d["database"]
    
    dbs_path = DATA_DIR / database.upper() / f"{database}.duckdb"
    db = SQLDatabase.from_uri(f"duckdb:///{dbs_path}")  
    
    db_schema = db.get_table_info() 
    
    if input_d["BASELINE"] == "MC":
        db_context = db.run(input_d["query"])
        
        output = {
            "schema_info" : db_schema,
            "raw_data" : db_context,
            "query" : input_d["query"]
        }
    
    elif input_d["BASELINE"] == "IK":
        
        output = {
            "schema_info" : db_schema,
            "query" : input_d["query"]
        }
    
    return output
        
        
def build_lcel_chain(config, database, query, baseline):
    
    parser = PydanticOutputParser(pydantic_object=Response)
    format_instructions = parser.get_format_instructions()
    
    LOG.info(f"Obtaining the db Context ...")
    db = RunnableLambda(get_db_context)
    
    if baseline == "MC":
        Syt_Prompt = SQL_MC_PROMPT
    else:
        Syt_Prompt = SQL_IK_PROMPT
        
    Hm_Prompt = SQL_HUMAN_PROMPT
    
    LOG.info("Building the LLM Prompt ...")
    
    FULL_PROMPT = ChatPromptTemplate.from_messages([
        ("system", Syt_Prompt),
        ("human", Hm_Prompt)
    ]).partial(format_instructions=format_instructions)
    
    llm_model = get_model(config)
    
    return  db | FULL_PROMPT | llm_model


