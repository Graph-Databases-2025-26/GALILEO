import ast
import re
import json
from src.utils import LOG, DATA_DIR, SYSTEM_PROMPT, HUMAN_PROMPT, BASELINE_OUTPUT,  PZ_QUERIES
from .llm_factory import LLMBaseWrapper
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableLambda
from typing import List, Dict, Union, Any, Optional
from pydantic import BaseModel, Field

from .palimpzest_baseline import pz_context
from ..db.duckdb_db_graphdb import get_duckdb_path


class Response(BaseModel):
   
    result_set: List[Dict[str, Union[str, int, float, Any]]] = Field( 
        default_factory=list,
        description="List of result records, each as a {column_name: value} dict where values can be mixed types."
    )

def parse_llm_response(raw_response, time: float, llm_wrapper: LLMBaseWrapper) -> dict:

    parser = PydanticOutputParser(pydantic_object=Response)
    
    watsonx_rsp = parser.parse(raw_response.content)
    LOG.debug(f"LLM Content Output: {watsonx_rsp}")
        
    tokens = llm_wrapper.get_output_tokens(raw_response)
    LOG.debug(f"LLM Tokens Output: {tokens}")
    
    fullJ_structure ={
        "result_set": watsonx_rsp.result_set,
        "time": round(time, 3),
        "tokens": tokens
    }
    
    return fullJ_structure

def save_baseline_to_json(dataset: str, baseline: list[dict], llm_wrapper: LLMBaseWrapper, b_type: str):
    
    bline_folder = BASELINE_OUTPUT[b_type][llm_wrapper.get_provider_name().upper()]/dataset
    print(bline_folder)
    
    bline_folder.mkdir(parents=True, exist_ok=True)    

    LOG.info(f"Saving dataset:{dataset} in JSON format")
    for i, result in enumerate(baseline):
        with open(bline_folder / f"query{i + 1}.json", "w") as f:
            json.dump(result, f, indent=4)


def get_db_context(input_d: dict) -> dict:
    
    database = input_d["database"]
    db = get_duckdb_path(database)
    db_schema = db.get_table_info()

#PALIMPZEST IMPLEMENTATION

    if (input_d["b_type"].lower() == "pz"):
        output = pz_context(input_d ,database ,db ,db_schema )


# NL IMPLEMENTATION

    if (input_d["b_type"] == "nl" or input_d["b_type"] == "NL") and  input_d["BASELINE"] == "IK":
        #db_context = input_d["prompt"]

        output = {
            "schema_info": db_schema,
            #"raw_data": db_context,
            "query": input_d["prompt"]
        }

# SQL IMPLEMENTATION
    if input_d["b_type"] == "sql" or input_d["b_type"] == "SQL":

        if input_d["BASELINE"] == "IK":

            output = {
                "schema_info" : db_schema,
                "query" : input_d["query"]
            }
    
    return output
        
        
def build_lcel_chain(llm_model: LLMBaseWrapper, b_type: str, d_type: str):
    
    parser = PydanticOutputParser(pydantic_object=Response)
    format_instructions = parser.get_format_instructions()
    
    db = RunnableLambda(get_db_context)
    
    Syst_Prompt = SYSTEM_PROMPT[b_type.upper()][d_type.upper()]
    Hm_Prompt = HUMAN_PROMPT[b_type.upper()]

    
    FULL_PROMPT = ChatPromptTemplate.from_messages([
        ("system", Syst_Prompt),
        ("human", Hm_Prompt)
    ]).partial(format_instructions=format_instructions)
    
    LOG.debug(f"FULL Prompt: \n{FULL_PROMPT}")
    
    return  db | FULL_PROMPT  | llm_model.get_llm_instance()


