import json
from typing import List, Dict, Union, Any
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.exceptions import OutputParserException
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableLambda
from pydantic import BaseModel, Field

from src.utils import LOG, SYSTEM_PROMPT, HUMAN_PROMPT, BASELINE_OUTPUT
from .llm_factory import LLMBaseWrapper
from .rag import pz_context
from ..db.duckdb_db_graphdb import get_duckdb_path
from ..utils.constants import RAG_RESOURCES
import re


class Response(BaseModel):
    """
    Pydantic model defining the strict JSON structure expected from the LLM response.

    The model expects a single key, 'result_set', which is a list of dictionaries representing database records.
    """

    result_set: List[Dict[str, Union[str, int, float, Any]]] = Field( 
        default_factory=list,
        description="List of result records, each as a {column_name: value} dict where values can be mixed types."
    )


def parse_llm_response(raw_response, time: float, llm_wrapper: LLMBaseWrapper) -> dict:
    """
    Parses the raw LLM output into the desired structured JSON format, and adds execution metadata (time, tokens).

    It uses a Pydantic parser to enforce the `Response` structure on the LLM's text output.

    Args:
        raw_response: The raw response object from the LangChain chain invocation.
        time: The total time taken for the LLM invocation, in seconds.
        llm_wrapper: The wrapper for the LLM, used to extract token count.

    Returns:
        dict: A complete structured dictionary containing the 'result_set', time' (rounded to 3 decimal places), and 'tokens' metadata.

    If the LLM output is not valid JSON, it logs the error and returns an empty `result_set`
    so that callers can stop iterating gracefully instead of crashing.
    """

    parser = PydanticOutputParser(pydantic_object=Response)

    # Get the text out of the response object (Gemini / Watsonx)
    text = raw_response.content if hasattr(raw_response, "content") else str(raw_response)
    text = re.sub(r'(\d),(\d)', r'\1\2', text)

    try:
        parsed = parser.parse(text)
    except OutputParserException as e:
        LOG.error(f"[parse_llm_response] Failed to parse LLM output as JSON: {e}")
        LOG.debug(f"[parse_llm_response] Raw LLM output (truncated): {text[:500]!r}")

        return {
            "result_set": [],
            "time": round(time, 3),
            "tokens": 0,
        }
    
   
    LOG.debug(f"LLM Content Output: {parsed}")
        
    tokens = llm_wrapper.get_output_tokens(raw_response)
    LOG.debug(f"LLM Tokens Output: {tokens}")
    
    fullJ_structure ={
        "result_set": parsed.result_set,
        "time": round(time, 3),
        "tokens": tokens
    }
    
    return fullJ_structure


def save_baseline_to_json(dataset: str, baseline: list[dict], llm_wrapper: LLMBaseWrapper, b_type: str):
    """
    Saves the results of a baseline run (a list of query results) into separate JSON files for a specific dataset and LLM provider.

    The files are saved to the directory structure defined in `BASELINE_OUTPUT`.

    Args:
        dataset: The name of the dataset being processed (e.g., "FLIGHT-2").
        baseline: A list of dictionaries, where each dictionary is the structured result for a single query.
        llm_wrapper: The wrapper for the LLM, used to determine the provider name for the output path.
        b_type: The type of baseline run ("SQL", "NL", "PZSQL" or "PZNL").
    """

    #bline_folder = BASELINE_OUTPUT[b_type][llm_wrapper.get_provider_name().upper()]/dataset
    bline_folder = BASELINE_OUTPUT["EXP3"]["NL_GPT"] / dataset
    print(bline_folder)
    
    bline_folder.mkdir(parents=True, exist_ok=True)    

    LOG.info(f"Saving dataset:{dataset} in JSON format")
    for i, result in enumerate(baseline):
        with open(bline_folder / f"query{i + 1}.json", "w") as f:
            json.dump(result, f, indent=4)

def get_db_context(input_d: dict) -> dict:
    """
    Retrieves the necessary database context (schema info ando, raw data) required for the LLM prompt.

    Args:
        input_d: A dictionary containing execution parameters, must include "database", "query", and "b_type".
        Optionally includes "prompt".

    Returns:
        dict: A dictionary containing the "schema_info", the "query" (or "prompt" for NL), and optionally "raw_type" (for PZ baseline).
    """

    database = input_d["database"]
    b_query = {"PZSQL": input_d["query"] , "PZNL":input_d.get("prompt",""), "SQL": input_d["query"], "NL": input_d.get("prompt","")}

    db = get_duckdb_path(database)
    db_schema = db.get_table_info()

    #NL/SQL IMPLEMENTATION
    if input_d["b_type"] == "NL" or input_d["b_type"] == "SQL" :
        output = {
            "schema_info": db_schema,
            "query": b_query[input_d["b_type"]]
        }

    #PALIMPZEST IMPLEMENTATION
    if input_d["b_type"] == "PZSQL" or input_d["b_type"] == "PZNL" :
        rag_path = RAG_RESOURCES / database.upper()

        output = pz_context(input_d ,db_schema ,rag_path, b_query[input_d["b_type"]] )

    return output


def build_lcel_chain(llm_model: LLMBaseWrapper, b_type: str):
    """
    Constructs a LangChain Expression Language (LCEL) chain for the LLM task.

    The chain consists of:
    1. `get_db_context` (RunnableLambda) for preparing the context.
    2. `ChatPromptTemplate` combining system and human prompts, including formatting instructions for the Pydantic parser.
    3. The LLM instance itself.

    Args:
        llm_model: The LLMBaseWrapper instance (e.g., GeminiWrapper).
        b_type: The type of baseline run ("SQL", "NL", or "PZ").

    Returns:
        Runnable: The complete LCEL chain ready for invocation.
    """
    
    parser = PydanticOutputParser(pydantic_object=Response)
    format_instructions = parser.get_format_instructions()
    
    db = RunnableLambda(get_db_context)
    
    Syst_Prompt = SYSTEM_PROMPT[b_type]
    Hm_Prompt = HUMAN_PROMPT[b_type]

    FULL_PROMPT = ChatPromptTemplate.from_messages([
        ("system", Syst_Prompt),
        ("human", Hm_Prompt)
    ]).partial(format_instructions=format_instructions)
    LOG.debug(f"FULL Prompt: \n{FULL_PROMPT}")
    
    return  db | FULL_PROMPT  | llm_model.get_llm_instance()


