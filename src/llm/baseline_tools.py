import ast
import re
from src.utils import LOG, DATA_DIR, SYSTEM_PROMPT, HUMAN_PROMPT, BASELINE_OUTPUT
from .llm_factory import LLMBaseWrapper
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableLambda
from typing import List, Dict, Union, Any, Optional
from pydantic import BaseModel, Field
import json
from ..db.duckdb_db_graphdb import get_duckdb_path


class Response(BaseModel):
   
    result_set: List[Dict[str, Union[str, int, float, Any]]] = Field( 
        default_factory=list,
        description="List of result records, each as a {column_name: value} dict where values can be mixed types."
    )
    time: Optional[float] = Field(None,
                                  description="Time required for the computation as float (ignored, calculated in Python code).")
    tokens: Optional[int] = Field(None,
                                  description="Total tokens used as integer (ignored, calculated in Python code).")

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


# Helper function to dynamically extract column names from schema string
def extract_column_names_from_schema(schema_str: str) -> List[str]:
    """
    Dynamically extracts column names from a schema string (e.g., 'CREATE TABLE...').
    This is necessary because LangChain's SQLDatabase.run() returns tuples for DuckDB,
    and we need column names for tuple-to-dict mapping.
    """

    # Search for the column definition block (everything between the first '(' and the first ')')
    match = re.search(r'\((.*?)\)', schema_str, re.DOTALL)
    if not match:
        LOG.error("Failed to find column definition block in schema string.")
        return []

    column_definitions_block = match.group(1)

    columns = []
    # Iterate over each column definition separated by a comma
    for line in column_definitions_block.split(','):
        line = line.strip()
        if not line:
            continue

        # Get the first word (the column name) and clean up quotes/backticks
        col_match = re.match(r'["`\']?(\w+)["`\']?', line)
        if col_match:
            col_name = col_match.group(1)
            columns.append(col_name)
        else:
            LOG.warning(f"Skipping line in schema parsing: {line}")

    return columns

def get_db_context(input_d: dict) -> dict:
    
    database = input_d["database"]
    db = get_duckdb_path(database)
    db_schema = db.get_table_info()

#---------------------------------
    #PALIMPZEST IMPLEMENTATION

    if (input_d["b_type"].lower() == "pz"):
        if database.upper() == "FORTUNE":
            main_table_name = "target.fortune_2024"
        elif database.upper() == "PREMIER":
            main_table_name = "target.premier_league_2024_2025_match_result"
        else:
            # Fallback or error handling
            LOG.error(f"Dataset {database} not recognized for the PZ baseline.")
            raise ValueError(f"PZ configuration missing for dataset: {database}")

        # Get the schema ONLY for the target table (cleaner context)
        # Dynamic Column Name Extraction
        columns = extract_column_names_from_schema(db_schema)
        if not columns:
            LOG.error(f"Failed to dynamically extract column names for {main_table_name}. Cannot proceed.")
            raise ValueError("Schema parsing failed, cannot proceed with RAG context creation.")

        # RAG Retrieval (Top-K Simulation)
        # We use LIMIT 50 to simulate retrieving the top-k "chunks".
        retrieval_query = f"SELECT * FROM {main_table_name} LIMIT 200;"


        try:
            retrieved_records: List[Union[tuple, dict]] = db.run(retrieval_query)
            #LOG.info(f"I PRIMI 50 RISULTATI RESTITUITI: {retrieved_records}")
        except Exception as e:
            LOG.error(f"Error in RAG simulation (PZ) on {main_table_name}: {e}")
            retrieved_records = []

        if isinstance(retrieved_records, str):
            LOG.warning("DUCKDB FIX: db.run() returned a STRING. Attempting ast.literal_eval...")
            try:
                # Use regex to find and replace datetime.date(...) with a string literal containing the date parts
                # e.g., 'datetime.date(2024, 8, 5)' -> '"2024, 8, 5"'
                records_str_clean = re.sub(r'datetime\.date\((.*?)\)', r'"\1"', retrieved_records)

            except Exception as e:
                LOG.error(f"FATAL: Regex replacement failed: {e}")
                records_str_clean = retrieved_records
            try:
                # Use ast.literal_eval for safe parsing of the string representation of a list/tuple
                retrieved_records = ast.literal_eval(records_str_clean)
                # Re-check type after successful parsing
                if not isinstance(retrieved_records, (list, tuple)):
                    raise ValueError("Parsed structure is not a list or tuple.")
                LOG.info("DUCKDB FIX: Successfully parsed string into list of tuples.")
            except (ValueError, SyntaxError) as e:
                LOG.error(
                    f"FATAL: Failed to parse db.run() string output as a data structure: {e}. Output starts with: {retrieved_records[:50]}...")
                retrieved_records = []  # Set to empty list to prevent iteration errors

        # Tuple-to-Dict Conversion (Dynamic and Robust)
        processed_records = []

        for record_tuple in retrieved_records:
            # Check type and length consistency (DuckDB returns tuples)
            if isinstance(record_tuple, tuple) and len(record_tuple) == len(columns):
                # Use dynamically extracted column names to create the dictionary
                processed_records.append(dict(zip(columns, record_tuple)))
            #else:
                #LOG.warning(f"Skipping malformed or non-tuple record: {record_tuple}")

        # Context Formatting (Transforming records into text/document format)
        rag_context_list = []
        for i, record in enumerate(processed_records):
            if not isinstance(record, dict):
                LOG.warning(
                    f"Skipping record {i + 1}: expected dictionary, got {type(record).__name__}. Raw data: {record}")
                continue

            try:
                # Format each record as a "document chunk"
                record_text = f"--- Record {i + 1} ---\n"
                for col, val in record.items():
                    record_text += f"{col}: {val}\n"
                rag_context_list.append(record_text)
            except Exception as e:
                LOG.error(f"Error formatting record {i + 1}: {e}")
                continue

        final_rag_context = "\n".join(rag_context_list)

        output = {
            "schema_info": db_schema,
            "raw_data": final_rag_context,  # Textual RAG context (50 simulated chunks)
            "query": input_d["prompt"],
        }


 #------------------------------

    if (input_d["b_type"] == "nl" or input_d["b_type"] == "NL") and  input_d["BASELINE"] == "MC":
        db_context = db.run(input_d["query"])
        print("DB CONTEXT: ",db_context)
        output = {
            "schema_info": db_schema,
            "raw_data": db_context,
            "query": input_d["prompt"] ,
        }

    if (input_d["b_type"] == "nl" or input_d["b_type"] == "NL") and  input_d["BASELINE"] == "IK":
        #db_context = input_d["prompt"]

        output = {
            "schema_info": db_schema,
            #"raw_data": db_context,
            "query": input_d["prompt"]
        }

    if input_d["b_type"] == "sql" or input_d["b_type"] == "SQL":
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


