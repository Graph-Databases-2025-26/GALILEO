import ast
import re
from typing import List, Union

from src import LOG
from src.utils.constants import *


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

def pz_context(input_d: dict, database: str, duckdb_path:str, db_schema: str):
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
    retrieval_query = PZ_QUERIES.get(database.upper())


    try:
        retrieved_records: List[Union[tuple, dict]] = duckdb_path.run(retrieval_query)
        # LOG.info(f"I PRIMI 50 RISULTATI RESTITUITI: {retrieved_records}")
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
        # else:
        # LOG.warning(f"Skipping malformed or non-tuple record: {record_tuple}")

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

    return output