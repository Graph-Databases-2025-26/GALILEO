from pathlib import Path
import os, re

ROOT = Path(__file__).resolve().parent.parent.parent
VENV = ROOT / ".venv"

CONFIG_PATH = ROOT / "config.yaml"

DATA_DIR = ROOT / "data"

PROMPTS = DATA_DIR / ".prompts"

SUBMISSIONS_PATH = DATA_DIR / ".output"

SUBMISSIONS_PATH_GALOIS = DATA_DIR / ".galois_output"

NL_BLINE_DIR = SUBMISSIONS_PATH / ".nl_output"

SQL_BLINE_DIR = SUBMISSIONS_PATH / ".sql_output"

PZ_BLINE_DIR = SUBMISSIONS_PATH / ".rag_output"

RAG_RESOURCES = DATA_DIR / "RAG"

QUOTA_ERROR_REGEX = re.compile(r"quota|exceeded|ResourceExhausted|429", re.IGNORECASE)


BASELINE_OUTPUT = {
    "SQL" : {
        "GEMINI" : SQL_BLINE_DIR / "gemini",
        "WATSONX" : SQL_BLINE_DIR / "watsonx"
    },
    
    "NL" : {
        "GEMINI" : NL_BLINE_DIR / "gemini",
        "WATSONX" : NL_BLINE_DIR / "watsonx"
    },

    "PZSQL" : {
        "GEMINI" : PZ_BLINE_DIR / "sql" / "gemini",
        "WATSONX" : PZ_BLINE_DIR / "sql" / "watsonx"
    },
    "PZNL" : {
            "GEMINI" : PZ_BLINE_DIR / "nl" / "gemini",
            "WATSONX" : PZ_BLINE_DIR / "nl" / "watsonx"
        },
    "GALOIS" : {
            "F" : SUBMISSIONS_PATH_GALOIS / "F",
            "A" : SUBMISSIONS_PATH_GALOIS / "A",
            "S" : SUBMISSIONS_PATH_GALOIS / "S",
            "WO" : SUBMISSIONS_PATH_GALOIS / "WO"
        },
}



GROUND_PATH = DATA_DIR / ".ground_truth"

DATASETS = ["FLIGHT-2", "FLIGHT-4", "FORTUNE", "GEO", "MOVIES", "PREMIER", "PRESIDENTS", "WORLD"]

IK_DATASETS = ["FLIGHT-2", "FLIGHT-4", "GEO", "MOVIES", "PRESIDENTS", "WORLD"]

MC_DATASETS = ["FORTUNE", "PREMIER"]

if os.name == "nt":
    PIP = VENV / "Scripts" / "pip"
    PY  = VENV / "Scripts" / "python"
else:
    PIP = VENV / "bin" / "pip"
    PY  = VENV / "bin" / "python"

LOGS_DIR = ROOT / "logs"

REQS_PATH = ROOT / "requirements.txt"

LLM_TEMPLATE = ROOT / "data" / ".prompts" / "response_template.jinja"

SQL_IK_PROMPT = """
    You are an expert SQL interpreter. Your ONLY task is to generate a JSON response
    that simulates the result of the SQL query based on your internal knowledge
    and the structure of the database schema provided to you.

    CONTEXTUAL DATA:
    The following information is provided for your task:
    ---
    DATABASE SCHEMA:
    {schema_info}

    STRICT INSTRUCTIONS:
    1. RESPONSE MUST be ONLY ONE, single, valid JSON object.
    2. DO NOT include any description, explanation, or markdown.
    3. START your response IMMEDIATELY with the single '{{' character and
       END with the single '}}' character.
    4. Use this JSON structure exactly:
    {{
      "result_set": [
        {{ "<column_name_1>": "<value_1>", ... }}
      ]
    }}
    5. "result_set" must be an array of JSON objects where keys match the
       columns selected in the query.
    6. "result_set" MUST NOT be empty. Never return [].
       Even if the true result would contain zero rows, you MUST still simulate
       at least one row that is consistent with the schema and the query.
    7. Do NOT output empty objects {{}}. Every object in "result_set" must contain
       values for all the selected columns of the query.
    """

NL_IK_PROMPT = """
    You are an AI assistant that answers questions. Your ONLY task is to generate a JSON response that answers the question in natural language based on your internal knowledge and the structure of the database schema provided to you.

    CONTEXTUAL DATA:
    The following information is provided for your task:
    ---
    DATABASE SCHEMA:
    {schema_info}
    
    STRICT INSTRUCTIONS:
    1. RESPONSE MUST be ONLY ONE, single, valid JSON object.
    2. DO NOT include any text, description, explanation, or markdown.
    3. START your response IMMEDIATELY with the single '{{' character and END with the single '}}' character.
    4. Use this JSON structure exactly:
    {{
    "result_set": [
        {{ "<column_name_1>": "<value_1>", ... }}
        ]
    }}
    5. "result_set" must be an array of JSON objects where keys match the columns of the database schema involved in the prompt .

    """

PZ_PROMPT = """
    You are an expert assistant in in-context data retrieval. 
    Your task is to answer the user's 'query' based on the provided 'raw_data'. 
    The 'raw_data' represents 50 pre-selected records (simulating the 50 most relevant segments) from the resource. 
    **Ignore your internal knowledge and use ONLY the information present in raw_data and schema_info to formulate the 'result_set' in JSON format.** 
    
    CONTEXTUAL DATA:
    The following information is provided for your task:
    ---
    DATABASE SCHEMA: The table schema.
    {schema_info}
    ---
    RAW DATA: A list of 50 records, formatted as text 'Column: Value'.
    {raw_data}
    ---
    QUERY: The natural language question. 
    {query}
    ---

    STRICT INSTRUCTIONS:
    1. RESPONSE MUST be ONLY ONE, single, valid JSON object.
    2. DO NOT include any text, description, explanation, or markdown.
    3. START your response IMMEDIATELY with the single '{{' character and END with the single '}}' character.
    4. Use this JSON structure exactly:
    {{
    "result_set": [
        {{ "<column_name_1>": "<value_1>", ... }}
        ]
    }}
    5. "result_set" must be an array of JSON objects where keys match the columns selected in the query.

"""


SYSTEM_PROMPT = {
    "NL": NL_IK_PROMPT,
    "SQL": SQL_IK_PROMPT,
    "PZSQL": PZ_PROMPT,
    "PZNL": PZ_PROMPT
}

    
HUMAN_PROMPT = {
    "SQL": "Now process this SQL query: {query}",
    "NL" : "Now process this question: {query}",
    "PZNL" : "Now process this question: {query}",
    "PZSQL" : "Now process this SQL query: {query}"}


#---------------------------------------------------------

#SQL QUERIES

def get_tables() -> str:
    """Returns the query for retrieving all table names in the current schema."""
    return  """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = current_schema()
    ORDER BY table_name;
    """

def get_attributes() -> str:
    """Returns the query for retrieving attributes of a table."""
    return """
    PRAGMA table_info('{table_name}')
    """

def get_exact_table_name() -> str:
    """Returns the query for retrieving the exact table name."""
    return  """
    SELECT table_name
    FROM information_schema.tables
    WHERE LOWER(table_name) = '{table_name}'
    """


def get_primary_keys(table_name: str) -> str:
    """Returns the query for retrieving primary keys of a table."""
    return f"""
    SELECT cols
    FROM (
        SELECT unnest(constraint_column_names) AS cols, constraint_type, table_name
        FROM duckdb_constraints()
    )
    WHERE table_name = '{table_name}'
      AND constraint_type = 'PRIMARY KEY';
    """


def q_get_sample_values(table_name: str, column_name: str, limit: int = 3) -> str:
    """Returns the query for retrieving distinct value samples."""
    return f"""
        SELECT DISTINCT CAST({column_name} AS VARCHAR) 
        FROM {table_name} 
        WHERE {column_name} IS NOT NULL 
        LIMIT {limit}
    """

def debug_query() -> str:
    return """ 
    SELECT distinct t2.region
    FROM target.country_language AS t1
    JOIN target.country AS t2
    ON t1.country_code_3_letters = t2.code_3_letters
    WHERE t1.language = 'English'
    OR t1.language = 'Dutch';
    """