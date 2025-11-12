import os
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent.parent
VENV = ROOT / ".venv"

CONFIG_PATH = ROOT / "config" / "config.yaml"

DATA_DIR = ROOT / "data"

PROMPTS = DATA_DIR / ".prompts"

SUBMISSIONS_PATH = DATA_DIR / ".output"

NL_BLINE_DIR = SUBMISSIONS_PATH / ".nl_output"

SQL_BLINE_DIR = SUBMISSIONS_PATH / ".sql_output"

PZ_BLINE_DIR = SUBMISSIONS_PATH / ".palimpzest_output"

BASELINE_OUTPUT = {
    "SQL" : {
        "GEMINI" : SQL_BLINE_DIR / "gemini",
        "WATSONX" : SQL_BLINE_DIR / "watsonx"
    },
    
    "NL" : {
        "GEMINI" : NL_BLINE_DIR / "gemini",
        "WATSONX" : NL_BLINE_DIR / "watsonx"
    },

    "PZ" : {
        "GEMINI" : PZ_BLINE_DIR / "gemini",
        "WATSONX" : PZ_BLINE_DIR / "watsonx"
    }
}

GROUND_PATH = DATA_DIR / ".ground_truth"

DATASETS = ["FLIGHT-2", "FLIGHT-4", "FORTUNE", "GEO", "MOVIES", "PREMIER", "PRESIDENTS", "WORLD"]

IK_DATASETS = ["FLIGHT-2", "FLIGHT-4", "GEO", "MOVIES", "PRESIDENTS", "WORLD"]

MC_DATASETS = ["FORTUNE", "PREMIER"]

PZ_DATASETS = ["FORTUNE", "PREMIER"]

if(os.name == "nt"):
    PIP = VENV / "Scripts" / "pip"
    PY  = VENV / "Scripts" / "python"
else:
    PIP = VENV / "bin" / "pip"
    PY  = VENV / "bin" / "python"

LOGS_DIR = ROOT / "logs"

REQS_PATH = ROOT / "requirements.txt"

LLM_TEMPLATE = ROOT / "data" / ".prompts" / "response_template.jinja"

SQL_IK_PROMPT = """
    You are an expert SQL interpreter. Your ONLY task is to generate a JSON response that simulates the result of the SQL query based on your internal knowledge.

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

SQL_MC_PROMPT= """
    You are an expert SQL interpreter. Your ONLY task is to generate a JSON response that simulates the result of the SQL query based on your internal knowledge and the provided database structure.

    CONTEXTUAL DATA:
    The following information is provided for your task:
    ---
    DATABASE SCHEMA:
    {schema_info}
    ---
    RAW DATA:
    {raw_data}
    ---

    STRICT INSTRUCTIONS:
    1. RESPONSE MUST be ONLY ONE, single, valid JSON object.
    2. DO NOT include any text, description, explanation, or markdown.
    3. START your response IMMEDIATELY with the single '{{' character and END with the single '}}' character.
    4. Use this JSON structure exactly:
    {{
    "result_set": [
        {{ "<column_name_1>": "<value_1>", ... }}
        ],
        "time": <time required for the computation as float>,
        "tokens": <total tokens used  as integer>
    }}
    5. "result_set" must be an array of JSON objects where keys match the columns selected in the query.
    
    """

NL_IK_PROMPT = """
    You are an AI assistant that answers questions. Your ONLY task is to generate a JSON response that answers the question in natural language based on your internal knowledge and the structure of the database schema provided to you.

    STRICT INSTRUCTIONS:
    1. RESPONSE MUST be ONLY ONE, single, valid JSON object.
    2. DO NOT include any text, description, explanation, or markdown.
    3. START your response IMMEDIATELY with the single '{{' character and END with the single '}}' character.
    4. Use this JSON structure exactly:
    {{
    "result_set": [
        {{ "<column_name_1>": "<value_1>", ... }}
        ],
        "time": <time required for the computation as float>,
        "tokens": <total tokens used  as integer> 
    }}
    5. "result_set" must be an array of JSON objects where keys match the columns selected in the query.

    """

NL_MC_PROMPT = """
    You are an AI assistant that answers questions. Your ONLY task is to generate a JSON response with the results of the question in natural language based on the structure of the database schema provided to you,
    and analyze the additional cntext provided about the database sample data query result for the prompt in 'raw_data'.
    
     CONTEXTUAL DATA:
    The following information is provided for your task:
    ---
    DATABASE SCHEMA:
    {schema_info}
    ---
    RAW DATA:
    {raw_data}
    ---
    
    STRICT INSTRUCTIONS:
    1. RESPONSE MUST be ONLY ONE, single, valid JSON object.
    2. DO NOT include any text, description, explanation, or markdown.
    3. START your response IMMEDIATELY with the single '{{' character and END with the single '}}' character.
    4. Use this JSON structure exactly:
    {{
    "result_set": [
        {{ "<column_name_1>": "<value_1>", ... }}
        ],
        "time": <time required for the computation as float>,
        "tokens": <total tokens used  as integer>
    }}
    5. "result_set" must be an array of JSON objects where keys match the columns selected in the query.

    """

PZ_PROMPT = """
    You are an expert assistant in in-context data retrieval. 
    Your task is to answer the user's 'query' based on the provided 'raw_data'. 
    The 'raw_data' represents 50 pre-selected records (simulating the 50 most relevant segments) from the database. 
    **Ignore your internal knowledge and use ONLY the information present in raw_data and schema_info to formulate the 'result_set' in JSON format.** 
    PROVIDED DATA STRUCTURE: 
    - schema_info: {schema_info} The table schema. 
    - raw_data: {raw_data} A list of 50 records, formatted as text 'Column: Value'. 
    - query: The natural language question. 
    STRICT INSTRUCTIONS:
    1. RESPONSE MUST be ONLY ONE, single, valid JSON object.
    2. DO NOT include any text, description, explanation, or markdown.
    3. START your response IMMEDIATELY with the single '{{' character and END with the single '}}' character.
    4. Use this JSON structure exactly:
    {{
    "result_set": [
        {{ "<column_name_1>": "<value_1>", ... }}
        ],
        "time": <time required for the computation as float>,
        "tokens": <total tokens used  as integer> 
    }}
    5. "result_set" must be an array of JSON objects where keys match the columns selected in the query.

"""

PZ_QUERIES = {
    "FORTUNE": "SELECT * FROM target.fortune_2024 LIMIT 200;",
    "PREMIER": "SELECT * FROM target.premier_league_2024_2025_match_result LIMIT 200;"
}


SYSTEM_PROMPT = {
    "NL": {
        "IK": NL_IK_PROMPT,
        "MC": NL_MC_PROMPT
    },
    
    "SQL":{
        "IK": SQL_IK_PROMPT,
        "MC": SQL_MC_PROMPT
    },

    "PZ":{
        "MC": SQL_MC_PROMPT
    },
}

    
HUMAN_PROMPT = {
    "SQL": "Now process this SQL query: {query}",
    "NL" : "Now process this question: {query}",
    "PZ" : "Now process this question: {query}"
}
    

FULL_JSON_FORMAT = """
{{
        "result_set": [
            {{key: value}}
        ],
        "time": elapsed_time,
        "tokens": total_tokens
}}
"""