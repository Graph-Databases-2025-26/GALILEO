import os
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent.parent
VENV = ROOT / ".venv"

CONFIG_PATH = ROOT / "config" / "config.yaml"

DATA_DIR = ROOT / "data"

PROMPTS = DATA_DIR / ".prompts"

SUBMISSIONS_PATH = DATA_DIR / ".output"

NL_OUTPUT = SUBMISSIONS_PATH / ".nl_output"

SQL_BLINE_DIR = SUBMISSIONS_PATH / ".sql_output"

SQL_OUTPUT = SQL_BLINE_DIR

GROUND_PATH = DATA_DIR / ".ground_truth"

DATASETS = ["FLIGHT-2", "FLIGHT-4", "FORTUNE", "GEO", "MOVIES", "PREMIER", "PRESIDENTS", "WORLD"]

IK_DATASETS = ["FLIGHT-2", "FLIGHT-4", "GEO", "MOVIES", "PRESIDENTS", "WORLD"]

MC_DATASETS = ["FORTUNE", "PREMIER"]

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
    3. START your response IMMEDIATELY with the single '{' character and END with the single '}' character.
    4. Use this JSON structure exactly:
    {
    "result_set": [
        { "<column_name_1>": "<value_1>", ... }
    ],
    }
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

    """


SQL_TO_NL_PROMPT = (
        "Translate the following SQL query into a clear, concise question in natural English. "
        "Only output the question itself — do not include SQL code or explanations.\n\n"
        "Example:\n"
        "SQL: SELECT name FROM students;\n"
        "Question: What are the names of the students?\n\n"
)

PROMPT_FOR_IK_DATASETS = ( "You are an AI assistant that must answer questions strictly using your internal knowledge. "
    "Respond ONLY in the following format: <key>:<value>, <key>:<value>, ... "
    "where each <key> is the name of the entity requested in the question and <value> is the exact answer. "
    "Do not include any additional text, explanations, punctuation, or formatting beyond this structure. "
    "Your output must be based on your knowledge and match exactly the required format. "
    "If you are uncertain about the answer, respond with the value that is most coherent and plausible according to your knowledge."
    "and you must provide an answer in the <value> field even if you are not sure about it.")

FULL_JSON_FORMAT = """
{{
        "result_set": [
            {{key: value}}
        ],
        "time": elapsed_time,
        "tokens": total_tokens
}}
"""