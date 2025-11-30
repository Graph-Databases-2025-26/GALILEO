from typing import List, Optional
from src.utils.logging_config import LOG

# TABLE_SCAN_FIRST_PROMPT = """You are an expert SQL data extractor.

# You MUST return only JSON, with no natural language, markdown, or explanations.

# Use the following SQL query as a specification of the tuples you must return:

# SQL_QUERY:
# {base_sql};

# TASK:
#   - Retrieve tuples that satisfy the SQL query.
#   - Represent the result as JSON. Each tuple MUST contain the attributes:
#     {attributes}
#   - The JSON MUST follow this structure, where each attribute is mapped to its value:

# JSON_EXAMPLE:
# {json_example}

# IMPORTANT CONSTRAINTS:
#   - Return ONLY valid JSON.
#   - Do NOT wrap the answer in backticks or markdown.
#   - The top-level JSON MUST be either:
#       {{
#         "{table_name}": [ {{...}}, {{...}}, ... ]
#       }}
#     or a plain list:
#       [ {{...}}, {{...}}, ... ]
#   - You MUST ensure the JSON is syntactically valid (no truncated or partial objects).
#   - It is BETTER to return FEWER rows than to produce invalid or truncated JSON.

# LIMIT ON NUMBER OF TUPLES:
#   - In this FIRST answer, return AT MOST 20 tuples.
#   - Do NOT exceed {limit_rows} tuples, even if many more satisfy the query.
#   - The Table-Scan process will ask for more tuples in later iterations.

# Return ONLY the JSON now.
# """

# TABLE_SCAN_ITER_PROMPT = """Continue the previous task.
#     You have already returned some tuples that satisfy the SQL query.
#     Now:

#       - Return more tuples that satisfy the same query, if any remain.
#       - All tuples MUST follow exactly the same JSON structure as before.
#       - Do NOT repeat tuples that were already returned.
#       - Return AT MOST {limit_rows} additional tuples in this answer.
#       - If there are no more tuples to return, answer with an empty JSON object ({}) or an empty list ([]).

#     IMPORTANT:
#       - Return ONLY valid JSON, no explanations, no markdown.
#       - It is BETTER to return fewer tuples than to produce invalid or truncated JSON.
#     """
  
  
TABLE_SCAN_FIRST_PROMPT ="""
    Given the following query, populate the table {table_name} with actual values.
    query: {base_sql}.
    Respond with JSON only. Don't add any comment.
    Use the following JSON schema: {json_example}.
    Return at most {limit_rows} rows.
""" 

TABLE_SCAN_ITER_PROMPT = """
    List more values if there are more, otherwise return an empty JSON.  Respond with JSON only.
    Return at most {limit_rows} rows.
""" 

KEY_SCAN_FIRST_PROMPT = """
    List the {key} of {table} {condition}.
    Respond with JSON only.
    Use the following JSON schema: 
    {jsonSchema}
"""

KEY_SCAN_ITER_PROMPT = """
    List more unique values if there are more, otherwise return an empty response. Don't repeat the previous values.
"""

KEY_SCAN_TUPLE_PROMPT="""
    List the {attributes} of the {table} for {keyValue}. 
    Respond with JSON only.
    Use the following JSON schema:
    {jsonSchema}
"""

SYSTEM_PROMPT_GALOIS_CONFIDENCE = """
        Please use the following informations to ask the question.

        Table Name: {table}
        Schema Summary: {schema_summary}

        Query to Evaluate: 
        {query}

        Based on the table structure, the schema of the database and the query, please answer the following question without extra text or explanations:
    """


def build_condition(condition: str) -> str:
    return f"(where the following condition holds: {condition})"
  

def _format_attribute_list(attributes: List[str]) -> str:
    """
    Join a list of attributes into a comma-separated string for SQL.
    """
    return ", ".join(attributes)


def build_table_scan_first_prompt(
    table_name: str,
    attributes: List[str],
    where_clause: Optional[str],
    json_example: str,
) -> str:
    """
    Build the FIRST Table-Scan prompt (Algorithm 1 – genFirstPrompt).

    This prompt:
      - describes the SQL query,
      - explains the expected JSON structure,
      - explicitly limits the number of tuples (to avoid truncation and invalid JSON).
    """
    select_list = _format_attribute_list(attributes)

    base_sql = f"SELECT {select_list} FROM {table_name}"
    if where_clause:
        base_sql += f" WHERE {where_clause}"

    LOG.debug(f"[Table-Scan] First prompt SQL: {base_sql}")

    return TABLE_SCAN_FIRST_PROMPT.format(
        base_sql=base_sql, table_name=table_name, attributes=attributes, json_example=json_example, limit_rows=5)


def build_table_scan_iter_prompt() -> str:
    """
    Build the ITERATIVE Table-Scan prompt (Algorithm 1 – genIterativePrompt).

    This prompt is appended as a new human turn while keeping the existing history.
    The model must:
      - produce new tuples not seen before, OR
      - return an empty JSON object / empty list if no more tuples are available.
    """
    return TABLE_SCAN_ITER_PROMPT


#system prompt that is common to the 2 cases: Asking the LLM the confidence for WHERE CONDITIONS or for the QUERY
def system_prompt_galois_confidence() -> str:
    return SYSTEM_PROMPT_GALOIS_CONFIDENCE

#custom human prompt for each of the two cases: Asking the LLM the confidence for WHERE CONDITIONS or for the QUERY
def human_prompt_galois_confidence(usage: str) -> str:
    prompts_map = {
        "CONDITION": "How confident you are of evaluating correctly the condition reported in the query? Please don't overestimate. Answer at the question only with 'HIGH' or 'LOW'.",
        "QUERY": "How confident you are of retrieving coherent data for the query from 0 to 1? Please don't overestimate. Answer only in this way: TAU confidence: <value>"
    }

    return prompts_map.get(usage, "How confident are you?")


