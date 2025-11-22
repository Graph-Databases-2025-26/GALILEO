from typing import List, Optional

from src.utils.logging_config import LOG


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

    prompt = f"""You are an expert SQL data extractor.

You MUST return only JSON, with no natural language, markdown, or explanations.

Use the following SQL query as a specification of the tuples you must return:

SQL_QUERY:
{base_sql};

TASK:
  - Retrieve tuples that satisfy the SQL query.
  - Represent the result as JSON. Each tuple MUST contain the attributes:
    {attributes}
  - The JSON MUST follow this structure, where each attribute is mapped to its value:

JSON_EXAMPLE:
{json_example}

IMPORTANT CONSTRAINTS:
  - Return ONLY valid JSON.
  - Do NOT wrap the answer in backticks or markdown.
  - The top-level JSON MUST be either:
      {{
        "{table_name}": [ {{...}}, {{...}}, ... ]
      }}
    or a plain list:
      [ {{...}}, {{...}}, ... ]
  - You MUST ensure the JSON is syntactically valid (no truncated or partial objects).
  - It is BETTER to return FEWER rows than to produce invalid or truncated JSON.

LIMIT ON NUMBER OF TUPLES:
  - In this FIRST answer, return AT MOST 20 tuples.
  - Do NOT exceed 20 tuples, even if many more satisfy the query.
  - The Table-Scan process will ask for more tuples in later iterations.

Return ONLY the JSON now.
"""

    return prompt


def build_table_scan_iter_prompt() -> str:
    """
    Build the ITERATIVE Table-Scan prompt (Algorithm 1 – genIterativePrompt).

    This prompt is appended as a new human turn while keeping the existing history.
    The model must:
      - produce new tuples not seen before, OR
      - return an empty JSON object / empty list if no more tuples are available.
    """
    prompt = """Continue the previous task.

You have already returned some tuples that satisfy the SQL query.
Now:

  - Return more tuples that satisfy the same query, if any remain.
  - All tuples MUST follow exactly the same JSON structure as before.
  - Do NOT repeat tuples that were already returned.
  - Return AT MOST 20 additional tuples in this answer.
  - If there are no more tuples to return, answer with an empty JSON object ({}) or an empty list ([]).

IMPORTANT:
  - Return ONLY valid JSON, no explanations, no markdown.
  - It is BETTER to return fewer tuples than to produce invalid or truncated JSON.
"""

    return prompt
