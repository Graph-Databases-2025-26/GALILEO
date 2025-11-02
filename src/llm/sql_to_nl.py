from .watsonx_ai_connection import query_watsonx
from ..utils.constants import SQL_TO_NL_PROMPT

def sql_to_nl(sql_query: str) -> str:
    prompt = SQL_TO_NL_PROMPT + f"SQL: {sql_query}\nQuestion:"
    return query_watsonx(prompt)