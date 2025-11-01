from .watsonx_ai_connection import query_watsonx


def sql_to_nl(sql_query: str) -> str:
    prompt = f"Convert this SQL query in a natural language prompt useful for the LLM:\n{sql_query}"
    return query_watsonx(prompt)