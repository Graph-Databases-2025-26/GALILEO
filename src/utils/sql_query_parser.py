import sqlglot

from src import DATA_DIR
from src.db import load_queries_from_folder
from src.utils.logging_config import LOG
import json
from sqlglot import expressions as exp
from typing import List, Dict, Any, Optional


def _extract_conditions(node: exp.Expression) -> List[str]:
    """
    Recursively traverses the WHERE clause to flatten AND conditions
    into a simple list of SQL strings.
    """
    conditions = []

    if isinstance(node, exp.And):
        # If it's an AND node, recursively visit both sides
        conditions.extend(_extract_conditions(node.left))
        conditions.extend(_extract_conditions(node.right))
    else:
        # This is a leaf condition (e.g., EQ, GT, LIKE)
        # Using .sql() to translate it directly back to a SQL string.
        conditions.append(node.sql(dialect="duckdb"))

    return conditions


def parse_sql(sql_query: str) -> Dict[str, Any]:
    """
    Parses a raw SQL query string into its core components needed by GALOIS.
    This corresponds to the 'sql-parser' module in the Java repository.

    Args:
        sql_query_string_str: The raw SQL query string.

    Returns:
        A structured dictionary containing the query components.
    """
    if isinstance(sql_query, tuple):
        print(f"Warning: parse_sql received a tuple. Assuming (filename, sql) format.")
        # Find the first string in the tuple that looks like a query
        sql_query_string = ""
        for item in sql_query_string:
            if isinstance(item, str) and (item.strip().upper().startswith("SELECT") or item.strip().startswith("--")):
                sql_query_string_str_str = item
                break
        if not sql_query_string:
            raise ValueError(f"Could not find a valid SQL string in the input tuple: {sql_query_string}")
    elif isinstance(sql_query, str):
        sql_query_string = sql_query
    else:
        raise ValueError(f"parse_sql expects a string or tuple, but got: {type(sql_query)}")

    # Clean up any file comments (like --query1)
    sql_query_string = "\n".join([line for line in sql_query_string.split('\n') if not line.strip().startswith('--')])
        
    try:
        # Parse the SQL into an Abstract Syntax Tree (AST)
        # We use parse_one to ensure it's a single query
        parsed_ast = sqlglot.parse_one(sql_query_string, read="duckdb")

        # Extract SELECT columns
        # .expressions holds the list of selected items (e.g., columns, functions)
        select_cols = [col.sql(dialect="duckdb") for col in parsed_ast.expressions]

        # Extract FROM table
        #.find(exp.From) locates the FROM node, .this is the table expression
        from_table_node = parsed_ast.find(exp.From).this
        table_name = from_table_node.name

        # Handle aliases (e.g., "FROM world_presidents p")
        if from_table_node.alias:
            table_name = from_table_node.name

        #  Extract WHERE conditions
        where_conditions = []
        where_node = parsed_ast.find(exp.Where)
        if where_node:
            # .this is the full expression following the WHERE keyword
            where_conditions = _extract_conditions(where_node.this)

        # Extract GROUP BY columns
        group_by_cols = []
        group_by_node = parsed_ast.find(exp.Group)
        if group_by_node:
            group_by_cols = [col.sql(dialect="duckdb") for col in group_by_node.expressions]

        # Extract ORDER BY clauses
        order_by_clauses = []
        order_by_node = parsed_ast.find(exp.Order)
        if order_by_node:
            # .expressions contains the list of ordering columns/expressions
            # .sql() automatically includes 'ASC' or 'DESC' if specified
            order_by_clauses = [col.sql(dialect="duckdb") for col in order_by_node.expressions]

        #  Extract LIMIT value
        limit_val = None
        limit_node = parsed_ast.find(exp.Limit)
        if limit_node:
            limit_val = int(limit_node.expression.sql(dialect="duckdb"))

        # Return the clean dictionary plan
        parsed_plan = {
            "select_columns": select_cols,
            "from_table": table_name,
            "where_conditions": where_conditions,
            "group_by_columns": group_by_cols,
            "order_by_clauses": order_by_clauses,  # <-- NEW
            "limit_value": limit_val,
            "original_query": sql_query_string
            # NOTE: We are ignoring GROUP BY, ORDER BY, LIMIT for now,
            # as the core GALOIS optimizer logic focuses on SELECT, FROM, WHERE.
        }

        LOG.info(f"SQL query parsed successfully (using sqlglot):\n{json.dumps(parsed_plan, indent=2)}")
        return parsed_plan

    except Exception as e:
        LOG.error(f"FATAL ERROR during SQL parsing: {e}")
        LOG.error(f"Problematic query: {sql_query_string}")
        raise ValueError(f"Could not parse query: {sql_query_string}")


# --- Example Test (you can run this with 'python galois_parser.py') ---
if __name__ == "__main__":

    try_dataset = "PRESIDENTS"
    try_dataset_path = DATA_DIR / try_dataset
    queries= load_queries_from_folder(try_dataset_path)

    for i, query in enumerate(queries):
        LOG.info(f"\n--- Parsing Query {i+1} ---")
        parsed = parse_sql(query[1])

