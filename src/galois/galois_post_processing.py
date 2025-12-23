import duckdb
from typing import List, Dict, Any, cast
from src.utils.logging_config import LOG


class GaloisPostProcessor:
    """
    Handles local filtering using DuckDB's Relational API
    instead of inline textual SQL queries.
    """

    def filter_results(self, rows: List[Dict[str, Any]], residual_conditions: List[str]) -> List[Dict[str, Any]]:
        # 1. Safety checks
        if not rows:
            return []
        if not residual_conditions:
            return rows

        LOG.info(f"PROC | [FILTER] Processing {len(rows)} rows | Conditions: {residual_conditions}")

        try:
            # Setup in-memory database
            conn = duckdb.connect(database=':memory:')
            # Register the data as a virtual view
            conn.register('virtual_table', rows)

            # Create a Relation object (table abstraction)
            relation = conn.table('virtual_table')

            # Apply filters in a chain
            # DuckDB accepts condition strings in the .filter() method
            # Concatenating multiple clauses with AND
            combined_condition = " AND ".join(residual_conditions)
            filtered_relation = relation.filter(combined_condition)

            # Execute and convert (lazy evaluation)
            filtered_df = filtered_relation.df()
            # Convert to list of dictionaries
            result_rows = filtered_df.to_dict(orient='records')

            LOG.info(f"PROC | [FILTER] Filtering Complete: {len(result_rows)} rows remaining.")
            return cast(List[Dict[str,Any]], result_rows)

        except Exception as e:
            LOG.error(f"ERR  | [FILTER] Relational API Error: {e}")
            # Fallback: return original data in case of critical error
            return rows