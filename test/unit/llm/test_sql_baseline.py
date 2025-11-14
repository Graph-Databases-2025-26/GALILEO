from src.config import AppConfig 
from src.llm import execute_baseline_sql

from conftest import MOCK_PARSED_RESULT

from unittest.mock import MagicMock
import pytest, time


def test_execute_baseline_sql_query_success(app_config: AppConfig, mock_dependencies):
    """
    Verifies the standard successful execution flow for the baseline SQL query function.

    The test confirms that for a list of multiple queries:
    1. All setup dependencies (LLM builder, chain builder) are called once.
    2. The chain's `invoke` method is called once for each query.
    3. The `parse_llm_response` function is called once for each query.
    4. The `save_baseline_to_json` function is called once at the end with the complete set of results.

    Args:
        app_config (AppConfig): Fixture containing the application configuration.
        mock_dependencies (dict): Fixture containing mocks for all external functions/classes used.
    """
    
    queries = ["query 1", "query 2", "query 3"]
    
    mock_dependencies["time"].side_effect = [10.0, 10.1, 10.1, 10.2, 10.2, 10.3]
    
    execute_baseline_sql(
        config=app_config, 
        database="db_name", 
        queries=queries, 
        b_type="SQL"
    )

    mock_dependencies["llm_builder"].assert_called_once_with(app_config)
    mock_dependencies["chain_builder"].assert_called_once()
    
    assert mock_dependencies["chain"].invoke.call_count == len(queries)
    
    assert mock_dependencies["parse"].call_count == len(queries)
    
    mock_dependencies["save"].assert_called_once()
    
    saved_results = mock_dependencies["save"].call_args[0][1] 
    assert len(saved_results) == len(queries)


def test_execute_baseline_sql_query_with_llm_error(app_config: AppConfig, mock_dependencies):
    """
    Verifies robust error handling: a failure in one query does not halt the execution of subsequent queries.

    The test configures the chain's `invoke` method to fail on the second call. It asserts that:
    1. The loop continues, and all queries are attempted (`invoke` called for all).
    2. The error is logged exactly once.
    3. Only the successful results are collected and saved (2 out of 3 total queries).

    Args:
        app_config (AppConfig): Fixture containing the application configuration.
        mock_dependencies (dict): Fixture containing mocks for all external functions/classes used.
    """
    
    queries = ["good query 1", "failing query", "good query 2"]

    mock_dependencies["chain"].invoke.side_effect = [
        "raw response 1",                      
        Exception("Mock Parsing/LLM Error"),   
        "raw response 3"                      
    ]
    
    mock_dependencies["parse"].side_effect = [
        MOCK_PARSED_RESULT,
        MOCK_PARSED_RESULT 
    ]
    
    execute_baseline_sql(app_config, "db_name", queries, "SQL")
    
    assert mock_dependencies["chain"].invoke.call_count == len(queries)
    
    mock_dependencies["log"].error.assert_called_once()
    
    mock_dependencies["save"].assert_called_once()
    
    saved_results = mock_dependencies["save"].call_args[0][1]
    assert len(saved_results) == 2


def test_execute_baseline_sql_query_empty_queries(app_config: AppConfig, mock_dependencies):
    """
    Verifies the function's behavior when the input list of queries is empty.

    It asserts that:
    1. The chain's `invoke` method is never called.
    2. The `save_baseline_to_json` function is called once with an empty list of results.

    Args:
        app_config (AppConfig): Fixture containing the application configuration.
        mock_dependencies (dict): Fixture containing mocks for all external functions/classes used.
    """
    
    queries = []

    execute_baseline_sql(app_config, "db_name", queries, "SQL")

    mock_dependencies["chain"].invoke.assert_not_called()
    
    mock_dependencies["save"].assert_called_once()
    
    saved_results = mock_dependencies["save"].call_args[0][1]
    assert saved_results == []