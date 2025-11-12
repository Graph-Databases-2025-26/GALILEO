import json
from unittest.mock import patch
import pytest
from src.llm.baseline_tools import (
    parse_llm_response,
    save_baseline_to_json,
    BASELINE_OUTPUT,
)


# ---------------------------------------------------------------------------
# Dummy classes for helper tests
# ---------------------------------------------------------------------------

class DummyRawResponse:
    """Simple object that mimics the raw LLM response with a .content attribute."""
    def __init__(self, content: str):
        self.content = content

    # These methods are not used here, but we keep them for compatibility
    def _create_llm_instance(self):
        return None

    def get_provider_name(self):
        return "dummy"

    def get_output_tokens(self, raw_response):
        return ["token1", "token2"]


class DummyLLMWrapper:
    """
    Dummy wrapper used only to satisfy parse_llm_response/save_baseline_to_json
    interface requirements.
    """
    def __init__(self):
        self.config = {}

    def _create_llm_instance(self):
        return None

    def get_provider_name(self):
        # Used by save_baseline_to_json to choose output folder
        return "dummy"

    def get_output_tokens(self, raw_response):
        # Used by parse_llm_response to fill the 'tokens' field
        return ["token1", "token2"]


# ---------------------------------------------------------------------------
# Tests for parse_llm_response
# ---------------------------------------------------------------------------

@patch("src.llm.baseline_tools.PydanticOutputParser.parse")
def test_parse_llm_response_valid(mock_parse):
    """
    Ensure that parse_llm_response:
    - uses PydanticOutputParser.parse()
    - returns a dict with result_set, time (rounded), tokens.
    """
    # Mock Pydantic parser returned object
    mock_parse.return_value = type("Resp", (), {"result_set": [{"col1": "val1"}]})()

    dummy_raw = DummyRawResponse('{"result_set":[{"col1":"val1"}]}')
    dummy_wrapper = DummyLLMWrapper()
    dummy_time = 1.2345

    # Call function under test
    result = parse_llm_response(dummy_raw, dummy_time, dummy_wrapper)

    # Assertions
    assert isinstance(result, dict)
    assert "result_set" in result
    assert result["result_set"] == [{"col1": "val1"}]
    assert result["time"] == round(dummy_time, 3)
    assert result["tokens"] == ["token1", "token2"]


# ---------------------------------------------------------------------------
# Tests for save_baseline_to_json
# ---------------------------------------------------------------------------

def test_save_baseline_to_json(tmp_path):
    """
    Ensure that save_baseline_to_json:
    - creates a dataset subfolder
    - writes one JSON file per result
    - preserves data content.
    """
    dataset = "test_dataset"
    baseline = [
        {"col1": "val1"},
        {"col1": "val2"},
    ]
    dummy_wrapper = DummyLLMWrapper()
    b_type = "TEST"

    # Configure BASELINE_OUTPUT to use pytest tmp_path
    BASELINE_OUTPUT[b_type] = {dummy_wrapper.get_provider_name().upper(): tmp_path}

    # Call function under test
    save_baseline_to_json(dataset, baseline, dummy_wrapper, b_type)

    # The folder where JSON files are saved
    folder_path = tmp_path / dataset
    assert folder_path.exists()
    assert folder_path.is_dir()

    # Each result should be saved into query1.json, query2.json, ...
    for i, result in enumerate(baseline, start=1):
        file_path = folder_path / f"query{i}.json"
        assert file_path.exists()
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            assert data == result