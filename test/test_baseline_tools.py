import json

import pytest
from unittest.mock import patch
from src.llm.baseline_tools import parse_llm_response, save_baseline_to_json
from src.llm.baseline_tools import BASELINE_OUTPUT

# Dummy class that simulates a raw_response object
class DummyRawResponse:
    def __init__(self, content):
        self.content = content

# Dummy llm wrapper
class DummyLLMWrapper:
    def __init__(self):
        # dummy config for the constructor
        super().__init__()  # LLMBaseWrapper richiede config: se serve, usare super().__init__(config={})
        self.config = {}

    def _create_llm_instance(self):
        return None

    def get_provider_name(self):
        return "dummy"

    def get_output_tokens(self, raw_response):
        return ["token1", "token2"]

# Test of parse_llm_response with valid input
@patch("src.llm.baseline_tools.PydanticOutputParser.parse")
def test_parse_llm_response_valid(mock_parse):
    # Mock del parser
    mock_parse.return_value = type("Resp", (), {"result_set": [{"col1":"val1"}]})()

    dummy_raw = DummyRawResponse('{"result_set":[{"col1":"val1"}]}')
    dummy_wrapper = DummyLLMWrapper()
    dummy_time = 1.2345

    # Chiamo la funzione
    result = parse_llm_response(dummy_raw, dummy_time, dummy_wrapper)

    # Asserzioni
    assert isinstance(result, dict)
    assert "result_set" in result
    assert result["result_set"] == [{"col1":"val1"}]
    assert result["time"] == round(dummy_time, 3)
    assert result["tokens"] == ["token1", "token2"]


# Test of save_baseline_to_json function
def test_save_baseline_to_json(tmp_path, monkeypatch):
    # Dummy dataset and baseline data
    dataset = "test_dataset"
    baseline = [
        {"col1": "val1"},
        {"col1": "val2"}
    ]
    dummy_wrapper = DummyLLMWrapper()
    b_type = "TEST"

    BASELINE_OUTPUT[b_type] = {dummy_wrapper.get_provider_name().upper(): tmp_path}

    # call the function to test
    save_baseline_to_json(dataset, baseline, dummy_wrapper, b_type)

    # Check that the folder is created
    folder_path = tmp_path / dataset
    assert folder_path.exists()
    assert folder_path.is_dir()

    # Check that the files are created
    for i, result in enumerate(baseline):
        file_path = folder_path / f"query{i+1}.json"
        assert file_path.exists()
        # Check the content of the file
        with open(file_path, "r") as f:
            data = json.load(f)
            assert data == result