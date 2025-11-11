import json

import pytest
from unittest.mock import patch
from src.llm.baseline_tools import parse_llm_response, save_baseline_to_json
from src.llm.baseline_tools import BASELINE_OUTPUT
from src.llm.baseline_nl import llm_interaction_nl_baseline


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

class DummyChain:
    def __init__(self):
        self.calls = []  # store all inputs passed to invoke

    def invoke(self, input_dict):
        # record the call and return a dummy raw response
        self.calls.append(input_dict)
        return DummyRawResponse(content="FAKE_RAW_RESPONSE")
    

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

# Test of llm_interaction_nl_baseline 
@patch("src.llm.baseline_nl.save_baseline_to_json")
@patch("src.llm.baseline_nl.parse_llm_response")
@patch("src.llm.baseline_nl.load_queries_from_folder")
@patch("src.llm.baseline_nl.build_lcel_chain")
@patch("src.llm.baseline_nl.get_llm_wrapper")
def test_llm_interaction_nl_baseline_with_dummy_chain(
    mock_get_llm_wrapper,
    mock_build_lcel_chain,
    mock_load_queries_from_folder,
    mock_parse_llm_response,
    mock_save_baseline_to_json,
):
    """
    Test that llm_interaction_nl_baseline:
    - builds the LLM wrapper and the LCEL chain
    - loads SQL queries from the correct folder
    - invokes the chain once per (prompt, query)
    - parses each raw LLM response
    - saves the baseline results to JSON
    """
    # Fake inputs
    config = object()
    database = "WORLD"
    b_type = "NL"
    d_type = "IK"

    # Two fake prompts (NL questions)
    prompts = [
        "Who is the president of X?",
        "How many cities are in Y?",
    ]

    # Fake SQL queries loaded from the dataset folder
    mock_load_queries_from_folder.return_value = [
        ("queries_world.sql", "SELECT * FROM presidents;"),
        ("queries_world.sql", "SELECT COUNT(*) FROM cities;"),
    ]

    # Dummy LLM wrapper returned by get_llm_wrapper (quello definito sopra)
    dummy_llm = DummyLLMWrapper()
    mock_get_llm_wrapper.return_value = dummy_llm

    # DummyChain al posto della LCEL chain reale
    dummy_chain = DummyChain()
    mock_build_lcel_chain.return_value = dummy_chain

    # Fake parse_llm_response output: one dict per call
    mock_parse_llm_response.side_effect = [
        {"result_set": [{"a": 1}], "time": 0.1, "tokens": 10},
        {"result_set": [{"b": 2}], "time": 0.2, "tokens": 20},
    ]

    # ---- Act ----
    llm_interaction_nl_baseline(
        config=config,
        database=database,
        prompts=prompts,
        b_type=b_type,
        d_type=d_type,
    )

    # 1 LLM wrapper created with the given config
    mock_get_llm_wrapper.assert_called_once_with(config)

    # 2 LCEL chain built with the dummy LLM and the correct baseline type/dataset type
    mock_build_lcel_chain.assert_called_once_with(dummy_llm, b_type, d_type)

    # 3 Queries loaded from folder once
    mock_load_queries_from_folder.assert_called_once()

    # 4 The chain invoked once per (prompt, query)
    assert len(dummy_chain.calls) == 2

    # Check the structure of the first call
    first_call = dummy_chain.calls[0]
    assert first_call["database"] == database.lower()
    assert first_call["b_type"] == b_type
    assert "query" in first_call
    assert "prompt" in first_call
    assert "BASELINE" in first_call

    # 5 parse_llm_response called twice (one for each raw_response)
    assert mock_parse_llm_response.call_count == 2

    # 6 save_baseline_to_json called once with the aggregated results
    mock_save_baseline_to_json.assert_called_once()
    args, kwargs = mock_save_baseline_to_json.call_args

    # args = (dataset, baseline_results, llm_wrapper, baseline_type)
    assert args[0] == database
    assert len(args[1]) == 2          # one result per query
    assert args[2] is dummy_llm
    assert args[3] == b_type