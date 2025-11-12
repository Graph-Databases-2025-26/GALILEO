import src.llm.nl_baseline as baseline_nl


class DummyRawResponse:
    def __init__(self, content: str = "FAKE_RAW_RESPONSE"):
        self.content = content


class DummyChain:
    """
    Fake LCEL chain used to capture inputs passed to .invoke()
    instead of calling a real LLM.
    """
    def __init__(self):
        self.calls = []  # store all inputs passed to invoke

    def invoke(self, input_dict):
        # record the call and return a dummy raw response
        self.calls.append(input_dict)
        return DummyRawResponse()


class DummyLLMWrapper:
    """Minimal fake LLM wrapper: we don't need any real behavior here."""
    pass


def test_llm_interaction_nl_baseline_happy_path(monkeypatch):
    """
    Test that llm_interaction_nl_baseline:

    - builds the LLM wrapper via get_llm_wrapper
    - builds the LCEL chain via build_lcel_chain
    - loads SQL queries from the dataset folder
    - calls the chain once per (prompt, SQL query)
    - parses each raw LLM response with parse_llm_response
    - saves the aggregated baseline results with save_baseline_to_json

    All external dependencies (filesystem, LLM, parsing, saving) are mocked.
    """

    
    # 1 Arrange: fake inputs
  
    config = object()        # dummy config object
    database = "WORLD"
    b_type = "NL"
    d_type = "IK"

    prompts = [
        "Who is the president of X?",
        "How many cities are in Y?",
    ]

   
    # 2 Arrange: fake LLM wrapper and LCEL chain
   
    dummy_llm = DummyLLMWrapper()
    dummy_chain = DummyChain()

    # get_llm_wrapper(config) -> dummy_llm
    monkeypatch.setattr(
        baseline_nl,
        "get_llm_wrapper",
        lambda cfg: dummy_llm,
        raising=False,
    )

    # build_lcel_chain(llm, b_type, d_type) -> dummy_chain
    def fake_build_lcel_chain(llm_wrapper, baseline_type, dataset_type):
        # we can assert here the parameters if we want to be strict
        assert llm_wrapper is dummy_llm
        assert baseline_type == b_type
        assert dataset_type == d_type
        return dummy_chain

    monkeypatch.setattr(
        baseline_nl,
        "build_lcel_chain",
        fake_build_lcel_chain,
        raising=False,
    )

    
    # 3 Arrange: fake load_queries_from_folder
    
    # llm_interaction_nl_baseline builds the folder as os.path.join(DATA_DIR, database)
    # we don't care about the actual folder path here, so we ignore the argument.
    def fake_load_queries_from_folder(folder_path: str):
        return [
            "SELECT * FROM presidents;",
            "SELECT COUNT(*) FROM cities;",
        ]

    monkeypatch.setattr(
        baseline_nl,
        "load_queries_from_folder",
        fake_load_queries_from_folder,
        raising=False,
    )

    # 4 Arrange: fake parse_llm_response
   
    parsed_results_queue = [
        {"result_set": [{"a": 1}], "time": 0.1, "tokens": 10},
        {"result_set": [{"b": 2}], "time": 0.2, "tokens": 20},
    ]

    def fake_parse_llm_response(raw_response, elapsed, llm_wrapper):
        # simulate returning one parsed result per call
        assert llm_wrapper is dummy_llm
        assert isinstance(raw_response, DummyRawResponse)
        return parsed_results_queue.pop(0)

    monkeypatch.setattr(
        baseline_nl,
        "parse_llm_response",
        fake_parse_llm_response,
        raising=False,
    )

    # 5 Arrange: fake save_baseline_to_json
    
    saved = {}

    def fake_save_baseline_to_json(dataset, results, llm_wrapper, baseline_type):
        saved["dataset"] = dataset
        saved["results"] = results
        saved["llm_wrapper"] = llm_wrapper
        saved["baseline_type"] = baseline_type

    monkeypatch.setattr(
        baseline_nl,
        "save_baseline_to_json",
        fake_save_baseline_to_json,
        raising=False,
    )

   
    # 6 Act: call the function under test
    
    baseline_nl.llm_interaction_nl_baseline(
        config=config,
        database=database,
        prompts=prompts,
        b_type=b_type,
        d_type=d_type,
    )

   
    # 7Assert: chain was called correctly
    
    # One call per (prompt, query) pair
    assert len(dummy_chain.calls) == 2

    first_call = dummy_chain.calls[0]
    second_call = dummy_chain.calls[1]

    # Check basic structure of the payload
    for call, expected_prompt in zip(dummy_chain.calls, prompts):
        assert call["database"] == database.lower()
        assert call["b_type"] == b_type
        assert call["BASELINE"] == d_type
        assert "query" in call
        assert call["prompt"] in prompts

    
    # 8 Assert: results were saved correctly
    
    assert saved["dataset"] == database
    assert saved["llm_wrapper"] is dummy_llm
    assert saved["baseline_type"] == b_type

    # Two parsed results, one per query
    assert len(saved["results"]) == 2
    assert {"result_set": [{"a": 1}], "time": 0.1, "tokens": 10} in saved["results"]
    assert {"result_set": [{"b": 2}], "time": 0.2, "tokens": 20} in saved["results"]
