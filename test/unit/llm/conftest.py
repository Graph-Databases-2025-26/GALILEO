from src.config import Config_Loader, AppConfig

from unittest.mock import MagicMock
from pathlib import Path
import pytest, os, yaml

MOCK_PARSED_RESULT = {
    "result_set": [{"col1": "val1"}, {"col2": "val2"}],
    "time": 0.06,
    "tokens": 100
}

@pytest.fixture
def mock_yaml_data():

    return {
        "llm_provider": "watsonx",
        "database": {"run": "database_name"},
        "execution": {"max_retries": 3, "backoff_sec": 2.0, "scan": "full"},
        "io": {
            "queries_dir": "/mock/queries",
            "prompts_dir": "/mock/prompts",
            "outputs_dir": "/mock/outputs"
        },
        "logging": {"level": "INFO", "json_format": False},
        "gemini": {
            "model": "gemini-2.5-flash",
            "temperature": 0.5,
            "max_output_tokens": 1024
        },
        "watsonx": {
            "model": "ibm/granite-3-8b-instruct",
            "max_tokens": 1024,
            "temperature": 0.1
        }
    }


@pytest.fixture
def mock_config_loader(mocker, mock_yaml_data) -> Config_Loader:

    # 1. MOCK VARIABILI D'AMBIENTE (necessarie per i campi Field(default_factory=...))
    # 1. MOCK ENVIRONMENT VARIABLES (necessary for Field(default_factory=...) fields)
    mocker.patch.dict(os.environ, {
        "GEMINI_API_KEY": "mock_gemini_key",
        "WATSONX_API_KEY": "mock_watsonx_key",
        "WATSONX_PROJECT_ID": "mock_watsonx_project_id",
        "WATSONX_ENDPOINT": "mock_watsonx_endpoint"
    })

    # 2. MOCK yaml.safe_load per restituire i dati fittizi
    # 2. MOCK yaml.safe_load to return dummy data
    mocker.patch("yaml.safe_load", return_value=mock_yaml_data)

    # 3. MOCK Path.exists() per simulare l'esistenza del file
    # 3. MOCK Path.exists() to simulate the file's existence
    mock_path_exists = mocker.patch("pathlib.Path.exists", return_value=True)

    mock_open = mocker.patch("builtins.open", new_callable=mocker.mock_open)
    
    mock_open.return_value.read.return_value = yaml.dump(mock_yaml_data)
    
    # 4. Crea l'istanza di Config_Loader, che ora usa i mock
    # 4. Create the Config_Loader instance, which now uses the mocks
    loader = Config_Loader(config_path="/mock/config.yaml")

    # 5. Verifica che AppConfig sia stata creata correttamente
    # 5. Verify that AppConfig has been created correctly
    assert isinstance(loader.get_config(), AppConfig)

    return loader


@pytest.fixture
def app_config(mock_config_loader) -> AppConfig:

    return mock_config_loader.get_config()


@pytest.fixture
def mock_dependencies(mocker):
    """Mocks all external dependencies used by execute_baseline_sql_query."""

    # Mock of the LLM wrapper object (return value of get_llm_wrapper) 
    mock_llm_model = MagicMock(name="MockLLMModel")
    mock_llm_builder = mocker.patch("src.llm.sql_baseline.get_llm_wrapper", return_value=mock_llm_model)
    mock_llm_model.get_provider_name.return_value = "watsonx"

    # Mock of the LCEL chain (lcel_chain)
    mock_chain = MagicMock(name="MockLCELChain")
    mock_chain_builder = mocker.patch("src.llm.sql_baseline.build_lcel_chain", return_value=mock_chain)

    # Mock of the parsing function (final result of the pipeline per query)
    # Mock della funzione di parsing (risultato finale della pipeline per query)
    mock_parse = mocker.patch(
        "src.llm.sql_baseline.parse_llm_response",
        return_value=MOCK_PARSED_RESULT
    )

    # Mock of the saving function
    # Mock della funzione di salvataggio
    mock_save = mocker.patch("src.llm.sql_baseline.save_baseline_to_json")

    # Mock of LOG and time
    # Mock di LOG e time
    mock_log = mocker.patch("src.llm.sql_baseline.LOG")
    mock_time = mocker.patch("src.llm.sql_baseline.time", return_value=0)

    return {
        "llm_model": mock_llm_model,
        "llm_builder" : mock_llm_builder,
        "chain": mock_chain,
        "chain_builder": mock_chain_builder,
        "parse": mock_parse,
        "save": mock_save,
        "log": mock_log,
        "time": mock_time
    }


@pytest.fixture
def mock_langchain_classes(mocker):
    """Mocks external classes to prevent real initialization."""
    # Mock for Gemini
    # Mock per Gemini
    mock_gemini_chat = mocker.patch("src.llm.llm_wrappers.ChatGoogleGenerativeAI")

    # Mock for Watsonx
    # Mock per Watsonx
    mock_watsonx_chat = mocker.patch("src.llm.llm_wrappers.ChatWatsonx")

    # Mock for LOG
    # Mock per LOG
    mocker.patch("src.llm.llm_wrappers.LOG")

    return mock_gemini_chat, mock_watsonx_chat


@pytest.fixture
def app_config_watsonx(app_config: AppConfig) -> AppConfig:
    """Returns a config with llm_provider set to 'watsonx'."""
    
    app_config.llm_provider = "watsonx"
    return app_config

@pytest.fixture
def app_config_gemini_no_key(app_config: AppConfig) -> AppConfig:
    """Returns a config with the Gemini key missing (None)."""

    app_config.gemini.api_key = None
    return app_config

@pytest.fixture
def app_config_watsonx_no_key(app_config: AppConfig) -> AppConfig:
    """Returns a config with the Watsonx key missing (None)."""
    app_config.watsonx.api_key = None
    return app_config