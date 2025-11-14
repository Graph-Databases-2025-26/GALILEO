from src.config import AppConfig 
from src.llm import GeminiWrapper, WatsonxWrapper 
from src.utils import ERR_GEMINI_API_KEY_MISSING, ERR_WATSONX_API_KEY_MISSING

from unittest.mock import MagicMock
import pytest


def test_gemini_wrapper_creation_success(app_config: AppConfig, mock_langchain_classes):
    """
    Verifies the correct creation of the GeminiWrapper instance and its internal LLM object.

    This test ensures that:
    1. The LangChain LLM class (mocked) is called exactly once.
    2. The LLM constructor receives configuration parameters (model, temperature, tokens, API key) 
       that match the values provided in the AppConfig fixture.
    3. The instance returned by `get_llm_instance()` is the mocked LLM object.
    4. The provider name is correctly returned as "gemini".

    Args:
        app_config (AppConfig): Fixture containing the application configuration.
        mock_langchain_classes (tuple): Fixture containing mocks for external LangChain classes.
    """
    
    mock_gemini_chat, _ = mock_langchain_classes

    wrapper = GeminiWrapper(app_config)
    
    instance = wrapper.get_llm_instance()

    mock_gemini_chat.assert_called_once_with(
        model=app_config.gemini.model,
        temperature=app_config.gemini.temperature,
        max_output_tokens=app_config.gemini.max_output_tokens,
        google_api_key=app_config.gemini.api_key,
    )
    
    assert instance == mock_gemini_chat.return_value
    
    assert wrapper.get_provider_name() == "gemini"


def test_gemini_wrapper_creation_no_key_raises_runtimeerror(mocker, app_config_gemini_no_key):
    """
    Verifies that a RuntimeError is raised if the Gemini API key is missing from the configuration.

    Args:
        mocker: pytest-mock fixture.
        app_config_gemini_no_key: Fixture with the Gemini API key intentionally unset.
    """
    
    with pytest.raises(RuntimeError, match=ERR_GEMINI_API_KEY_MISSING):
        wrapper = GeminiWrapper(app_config_gemini_no_key)
        wrapper.get_llm_instance()


def test_gemini_wrapper_get_output_tokens():
    """
    Verifies the correct extraction of output token count from a mocked LLM response object.

    Ensures that the `get_output_tokens` method correctly accesses the 'output_tokens' 
    field within the `usage_metadata` of the BaseMessage object.
    """
    
    mock_response = MagicMock()
    mock_response.usage_metadata = {"output_tokens": 420, "input_tokens": 100}
    
    wrapper = GeminiWrapper(MagicMock())
    
    assert wrapper.get_output_tokens(mock_response) == 420


def test_watsonx_wrapper_creation_success(app_config: AppConfig, mock_langchain_classes):
    """
    Verifies the correct creation of the WatsonxWrapper instance and its internal LLM object.

    This test ensures that:
    1. The LangChain Watsonx LLM class (mocked) is called exactly once.
    2. The LLM constructor receives the correct configuration parameters (model_id, API key, 
       endpoint, project_id, and temperature/max_tokens within the `params` dictionary)
       from the AppConfig fixture.
    3. The provider name is correctly returned as "watsonx".

    Args:
        app_config (AppConfig): Fixture containing the application configuration.
        mock_langchain_classes (tuple): Fixture containing mocks for external LangChain classes.
    """
    
    _, mock_watsonx_chat = mock_langchain_classes
    
    wrapper = WatsonxWrapper(app_config)
    
    instance = wrapper.get_llm_instance()
    
    # Assert 1: The external class ChatWatsonx was called
    mock_watsonx_chat.assert_called_once_with(
        model_id=app_config.watsonx.model,
        api_key=app_config.watsonx.api_key,
        url=app_config.watsonx.endpoint,
        project_id=app_config.watsonx.project_id,
        params={
            "temperature": app_config.watsonx.temperature,
            "max_new_tokens": app_config.watsonx.max_tokens,
        }
    )
    
    assert instance == mock_watsonx_chat.return_value
    assert wrapper.get_provider_name() == "watsonx"

def test_watsonx_wrapper_creation_no_key_raises_runtimeerror(app_config_watsonx_no_key, mocker):
    """
    Verifies that a RuntimeError is raised if the Watsonx API key is missing from the configuration.

    Args:
        app_config_watsonx_no_key: Fixture with the Watsonx API key intentionally unset.
        mocker: pytest-mock fixture.
    """
    
    with pytest.raises(RuntimeError, match=ERR_WATSONX_API_KEY_MISSING):
        wrapper = WatsonxWrapper(app_config_watsonx_no_key)
        wrapper.get_llm_instance()


def test_watsonx_wrapper_get_output_tokens():
    """
    Verifies the correct extraction of output token count from a mocked LLM response object.

    Ensures that the `get_output_tokens` method correctly retrieves the 'output_tokens' 
    value from the mocked `usage_metadata`.
    """
    
    mock_response = MagicMock()
    mock_response.usage_metadata = {"output_tokens": 777, "input_tokens": 50}
    
    wrapper = WatsonxWrapper(MagicMock())
    
    assert wrapper.get_output_tokens(mock_response) == 777