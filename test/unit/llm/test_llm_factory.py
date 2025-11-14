from src.config import AppConfig
from src.llm import get_llm_wrapper 

from src.utils import ERR_UNSUPPORTED_PROVIDER


from unittest.mock import MagicMock
import pytest

def test_get_llm_wrapper_gemini_success(mocker, app_config: AppConfig):
    """
    Verifies that the factory function returns the GeminiWrapper when 'gemini' is the provider.

    The test uses mocking to ensure that:
    1. The correct wrapper class (mocked GeminiWrapper) is called once with the AppConfig.
    2. The returned object is the instance of the mocked wrapper.

    Args:
        mocker: pytest-mock fixture.
        app_config (AppConfig): Fixture containing the application configuration.
    """
    
    mock_gemini = MagicMock(name='MockGeminiClass')
    mocker.patch.dict("src.llm.llm_factory.PROVIDER_MAP", {"gemini": mock_gemini})
    
    app_config.llm_provider = "gemini"
    
    wrapper = get_llm_wrapper(app_config)
    
    mock_gemini.assert_called_once_with(app_config)
    
    assert wrapper == mock_gemini.return_value
    
    
def test_get_llm_wrapper_watsonx_success(mocker, app_config: AppConfig):
    """
    Verifies that the factory function returns the WatsonxWrapper when 'watsonx' is the provider.

    Ensures the correct wrapper class (mocked WatsonxWrapper) is called once with the AppConfig.

    Args:
        mocker: pytest-mock fixture.
        app_config (AppConfig): Fixture containing the application configuration.
    """
    
    mock_watsonx = MagicMock(name='MockWatsonxClass')
    mocker.patch.dict("src.llm.llm_factory.PROVIDER_MAP", {"watsonx": mock_watsonx})
    
    app_config.llm_provider = "watsonx"
    
    wrapper = get_llm_wrapper(app_config)
    
    mock_watsonx.assert_called_once_with(app_config)
    
    assert wrapper == mock_watsonx.return_value


def test_get_llm_wrapper_case_insensitivity(mocker, app_config: AppConfig):
    """
    Verifies that the provider selection in the factory function is case-insensitive.

    Sets the provider to "GeMiNi" and asserts that the correct (mocked) wrapper class is still called.

    Args:
        mocker: pytest-mock fixture.
        app_config (AppConfig): Fixture containing the application configuration.
    """
    
    mock_gemini = MagicMock(name='MockGeminiClass')
    mocker.patch.dict("src.llm.llm_factory.PROVIDER_MAP", {"gemini": mock_gemini})
    
    app_config.llm_provider = "GeMiNi"
    
    get_llm_wrapper(app_config)
    
    mock_gemini.assert_called_once_with(app_config)


def test_get_llm_wrapper_unsupported_provider_raises_error(app_config: AppConfig):
    """
    Verifies that a ValueError is raised when an unsupported provider is specified.

    Args:
        app_config (AppConfig): Fixture containing the application configuration.
    """
    
    app_config.llm_provider = "openai"
    
    with pytest.raises(ValueError, match=ERR_UNSUPPORTED_PROVIDER.format(app_config.llm_provider)):
        get_llm_wrapper(app_config)