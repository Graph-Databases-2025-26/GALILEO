from src.utils import ERR_UNSUPPORTED_PROVIDER
from .llm_wrappers import LLMBaseWrapper, GeminiWrapper, WatsonxWrapper

PROVIDER_MAP = {
    "gemini": GeminiWrapper,
    "watsonx": WatsonxWrapper
}

def get_llm_wrapper(config) -> LLMBaseWrapper:
    """
    Factory function to create the appropriate LLM wrapper instance based on the configuration.

    It reads the `llm_provider` from the configuration and instantiates the corresponding wrapper class.

    Args:
        config: The application configuration object which specifies the LLM provider (e.g., `config.llm_provider`).

    Returns:
        LLMBaseWrapper: An initialized wrapper instance (e.g., `GeminiWrapper`).

    Raises:
        ValueError: If the specified LLM provider is not supported in `PROVIDER_MAP`.
    """
    
    provider = config.llm_provider.lower()
    
    if provider not in PROVIDER_MAP:
        raise ValueError(ERR_UNSUPPORTED_PROVIDER.format(provider))

    WrapperClass = PROVIDER_MAP[provider]
    return WrapperClass(config)
