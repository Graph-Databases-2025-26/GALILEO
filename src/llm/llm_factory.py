from .llm_wrappers import LLMBaseWrapper, GeminiWrapper, WatsonxWrapper
from src.utils import LOG

PROVIDER_MAP = {
    "gemini": GeminiWrapper,
    "watsonx": WatsonxWrapper
}

def get_llm_wrapper(config) -> LLMBaseWrapper:
    
    provider = config.llm_provider.lower()
    
    if provider not in PROVIDER_MAP:
        raise ValueError(f"LLM provider non supportato: {provider}")

    WrapperClass = PROVIDER_MAP[provider]
    return WrapperClass(config)

        