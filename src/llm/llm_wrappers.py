from langchain_core.language_models import BaseChatModel
from langchain_core.messages import BaseMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_ibm import ChatWatsonx

from abc import ABC, abstractmethod

from src.utils import LOG

class LLMBaseWrapper(ABC):
    
    def __init__(self, config):
        self.config = config
        self.llm_instance: BaseChatModel = None

    @abstractmethod
    def _create_llm_instance(self, config):
        pass

    @abstractmethod
    def get_output_tokens(self, raw_response: BaseMessage) -> int:
        pass
        
    @abstractmethod
    def get_provider_name(self) -> str:
        pass
        
    def get_llm_instance(self) -> BaseChatModel:
        if not self.llm_instance:
            self.llm_instance = self._create_llm_instance(self.config)
        
        return self.llm_instance
    

class GeminiWrapper(LLMBaseWrapper):
    
    def get_provider_name(self) -> str:
        return "gemini"
        
    def _create_llm_instance(self, config):
        if config.gemini.api_key:
            
            LOG.info(f"Creating Gemini LLM ...")
            
            return ChatGoogleGenerativeAI(
                model = config.gemini.model,
                temperature = config.gemini.temperature,
                max_output_tokens = config.gemini.max_output_tokens,
                google_api_key = config.gemini.api_key,
            )
            
        else:
            raise RuntimeError("GOOGLE_API_KEY non configurata per Gemini.")

    def get_output_tokens(self, raw_response: BaseMessage) -> int:
        return raw_response.usage_metadata.get("output_tokens", 0)


class WatsonxWrapper(LLMBaseWrapper):
    
    def get_provider_name(self) -> str:
        return "watsonx"
        
    def _create_llm_instance(self, config):
        
        if config.watsonx.api_key:
            
            LOG.info(f"Creating Watsonx LLM: model ...")
            
            return ChatWatsonx(
                model_id  = config.watsonx.model,
                api_key = config.watsonx.api_key,
                url = config.watsonx.endpoint, 
                project_id = config.watsonx.project_id,
                params = {
                    "temperature": config.watsonx.temperature,
                    "max_new_tokens": config.watsonx.max_tokens,
                }
            )
            
        else:
            raise RuntimeError("WATSONX_API_KEY non configurata per Watsonx.")

    def get_output_tokens(self, raw_response: BaseMessage) -> int:
        return raw_response.usage_metadata.get("output_tokens", 0) 
       