from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_ibm import ChatWatsonx

from src.utils import LOG

def get_model(config):
    
    provider = config.llm_provider
    
    if provider == "gemini":
        if config.gemini.api_key:
        
            LOG.info(f"Creating Gemini LLM ...")
            
            return ChatGoogleGenerativeAI(
                model = config.gemini.model,
                temperature = config.gemini.temperature,
                max_output_tokens = config.gemini.max_output_tokens,
                google_api_key = config.gemini.api_key,
            )
        else:
            LOG.error("GOOGLE_API_KEY environment variable not set")
            raise RuntimeError("GOOGLE_API_KEY environment variable not set")
        
    elif provider == "watsonx":
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
            LOG.error("WATSONX_API_KEY environment variable not set")
            raise RuntimeError("WATSONX_API_KEY environment variable not set")
    
    else:
        raise ValueError(f"LLM provider not supported: {provider}")
        