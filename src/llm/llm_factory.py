import json
import os

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_ibm import ChatWatsonx
from config import Config_Loader
from src.utils.constants import *
from dotenv import load_dotenv
from src.utils.logging_config import logger


def create_llm():
    """
    Provide an LLM instance for the specified provider.
    """

    load_dotenv()
    config = Config_Loader().get_config()
    provider = config.llm.provider
    model = config.llm.model
    temperature = config.llm.temperature



    if provider == "gemini":
        #load environment variables
        google_api_key = os.getenv("GOOGLE_API_KEY")

        if not google_api_key:
            logger.error("GOOGLE_API_KEY environment variable not set")

            model = config.gemini.model
            temperature = config.gemini.temperature
            max_tokens = config.gemini.max_output_tokens

            logger.info(
                f"Creating Gemini LLM: model={model}, temperature={temperature}, max_tokens={max_tokens}"
            )

            os.environ["GOOGLE_API_KEY"] = google_api_key
            return ChatGoogleGenerativeAI(
                    model=model,
                    temperature=temperature,
                    google_api_key=google_api_key,
                )

    if provider == "watsonx":
        # configure WatsonX LLM
        load_dotenv()
        ibm_api_key = os.getenv("WATSONX_API_KEY")
        ibm_project_id = os.getenv("WATSONX_PROJECT_ID")
        ibm_url = os.getenv("WATSONX_ENDPOINT")
        ibm_user = os.getenv("WATSONX_USERNAME")

        if not ibm_api_key or not ibm_project_id:
            logger.error("WATSONX_API_KEY or WATSONX_PROJECT_ID environment variable not set")
        os.environ["IBM_API_KEY"] = ibm_api_key
        # Esempio di configurazione WatsonX (in base al tuo setup)
        return ChatWatsonx(
            model_id=model,
            project_id=ibm_project_id,
            credentials=json.dumps({"apikey": ibm_api_key}),
            url=ibm_url,
            username=ibm_user,
            temperature=temperature,
        )

    else:
        raise ValueError(f"LLM provider not supported: {provider}")
