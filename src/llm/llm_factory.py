from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_ibm import ChatWatsonx  # ipotetico, o wrapper simile
from src.utils.constants import *
from dotenv import load_dotenv
from src.utils.logging_config import logger


def create_llm(provider: str, temperature: float = 0.1):
    """
    Provide an LLM instance for the specified provider.
    """
    provider = provider.lower()

    if provider == "gemini":
        # Configure the API KEY
        load_dotenv()
        google_api_key = os.getenv("GEMINI_API_KEY")
        if not google_api_key:
            logger.error("GOOGLE_API_KEY environment variable not set")
        os.environ["GOOGLE_API_KEY"] = google_api_key
        return ChatGoogleGenerativeAI(
            model=SUPPORTED_MODELS["gemini"],
            temperature=temperature,
            google_api_key=google_api_key,
        )

    elif provider == "watsonx":
        # Configure the API KEY
        load_dotenv()
        ibm_api_key = os.getenv("WATSONX_API_KEY")
        ibm_project_id = os.getenv("WATSONX_PROJECT_ID")
        if not ibm_api_key or not ibm_project_id:
            logger.error("WATSONX_API_KEY or WATSONX_PROJECT_ID environment variable not set")
        os.environ["IBM_API_KEY"] = ibm_api_key
        # Esempio di configurazione WatsonX (in base al tuo setup)
        return ChatWatsonx(
            model_id=SUPPORTED_MODELS["watsonx"],
            project_id=ibm_project_id,
            credentials={"apikey": ibm_api_key},
            temperature=temperature,
        )

    else:
        raise ValueError(f"LLM provider not supported: {provider}")
