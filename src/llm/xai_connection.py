"""
xAI (Grok) connection with structured logging.
- Measures latency for chat.sample()
- Logs errors cleanly
"""

import time
import traceback

from xai_sdk import Client
from xai_sdk.chat import user
from src.utils.logging_config import logger, log_query_event



def query_grok(prompt: str, api_key: str, api_host: str, model: str) -> str:
    """
    Basic synchronous chat completion using xAI SDK.
    `api_key`, `api_host`, and `model` should come from your config system.
    """
    try:
        client = Client(api_key=api_key, api_host=api_host)
        chat = client.chat.create(model=str(model))
        chat.append(user(prompt))

        t0 = time.time()
        completion = chat.sample()
        latency_ms = (time.time() - t0) * 1000.0
        logger.info(f"xai.chat.sample latency_ms={latency_ms:.1f}")

        text = str(completion)
        logger.info(f"xai_response_len chars={len(text)}")
        return text

    except Exception as e:
        logger.error(f"xai error: {e}\nTrace:\n{traceback.format_exc()}")
        return f"xai error: {e}"


if __name__ == "__main__":
    # Replace with your config retrieval (e.g., get_llm_settings("grok"))
    API_KEY = "YOUR_XAI_API_KEY"
    API_HOST = "https://api.x.ai"
    MODEL = "grok-beta"

    ans = query_grok("What is the meaning of life?", API_KEY, API_HOST, MODEL)
    logger.info(f"Grok sample response: {ans[:200]}...")
