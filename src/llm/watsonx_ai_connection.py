"""
IBM watsonx.ai connection with structured logging.
- Measures latency for generate()
- Logs errors cleanly
"""

import os
import time
import traceback
from dotenv import load_dotenv
from ibm_watsonx_ai import Credentials
from ibm_watsonx_ai.foundation_models import ModelInference

from src.utils.logging_config import logger, log_query_event


#CONFIGURE THE API KEY
load_dotenv()
api_key = os.getenv("WATSONX_API_KEY", "").strip()

def query_watsonx(prompt: str,
                   model_id: str = "openai/gpt-oss-120b",
                   project_id: str | None = None) -> str:
    """
    Send a prompt to IBM watsonx.ai and return the generated text.
    Requires:
      - WATSONX_URL (env)
      - WATSONX_API_KEY (env)
      - WATSONX_PROJECT_ID (env or provided)
    """
    try:

        url = os.getenv("WATSONX_URL", "https://us-south.ml.cloud.ibm.com").strip()
        project = project_id or os.getenv("WATSONX_PROJECT_ID", "").strip()

        if not api_key:
            logger.error("WATSONX_API_KEY environment variable not set")
            raise SystemExit(1)
        if not project:
            logger.error("WATSONX_PROJECT_ID missing (provide arg or env var)")
            raise SystemExit(1)

        logger.info(f"Initializing watsonx.ai model_id={model_id}")
        creds = Credentials(url=url, api_key=api_key)
        model = ModelInference(model_id=model_id, credentials=creds, project_id=project)

        t0 = time.time()
        response = model.generate(prompt=prompt)
        latency_ms = (time.time() - t0) * 1000.0
        logger.info(f"watsonx.generate latency_ms={latency_ms:.1f}")

        # Some SDKs return complex objects; stringify safely
        text = str(response)
        logger.info(f"watsonx_response_len chars={len(text)}")
        return text

    except Exception as e:
        logger.error(f"watsonx error: {e}\nTrace:\n{traceback.format_exc()}")
        return f"watsonx error: {e}"


if __name__ == "__main__":
    ans = query_watsonx("Hello, Watsonx!")
    logger.info(f"Watsonx sample response: {ans[:200]}...")
