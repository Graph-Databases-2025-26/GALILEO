"""
IBM watsonx.ai connection with structured logging.
- Measures latency for generate()
- Logs errors cleanly
"""

import os
import time
import traceback
import json
from dotenv import load_dotenv
from ibm_watsonx_ai import Credentials
from ibm_watsonx_ai.foundation_models import ModelInference
from openai import max_retries

from src.utils.logging_config import logger, log_query_event


def query_watsonx(prompt: str,
                   model_id: str = "ibm/granite-3-8b-instruct",
                   project_id: str | None = None) -> str:
    """
    Send a prompt to IBM watsonx.ai and return the generated text.
    Requires:
      - WATSONX_URL (env)
      - WATSONX_API_KEY (env)
      - WATSONX_PROJECT_ID (env or provided)
    """

    # CONFIGURE THE API KEY
    load_dotenv()
    api_key = os.getenv("WATSONX_API_KEY", "").strip()

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
        response = model.generate(prompt=prompt,  params={"max_new_tokens": 200})
        latency_ms = (time.time() - t0) * 1000.0
        logger.info(f"watsonx.generate latency_ms={latency_ms:.1f}")

        # Supponiamo response sia JSON o un oggetto simile a dizionario
        if isinstance(response, str):
            response_json = json.loads(response)  # se è stringa JSON
        else:
            response_json = response  # se è già dict-like

        # Estrai la parte di interesse
        generated_text = response_json.get('results', [{}])[0].get('generated_text', '')

        logger.info(f"watsonx_response_len chars={len(generated_text)}")
        return response

    except Exception as e:
        logger.error(f"watsonx error: {e}\nTrace:\n{traceback.format_exc()}")
        return f"watsonx error: {e}"


if __name__ == "__main__":
    ans = query_watsonx("Hi watsonx!")
    logger.info(f"Watsonx sample response: {ans}...")

