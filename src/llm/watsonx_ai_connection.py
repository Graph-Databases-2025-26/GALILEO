"""
IBM watsonx.ai connection with structured logging.
- Measures latency for generate()
- Logs errors cleanly
"""
import logging
import os
import time
import re
import traceback
import json
from dotenv import load_dotenv
from ibm_watsonx_ai import Credentials
from ibm_watsonx_ai.foundation_models import ModelInference
from langchain.chains.llm import LLMChain
from langchain.output_parsers import StructuredOutputParser, ResponseSchema
from langchain_core.prompts import PromptTemplate
from src.utils.WatsonxResponse import WatsonxResponse
from langchain_ibm import WatsonxLLM
from langchain_community.utilities import SQLDatabase
from langchain.output_parsers import PydanticOutputParser
from src.utils.constants import FULL_JSON_FORMAT
from src.utils.logging_config import logger, log_query_event
from src.utils.parse_llm_response import extract_all_json_objects


class WatsonxCredentials:
    pass


def process_nl_query_with_watsonx(prompt: str, duckdb_path: str) -> dict:
    """
    Runs an NL query on the specified DuckDB instance using Watsonx + LangChain SQL agent.
    The model_id and project_id parameters are read from ENV if not provided.
    """
    load_dotenv()
    api_key = os.getenv("WATSONX_API_KEY", "").strip()
    project_id = os.getenv("WATSONX_PROJECT_ID", "").strip()
    url = os.getenv("WATSONX_URL", "https://us-south.ml.cloud.ibm.com").strip()
    model_id = os.getenv("WATSONX_MODEL_ID", "ibm/granite-3-8b-instruct").strip()

    if not (api_key and project_id and url and model_id):
        raise ValueError("ENV Watsonx variables not initialised correctly!")

    llm = WatsonxLLM(
        model_id=model_id,
        apikey=api_key,  # 🔹 non api_key
        url=url,
        project_id=project_id,
        params={
            "temperature": 0.1,
            "max_new_tokens": 200
        }
    )

    # DuckDB connection and DB schema retrieval
    db = SQLDatabase.from_uri(f"duckdb:///{duckdb_path}")
    db_schema = db.get_table_info()

    output_parser = PydanticOutputParser(pydantic_object=WatsonxResponse)
    format_instructions = output_parser.get_format_instructions()

    prompt_template = PromptTemplate(
        template= ( """
            You are a precise data extraction model. 
            Return ONLY a valid JSON object that strictly matches the following schema.
            Do NOT include any additional text, commentary, or multiple responses.
            DO NOT omit any key. Always include all keys even if values are empty or zero.
            
            Format:
            {format_instructions}
            
            User query: {user_query}
            """
        ),
        input_variables=["user_query","db_schema"],
        partial_variables={"format_instructions": format_instructions},
    )

    chain=LLMChain(
        llm=llm,
        prompt=prompt_template,
    )

    # 🔹 Execute and measure the time
    t0 = time.time()
    try:
        # 🔹 Obtain the raw response
        raw_response = chain.run(user_query=prompt, db_schema=db_schema)

        exec_time = time.time() - t0

        # 🔹 Extract and merge all the blocks
        parsed_response = extract_all_json_objects(raw_response)

        if isinstance(parsed_response, list):
            combined = {}
            for d in parsed_response:
                if isinstance(d, dict):
                    combined.update(d)
        else:
            combined = parsed_response

        # 🔹 Create the final dictionary in a format consistent with WatsonxResponse.
        result = {
            "result_set": combined,
            "time": round(exec_time, 3),
            "tokens": parsed_response.get("tokens", 0)
        }

        return result

    except Exception as e:
        logger.error(f"Error during the running time: {e}")
        return {"error": str(e)}







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

        if isinstance(response, str):
            response_json = json.loads(response)  # se è stringa JSON
        else:
            response_json = response  # se è già dict-like

        # Extract the the generated_text from the response
        generated_text = response_json.get('results', [{}])[0].get('generated_text', '')

        logger.info(f"watsonx_response_len chars={len(generated_text)}")
        return response

    except Exception as e:
        logger.error(f"watsonx error: {e}\nTrace:\n{traceback.format_exc()}")
        return f"watsonx error: {e}"


if __name__ == "__main__":
    ans = query_watsonx("Hi watsonx!")
    logger.info(f"Watsonx sample response: {ans}...")

