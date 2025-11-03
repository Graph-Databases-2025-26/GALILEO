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
import pandas as pd
from langchain_experimental.sql import SQLDatabaseChain
from langchain_ibm import WatsonxLLM
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits.sql.base import create_sql_agent



from src.utils.logging_config import logger, log_query_event

def process_nl_query_with_watsonx(prompt: str, duckdb_path: str) -> str:
    """
    Esegue una query NL sull'istanza DuckDB specificata usando Watsonx + LangChain SQL agent.
    I parametri model_id, project_id vengono letti da ENV se non forniti.
    """
    load_dotenv()
    api_key = os.getenv("WATSONX_API_KEY", "").strip()
    project_id = os.getenv("WATSONX_PROJECT_ID", "").strip()
    url = os.getenv("WATSONX_URL", "https://us-south.ml.cloud.ibm.com").strip()
    model_id = os.getenv("WATSONX_MODEL_ID", "ibm/granite-3-8b-instruct").strip()

    if not (api_key and project_id and url and model_id):
        raise ValueError("Variabili ENV Watsonx non inizializzate correttamente!")

    llm = WatsonxLLM(
        api_key=api_key,
        project_id=project_id,
        url=url,
        model_id=model_id,
        params={
            "temperature": 0.1,
            "max_new_tokens": 200,
        },
    )

    # Connessione DuckDB (URI con triplo slash, percorsi assoluti/relativi)
    db = SQLDatabase.from_uri(f"duckdb:///{duckdb_path}")

    chain = SQLDatabaseChain.from_llm(
        llm=llm,
        db=db,
        verbose=False,  # mostra i passaggi intermedi
        return_intermediate_steps=True
    )


    """
    system_prompt = (
        "You are an expert data analyst using SQL to answer user questions. "
        "Always decide what SQL query to run, and respond in the following format:\n"
        "Action: the SQL command to run\n"
        "Action Input: SQL query\n"
        "Final Answer: result or explanation.\n"
        "Now think step by step."
    )
    """

    context = db.get_table_info()
    final_prompt = f"{prompt}\n\nSchema:\n{context}"

    try:
        response = chain.invoke(final_prompt)
        if "intermediate_steps" in response:
            for step in response["intermediate_steps"]:
                if isinstance(step, tuple) and "Error" in str(step):
                    print("⚠️ SQL execution error:", step)

    except Exception as e:
        logging.error("Query execution failed: %s", e)
        logging.error("Skipping this query.")
        return None

    #print("MODEL RESPONSE:\n", response)

    steps = response.get("intermediate_steps", [])
    final_result = response.get("result", "")

    sql_query = None
    sql_result = None

    # --- 1️⃣ Prova a estrarre da intermediate_steps ---
    if steps and isinstance(steps[0], dict):
        sql_query = steps[0].get("sql_query") or steps[0].get("sql_cmd")
        sql_result = steps[0].get("result") or steps[0].get("SQLResult")

    # --- 2️⃣ Se non trovato, prova a estrarre dal testo completo del modello ---
    match = re.search(r"SQLQuery[:\s]*(.*?)(?:SQLResult[:\s]*|Answer:|$)", final_result, re.S)
    if match:
        sql_query = match.group(1).strip()
    # estrai SQLResult in modo analogo
    match = re.search(r"SQLResult[:\s]*(.*?)(?:Answer:|$)", final_result, re.S)
    if match:
        sql_result = match.group(1).strip()

    # --- 3️⃣ Stampa i risultati trovati ---
    if sql_query:
        print("\n=== Query generata ===")
        print(sql_query)
    else:
        print("\n⚠️ Nessuna query SQL trovata negli intermediate_steps o nella risposta finale.")

    if sql_result:
        print("\n=== Risultato SQL ===")
        print(sql_result)
    else:
        print("\n⚠️ Nessun risultato SQL trovato negli intermediate_steps o nella risposta finale.")

    print("\n=== Risposta finale LLM ===")
    print(final_result)

    return response






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

