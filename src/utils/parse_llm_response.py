# python
import datetime
import re
import json
import time

from dateutil import parser

from src.utils.constants import *
from src.utils.logging_config import logger

def parse_llm_response(response):
    """
    Extracts JSON blocks from the LLM response and builds a unified final JSON.
    Uses time and tokens values returned by the WatsonX model if available.
    """
    try:
        raw_text = response["results"][0]["generated_text"]
    except (KeyError, IndexError):
        return {"error": "Unexpected LLM response structure", "raw": response}

    # Regex to extract JSON blocks
    logger.info(f"raw text: {raw_text}")
    json_blocks = re.findall(r'\{(?:[^{}]|(?:\{[^{}]*\}))*\}', raw_text)
    result_list = []
    total_time = 0.0
    total_tokens = 0
    valid_blocks = 0

    print("\n--- DEBUG: JSON blocks found ---\n")
    for jb in json_blocks:
        print("[Block found]")
        print(jb)
        print()
        try:
            parsed = json.loads(jb)
        except json.JSONDecodeError:
            continue

        """
        if "result_set" in parsed:
            for row in parsed["result_set"]:
                if isinstance(row, dict) and len(row) > 0:
                    value = list(row.values())[0]
                    result_list.append({"originaltitle": value})
        """

        if "result_set" in parsed and parsed["result_set"]:
            result_list.extend(parsed["result_set"])

        # Sum block values (even if they are 0)
        total_time += parsed.get("time", 0.0)
        total_tokens += parsed.get("tokens", 0)
        valid_blocks += 1

    print("--- END DEBUG ---")

    # 🔹 Take tokens returned by the WatsonX model
    generated_tokens = response["results"][0].get("generated_token_count", 0)
    input_tokens = response["results"][0].get("input_token_count", 0)
    print(f"\n--- DEBUG: TOKEN MODEL --- generated={generated_tokens}, input={input_tokens} ---\n")

    # 🔹 If blocks have no tokens, use the model's tokens
    if total_tokens == 0:
        total_tokens = generated_tokens + input_tokens

    # 🔹 If there's no time in blocks, set 0.0
    if total_time == 0.0:
        total_time = 0.0

    return {
        "result_set": result_list,
        "time": total_time,
        "tokens": total_tokens
    }


def format_chain_response(response: dict) -> dict:
    """
    Converte la risposta di chain.invoke() in un JSON strutturato nel formato:
    {
        "result_set": [
            { "column_name": "value" },
            { "column_name": "value" }
        ],
        "time": 0.0,
        "tokens": 0
    }
    """
    start_time = time.time()

    # --- Estrarre il campo 'result' ---
    raw_result = response.get("result", "")
    if not raw_result:
        return {"result_set": [], "time": 0.0, "tokens": 0}

    # --- 1️⃣ Prova a leggere come JSON già formattato ---
    result_set = []
    try:
        result_set = json.loads(raw_result)
        if isinstance(result_set, dict):  # se è singolo oggetto
            result_set = [result_set]
    except json.JSONDecodeError:
        # --- 2️⃣ Se non è JSON valido, prova a interpretare tuple tipo [(1, 'Apple'), ...] ---
        tuple_pattern = re.findall(r"\((.*?)\)", raw_result)
        if tuple_pattern:
            # Otteniamo lista di tuple, cerchiamo di convertirle in dizionari
            rows = []
            for t in tuple_pattern:
                values = [v.strip(" '") for v in t.split(",")]
                rows.append(values)
            # Creiamo dizionari con chiavi generiche se non conosciamo le colonne
            result_set = [{"col" + str(i + 1): v for i, v in enumerate(row)} for row in rows]
        else:
            # --- 3️⃣ Se tutto fallisce, mettiamo il testo grezzo ---
            result_set = [{"text": raw_result.strip()}]

    # --- Metadati ---
    elapsed = round(time.time() - start_time, 3)
    tokens = len(raw_result.split())

    formatted = {
        "result_set": result_set,
        "time": elapsed,
        "tokens": tokens
    }

    return formatted


def parse_response_to_json(response, start_time=None, default_key="result"):
    """
    Converte la response del modello in JSON strutturato con chiave/valore,
    calcola token e tempo di generazione.
    - response: dict generato da model.generate()
    - start_time: datetime, opzionale, se fornito calcola il tempo in secondi
    """
    # Estrazione testo e token
    result = response['results'][0]
    generated_text = result['generated_text'].strip()
    input_tokens = result.get('input_token_count', 0)
    output_tokens = result.get('generated_token_count', 0)
    total_tokens = input_tokens + output_tokens

    # Estrazione chiave/valore dal testo
    matches = re.findall(r"\s*([^:,]+)\s*:\s*([^,]+)", generated_text)
    if matches:
        result_dict = {k.strip(): v.strip() for k, v in matches}
    else:
        result_dict = {default_key: generated_text.strip()}


    # Calcolo tempo
    created_at = parser.isoparse(response['created_at'])
    if start_time is not None:
        elapsed_time = (start_time-created_at).total_seconds()
    else:
        elapsed_time = 0.0  # fallback se start_time non è fornito

    # Costruzione JSON finale
    output_json = {
        "result_set": [result_dict],
        "time": elapsed_time,
        "tokens": total_tokens
    }

    return output_json




def save_llm_response_to_file(dataset_name, data, index):
    # Save the response in a text file

        response_filename = f"query{index}.json"
        response_llm_folder = os.path.join(NL_OUTPUT, dataset_name)
        os.makedirs(response_llm_folder, exist_ok=True)
        json_path = os.path.join(response_llm_folder, response_filename)

        with open(json_path, 'w', encoding='utf-8') as rf:
            json.dump(data, rf, indent=2, ensure_ascii=False)

        logger.info(f"LLM response saved in: {json_path}")