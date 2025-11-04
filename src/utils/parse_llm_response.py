# python
import datetime
import re
import json
import time

from dateutil import parser

from src.utils.constants import *
from src.utils.logging_config import logger

def extract_all_json_objects(text: str):
    json_objects = []
    # regex che tenta di catturare oggetti JSON {...}
    matches = re.findall(r'\{[^{}]*\}', text, re.DOTALL)
    for match in matches:
        try:
            obj = json.loads(match)
            json_objects.append(obj)
        except json.JSONDecodeError:
            continue
    if not json_objects:
        return {"error": "No valid JSON found"}
    # qui puoi decidere come combinare, ad esempio in un unico dict o lista
    combined = {}
    for obj in json_objects:
        combined.update(obj)
    return combined



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