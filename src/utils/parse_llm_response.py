import re
import json


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


def extract_json_from_response(response_text: str):
    """
        Estrae il blocco JSON da una risposta LLM in formato Jinja o Markdown.
        Supporta sia stringhe che dizionari.
        """
    # Se è un dict (come nel tuo caso), prendiamo il campo 'text'
    if isinstance(response_text, dict):
        if "text" in response_text:
            response_text = response_text["text"]
        else:
            response_text = str(response_text)

    # Se non è ancora stringa, forziamo la conversione
    if not isinstance(response_text, str):
        response_text = str(response_text)

    # Cerca il blocco tra ```json ... ```
    match = re.search(r"```json\s*(\{.*?\})\s*```", response_text, re.DOTALL)
    if not match:
        print("⚠️ Nessun blocco JSON trovato nella risposta.")
        return {}

    json_str = match.group(1)
    try:
        data = json.loads(json_str)
        return data
    except json.JSONDecodeError as e:
        print(f"❌ Errore nel parsing JSON: {e}")
        print(f"Contenuto problematico:\n{json_str[:200]}...")
        return {}

def save_llm_response_to_file(dataset_name, data, index):
    # Save the response in a text file

        response_filename = f"query{index}.json"
        response_llm_folder = os.path.join(NL_OUTPUT, dataset_name)
        os.makedirs(response_llm_folder, exist_ok=True)
        json_path = os.path.join(response_llm_folder, response_filename)

        with open(json_path, 'w', encoding='utf-8') as rf:
            json.dump(data, rf, indent=2, ensure_ascii=False)

        logger.info(f"LLM response saved in: {json_path}")