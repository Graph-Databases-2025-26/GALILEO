import json
import re
from src.utils.constants import *
from src.utils.logging_config import logger

def extract_all_json_objects(text: str):
    json_objects = []
    # regex for capture JSON structures {...}
    matches = re.findall(r'\{[^{}]*\}', text, re.DOTALL)
    for match in matches:
        try:
            obj = json.loads(match)
            json_objects.append(obj)
        except json.JSONDecodeError:
            continue
    if not json_objects:
        return {"error": "No valid JSON found"}
    combined = {}
    for obj in json_objects:
        combined.update(obj)
    return combined




def extract_json_from_response(response_text):
    """
    Extract and try to fix a faulty from the LLM reply to return a valid JSON object.
    """
    # If the response is already a dict, use it directly
    if isinstance(response_text, dict):
        response_text = response_text.get("text", str(response_text))
    elif not isinstance(response_text, str):
        response_text = str(response_text)

    # Search the main JSON block
    match = re.search(r"(\{.*\}|\[.*\])", response_text, re.DOTALL)
    if not match:
        logger.warning("⚠️ Nessun blocco JSON trovato.")
        return {}

    json_str = match.group(1)

    # Default cleaning: remove extra text
    json_str = re.split(r"\.\.\.|Explanation:|\[Explanation\]", json_str)[0]

    #  Some standard fixes
    json_str = json_str.strip()
    json_str = json_str.replace("}}]", "}]")
    json_str = json_str.replace("}}", "}")
    json_str = re.sub(r",\s*([\]}])", r"\1", json_str)
    json_str = re.sub(r"(\])\}.*$", r"\1}", json_str)

    # Close arrays or objects if missing
    open_braces, close_braces = json_str.count("{"), json_str.count("}")
    open_brackets, close_brackets = json_str.count("["), json_str.count("]")

    if open_braces > close_braces:
        json_str += "]" * (open_braces - close_braces)
    if open_brackets > close_brackets:
        json_str += "}" * (open_brackets - close_brackets)

    try:
        return json.loads(json_str)
    except json.JSONDecodeError as e:
        logger.info(f"Error in JSON parsing: {e}")
        logger.debug(f"Not valid content:\n{json_str[:500]}...")
        # Other aggressive fixes
        json_str = re.sub(r"[^{}\[\]:,\"\w\s.-]", "", json_str)  # remove strange characters
        print("Fixed string:", json_str)

        try:
            return json.loads(json_str)
        except Exception:
            logger.warning("Also the aggressive fix try failed.")
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
 
       
def save_baseline_to_json(dataset: str, baseline: list[dict]):
    bline_folder = SQL_BLINE_DIR / dataset
    bline_folder.mkdir(parents=True, exist_ok=True)    

    for i, result in enumerate(baseline):
        with open(bline_folder / f"query{i + 1}.json", "w") as f:
            json.dump(result, f, indent=4)