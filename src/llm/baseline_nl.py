import json
from dotenv import load_dotenv
from src.utils.parse_llm_response import parse_response_to_json, save_llm_response_to_file
from ..utils.constants import *
from .watsonx_ai_connection import query_watsonx, process_nl_query_with_watsonx
from ..db.run_queries_to_json import load_queries_from_folder, load_nl_queries_from_txt
from ..utils.build_prompt_context import build_prompt_context
from config import Config_Loader
from ..utils.dataset_selection import get_dataset_selection
from ..utils.logging_config import logger
from datetime import datetime, timezone

#CONFIGURE THE API KEY
load_dotenv()
api_key = os.getenv("WATSONX_API_KEY", "").strip()

"""
    The goal of this script is to query the LLM model with interrogations in Natural Language
    receive the answer from the LLM model and stores it.
    For query the LLM will be used the sql_to_nl script for converting the query took from the queries_*.sql files of each dataset in a prompt in natural language.

"""
def llm_interaction(chosen_datasets: list[str] | None = None):
    """
    Per ogni cartella in base_folder (dataset),
    carica file queries_<dataset>.sql,
    converte ogni query in NL e salva json con i prompt,
    quindi interroga il modello e stampa risposte.
    """
    if not os.path.exists(PROMPTS):
        os.makedirs(PROMPTS)

    print(f"DATA_DIR = {DATA_DIR}")
    print(f"DATASETS = {DATASETS}")
    print(f"Found folders: {os.listdir(DATA_DIR)}")

    for dataset_name in chosen_datasets:
        logger.info(f"Checking folder: {dataset_name}")
        if dataset_name.upper() in DATASETS:
            dataset_path = os.path.join(DATA_DIR, dataset_name)
            if not os.path.isdir(dataset_path):
                continue

            logger.info(f"Processing dataset: {dataset_name}")
            nl_queries = load_nl_queries_from_txt(dataset_path)

            nl_prompts = []
            for i, prompt in enumerate(nl_queries):

                logger.info(f" NL prompt: {prompt}")

                # Differentiate the datasets between IK & MC, and next query the model with NL prompts

                if dataset_name.upper() in MC_DATASETS:

                    #.duckdb path
                    duckdb_path = os.path.join(DATA_DIR, dataset_name, f"{dataset_name.lower()}.duckdb")
                    if duckdb_path:
                            print(f"Found duckdb path: {duckdb_path}")

                    #Embedd some context
                    context_prompt = build_prompt_context(dataset_name)
                    full_prompt = f"{context_prompt} {prompt}"

                    response = process_nl_query_with_watsonx(full_prompt, duckdb_path)
                    logger.info(f"LLM raw response for the query {prompt} in dataset {dataset_name}:\n{response}\n{'-'*50}")

                    # Use ad hoc function for parsing the response and build a FULL JSON file
                    #parsed_response = parse_llm_response(response)
                    #logger.info(" LLM parsed RESPONSE:\n", parsed_response)
                    save_llm_response_to_file(dataset_name, response, i+1)
                    logger.info(f"LLM response saved ")
                    #data = json.dumps(response, indent=4)
                    #print("response for the LLM:", data)
                    #formatted_response = data['SQLResult']
                    #logger.info(f"Formatted response:f{formatted_response}")


                elif dataset_name.upper() in IK_DATASETS:

                    #Directly quering the LLM without any context or external knowledge
                    full_prompt = f"{PROMPT_FOR_IK_DATASETS} {prompt}"
                    response = query_watsonx(full_prompt)
                    logger.info(f"LLM raw response for the query {full_prompt} in dataset {dataset_name}:\n{response}\n{'-'*50}")


                    # Stampa in formato JSON
                    time = datetime.now(timezone.utc)
                    formatted_response = parse_response_to_json(response,time)
                    save_llm_response_to_file( dataset_name, formatted_response, i + 1)

                    # Use ad hoc function for parsing the response and build a FULL JSON file
                    #parsed_response = parse_llm_response(response)
                    #logger.info(f" LLM parsed RESPONSE:{formatted_response}\n")
                    #logger.info(f"LLM response saved ")

if __name__ == "__main__":
    config = Config_Loader().get_config()
    datasets = get_dataset_selection(config.database.run)
    llm_interaction(datasets)