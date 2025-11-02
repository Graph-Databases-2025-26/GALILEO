import logging
import re
import json

from dotenv import load_dotenv

from src.utils.parse_llm_response import parse_llm_response
from .sql_to_nl import sql_to_nl
from pathlib import Path
from ..utils.constants import *
from .watsonx_ai_connection import query_watsonx
from ..db.run_queries_to_json import load_queries_from_folder
from ..utils.build_prompt_context import build_prompt_context
from config import Config_Loader
from ..utils.dataset_selection import get_dataset_selection
from ..utils.clean_llm_response import extract_json_from_text

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
        print(f"Checking folder: {dataset_name}")
        if dataset_name.upper() in DATASETS:
            dataset_path = os.path.join(DATA_DIR, dataset_name)
            if not os.path.isdir(dataset_path):
                continue

            logging.info(f"Processing dataset: {dataset_name}")
            queries = load_queries_from_folder(dataset_path)

            nl_prompts = []
            for i, (filename, sql_query) in enumerate(queries):
                nl_output = sql_to_nl(sql_query)

                # extract only the NL text
                try:
                    generated_text = nl_output["results"][0]["generated_text"]
                    # take only the rows with  "--natural language prompt..."
                    nl_lines = [
                        line.strip()
                        for line in generated_text.splitlines()
                        if line.strip().lower().startswith("which")
                           or line.strip().lower().startswith("what")
                           or line.strip().lower().startswith("who")
                           or line.strip().lower().startswith("when")
                           or line.strip().lower().startswith("where")
                           or line.strip().lower().startswith("how")
                    ]
                    nl_prompt = " ".join(nl_lines)
                except Exception as e:
                    logging.error(f"Error in the NL prompt extraction: {e}")
                    nl_prompt = "Could not extract prompt"

                logging.info(f" NL prompt generated: {nl_prompt}")

                # Create a folder for the dataset within the prompt folder
                dataset_nl_prompt_folder = os.path.join(PROMPTS, dataset_name)
                os.makedirs(dataset_nl_prompt_folder, exist_ok=True)

                # Save the NL prompts in JSON
                json_filename = f"nl_prompt_query{i+1}_{dataset_name}.json"
                json_path = os.path.join(dataset_nl_prompt_folder, json_filename)
                with open(json_path, 'w', encoding='utf-8') as jf:
                    json.dump(nl_prompt, jf, indent=2, ensure_ascii=False)

                logging.info(f"Prompts NL saved in: {json_path}")

                # Query the model with NL prompts generated before

                #Build the right context
                context_prompt = build_prompt_context(dataset_name)
                full_prompt = f"{context_prompt} {nl_prompt}"

                response = query_watsonx(full_prompt)
                print(" LLM RESPONSE:\n", response)

                logging.info(f"Answer for the query {sql_query} in dataset {dataset_name}:\n{response}\n{'-'*50}")

                # Use ad hoc function for parsing the responde and build a FULL JSON file
                parsed_response = parse_llm_response(response)
                print(" LLM parsed RESPONSE:\n", parsed_response)

                # Save the response in a text file
                response_filename = f"llm_response_query{i+1}_{dataset_name}.json"
                response_llm_folder = os.path.join(NL_OUTPUT, dataset_name)
                os.makedirs(response_llm_folder, exist_ok=True)
                json_path = os.path.join(response_llm_folder, response_filename)

                with open(json_path, 'w', encoding='utf-8') as rf:
                    json.dump(parsed_response, rf, indent=2, ensure_ascii=False)

                logging.info(f"LLM response saved in: {json_path}")

if __name__ == "__main__":
    config = Config_Loader().get_config()
    datasets = get_dataset_selection(config.database.run)
    llm_interaction(datasets)