import logging
import os
import json

from dotenv import load_dotenv
from .sql_to_nl import sql_to_nl
from pathlib import Path
from ..utils.constants import *
from .watsonx_ai_connection import query_watsonx
from ..db.run_queries_to_json import load_queries_from_folder
from ..utils.build_prompt_context import build_prompt_context

#CONFIGURE THE API KEY
load_dotenv()
api_key = os.getenv("WATSONX_API_KEY", "").strip()

"""
    The goal of this script is to query the LLM model with interrogations in Natural Language
    receive the answer from the LLM model and stores it.
    For query the LLM will be used the sql_to_nl script for converting the query took from the queries_*.sql files of each dataset in a prompt in natural language.

"""
def llm_interaction():
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

    for dataset_name in os.listdir(DATA_DIR):
        print(f"Checking folder: {dataset_name}")
        if dataset_name in DATASETS:
            dataset_path = os.path.join(DATA_DIR, dataset_name)
            if not os.path.isdir(dataset_path):
                continue

            logging.info(f"Processing dataset: {dataset_name}")
            queries = load_queries_from_folder(dataset_path)

            for i, (filename, query) in enumerate(queries, start=1):
                print(f"{query}")

            nl_prompts = []
            for i, (filename, sql_query) in enumerate(queries):
                nl_prompt = sql_to_nl(sql_query)
                logging.info(f"🧠 NL prompt generated: {nl_prompt}")

                context_prompt = build_prompt_context(dataset_name)
                full_prompt = f"{context_prompt}\nQuestion: {nl_prompts}"

                # Create a folder for the dataset within the prompt folder
                dataset_prompt_folder = os.path.join(PROMPTS, dataset_name)
                os.makedirs(dataset_prompt_folder, exist_ok=True)

                # Save the NL prompts in JSON
                json_filename = f"nl_prompt_query{i}_{dataset_name}.json"
                json_path = os.path.join(dataset_prompt_folder, json_filename)
                with open(json_path, 'w', encoding='utf-8') as jf:
                    json.dump(nl_prompt, jf, indent=2, ensure_ascii=False)

                logging.info(f"Prompts NL saved in: {json_path}")

                # Query the model with NL prompts generated before

                response = query_watsonx(full_prompt)
                logging.info(f"Answer for the query {sql_query} in dataset {dataset_name}:\n{response}\n{'-'*50}")

if __name__ == "__main__":

    llm_interaction()