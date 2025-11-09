import sys
from time import time

import duckdb
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader
from langchain_classic.chains.llm import LLMChain
from langchain_community.utilities import SQLDatabase
from langchain_core.prompts import PromptTemplate

from src.llm.baseline_tools import parse_llm_response, save_baseline_to_json, build_lcel_chain
from src.llm.llm_factory import get_llm_wrapper
from src.main import parse_args
from src.utils.parse_llm_response import save_llm_response_to_file, extract_json_from_response
from ..utils.constants import *
from .google_genai_connection import query_internal_knowledge, query_nl_qa_contextual
from ..db.run_queries_to_json import load_queries_from_folder, load_nl_queries_from_txt
from ..utils.build_prompt_context import build_prompt_context
from config import Config_Loader
from ..utils.dataset_selection import get_dataset_selection
from ..utils.logging_config import logger, LOG

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
    For each folder in the base_folder (dataset),
    load the `queries_<dataset>.sql` file(s),
    convert each query to natural language and save a JSON with the prompts,
    then query the model and print the responses.
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
                            logger.info(f"Found duckdb path: {duckdb_path}")

                    #Embedd some context
                    context_prompt = build_prompt_context(dataset_name)

                    #WATSONX
                    #response = process_nl_query_with_watsonx(full_prompt, duckdb_path)
                    #logger.info(f"LLM raw response for the query {prompt} in dataset {dataset_name}:\n{response}\n{'-'*50}")

                    #GEMINI
                    response = query_nl_qa_contextual(prompt,duckdb_path)
                    logger.info(f"Response from GEMINI: {response}")

                    # Use ad hoc function for parsing the response and build a FULL JSON file
                    #parsed_response = parse_llm_response(response)
                    #logger.info(" LLM parsed RESPONSE:\n", parsed_response)
                    #save_llm_response_to_file(dataset_name, response, i+1)
                    #logger.info(f"LLM response saved ")
                    #data = json.dumps(response, indent=4)
                    #print("response for the LLM:", data)
                    #formatted_response = data['SQLResult']
                    #logger.info(f"Formatted response:f{formatted_response}")


                elif dataset_name.upper() in IK_DATASETS:

                    #Directly quering the LLM without any context or external knowledge
                    full_prompt = f"{NL_IK_PROMPT} {prompt}"

                    #WATSONX
                    #response = query_watsonx(full_prompt)
                    #logger.info(f"LLM raw response for the query {full_prompt} in dataset {dataset_name}:\n{response}\n{'-'*50}")

                    #GEMINI
                    response = query_internal_knowledge(prompt)
                    logger.info(f"Response from GEMINI: {response}")

                    # Stampa in formato JSON
                    #time = datetime.now(timezone.utc)
                    #formatted_response = parse_response_to_json(response,time)
                    #save_llm_response_to_file( dataset_name, formatted_response, i + 1)

                    # Use ad hoc function for parsing the response and build a FULL JSON file
                    #parsed_response = parse_llm_response(response)
                    #logger.info(f" LLM parsed RESPONSE:{formatted_response}\n")
                    #logger.info(f"LLM response saved ")



def build_chain(prompt: str, template_path: str, llm, extra_context: str = ""):
    """Create the LangChain chain using a generic LLM model via Jinja2."""
    env = Environment(loader=FileSystemLoader(searchpath=os.path.dirname(LLM_TEMPLATE)))
    template = env.get_template(os.path.basename(template_path))

    rendered_text = template.render(
        template_text=open(template_path).read(),
        extra_context=extra_context,
        prompt=prompt,
        full_json_format=FULL_JSON_FORMAT
    )

    full_template = (
        f"{template}\n\n"
        f"Context: {extra_context}\n\n"
        f"Prompt: {prompt}\n\n"
        f"Answer with he following JSON format:\n{FULL_JSON_FORMAT}"
    )

    prompt_template = PromptTemplate.from_template(full_template)
    chain = LLMChain(llm=llm, prompt=prompt_template)
    return chain



def load_tables_knowledge_from_db(duck_db_path: str) -> str:
    """Extract the knowledge from .dckdb file to provide it as context to the LLM."""
    try:
        db = SQLDatabase.from_uri(f"duckdb:///{duck_db_path}")
        knowledge = db.get_table_info()
        return knowledge
    except Exception as e:
        return f"Error in the DuckDB loading: {e}"

def load_dataset_additional_info_(duck_db_path: str, query: str) -> str:
    """Extract the knowledge from .dckdb file to provide it as context to the LLM."""
    try:
        db = SQLDatabase.from_uri(f"duckdb:///{duck_db_path}")
        more_info = db.run(query)
        return more_info
    except Exception as e:
        return f"Error in the DuckDB loading: {e}"


def llm_interaction_second_version(config, database: str, prompts: list[str], b_type: str, d_type: str):

    logger.info(f"baseline: {b_type}, dataset type: {d_type}")
    llm = get_llm_wrapper(config)
    chain = build_lcel_chain(llm,b_type, d_type)

    folder = os.path.join(DATA_DIR, database)
    print("STO LEGGENDO LE QUERY DEL DATASET: ", folder)
    queries = load_queries_from_folder(folder)
    assert len(prompts) == len(queries), (
        f"Mismatch: {len(prompts)} prompts vs {len(queries)} queries in {database}"
    )

    results = []
    for prompt, sql_query in zip(prompts, queries):

        if isinstance(sql_query, tuple):
            sql_query = sql_query[1]

        LOG.info(f"Executing baseline NL prompts: {prompt} with corresponding SQL query: {sql_query}")

        t_start = time()
        try:

            raw_response = chain.invoke({"database": database,"b_type": b_type, "query": sql_query, "prompt":prompt, "BASELINE": d_type })

            t_end = time()

            result = parse_llm_response(raw_response, t_end - t_start, llm)
            results.append(result)

        except Exception as e:
            LOG.error(f"LLM/Parsing Error: {e}. Failed to parse required JSON structure.")

    save_baseline_to_json(database, results, llm, b_type)
    """
    context = build_prompt_context(dataset_name)
    knowledge = load_tables_knowledge_from_db(duck_db_path)
    full_prompt = NL_MC_PROMPT + "\n\n" + context + "\n\n" + prompt


    if dataset_name in IK_DATASETS:
        logger.info(f"→ Dataset {dataset_name} identified as IK.")
        chain = build_chain(
            prompt=full_prompt,
            template_path=LLM_TEMPLATE,
            llm=llm,
            extra_context=knowledge,
        )
        response = chain.invoke({"prompt": prompt})

    elif dataset_name in MC_DATASETS:
        logger.info(f"→ Dataset {dataset_name} identified as MC.")
        additional_info = load_dataset_additional_info_(duck_db_path, query)

        chain = build_chain(
            prompt=full_prompt,
            template_path=LLM_TEMPLATE,
            llm=llm,
            extra_context=knowledge + "\n" + str(additional_info),
        )
        response = chain.invoke({"prompt": prompt})
    else:
        logger.error("Dataset group does not match IK or MC.")
        sys.exit(1)


    json_response = extract_json_from_response(response)
    logger.info(f"Extracted JSON response: {json_response}")
    save_llm_response_to_file(dataset_name, json_response, i+1)

    print("\n=== Model Answer ===")
    print(response)
    """

if __name__ == "__main__":
    config = Config_Loader().get_config()
    args = parse_args()
    d_type=""
    datasets = args.datasets or get_dataset_selection(config.database.run)
    for d in datasets:

        d = d.upper()
        if d in IK_DATASETS:
            d_type = "IK"
        elif d in MC_DATASETS:
            d_type = "MC"
        logger.info(f"Checking folder: {d}")
        if d in DATASETS:
            dataset_path = os.path.join(DATA_DIR, d)
            print(dataset_path)
            duckdb_path = os.path.join(DATA_DIR, d, f"{d.lower()}.duckdb")
            if not os.path.isdir(dataset_path):
                continue
            logger.info(f"Processing dataset: {d}")

            nl_queries = load_nl_queries_from_txt(dataset_path)
            llm_interaction_second_version(config,d , nl_queries, args.mode.upper(), d_type)
