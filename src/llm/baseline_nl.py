import sys
import duckdb
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader
from langchain_classic.chains.llm import LLMChain
from langchain_core.prompts import PromptTemplate
from src.llm.llm_factory import create_llm
from src.utils.parse_llm_response import save_llm_response_to_file, extract_json_from_response
from ..utils.constants import *
from .google_genai_connection import query_internal_knowledge, query_nl_qa_contextual
from ..db.run_queries_to_json import load_queries_from_folder, load_nl_queries_from_txt
from ..utils.build_prompt_context import build_prompt_context
from config import Config_Loader
from ..utils.dataset_selection import get_dataset_selection
from ..utils.logging_config import logger

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
                    full_prompt = f"{PROMPT_FOR_IK_DATASETS} {prompt}"

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



def load_knowledge_from_db(duck_db_path: str) -> str:
    """Extract the knowledge from .dckdb file to provide it as context to the LLM."""
    try:
        conn = duckdb.connect(duck_db_path, read_only=True)
        tables = conn.execute("SHOW TABLES").fetchall()
        knowledge = f"Tables: {[t[0] for t in tables]}"
        conn.close()
        return knowledge
    except Exception as e:
        return f"Error in the DuckDB loading: {e}"


    # Load knowledge from DuckDB database for internal knowledge querying

def llm_interaction_second_version(dataset_name: str, duck_db_path: str, prompt: str, provider: str):
    llm = create_llm(provider)
    context = build_prompt_context(dataset_name)


    if dataset_name in IK_DATASETS:
        logger.info(f"→ Dataset {dataset_name} identified as IK.")
        chain = build_chain(
            prompt=prompt,
            template_path=LLM_TEMPLATE,
            llm=llm,
            extra_context=context,
        )
        response = chain.invoke({"prompt": prompt})

    elif dataset_name in MC_DATASETS:
        logger.info(f"→ Dataset {dataset_name} identified as MC.")
        knowledge = load_knowledge_from_db(duck_db_path)

        chain = build_chain(
            prompt=prompt,
            template_path=LLM_TEMPLATE,
            llm=llm,
            extra_context=knowledge + "\n" + context,
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

if __name__ == "__main__":
    config = Config_Loader().get_config()
    datasets = get_dataset_selection(config.database.run)
    for d in datasets:
        logger.info(f"Checking folder: {d}")
        if d.upper() in DATASETS:
            dataset_path = os.path.join(DATA_DIR, d)
            duckdb_path = os.path.join(DATA_DIR, d, f"{d.lower()}.duckdb")
            if not os.path.isdir(dataset_path):
                continue

            logger.info(f"Processing dataset: {d}")
            nl_queries = load_nl_queries_from_txt(dataset_path)
            for i, prompt in enumerate(nl_queries):
                llm_interaction_second_version(d,duckdb_path,prompt,"gemini")