import os
import time
import json
from langchain_core.prompts import ChatPromptTemplate, PromptTemplate
from langchain_community.utilities import SQLDatabase
from operator import itemgetter
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI

from src.utils.WatsonxResponse import WatsonxResponse
from src.utils.logging_config import logger, log_query_event


# Configure the API KEY
load_dotenv()
google_api_key = os.getenv("GEMINI_API_KEY")
ibm_api_key = os.getenv("IBM_API_KEY")

if not google_api_key:
    print("GOOGLE_API_KEY environment variable not set")
    exit(1)

os.environ["GOOGLE_API_KEY"] = google_api_key

# Modello da usare
MODEL_NAME = 'gemini-2.5-flash'

#Initialize the LLM model
llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", temperature=0.1)

# Structure the llm model for FULL JSON format output
llm_structured = llm.with_structured_output(WatsonxResponse)
#llm_structured = llm_lc

def query_internal_knowledge(prompt: str):
    """
    Interroga Gemini per la conoscenza interna, forzando l'output JSON.
    """

    # Istruzione di sistema per forzare il contenuto all'interno del JSON
    system_instruction = (
        "Sei un assistente esperto in conoscenza generale. Rispondi in modo conciso e in italiano alla domanda. "
        "Forma la tua risposta e inseriscila nel campo 'Risposta' all'interno della lista 'result_set'."
    )

    prompt_template = ChatPromptTemplate.from_messages([
        ("system", system_instruction),
        ("human", "{question}")
    ])

    chain = prompt_template | llm

    print(f"Prompt NL: {prompt}")

    try:
        print("Invio della richiesta a Gemini...")

        # response_obj sarà ora una stringa (BaseMessage)
        response = chain.invoke({"question": prompt}, config={'timeout': 60})

        print("Risposta ricevuta da Gemini.")

        # Stampa la risposta testuale
        print(f"**Risposta di Gemini (testo puro)**:\n{response.content}")

        # Restituisci la risposta testuale (o adatta il ritorno per il tuo ciclo)
        return response.content
    except Exception as e:
        print(f"ERRORE nell'esecuzione della catena LangChain: {e}")


def query_nl_qa_contextual(nl_prompt: str, db_file: str):
    """
    Utilizza LangChain per rispondere alla domanda NL fornendo a Gemini lo schema
    del database come contesto.
    """
    print(f"\n--- 📚 Interrogazione (NL QA Contestuale)")

    db_uri = f"duckdb:///{db_file}"

    try:
        # Crea l'oggetto SQLDatabase e ottieni lo schema come stringa
        db = SQLDatabase.from_uri(db_uri)
        # Metodo per ottenere lo schema in modo leggibile per il prompt
        schema = db.get_table_info()
    except Exception as e:
        logger.error(f"ERRORE: Impossible connecting to the database ({db_file}). Error: {e}")
        return

    # Provide a template for the LLM to follow
    template = """
        You are an assistant that responds ONLY in JSON.
        Question: {question}
        Database schema:
        {schema}
        
        The output format must be exactly this:
        
        {{
          "result_set": [
            {{ "column_name": "value" }},
            {{ "column_name": "value" }}
          ],
          "time": <time in seconds as float>,
          "tokens": <estimated number of tokens used>
        }}
        """

    prompt = ChatPromptTemplate.from_template(template)

    # Create the chain
    # The chain takes the question, injects the schema within the template and invokes the LLM.
    chain = (
            {
                "schema": lambda x: schema,
                "question": itemgetter("question"),
            }
            | prompt
            | llm_structured
    )

    start = time.time()
    response = chain.invoke({"question": nl_prompt})
    elapsed = time.time() - start

    try:
        parsed = json.loads(response.content)
    except Exception:
        parsed = {"result_set": [{"Answer": response}]}

   # parsed["time"] = round(elapsed, 3)
    #parsed["tokens"] = len(response.content.split())


    return parsed

