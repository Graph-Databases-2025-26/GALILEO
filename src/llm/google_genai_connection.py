from langchain_core.exceptions import LangChainException
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
import os
import traceback

# Configure the API KEY
try:
    os.environ["GOOGLE_API_KEY"] = "You have to insert you own API key"
except KeyError:
    print("GOOGLE_API_KEY environment variable not set")
    exit(1)

def query_llm(query: str, model: str = "gemini-2.5-flash"):
    """
    Send a prompt to LLM and return the response as a string.
    """
    print("Inizio query_llm")
    try:
        # Initialize the LLM
        print("LLM Initialization ...")
        llm = ChatGoogleGenerativeAI(
            model=model,
            temperature=0.7,
            max_tokens=2000
        )
        print(f"LLM inizialized!: {model}")

        # Create a prompt template for direct test
        direct_prompt = f"Answer the question in a generic way: {query}"
        print(f"Direct prompt: {direct_prompt}")
        raw_response = llm.invoke(direct_prompt)
        print(f"Risposta grezza del modello: {raw_response}")

        # Create a prompt template for chain
        print("Creazione prompt template per chain...")
        prompt = ChatPromptTemplate.from_template(
            "Rispondi in italiano alla seguente domanda in modo chiaro e conciso: {query}"
        )
        print(f"Template prompt created: {prompt.format(query=query)}")

        # Create the chain
        print("Building the chain...")
        chain = prompt | llm | StrOutputParser()
        print("Chain created")

        # Execute the chain
        print(f"Execute chain with query: {query}")
        response = chain.invoke({"query": query})
        print(f"Response: {response}")
        return response

    except LangChainException as e:
        error_msg = f"LangChain's error: {str(e)}"
        print(error_msg)
        return error_msg
    except Exception as e:
        error_msg = f"Generic error: {str(e)}. Details: {traceback.format_exc()}"
        print(error_msg)
        return error_msg

# Query di test
test_query = "name the major lakes in michigan?"

# Esegui il test
print("Test running...")
result = query_llm(test_query)

# Stampa il risultato
print("\nModel response:")
print(result)