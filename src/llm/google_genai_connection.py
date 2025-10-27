"""
Google Generative AI (Gemini) connection with structured logging.
- Measures latency for direct invoke and chain execution
- Logs response length and errors
"""

import os
import time
import traceback

from langchain_core.exceptions import LangChainException
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_google_genai import ChatGoogleGenerativeAI

from src.utils.logging_config import logger, log_query_event



def query_llm(query: str, model: str = "gemini-2.5-flash", temperature: float = 0.7) -> str:
    """
    Send a prompt to Gemini and return the response as a string.
    Logs timings and errors with loguru.
    """
    # Ensure key is present (prefer env var)
    api_key = os.getenv("GOOGLE_API_KEY", "").strip()
    if not api_key:
        logger.error("GOOGLE_API_KEY environment variable not set")
        raise SystemExit(1)

    try:
        logger.info(f"Initializing Google GenAI model={model} temperature={temperature}")
        llm = ChatGoogleGenerativeAI(model=model, temperature=temperature, max_tokens=2000)

        # 1) Direct invoke (quick probe / warmup)
        direct_prompt = f"Answer the question in a generic way: {query}"
        t0 = time.time()
        _ = llm.invoke(direct_prompt)
        logger.info(f"gemini_invoke latency_ms={(time.time() - t0) * 1000.0:.1f}")

        # 2) Chain with a template (example in Italian to match your slides)
        template = "Rispondi in italiano alla seguente domanda in modo chiaro e conciso: {query}"
        prompt = ChatPromptTemplate.from_template(template)
        chain = prompt | llm | StrOutputParser()

        t1 = time.time()
        response = chain.invoke({"query": query})
        latency_ms = (time.time() - t1) * 1000.0
        logger.info(f"gemini_chain latency_ms={latency_ms:.1f} response_len={len(str(response))}")
        return response

    except LangChainException as e:
        logger.error(f"LangChain error: {e}")
        return f"LangChain error: {e}"
    except Exception as e:
        logger.error(f"Generic error: {e}. Trace:\n{traceback.format_exc()}")
        return f"Generic error: {e}"


if __name__ == "__main__":
    # Simple smoke test
    q = "Elenca 3 città italiane famose per l'arte."
    ans = query_llm(q)
    logger.info(f"Gemini response: {ans}")
