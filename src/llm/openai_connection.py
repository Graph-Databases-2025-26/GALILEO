import openai
import json
import time

from src.utils.logging_config import logger, log_query_event


# Configure your API key
openai.api_key = "YOUR_OPENAI_API_KEY"


def query_llm(prompt: str, model: str = "gpt-5-mini") -> list:
    """
    Send a prompt to LLM and return a JSON as a list of dictionaries.
    """
    response = openai.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are a helpful assistant that outputs data in JSON format."},
            {"role": "user", "content": prompt}
        ],
        temperature=0
    )
    elapsed_ms = (time.time() - start) * 1000.0  

    # Log usage if available
    usage = getattr(response, "usage", None)  
    if usage:
        logger.info(
            f"openai_response model={model} latency_ms={elapsed_ms:.1f} "
            f"prompt_tokens={getattr(usage,'prompt_tokens',None)} "
            f"completion_tokens={getattr(usage,'completion_tokens',None)} "
            f"total_tokens={getattr(usage,'total_tokens',None)}"
        )
    else:
        logger.info(f"openai_response model={model} latency_ms={elapsed_ms:.1f}")

    # Extract text from the response
    text = response.choices[0].message.content.strip()

    # JSON conversion
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        raise ValueError(f"LLM did not return valid JSON:\n{text}")

    return data


if __name__ == "__main__":
    # Esempio NL Prompt
    nl_prompt = "List all employees in the Sales department and include their names and emails in JSON format."

    # Esempio SQL Prompt
    sql_prompt = "SELECT name, email FROM employees WHERE department = 'Sales';"

    # Chiamata LLM con NL
    nl_result = query_llm(nl_prompt)
    logger.info("NL Result:")
    print(json.dumps(nl_result, indent=2))

    # Chiamata LLM con SQL
    sql_result = query_llm(sql_prompt)
    logger.info("SQL Result:")
    print(json.dumps(sql_result, indent=2))
