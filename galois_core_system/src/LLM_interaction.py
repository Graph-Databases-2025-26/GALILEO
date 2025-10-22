import openai
import json

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
    print("NL Result:")
    print(json.dumps(nl_result, indent=2))

    # Chiamata LLM con SQL
    sql_result = query_llm(sql_prompt)
    print("\nSQL Result:")
    print(json.dumps(sql_result, indent=2))
