# python
import re
import json

def parse_llm_response(response):
    """
    Extracts JSON blocks from the LLM response and builds a unified final JSON.
    Uses time and tokens values returned by the WatsonX model if available.
    """
    try:
        raw_text = response["results"][0]["generated_text"]
    except (KeyError, IndexError):
        return {"error": "Unexpected LLM response structure", "raw": response}

    # Regex to extract JSON blocks
    json_blocks = re.findall(r'\{(?:[^{}]|(?:\{[^{}]*\}))*\}', raw_text)
    result_list = []
    total_time = 0.0
    total_tokens = 0
    valid_blocks = 0

    print("\n--- DEBUG: JSON blocks found ---\n")
    for jb in json_blocks:
        print("[Block found]")
        print(jb)
        print()
        try:
            parsed = json.loads(jb)
        except json.JSONDecodeError:
            continue

        if "result_set" in parsed:
            for row in parsed["result_set"]:
                if isinstance(row, dict) and len(row) > 0:
                    value = list(row.values())[0]
                    result_list.append({"originaltitle": value})

        # Sum block values (even if they are 0)
        total_time += parsed.get("time", 0.0)
        total_tokens += parsed.get("tokens", 0)
        valid_blocks += 1

    print("--- END DEBUG ---")

    # 🔹 Take tokens returned by the WatsonX model
    generated_tokens = response["results"][0].get("generated_token_count", 0)
    input_tokens = response["results"][0].get("input_token_count", 0)
    print(f"\n--- DEBUG: TOKEN MODEL --- generated={generated_tokens}, input={input_tokens} ---\n")

    # 🔹 If blocks have no tokens, use the model's tokens
    if total_tokens == 0:
        total_tokens = generated_tokens + input_tokens

    # 🔹 If there's no time in blocks, set 0.0
    if total_time == 0.0:
        total_time = 0.0

    return {
        "result_set": result_list,
        "time": total_time,
        "tokens": total_tokens
    }
