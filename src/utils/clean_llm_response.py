# python
import re
import json
import logging

def extract_json_from_text(text: str):
    """
    Try to extract a valid JSON block from model-generated text.
    Returns the JSON dictionary or None if not valid.
    """
    # Find all blocks that look like JSON
    candidates = re.findall(r'\{[\s\S]*?\}', text)

    for block in reversed(candidates):  # try from the end; usually the last one is the correct one
        try:
            return json.loads(block)
        except json.JSONDecodeError:
            continue

    return None
