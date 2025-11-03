import os
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent.parent
VENV = ROOT / ".venv"

CONFIG_PATH = ROOT / "config" / "config.yaml"

DATA_DIR = ROOT / "data"

PROMPTS = ROOT / "data" / ".prompts"

SUBMISSIONS_PATH = ROOT / "data" / ".output"

NL_OUTPUT = ROOT / "data" / ".output" / ".nl_ouput"

GROUND_PATH = ROOT / "data" / ".ground_truth"

DATASETS = ["FLIGHT-2", "FLIGHT-4", "FORTUNE", "GEO", "MOVIES", "PREMIER", "PRESIDENTS", "WORLD"]

IK_DATASETS = ["FLIGHT-2", "FLIGHT-4", "GEO", "MOVIES", "PRESIDENTS", "WORLD"]

MC_DATASETS = ["FORTUNE", "PREMIER"]

if(os.name == "nt"):
    PIP = VENV / "Scripts" / "pip"
    PY  = VENV / "Scripts" / "python"
else:
    PIP = VENV / "bin" / "pip"
    PY  = VENV / "bin" / "python"

LOGS_DIR = ROOT / "logs"

REQS_PATH = ROOT / "requirements.txt"

SQL_TO_NL_PROMPT = (
        "Translate the following SQL query into a clear, concise question in natural English. "
        "Only output the question itself — do not include SQL code or explanations.\n\n"
        "Example:\n"
        "SQL: SELECT name FROM students;\n"
        "Question: What are the names of the students?\n\n"
)

FULL_JSON_FORMAT = """
You must ALWAYS respond **only** in valid JSON format as shown above.
Do not include any text or explanation outside of the JSON.
If you want to say "UAL is the call sign for United Airlines.",
respond as:
{
    "result_set": [{"call_sign": "UAL"}],
    "time": 0.0,
    "tokens": 0
}
"""

