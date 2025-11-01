import os
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent.parent
VENV = ROOT / ".venv"

CONFIG_PATH = ROOT / "config" / "config.yaml"

DATA_DIR = ROOT / "data"

PROMPTS = ROOT / "data" / ".prompts"

SUBMISSIONS_PATH = ROOT / "data" / ".output"

GROUND_PATH = ROOT / "data" / ".ground_truth"

DATASETS = ["FLIGHT-2", "FLIGHT-4", "FORTUNE", "GEO", "MOVIES", "PREMIER", "PRESIDENTS", "WORLD"]

if(os.name == "nt"):
    PIP = VENV / "Scripts" / "pip"
    PY  = VENV / "Scripts" / "python"
else:
    PIP = VENV / "bin" / "pip"
    PY  = VENV / "bin" / "python"

LOGS_DIR = ROOT / "logs"

REQS_PATH = ROOT / "requirements.txt"
