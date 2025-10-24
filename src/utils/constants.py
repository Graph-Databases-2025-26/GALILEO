import os
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent.parent
VENV = ROOT / ".venv"

DATA = ROOT / "data"

SUBMISSIONS_PATH = ROOT / "data" / ".output"

GROUND_PATH = ROOT / "data" / ".ground_truth"

if(os.name == "nt"):
    PIP = VENV / "Scripts" / "pip"
    PY  = VENV / "Scripts" / "python"
else:
    PIP = VENV / "bin" / "pip"
    PY  = VENV / "bin" / "python"

REQS = ROOT / "requirements.txt"
