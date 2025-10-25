from dotenv import load_dotenv
from pathlib import Path
import os, yaml

CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"

# Carica variabili d'ambiente
load_dotenv()

# Chiavi API
API_KEYS = {
    "grok": os.getenv("XAI_API_KEY"),
    "gemini": os.getenv("GOOGLE_GEMINI_API_KEY"),
}

API_HOSTS = {
    "grok": os.getenv("XAI_URL"),
    "gemini": os.getenv("GOOGLE_GEMINI_ENDPOINT")
}

# Config YAML
with open(CONFIG_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)

def get_llm_settings(llm_name):
    return {
        "api_key": API_KEYS.get(llm_name),
        "api_host": API_HOSTS.get(llm_name),
        "config": CONFIG.get(llm_name)
    }
