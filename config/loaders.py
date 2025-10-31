from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
from pathlib import Path
from src import CONFIG_PATH
import os, yaml

# Carica variabili d'ambiente
load_dotenv()

class ExecutionConfig(BaseModel):
    max_retries: int
    backoff_sec: float
    scan: str

class IOConfig(BaseModel):
    queries_dir: Path
    prompts_dir: Path
    outputs_dir: Path

class LoggingConfig(BaseModel):
    level: str
    json_format: bool

class GeminiConfig(BaseModel):
    model: str
    temperature: float
    max_output_tokens: int
    api_key: str | None = Field(default=None, env="GOOGLE_API_KEY")
    api_endpoint: str | None = Field(default=None, env="GOOGLE_API_ENDPOINT")


class GrokConfig(BaseModel):
    model: str
    max_tokens: int
    api_key: str | None = Field(default=None, env="XAI_API_KEY")
    api_endpoint: str | None = Field(default=None, env="XAI_URL")

class DatasetConfig(BaseModel):
    run: str
    
class AppConfig(BaseSettings):
    database: DatasetConfig
    execution: ExecutionConfig
    io: IOConfig
    logging: LoggingConfig
    gemini: GeminiConfig
    grok: GrokConfig

class Config_Loader:

    def __init__(self, config_path: str | Path = str(CONFIG_PATH)) -> None:
        self.config_path = Path(config_path)
        self.config = self._load_config()

    def _load_config(self) -> AppConfig:
        if not self.config_path.exists():
            raise FileNotFoundError(f"File di configurazione non trovato: {self.config_path}")

        with open(self.config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        return AppConfig(**data)  

    def get_config(self) -> AppConfig:
        return self.config
