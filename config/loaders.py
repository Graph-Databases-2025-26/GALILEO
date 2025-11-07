from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from src import CONFIG_PATH
from pathlib import Path
import os, yaml

load_dotenv()

class ExecutionConfig(BaseModel):
    max_retries: int
    backoff_sec: float
    scan: str

class LLMConfig(BaseModel):
    provider: str
    model: str
    temperature: float

class IOConfig(BaseModel):
    queries_dir: Path
    prompts_dir: Path
    outputs_dir: Path

class LoggingConfig(BaseModel):
    level: str
    json_format: bool

class GeminiConfig(BaseSettings):
    model: str
    temperature: float
    max_output_tokens: int
    gemini_api_key: str | None = Field(default=None)
    gemini_api_endpoint: str | None = Field(default=None)

class WatsonxConfig(BaseSettings):
    model: str
    max_tokens: int
    temperature: float 
    watsonx_api_key: str | None = Field(default=None)
    watsonx_endpoint: str | None = Field(default=None)
    watsonx_project_id: str | None = Field(default=None)

class DatasetConfig(BaseModel):
    run: str
    
class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(extra='ignore', env_file='.env', env_file_encoding='utf-8')

    database: DatasetConfig
    llm : LLMConfig
    execution: ExecutionConfig
    io: IOConfig
    logging: LoggingConfig
    gemini: GeminiConfig
    watsonx: WatsonxConfig


class Config_Loader:

    def __init__(self, config_path: str | Path = str(CONFIG_PATH)) -> None:
        self.config_path = Path(config_path)
        self.config = self._load_config()

    def _load_config(self) -> AppConfig:
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        with open(self.config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        return AppConfig(**data)  

    def get_config(self) -> AppConfig:
        return self.config
