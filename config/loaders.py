from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel, Field, model_validator
from dotenv import load_dotenv
from src import CONFIG_PATH
from pathlib import Path
import os, yaml

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
    api_key: str | None = Field(default_factory=lambda: os.getenv("GEMINI_API_KEY"))
    endpoint: str | None = Field(default_factory=lambda: os.getenv("GEMINI_ENDPOINT"))

class WatsonxConfig(BaseModel):
    model: str
    max_tokens: int
    temperature: float 
    api_key: str | None = Field(default_factory=lambda: os.getenv("WATSONX_API_KEY"))
    endpoint: str | None = Field(default_factory=lambda: os.getenv("WATSONX_ENDPOINT"))
    project_id: str | None = Field(default_factory=lambda: os.getenv("WATSONX_PROJECT_ID"))

class DatasetConfig(BaseModel):
    run: str
    
class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(extra='ignore', env_file='.env', env_file_encoding='utf-8')

    database: DatasetConfig
    llm_provider: str 
    execution: ExecutionConfig
    io: IOConfig
    logging: LoggingConfig
    gemini: GeminiConfig
    watsonx: WatsonxConfig
    top_k: int

    def validate_provider_config_exists(self) -> 'AppConfig':         
        provider = self.llm_provider
        
        if provider == 'watsonx':
            if self.watsonx is None: 
                raise ValueError("Se 'llm_provider' è 'watsonx', la sezione 'watsonx' è obbligatoria.")
        elif provider == 'gemini':
            if self.gemini is None: 
                raise ValueError("Se 'llm_provider' è 'gemini', la sezione 'gemini' è obbligatoria.")
        
        return self

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
