from src.utils import CONFIG_PATH, ERR_CONFIG_FILE_NOT_FOUND, ERR_PROVIDER_SECTION_MISSING

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel, Field

from dotenv import load_dotenv
from pathlib import Path
import os, yaml

load_dotenv()

class ExecutionConfig(BaseModel):
    """Configuration settings for controlling the execution flow of the pipeline."""
    
    max_retries: int
    backoff_sec: float
    scan: str

class IOConfig(BaseModel):
    """Configuration settings for input and output directory paths."""
    
    queries_dir: Path
    prompts_dir: Path
    outputs_dir: Path

class LoggingConfig(BaseModel):
    """Configuration settings for the application logger."""
    
    level: str
    json_format: bool

class GeminiConfig(BaseModel):
    """
    Configuration settings for the Gemini LLM provider.

    Attributes:
        model (str): The specific model ID to use (e.g., 'gemini-2.5-flash').
        temperature (float): The sampling temperature.
        max_output_tokens (int): The maximum number of tokens to generate.
        api_key (str | None): API key, loaded from 'GEMINI_API_KEY' environment variable.
        endpoint (str | None): Custom API endpoint, loaded from 'GEMINI_ENDPOINT' environment variable.
    """
    
    model: str
    temperature: float
    max_output_tokens: int
    api_key: str | None = Field(default_factory=lambda: os.getenv("GEMINI_API_KEY"))
    endpoint: str | None = Field(default_factory=lambda: os.getenv("GEMINI_ENDPOINT"))

class WatsonxConfig(BaseModel):
    """
    Configuration settings for the IBM Watsonx LLM provider.

    Attributes:
        model (str): The specific model ID to use.
        max_tokens (int): The maximum number of tokens to generate.
        temperature (float): The sampling temperature.
        api_key (str | None): API key, loaded from 'WATSONX_API_KEY' environment variable.
        endpoint (str | None): API endpoint, loaded from 'WATSONX_ENDPOINT' environment variable.
        project_id (str | None): Project ID, loaded from 'WATSONX_PROJECT_ID' environment variable.
    """
    
    model: str
    max_tokens: int
    temperature: float 
    api_key: str | None = Field(default_factory=lambda: os.getenv("WATSONX_API_KEY"))
    endpoint: str | None = Field(default_factory=lambda: os.getenv("WATSONX_ENDPOINT"))
    project_id: str | None = Field(default_factory=lambda: os.getenv("WATSONX_PROJECT_ID"))

class DatasetConfig(BaseModel):
    """Configuration specifying which datasets to run."""
    
    run: str
    
class AppConfig(BaseSettings):
    """
    The main application configuration model, loaded from a YAML file and environment variables.

    It aggregates all other configuration models and includes top-level settings like the selected LLM provider.

    Attributes:
        database (DatasetConfig): Dataset run configuration.
        llm_provider (str): The selected LLM provider (e.g., 'gemini', 'watsonx').
        execution (ExecutionConfig): Execution parameters.
        io (IOConfig): I/O paths.
        logging (LoggingConfig): Logging parameters.
        gemini (GeminiConfig): Gemini-specific configuration.
        watsonx (WatsonxConfig): Watsonx-specific configuration.
    """
    
    model_config = SettingsConfigDict(extra='ignore', env_file='.env', env_file_encoding='utf-8')

    database: DatasetConfig
    llm_provider: str 
    execution: ExecutionConfig
    io: IOConfig
    logging: LoggingConfig
    gemini: GeminiConfig
    watsonx: WatsonxConfig

    def validate_provider_config_exists(self) -> 'AppConfig':
        """
        Validates that the required configuration section (gemini or watsonx) exists based on the value of `llm_provider`.

        Returns:
            AppConfig: The validated configuration instance.

        Raises:
            ValueError: If `llm_provider` is set but the corresponding provider configuration section is missing.
        """
                 
        provider = self.llm_provider
        
        if provider == 'watsonx':
            if self.watsonx is None: 
                raise ValueError(ERR_PROVIDER_SECTION_MISSING.format("watsonx"))
        elif provider == 'gemini':
            if self.gemini is None: 
                raise ValueError(ERR_PROVIDER_SECTION_MISSING.format("gemini"))
        
        return self

class Config_Loader:
    """
    A utility class responsible for loading and parsing the application configuration from a YAML file into the `AppConfig` Pydantic model.
    """

    def __init__(self, config_path: str | Path = str(CONFIG_PATH)) -> None:
        """
        Initializes the loader and immediately loads the configuration.

        Args:
            config_path: The path to the YAML configuration file. Defaults to the constant `CONFIG_PATH`.
        """
        
        self.config_path = Path(config_path)
        self.config = self._load_config()


    def _load_config(self) -> AppConfig:
        """
        Reads the YAML configuration file, parses it into a dictionary, and validates/loads the data into an `AppConfig` instance.

        Returns:
            AppConfig: The instantiated application configuration object.

        Raises:
            FileNotFoundError: If the configuration file does not exist.
        """
        if not self.config_path.exists():
            raise FileNotFoundError(ERR_CONFIG_FILE_NOT_FOUND.format(self.config_path))

        with open(self.config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        return AppConfig(**data)  


    def get_config(self) -> AppConfig:
        """
        Returns the loaded application configuration object.

        Returns:
            AppConfig: The application configuration.
        """

        return self.config
