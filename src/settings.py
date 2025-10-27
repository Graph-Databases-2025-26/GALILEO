"""
settings.py
------------
Centralized configuration management for the project.

This module loads and validates the YAML configuration file using:
- PyYAML for parsing
- Pydantic / pydantic-settings for structured validation

Usage:
    from src.settings import settings
    print(settings.model.name)
"""

from pathlib import Path
import yaml
from pydantic import BaseModel
from pydantic_settings import BaseSettings


# ===  Define structured config sections ===

class ModelConfig(BaseModel):
    provider: str
    name: str
    temperature: float


class ConfidenceConfig(BaseModel):
    tau: float
    key_fn: str


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
    json: bool


class GeminiConfig(BaseModel):
    model: str
    temperature: float
    max_output_tokens: int


# === Main App Configuration ===

class AppConfig(BaseSettings):
    """
    Root configuration model that aggregates all sections.
    """

    model: ModelConfig
    confidence: ConfidenceConfig
    execution: ExecutionConfig
    io: IOConfig
    logging: LoggingConfig
    gemini: GeminiConfig

    class Config:
        env_file = ".env"  # Optional: allows environment variable overrides
        extra = "ignore"


# ===  Load configuration file ===

def load_settings(config_path: Path | None = None) -> AppConfig:
    """
    Load the YAML configuration and validate it with Pydantic.
    """

    # Resolve config path relative to project root
    if config_path is None:
        base_dir = Path(__file__).resolve().parent.parent
        config_path = base_dir / "config" / "config.yaml"

    # Read YAML file
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    # Parse and validate with Pydantic
    return AppConfig(**data)


# === Global instance for convenience ===

# You can import this directly:
#   from src.settings import settings
settings = load_settings()
