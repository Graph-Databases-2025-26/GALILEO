# Configuration Errors (Used in config.py or similar setup files)
ERR_CONFIG_FILE_NOT_FOUND = "Configuration file not found: {}"

# Configuration Errors (Used in config.py for structure validation)
ERR_PROVIDER_SECTION_MISSING = "Configuration section '{}' is mandatory when 'llm_provider' is set to it."

#Generic base error for LLM configuration issues
ERR_CONFIG_BASE = "LLM configuration error."

# File/I/O Errors (Used in data processing or utility functions)
ERR_FILE_READ_FAILURE = "Error reading file {}: {}"

# JSON Errors (Used in JSON loading/parsing functions)
ERR_INVALID_JSON_FORMAT = "Invalid JSON format in file {}: {}"

# Database Errors (Used in database connection or querying functions)
ERR_INVALID_TABLE_NAME = "Table '{}' does not exist in the database."

# Parsing/LLM Execution Errors (Used in execute_baseline_sql_query or parse_llm_response)
ERR_LLM_PARSING_FAILURE = "Failed to parse required JSON structure or LLM output: {}"

# LLM Providers (Used in llm_factory.py)
ERR_UNSUPPORTED_PROVIDER = "LLM provider not supported: {}" 

# Gemini Specific Errors (Used in llm_wrappers.py)
ERR_GEMINI_API_KEY_MISSING = "GOOGLE_API_KEY not configured for Gemini."

ERR_OPENROUTER_API_KEY_MISSING = "OPENROUTER_API_KEY not configured for OpenRouter."

# Watsonx Specific Errors (Used in llm_wrappers.py)
ERR_WATSONX_API_KEY_MISSING = "WATSONX_API_KEY not configured for Watsonx."

# Galois Baseline not supported (Used in schema_factory.py)
ERR_UNSUPPORTED_BASELINE = "Baseline type '{}' not Supported."