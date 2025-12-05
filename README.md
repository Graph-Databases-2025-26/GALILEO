# GALOIS Project: LLM Integration in Query Execution Pipelines

### University of Padova – Graph Database Course

---

## I. Group Information

**Group Name:** GALILEO

| Role | Member | Contact |
| :--- | :--- | :--- |
| Member | Giorgia Amato | giorgia.amato@studenti.unipd.it |
| Member | Alessio Demo | alessio.demo@studenti.unipd.it |
| Member | Francesco Pivotto | francesco.pivotto.1@studenti.unipd.it |

---

## II. Project Overview and Objectives

This project explores the integration of **Large Language Models (LLMs)** into database query execution pipelines, focusing on the concepts established by the **GALOIS framework**.

### Objectives

* **Data Retrieval Improvement:** Understand how LLM integration can support or improve **data retrieval efficiency and accuracy**.
* **Analysis:** Analyze the **strengths and limitations** of this hybrid approach.
* **Implementation:** Implement and evaluate **three practical baselines** (SQL Prompting, NL Prompting, and RAG/Palimpzest).

---

## III. Repository Structure

The project follows a **modular organization** designed to separate configuration, data, source code, and testing components, enhancing maintainability and code clarity.

### Main Directories

| Directory | Description |
| :--- | :--- |
| **`/data`** | Stores raw datasets and all related database files (`.duckdb`). Includes: `/.ground_truth/` (Baseline truth for evaluation) and `/.output/` (Generated query results). |
| **`/results`** | Holds the final outputs from analysis, including `EXPLAIN` and `EXPLAIN ANALYZE` plans (`.json` and `.txt` files). |
| **`/src`** | **Main Source Code.** Contains entry point (`main.py`) and all core logic:<br> &bull; `/config`: Configuration loading.<br> &bull; `/db`: Database interaction and query execution.<br> &bull; `/llm`: LLM Adapter interfaces and wrappers.<br> &bull; `/galois`: Core query optimization/execution logic.<br> &bull; `/utils`: Utility scripts (logging, evaluation, ground truth build). |
| **`/test`** | Contains unit and integration testing scripts. |

### Root-Level Files

| File | Description |
| :--- | :--- |
| **`README.md`** | High-level project documentation. |
| **`requirements.txt`** | List of all required Python dependencies. |
| **`setup_project.py`** | Initialization script for virtual environment creation and dependency installation. |
| **`config.yaml`** | Central configuration file for project settings. |

---

## IV. Core Architecture and Dependencies

### Dependency Stack

The project relies on a robust Python stack for data analysis, LLM interaction, and structured configuration.

| Category | Key Packages | Purpose |
| :--- | :--- | :--- |
| **LLM Orchestration** | `langchain-google-genai`, `langchain-ibm` | Manages standardized prompt handling, communication, and response parsing for **Gemini** and **Watsonx.ai**. |
| **Database/SQL** | `duckdb`, `sqlglot` | **`duckdb`** serves as the high-performance analytical database. **`sqlglot`** is used for SQL parsing, analysis, and manipulation. |
| **Configuration** | `python-dotenv`, `pydantic`, `pyyaml` | **`python-dotenv`** loads secrets. **`pydantic`** enforces strict type validation and structured models for configuration. |
| **Utilities** | `pandas`, `numpy`, `loguru`, `jinja2` | Standard data manipulation, advanced structured logging, and dynamic prompt generation. |

### Key Design Patterns Implemented

The codebase's central logic is structured around established software design patterns to ensure high modularity, extensibility, and maintainability.

| Pattern | Key Location | Purpose |
| :--- | :--- | :--- |
| **Adapter Pattern** | `llm_wrappers.py` | Standardizes the interface for interacting with heterogeneous LLM providers, enabling the core engine to use a uniform API. |
| **Factory Method** | `llm_factory.py` | Decouples client components from the specific instantiation logic of concrete LLM wrapper objects, promoting easy extensibility to new providers. |
| **Template Method** | `LLMBaseWrapper` | Defines the invariant execution flow (the *template*) for LLM instance retrieval, deferring provider-specific details to subclasses. |
| **Pipeline (LCEL)** | `build_lcel_chain` | Defines a structured, sequential execution flow using LangChain Expression Language: **Context** $\rightarrow$ **Prompting** $\rightarrow$ **LLM Invocation**. |

### Standardized Execution Workflow

The baseline execution follows a robust four-stage pipeline, ensuring consistent processing across all baseline types:

1.  **Context Generation:** Prepares the necessary input context (e.g., retrieving the database schema for NL/SQL baselines or simulating RAG data for Palimpzest).
2.  **Prompt Engineering:** Programmatically injects the context into a `ChatPromptTemplate`, manages system instructions, and enforces the required JSON output structure.
3.  **LLM Invocation:** Dispatches the structured prompt to the configured LLM instance.
4.  **Response Standardization:** Enforces the mandatory output structure using a `PydanticOutputParser`, and enriches the response into the **FULL JSON format** by calculating latency and extracting token consumption metadata.

---

## V. LLM Configuration and Secrets (`.env`)

The system relies on the **`.env`** file to load environment variables essential for establishing connections to the databases and the supported LLM providers. You **must configure your secrets** in the **`🤖 AI Provider`** section for the baselines to run correctly.

### Essential Variables to Configure

| Variable                 | Description                                              | Example Value                                  |
|:-------------------------|:---------------------------------------------------------|:-----------------------------------------------|
| **`WATSONX_API_KEY`**    | API Key required for authentication with IBM Watsonx.ai. | `2qeY-kSnPE2vKLk9aF93orYmkxvihSvyAI2IPDvWT4BD` |
| **`WATSONX_PROJECT_ID`** | The unique ID of the Watsonx project.                    | `f1e10140-3021-4a6f-97b2-befdae53d9a0`         |
| **`WATSONX_ENDPOINT`**   | The base URL for the Watsonx API.                        | `https://us-south.ml.cloud.ibm.com`            |
| **`WATSONX_USERNAME`**   | The username for the Watsonx account.                    | `nome.cognome@unipd.it`                        |
| **`GEMINI_API_KEY`**     | API Key for authentication with Google Gemini services.  | `your_xai_api_key`                             |
| **`GEMINI_ENDPOINT`**    | The URL for the Gemini API.                              | `api.x.ai`                                     |

### Example `.env` Structure (To be modified):

```ini
# ======================================
# 🤖 AI Provider
# ======================================
WATSONX_API_KEY=YOUR_PERSONAL_WATSONX_API_KEY
WATSONX_PROJECT_ID=YOUR_PERSONAL_PROJECT_ID
WATSONX_ENDPOINT=https://us-south.ml.cloud.ibm.com
WATSONX_USERNAME=YOUR_WATSONX_ACCOUNT_USERNAME

GEMINI_API_KEY=YOUR_PERSONAL_GEMINI_API_KEY
GEMINI_ENDPOINT=api.x.ai
```

---

## VI. Execution Commands

The main entry points are **`setup_project.py`** and **`main.py`** located in the **`root`** directory and **`/src`** respectively.

### 1. Project Setup (First Run Only)

This step creates the virtual environment (`.venv`) and installs dependencies.

```sh 
python setup_project.py
```

### 2. Running Baselines (Main Execution)

The `main.py` script accepts parameters to specify which datasets to process.

| Environment | Command Format                                                                           | Example                                                                |
| :--- |:-----------------------------------------------------------------------------------------|:-----------------------------------------------------------------------|
| **LINUX / macOS** | `python setup_project.py && .venv/bin/python -m src.main <datasets> --mode --provider`   | `... src.main world flight-2 presidents --mode sql --provider watsonx` |
| **WINDOWS (Bash/IDE Terminal)** | `python setup_project.py; .venv\Scripts\python -m src.main <datasets> --mode --provider` | `... src.main GEO MOVIES --mode sql --provider watsonx`                |
| **WINDOWS (Native CMD)** | `python setup_project.py && .venv\Scripts\python -m src.main <datasets> --mode --provider`                | `... src.main world flight-2 presidents --mode sql --provider watsonx`                              |

#### Dataset Parameters
The command line **has priority** over the configuration file.

* **Process all datasets**: `.venv\Scripts\python -m src.main ALL`
* **Process specific datasets**: `.venv\Scripts\python -m src.main GEO MOVIES FLIGHT-4`
* **Process baselines** you can choose the baseline and the provider by means the `--mode` and `--provider` parameter (see the Command-line Interface (CLI): section below).
* **Configuration Fallback**: If no arguments are provided, the script uses the datasets defined in `config/config.yaml`.

Notice that in our system we used `gemini-2.5-flash` for gemini, and `meta-llama/llama-3-3-70b-instruct` for ibm watsonx

---

## VII. Project Baselines

The system replicates three core baseline concepts: **SQL Prompting**, **Natural Language (NL) Prompting**.

### Standard Workflow

All baselines follow the same standardized execution pipeline, leveraging the LangChain Expression Language (LCEL) for sequential processing:

1.  **Context Building** (Schema/RAG Data preparation)
2.  **Prompt Preparation** (Template injection)
3.  **LLM Invocation** (API call)
4.  **Result Parsing** (FULL JSON validation)

### Baseline NL & SQL: Internal Knowledge Evaluation

This module is designed to assess the LLM's **intrinsic reasoning and logical capacity** when provided only with structural context. Both NL and SQL baselines share the **same provider-agnostic architecture** (`sql_baseline.py` and `nl_baseline.py`).

#### General Procedure and Context

* **Goal:** Verify how providers interpret complex inputs (SQL syntax or NL questions) and reason over the database schema.
* **Context Category:** Both baselines rely exclusively on the **Internal Knowledge (IK)** context. This means **only the database schema** is provided to the LLM; no factual raw data (tuples/rows) from the database is used.
* **Input Loading:** Queries are automatically loaded from `queries_<dataset>.sql` (for SQL) or `queries_<dataset>.txt` (for NL).
* **Provider Agnostic Execution:** The process relies on the `get_llm_wrapper()` factory to uniformly execute the LCEL chain for both Gemini and Watsonx providers, measuring latency and token usage automatically.

#### Result Standardization

Both baselines output a unified **FULL JSON format** for perfect comparability:

```json
{
  "result_set": ["..."],
  "time": "<seconds>",
  "tokens": "<int>"
}
```
### Command-line Interface (CLI)

The unified CLI (`src/main.py`) controls all baseline execution modes through the `--mode` parameter.

| Mode    | Description                                                            | Baselines Run |
|:--------|:-----------------------------------------------------------------------| :--- |
| `nl`    | Run only the Natural Language baseline.                                | NL |
| `sql`   | Run only the SQL baseline.                                             | SQL |
| `both`  | Run both NL and SQL baselines sequentially.                            | NL, SQL |
**Examples:**

```sh
# Run both NL and SQL baselines on the WORLD dataset using Gemini
python -m src.main WORLD --mode both --provider gemini 

# Run only the SQL baseline on GEO and MOVIES using Watsonx
python -m src.main GEO MOVIES --mode sql --provider watsonx 

# Run only the NL baseline on PRESIDENTS and WORLD using Watsonx
python -m src.main PRESIDENTS WORLD --mode nl --provider watsonx 
```
---

### RAG additional implementation
Out system also implements by means `src/llm/rag.py` a small RAG backend for indexing and retrieving chunks from Markdown or text files.
It uses a LangChain components:
* **TextLoader** for reading files.
* **RecursiveCharacterTextSplitter** for chunking.
* **HuggingFaceEmbeddings** for computing embeddings, and FAISS as the vector store.
* **MarkdownRABackend**:
  * Finds files with the `.md`, `.txt` or no format, load them and splits into chunks, created embeddings and builds a FAISS index.
  * Performs a similarity search against FAISS index and returns the top_k results.
  * Save and load the FAISS index from disk.

The main maethod of this subtask is `**pz_context()**` that instantiates the backend, builds the index, retrieves relevant chunks for the provided prompt and returns
a dictionary.

As source files we use the ones found in the former galois repository under `rag_premier` and `rag_fortune` folders.

---

## VIII. 📊 Dedicated Utility Scripts

Individual scripts are available for specific tasks, primarily located in `src/utils/` or `src/db/`.

| Task                        | Location                          | Execution Example (from `/GALILEO/` root)                                                            |
|:----------------------------|:----------------------------------|:-----------------------------------------------------------------------------------------------------|
| **EXPLAIN / ANALYZE Plans** | `src/db/run_explain_plans.py`     | `python -m src.db.run_explain_plans world geo`                                                       |
| **Ground Truth Generation** | `src/utils/build_ground_truth.py` | `python build_ground_truth.py --data-root ./data --ground-root ./data/.ground_truth`                 |
| **Evaluation**              | `src/utils/galois_eval.py`        | `python galois_eval.py --ground <path> --submissions <path>`                                         |
| **Avg. Expected Cells**     | `src/db/avg_cells_metric.py`      | `python -m src.db.avg_cells_metric`                                                                  |
| **RAG additional feature**  | `src/llm/rag.py`                  | `python -m src.main premier fortune --mode pznl` / `python -m src.main premier fortune --mode pzsql` |

---

---

## IX. GALOIS Implementation
We explain our GALOIS implementation in `reports/Deadline3/GD_Galileo_3_Report/GD_Galileo_Report_3.pdf` pdf