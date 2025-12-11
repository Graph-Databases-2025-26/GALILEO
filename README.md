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

##  System Architecture

The system is modular, dividing responsibilities between parsing, confidence estimation, schema management, and actual execution.

### 1. Main Orchestration (`Galois` Class)
The `Galois` class (in `galois.py`) acts as the *Query Planner* and *Orchestrator*.
* **SQL Parsing:** Uses a parser based on `sqlglot` to decompose the query into projections (`SELECT`), target tables (`FROM`), conditions (`WHERE`), and joins.
* **Logical Optimization (Push-Down):** Decides which SQL filters to "push" into the LLM prompt and which to apply locally during post-processing.
* **Physical Optimization:** Dynamically selects the optimal scan strategy (`Table-Scan` vs `Key-Scan`) or delegates it to an `auto` mode based on confidence estimation.

### 2. Execution Engine (`GaloisExecutor`)
The `GaloisExecutor` (in `executor.py`) manages direct interaction with the LLM.
* **Scan Algorithms:** Implements two fundamental data retrieval strategies:
    * **Table-Scan:** Requests the LLM to generate complete tuples in iterative batches, paginating results until the model's knowledge is exhausted.
    * **Key-Scan:** Splits the process into two phases: first extracting unique primary key values, then iterating over them to retrieve the remaining attributes (lookup).
* **Memory Management:** Uses `GaloisMemory` to deduplicate tuples generated across iterations and maintain conversation history for context.
* **Parsing and Validation:** Converts raw LLM text output into validated JSON objects, handling common formatting errors and truncation.

### 3. Schema Management (`SchemaManager`)
To query the LLM like a relational database, the system injects the schema structure into prompts.
* **Metadata Store:** Uses a local DuckDB database to retrieve column names, data types, and primary key constraints.
* **Wrapper Factory:** `GaloisSchemaManagerWrapper` instantiates the correct manager (e.g., `GaloisWO`, `GaloisA`) depending on the selected baseline, handling connections and logging.
* **Example Injection:** Enriches the prompt with real example values (sampled from the DB) to guide the LLM in generating the correct JSON format.

### 4. Confidence Estimation (`ConfidenceEstimator`)
A critical component (in `galois_estimator.py`) that evaluates the LLM's ability to handle specific filters.
* Queries the LLM asking for a confidence estimate ("HIGH" or "LOW") regarding the evaluation of a specific `WHERE` clause.
* This score determines whether to filter data "at the source" (saving tokens) or download the full dataset and filter locally.

### 5. Post-Processing & Local Join
Once "raw" data is obtained from the LLM, the `GaloisPostProcessor` (in `galois_post_processing.py`) takes over.
* **Virtual Tables:** JSON data is loaded into in-memory virtual tables on DuckDB.
* **Type Casting:** Robust casting is performed to convert approximate LLM strings (e.g., "None", "N/A") into correct SQL types (`NULL`, `INTEGER`, etc.).
* **Local Joins:** If the original query involved JOINs, Galois downloads the tables separately from the LLM and joins them locally using the DuckDB Relational API.
* **Residual Filters:** `WHERE` conditions discarded during the logical optimization phase are applied here with deterministic precision.

---

##  Execution Workflow

The execution flow of a query (e.g., `SELECT * FROM movies WHERE year > 2000`) follows these steps:

1.  **Parsing:** The query is analyzed to extract the target table and conditions.
2.  **Estimation (Optional):** The system estimates if the LLM can handle "year > 2000".
    * *High Confidence:* The condition is added to the prompt.
    * *Low Confidence:* The condition is removed from the prompt (becoming "residual").
3.  **Prompt Construction:** The template (`KEY_SCAN` or `TABLE_SCAN`) is selected. The prompt includes the table's JSON schema.
4.  **LLM Loop:** The Executor iteratively calls the LLM to paginate results until no new unique tuples are generated.
5.  **Data Assembly:** Responses are aggregated and cleaned.
6.  **Local Execution:** DuckDB executes the final SQL query on the extracted in-memory data.

---

##  Optimization Variants (Baselines)

The system supports several strategic configurations defined in `galois.py`:

| Variant | Push-Down Logic | Description |
| :--- | :--- | :--- |
| **GALOIS_WO** (Without Opt.) | **No Push** | Downloads all data or keys from the table without filters in the prompt. Filters everything locally. |
| **GALOIS_A** (Push-All) | **Full Push** | Pushes all `WHERE` conditions into the prompt. Relies completely on the LLM for filtering. |
| **GALOIS_S** (Push-Selective) | **Heuristic Push** | Pushes only the condition deemed most "selective" (e.g., based on confidence estimation). |
| **GALOIS_F** (Push-Confident) | **Dynamic Push** | Uses `ConfidenceEstimator` to decide pointwise which filters to push (only those with "HIGH" confidence). |

---

## GALOIS Results
### GALOIS WO Results comparison

| Metric | PAPER | Ours | Δ (PAPER - Ours) |
| :--- | ---: | ---: | ---: |
| F1-Cell | 0.518 | 0.157 | 0.361 |
| Cardinality | 0.691 | 0.357 | 0.334 |
| Tuple Constr. | 0.389 | 0.060 | 0.329 |
| **AVG-Score** | **0.531** | **0.191** | **0.340** |
| #Tokens (M) | 19.710 | 0.163 | 19.709 |
| Avg Time | 1460.0 | 43.103 | 1416.897 |

### GALOIS S Results comparison

| Metric | PAPER | Ours | Δ (PAPER - Ours) |
| :--- | ---: | ---: | ---: |
| F1-Cell | 0.480 | 0.434 | 0.046 |
| Cardinality | 0.655 | 0.667 | -0.012 |
| Tuple Constr. | 0.365 | 0.297 | 0.068 |
| **AVG-Score** | **0.500** | **0.466** | **0.034** |
| #Tokens (M) | 0.960 | 0.088 | 0.872 |
| Avg Time | 130.00 | 17.952 | 112.048 |

### GALOIS A Results comparison

| Metric | PAPER | Ours | Δ (PAPER - Ours) |
| :--- | ---: | ---: | ---: |
| F1-Cell | 0.543 | 0.417 | 0.126 |
| Cardinality | 0.799 | 0.614 | 0.185 |
| Tuple Constr. | 0.448 | 0.279 | 0.169 |
| **AVG-Score** | **0.592** | **0.436** | **0.156** |
| #Tokens (M) | 0.950 | 0.076 | 0.874 |
| Avg Time | 120.50 | 24.481 | 96.019 |

### GALOIS F Results comparison

| Metric | PAPER | Ours | Δ (PAPER - Ours) |
| :--- | ---: | ---: | ---: |
| F1-Cell | 0.563 | 0.319 | 0.244 |
| Cardinality | 0.835 | 0.528 | 0.307 |
| Tuple Constr. | 0.464 | 0.166 | 0.298 |
| **AVG-Score** | **0.622** | **0.338** | **0.284** |
| #Tokens (M) | 1.720 | 0.142 | 1.578 |
| Avg Time | 47.400 | 26.196 | 21.204 |