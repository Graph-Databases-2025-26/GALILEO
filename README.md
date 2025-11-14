# GALOIS Project
### University of Padova – Graph Database course

---

## Group Information
**Group Name:** GALILEO
**Members:**
- Giorgia Amato – [giorgia.amato@studenti.unipd.it]  
- [Alessio Demo] – [alessio.demo@studenti.unipd.it]  
- [Francesco Pivotto] – [francesco.pivotto.1@studenti.unipd.it]   



---

##  Project Description
We are a student group exploring how **Large Language Models (LLMs)** can collaborate with **databases**.  
Our focus is on **GALOIS**, a framework that integrates LLMs into query execution pipelines.

**Objective:**
- Understand how this approach can improve **data retrieval**.  
- Analyze its **strengths and limitations**.  
- Implement a **practical use case** to demonstrate and test these ideas.

---

## How to run the system:
For running the system the only scripts that you need is **`main.py`** **`setup_project.py`** that is located in the root folder.
In detail:
* **`setup_project.py`** install all the needed libraries for requirements.txt and creates the virtual environment.
* **`main.py`** runs all the system functionalities

In detail what **`setup_project.py`** does is the following:
* Defines the main paths of the project (query results folder, ground truth folder exc...)
* Creates an virtual environment for python 
* Install the needed python dependencies from **requirements.txt** for the system

In detail what **`main.py`** does is the following:
* Create a database for each data folder specified as input
* Executes queries for a set of datasets and saves results in JSON.
* Evaluates the results against the ground truth.
* Prints messages to track progress.


**RUN THE SYSTEM**: To run the system:
* If you are in a WINDOWS environment and you are using the windows native command line: execute **`python setup_project.py &&  .venv\Scripts\python -m src.main <parameters>`** (e.g. **`python setup_project.py && .venv\Scripts\python -m src.main world flight-2 presidents `**) from the root directory (**`/GALILEO/`**).
* If you are in a WINDOWS environment and you are using bash command line or a IDE terminal (e.g. PyCharm) : execute **`python setup_project.py;  .venv\Scripts\python -m src.main  <parameters> `** from the root directory (**`/GALILEO/`**).
* If you are in a LINUX environment: execute **`python setup_project.py &&  .venv\bin\python -m src.main <parameters>`** from the root directory (**`/GALILEO/`**).


Regarding the parameters you can specify which datasets to process directly from the command line or via YAML configuration:
REMEMBER: THE COMMAND LINE HAS THE PRIORITY
* **To process all datasets**: **`.venv\Scripts\python -m src.main ALL`** 
* **To process one or more specific datasets**: **`.venv\Scripts\python -m src.main GEO MOVIES FLIGHT-4`** 
* **Alternative via Configuration File**: If no command-line arguments are given, the script will fall back to the YAML configuration in which you can define the datasets in **`config/config.yaml`** changing the selected datasets under the **`database`** attribute.

Furthermore, if necessary, it's possible to execute individual scripts:
* **Evaluation queries**: from the following directory: `/GALILEO/src/utils/` run: **`python3  galileo_eval.py [-h] --ground GROUND --submissions SUBMISSIONS [--datasets [DATASETS ...]] [--cell-metric {exact,similarity}] [--tuple-metric {constraint,similarity}] [--format {table,csv,json,tex}]
                      [--latex-caption LATEX_CAPTION] [--latex-label LATEX_LABEL] [--latex-booktabs] [--overall] [--jobs JOBS] [--jobs-queries JOBS_QUERIES]`** .
* **Ground Truth generation**: from the following directory: `/GALILEO/src/utils/` run:  **`python3 build_ground_truth.py [-h] --data-root DATA_ROOT --ground-root GROUND_ROOT [--datasets [DATASETS ...]] [--schema-name SCHEMA_NAME]
build_ground_truth.py: error: the following arguments are required: --data-root, --ground-root`**.
* **Avg. expected cells metric**: For calculate this metric you need to locate in the root  folder `/GALILEO/` and run: **` python3 -m src.db.avg_cells_metric`**.
* **EXPLAIN / ANALYZE plans generation in .txt and .json format:** from the root folder `/GALILEO/` run: **`python3 -m src.db.run_explain_plans <dataset1> [<dataset2> ...] | all`** -> you can type ' all ' or ' ALL ' and the command works anyway, additionally you can specify a single or multiple dataset, if you don't specify anything the system will process al datasets.

---

## Structure of the Repository

**The project follows a modular organization designed to separate configuration, data, source code, and testing components.**
**This layout enhances maintainability, scalability, and code clarity.**

---

### Main Directories

| Directory       | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| :---------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **`/config`**   | Contains configuration files and logic for loading them. Includes: <br>   • `config.yaml` — Main configuration file.<br>   • `loaders.py` — Parses the YAML configuration into a Python structure.                                                                                                                                                                                                                                                                                                                                                                                   |
| **`/data`**    | Stores raw data, ground truth, and intermediate results. Contains: <br>   • `.ground_truth/` — Baseline truth for evaluation.<br>   • `.output/` — Generated results ready for comparison.                                                                                                                                                                                                                                                                                                                                                                                           |
| **`/results`** | Holds the final outputs produced by the analysis process. Typically includes `.json` and `.txt` files from the `explain` and `analyze`   operations.                                                                                                                                                                                                                                                                                                                                                                                                                                 |   
| **`/src`**     | Main source folder containing the project’s operational logic. Includes: <br>   • `main.py` — Entry point for execution.<br>   • `/db` — Database interaction modules (e.g., `db_connection.py`, `run_queries_to_json.py`).<br>   • `/llm` — Interfaces with Large Language Models (e.g., `google_genai_connection.py`, `watsonx_ai_connection.py`).<br>   • `/galois` — Implements query optimization and execution logic inspired by the Galois architecture.<br>   • `/utils` — Utility scripts (`constants.py`, `logging_config.py`, `build_ground_truth.py`, `galois_eval.py`). |
| **`/test`**    | Contains unit and integration testing scripts (e.g., `db_utils.py`).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
                                                         
---

### Root-Level Files

| File                   | Description                           |
| :--------------------- | :------------------------------------ |
| **`README.md`**        | High-level project documentation.     |
| **`requirements.txt`** | List of required Python dependencies. |
| **`setup_project.py`** | Script for project initialization.    |

---


## Architecture of the system:
### 🛠 Dependency Stack and Key Requirements

This project relies on a Python **dependency stack** designed to ensure robustness, efficient data analysis, and flexible configuration. All libraries required for execution (excluding development-only dependencies) are listed in `requirements.txt`.

### **Core Frameworks & Data Analysis (DataFrames)**

* **DataFrames/Analysis:** **`pandas`** and **`numpy`** are used in our project for the efficient manipulation and analysis of tabular data structures (DataFrames) and complex numerical calculations.

---

### **Database and SQL**

For connectivity, analysis, and management of structured data:

* **Database In-Process:** **`duckdb`** is used as a high-performance analytical database, ideal for rapid processing of data volumes directly within the application process.
* **SQL Parsing:** **`sqlglot`** is employed for the manipulation, translation, and abstract analysis of SQL queries.

---

### **LLM Models Used**

Our system interacts with two different LLM providers: Google Gemini and IBM Watsonx.ai., let's see the details:
* **Google Gemini AI:** The system leverages LangChain to manage interactions with Google Gemini AI models through the **`langchain-google-genai`** package.
The reason why we choose LangChain is the modularity of this framework for handling prompts and conversational pipelines with LLMs.
* **IBM Watsonx.ai:** The system also integrates with IBM Watsonx.ai models using the **`ibm_watsonx_ai.foundation_models.ModelInference`** orchestrator package that manages authentication, model selection, prompt submission, and response handling for foundation models in IBM watsonx.ai environments.
Regarding watsonx we use the model  **openai/gpt-oss-120b**, but we have a large set of choices for that.
In summary, this setup is flexible and scalable in order to establish a communication between our system and LLMs.

---

### **Config, Secrets, and Data Validation (Pydantic)**

Robust configuration management and input data validation are ensured via:

* **Configuration/Secrets:** **`python-dotenv`** loads environment variables (including secrets) from the `.env` file.
* **Data Validation:** We use  **`pydantic`** define structured configuration models, validate the data, and enforce type safety.
  * **Type validation**: ensures all config fields have the correct type.
  * **Data parsing / coercion**: converts types automatically when possible.
  * **Structured, nested models**: makes the configuration predictable and IDE-friendly.
  * **Error reporting**: immediately raises errors if the YAML is malformed or missing fields.
  * **Environment variable integration via BaseSettings**.


* **Config Parsing (YAML):** **`pyyaml`** handles reading and writing configuration files in YAML format.

---

### **Templating**

For the dynamic generation of code, reports, or structured output:

* Templates:We use  **`jinja2`** that allows separate presentation logic from data: indeed we generate dynamic text files (like HTML, XML, JSON) or prompt statements by combining static content with data from our system.

---

### **Logging and Utilities**

For monitoring and diagnostics:

* **Logging:** **`loguru`** is the chosen logging library for its simplicity, structured output, improving system log readability.
* **A folder named ** **`.log` ** contains all the logs generated during the running time. 

---

### **Development and Testing (Tests)**

These libraries are required only for the development environment and for running the project's test suite:

* **Test Runner:** **`pytest`** is the standard testing framework used to execute all tests.
* **Code Coverage:** **`coverage`** and **`pytest-cov`** measure the percentage of source code covered by automated tests.
* **Mocking:** **`pytest-mock`** facilitates the creation of mock objects and stubs to isolate external dependencies during unit tests.

---

## Implementation Design 

###  Database Preparation and Connection

### 1. Opening the Connection (db.connection.py)

The `db.connection.py` script is responsible for opening and managing the connection to the specific DuckDB database for the dataset being used.

**Operational Details:**

* **Path Calculation:** Calculates the exact path of the database file: `../data/<dataset_name>/<dataset_name>.duckdb`.
* **Existence Check:** Performs a check to ensure the database file exists at the specified path.
* **Connection Creation:** Creates and returns a DuckDB connection object (`duckdb.connect()`).
* **Feedback:** Prints a confirmation message indicating that the connection has been successfully established.

---

### 2. Ingestion and Setup (duckdb_db_graphdb.py)

The `duckdb_db_graphdb.py` script is used to initialize the database environment, creating or recreating the DuckDB databases for each dataset and populating them with initial data.

**Operational Details:**

* **Path Setup:** Sets essential paths (`data`, `project.duckdb`, etc.).
* **Dataset Scan:** Scans all subfolders inside `../data` (e.g., `geo`, `movies`, `world`, etc.), where each folder represents a dataset.
* **For Each Dataset:**
    1.  **Cleanup:** If it exists, deletes the old database (`dataset_name.duckdb`).
    2.  **Creation:** Creates a new, empty DuckDB database.
    3.  **SQL Script Execution:** Automatically runs all ingestion SQL scripts (`ingest_*.sql`) within that dataset folder (table creation, data loading, etc.).
    4.  **Verification:** Displays a list of the newly created tables for verification.
    5.  **Closure:** Closes the connection and persists the database state.

---

### 3.  Query Execution and Result Saving (JSON)

Following the database setup, the `run_queries_to_json.py` script manages the automatic execution of analysis queries and the saving of results for verification.

**Operational Details (`run_queries_to_json.py`):**

* **Target:** The script targets a specific dataset folder (e.g., `../data/geo/`).
* **Automatic Execution:** It automatically executes all SQL queries defined in files matching the pattern `queries_*.sql` within the selected dataset folder.
* **Output:** It saves the result of each executed SQL query into a dedicated **JSON** file.
* **Save Path:** The JSON files are deposited in the path: `../../verification-test-tools/tests/<dataset_name>/`.
* **Function:** This process is crucial for generating the reference results needed for testing and data verification.

---

### 4. Logical and Physical Plan Extraction (EXPLAIN & EXPLAIN ANALYZE)

After generating the query results, the next step is to automatically extract both the **logical** and **physical** query plans for all datasets.

#### Scripts Involved:
* `src/db/run_explain_plans.py`
* `src/db/duckdb_explain.py`


#### **Operational Details (`run_explain_plans.py`):**

1. **Dataset Iteration:**  
   Iterates through each dataset folder inside `data/` (e.g., `world`, `geo`, `flight-2`, `movies`, etc.).

2. **Query Extraction:**  
   Automatically reads all `.sql` files (e.g., `queries_world.sql`), splitting them into individual SQL statement.

3. **Plan Generation:**  
   For each query, connects to the corresponding `.duckdb` database and executes:
 * **`EXPLAIN`**: Produces the **logical plan** showing the operator tree  
  (e.g., `Projection`, `Filter`, `TableScan`, `Join`, `OrderBy`, etc.)
 * **`EXPLAIN ANALYZE`**: Executes the query and reports **profiling data** such as row counts, execution time, and cost estimation per operator.

4. **Saving the Output:**  
   The function `save_both()` (in `duckdb_explain.py`) automatically stores both outputs in separate folders under the project’s`results/` directory:

   
   Producing four files for each query:
   `<query_name>__explain.txt`
   `<query_name>__explain.json`
   `<query_name>__analyze.txt`
   `<query_name>__analyze.json`

5. **Text & JSON Representation:**
   **.txt** for human-readable format with ASCII boxes (logical/physical plan)
   **.jso**  for machine-readable structured format (escaped Unicode characters)

---

### 5.Avg. Expected Cells Metric**

Like in the GALOIS paper, we replicate the Table 2 of the paper, the **Avg_Expected_Cells** metric is calculated by  **`avg_cells_metric.py`**, for run it you have to locate in the root directory **`GALILEO/`** directory and next run: **`python -m src.db.avg_cells_metric`**.

---

## BASELINES
The system  replicates various baseline concepts and implementations (e.g. prompting the LLM with a raw SQL query, or with a NL prompt and finally also a RAG paradigm is implemented -> Palimpzest)
Before taking a look at the details of each baseline, we want to provide a general structure overview of the scripts that manage the baselines execution:

### Key Design Patterns Implemented

The central logic of the code relies on the following design patterns:

| Pattern | Key Location | Purpose |
| :--- | :--- | :--- |
| **1. Adapter Pattern** | `llm_wrappers.py` | Standardizes the interface for different LLM providers (e.g., Gemini, Watsonx), allowing the core code to treat them uniformly. |
| **2. Factory Method** | `llm_factory.py` | Decouples the client code (e.g., `sql_baseline.py`, `nl_baseline.py`) from the specific instantiation logic of the LLM wrapper objects. Adding new providers only requires updating the `PROVIDER_MAP`. |
| **3. Template Method** | `LLMBaseWrapper` in `llm_wrappers.py` | Defines the skeleton for retrieving the LLM instance, but delegates the specific connection implementation (`_create_llm_instance()`) to the concrete subclasses. |
| **4. Pipeline Pattern (LCEL)** | `build_lcel_chain` in `baseline_tools.py` | Defines a sequential execution flow (pipeline) for request handling: `Context -> Prompting -> LLM Invocation`. |

### Standard Workflow

The baseline execution process follows a clear pipeline, leveraging the LangChain Expression Language (LCEL):

* **Context Building** -> **Prompt Preparation:** -> **LLM Invocation:** -> **Result Parsing:**

After this brief introduction, let's take a closer look at each implementation:  

### 6. Baseline SQL -> Directly Prompting the LLM using a SQL query.
This module extends the baseline framework to evaluate the reasoning abilities of LLMs when prompted directly with **SQL statements** instead of natural language questions.  
The main goal is to verify how different providers (Gemini and Watsonx) interpret SQL queries, reason over the **database schema**, and produce a structured JSON answer comparable to that obtained from NL baselines.

---

#### **General Workflow**

1. **Query Loading**  
   SQL statements are automatically read from files named `queries_<dataset>.sql` using the helper `load_queries_from_folder()` located in `run_queries_to_json.py`.

2. **Schema Extraction and Context Creation**  
   For each dataset, the corresponding DuckDB database (`<dataset>.duckdb`) is opened and its schema extracted.  
   Datasets are divided into two categories:
   - **Internal Knowledge (IK)**: only the schema is provided as context to the LLM.
   - **Model Context (MC)**: in addition to the schema, the raw tuples obtained by executing the SQL query are passed as contextual data.

3. **Prompt Construction and Chain Execution**  
   The module leverages the **LangChain Expression Language (LCEL)** pipeline implemented in `baseline_tools.py`, which performs:
   - Context building (`get_db_context()`),
   - Prompt template creation (system + human prompts),
   - LLM invocation through the provider wrapper,
   - JSON parsing and validation via `PydanticOutputParser`.

4. **Result Parsing and Storage**  
   Each model response is parsed into a unified FULL JSON format containing:
   ```json
   {
     "result_set": [...],
     "time": <seconds>,
     "tokens": <int>
   }
   ``` 
       Results are saved into a consistent directory hierarchy:  
       `results/sql_output/<provider>/<dataset>/queryX.json`

   ---

   #### **Architecture and Implementation**

   The SQL baseline implementation is divided into two cooperating components:

   ##### **1. Generic SQL Baseline (`sql_baseline.py`)**

   This is the **official, provider-agnostic** baseline used by the system when running:
   **`python -m src.main --mode sql`**.

   **Features:**
   - Uses the `get_llm_wrapper()` factory to support both Gemini and Watsonx uniformly.
   - Builds an LCEL chain using `build_lcel_chain()`.
   - Invokes the LLM for each SQL query and measures latency and token usage.
   - Parses the response using `parse_llm_response()`.
   - Saves the results with `save_baseline_to_json()` under a structured folder hierarchy.
   - Supports both Internal Knowledge (IK) and Model Context (MC) dataset behavior transparently.

   This component ensures a unified, extensible architecture for all LLM providers.

   ##### **2. Gemini-specific Baseline (`baseline_sql_gemini.py`)**

   This is an alternative, Gemini-only implementation designed primarily for debugging, analysis, and prompt-engineering validation.

   **Includes:**
   - `execute_IK_baseline_sql_query_gemini()` — schema-only execution for IK datasets.
   - `execute_MC_baseline_sql_query_gemini()` — schema + raw data execution for MC datasets.
   - Explicit construction of system prompts, schema injection, and raw data formatting.
   - Handling of batch executions with rate limiting to avoid 429 quota errors.
   - JSON parsing using a dedicated Pydantic model `Response`.

   ##### **Unified JSON Format**

   Both implementations produce results following the same FULL JSON schema:

   ```json
   {
      "result_set": [...],
      "time": <seconds>,
      "tokens": <int>
   }
   ```

   This ensures perfect comparability across models and providers.

   ---

   ### Command-line Interface

   The unified CLI (`src/main.py`) controls all baselines through the `--mode` parameter.

   **Available modes:**
   - `nl` → Run only the NL baseline.
   - `sql` → Run only the SQL baseline.
   - `both` → Run both NL and SQL baselines sequentially.
   - `pz` → Run the Palimpzest (RAG-based) baseline.

   **Examples:**
   # Run both NL and SQL baselines on the WORLD dataset using Gemini
   **`python -m src.main WORLD --provider gemini --mode both`**.

   # Run only the SQL baseline on GEO and MOVIES using Watsonx
   **`python -m src.main GEO MOVIES --provider watsonx --mode sql`**.

   # Run the Palimpzest baseline
   **`python -m src.main WORLD --mode pz`**.
   


### 7. Baseline NL -> Directly Prompting the LLM with a NL question.

This module implements the Natural Language (NL) baseline approach for evaluating LLMs on the task of evaluating the knowledge of the LLM using a natural language prompt.
Furthermore together with the prompt also an extra-context will be provided to the LLM: the schema of the dataset produced by means DuckDB in the previous task.
Finally the results will be stored in a FULL JSON format for the evaluation phase.

**General Overview:**
1. **Load natural language prompts** from the 'queries_<nome-dataset>.txt' available in each dataset resource folder. 
2. **Query the LLM** using a LangChain chain that provides:
   - General information about the goal of the LLM.
   - Database schema information.
   - NL prompt itself.
3. **Parse** te LLM's output into a predefined FULL JSON format.
4. Save the structured results for each query as separate .json files forming a consistent baseline for evaluation.

**Dataset Categories and Context Injection**

All datasets are divided into two main categories:

1. **Internal Knowledge (IK)**  
   - The model relies exclusively on its own internal knowledge and understanding of the prompt.  
   - No external information or query results are provided — only the natural language question and schema context are used.

2. **Model Context (MC)**  
    For that task we have not take into account this datasets (FORTUNE and PREMIER) as indicated also in the Galois paper. 
**Architecture and main Components**
1. **Configuration and Setup**
   - The LLM provider API key are loaded form a `.env` file.
   - The LLM provider is selected through a configuration object (`Config_Loader`) -> out system uses **Watsonx** & **Google Gemini**.
   - Next, the correct wrapper is instantiated via `get_llm_wrapper()`, returning one of LLM provider Wrapper.
   - Each wrapper extends `LLMBaseWrapper` and implements provider-specific logic for model creation, output token counting and provider identification.
   
2. **Chain Construction**
   The `build_lcel_chain` builds a **LangChain Expression Language** pipeline composed of three main stages: **Database Context -> Prompt Template -> LLM Model**, in details:
   - `get_db_context()` retrieves database schema information and (for MC baselines) executes the original SQL query collect sample data via DuckDB.
   - `ChatPromptTemplate` combines two prompt layers:
     - `SYSTEM_PROMPT` that defines model role, behavior and task instructions.
     - `HUMAN_PROMPT` that injects the NL prompt and formatting guidelines.
   - `PydanticOutputParser` ensures that the model's response conforms to the  required structures FULL JSON schema (`Response`)

3. **Baseline Execution Flow**
   - Load SQL queries and corresponding NL prompt for each dataset.
   - For each `(prompt, query)` pair, invoke the LLM chain with the `chain.invoke()` method.
   - Parse the response with `parse_llm_response()`, which:
     - uses `PydanticOutputParser` to validate FULL JSON format.
     - computes inference time and token usage.
   - Save the parsed output to JSON file using `save_baseline_to_json()`.
    
Each generated file from these datasets follows the same FULL JSON structure:

```json
{
  "result_set": [
    { "originaltitle": "The Three Musketeers" },
    { "originaltitle": "The Count of Monte Cristo" }
  ],
  "time": 1.84,
  "tokens": 312
}
```

- `result_set` → Structured LLM output with mixed-type values.  
- `time` → Total execution time for that prompt.  
- `tokens` → Number of output tokens produced by the model.

---

### 8. Baseline Palimpzest -> RAG & in-context querying.
The proposed baseline automatically retrieves relevant text segments from Markdown or plain text files to construct additional contextual information for a Retrieval-Augmented Generation (RAG) pipeline. The resources used were collected from the GALOIS repository (`core/src/test/resources/rag-fortune` and `core/src/test/resources/rag-premier`).
The system builds a semantic knowledge base from textual documents, generating embeddings and indexing them using FAISS, an open-source library by Facebook AI Research designed for efficient similarity search and clustering of dense vectors.

To ensure portability and ease of testing, the baseline operates entirely on CPU, avoiding GPU dependencies due to limited hardware support and the higher setup time required for GPU libraries. For embedding generation, a lightweight transformer model **sentence-transformers/all-MiniLM-L6-v2** 
was selected for its balance between computational efficiency and semantic accuracy.

The baseline workflow follows the following process:
- **MarkdownRAGBackend**: handles file loading, chunking, embedding generation, and FAISS indexing.
- **pz_context()**: the main method that retrieves relevant contextual chunks for the Palimpzest model.
- **Backend**: core processing logic behind the baseline. 

So briefly:
During initialization, a HuggingFace embedding model is instantiated.
The loading and indexing phase identifies text or Markdown files, converts them into LangChain documents while preserving metadata,
and splits them into overlapping chunks (128 tokens for “Premier” and 400 for “Fortune”, with 20-token overlap). The resulting embeddings are indexed using FAISS library for efficient semantic retrieval. The retrieval module returns the top-k most semantically relevant chunks retrieved by means **sentence-transformers/all-MiniLM-L6-v2** model using similarity between embeddings, for a given prompt and next the FAISS index is persistently stored to enable reuse without re-indexing.

The GALOIS text files were intentionally used instead of standard dataset records (e.g., from Fortune or Premier) to align with the Palimpzest baseline’s original goal—evaluating large language models in a RAG setting using **external** textual resources distinct from the datasets themselves.

