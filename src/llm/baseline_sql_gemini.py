from pathlib import Path
import json
import time
from typing import List, Dict, Union, Any

from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_community.utilities import SQLDatabase
from pydantic import BaseModel, Field

from config import Config_Loader
from src.utils import LOG, DATA_DIR, SQL_IK_PROMPT, SQL_MC_PROMPT
from src.utils.constants import IK_DATASETS, MC_DATASETS, SQL_OUTPUT
from src.utils.dataset_selection import get_dataset_selection
from src.utils import build_sql_prompt
from src.db.run_queries_to_json import load_queries_from_folder
from src.llm.llm_factory import create_llm



# Pydantic Response 

class Response(BaseModel):
    result_set: List[Dict[str, Union[str, int, float, Any]]] = Field(
        default_factory=list,
        description="List of result records, each as a {column_name: value} dict where values can be mixed types."
    )



# helper function to invoke Gemini with parser

def _invoke_gemini_with_parser(llm, parser: PydanticOutputParser, messages: list) -> dict:
    """
    Invokes Gemini with a list of messages (System + Human) and parses the JSON
    using the PydanticOutputParser, returning a dict in the format:
    {
        "result_set": [...],
        "time": <seconds>,
        "tokens": <int or 0>
    }
    """
    t0 = time.time()
    raw = llm.invoke(messages)
    t1 = time.time()

    #EXTRACT RAW CONTENT
    content = getattr(raw, "content", str(raw))

    try:
        parsed = parser.parse(content)
        result_set = parsed.result_set
    except Exception as e:
        LOG.error(f"[Gemini] Parsing failed: {e}. Raw content: {content[:400]}")
        return {
            "result_set": [],
            "time": round(t1 - t0, 3),
            "tokens": 0,
            "error": f"Parsing failed: {e}"
        }

    # EXTRACT TOKENS USED (if available)
    tokens = 0
    usage = getattr(raw, "usage_metadata", None)
    if isinstance(usage, dict):
        tokens = usage.get("output_tokens", 0)

    return {
        "result_set": result_set,
        "time": round(t1 - t0, 3),
        "tokens": tokens
    }


# -----------------------------
# IK baseline (Gemini)
# -----------------------------

def execute_IK_baseline_sql_query_gemini(database: str, queries: list[tuple[str, str] | str]) -> list[dict]:
    """
    IK baseline (Gemini):
    - Always passes the database schema (as requested by tutor).
    - Does not pass records (schema only).
    - Uses SQL_IK_PROMPT and JSON parser identical to MC.
    """
    LOG.info(f"[Gemini IK] Starting IK baseline SQL queries for {database}...")

    llm = create_llm()
    parser = PydanticOutputParser(pydantic_object=Response)
    format_instructions = parser.get_format_instructions()


    # Load DuckDB schema
    dbs_path = DATA_DIR / database.upper() / f"{database.lower()}.duckdb"
    db = SQLDatabase.from_uri(f"duckdb:///{dbs_path}")
    db_schema = db.get_table_info()

    results: list[dict] = []

    RATE_LIMIT = 10          #max requests before cooldown
    COOLDOWN_SEC = 65        # seconds to wait
    total_queries = len(queries)

    for idx, qry in enumerate(queries, start=1):
        if isinstance(qry, tuple) and len(qry) == 2:
            file_name, sql_query = qry
        else:
            file_name, sql_query = None, qry

        LOG.info(
            f"[Gemini IK] Executing baseline SQL query on {database} "
            f"({idx}/{total_queries}): {file_name or '<no_filename>'} -> {sql_query}"
        )

        system_text = f"""
You are an expert SQL interpreter.
The following schema describes the database structure you can use to reason about the query.

DATABASE SCHEMA:
{db_schema}
---

{SQL_IK_PROMPT}
{format_instructions}
""".strip()

        full_prompt = [
            SystemMessage(system_text),
            HumanMessage(build_sql_prompt(sql_query)),
        ]

        result = _invoke_gemini_with_parser(llm, parser, full_prompt)
        LOG.info(f"[Gemini IK] Result: {result}")
        results.append(result)

        # Rate limiting:
        if idx % RATE_LIMIT == 0 and idx < total_queries:
            LOG.warning(
                f"[Gemini IK] Processed {idx}/{total_queries} queries for {database}. "
                f"Cooling down for {COOLDOWN_SEC} seconds to avoid 429 quota errors..."
            )
            time.sleep(COOLDOWN_SEC)

    return results


# -----------------------------
# MC baseline (Gemini)
# -----------------------------

def execute_MC_baseline_sql_query_gemini(
    database: str,
    queries: list
) -> list[dict]:
    """
    MC baseline (Gemini):
    - Executes the query on DuckDB (db.run(sql_query)) to obtain the actual RAW DATA.
    - Passes schema + raw_data into SQL_MC_PROMPT.
    - The LLM formats everything into JSON (result_set).
    """
    LOG.info(f"[Gemini MC] Starting MC baseline SQL queries for {database}...")

    llm = create_llm()
    parser = PydanticOutputParser(pydantic_object=Response)
    format_instructions = parser.get_format_instructions()

    results: list[dict] = []

    dbs_path = DATA_DIR / database.upper() / f"{database.lower()}.duckdb"
    db = SQLDatabase.from_uri(f"duckdb:///{dbs_path}")
    db_schema = db.get_table_info()

    for qry in queries:
        # qry can be ("filename.sql", "SELECT ...") or directly "SELECT ..."
        if isinstance(qry, tuple) and len(qry) == 2:
            file_name, sql_query = qry
        else:
            file_name, sql_query = None, qry

        LOG.info(
            f"[Gemini MC] Executing baseline SQL query on {database}: "
            f"{file_name or '<no_filename>'} -> {sql_query}"
        )

        # execute query to get RAW DATA
        raw_data = db.run(sql_query)

        # 🩵 Fix 1: convert raw_data to a readable, line-by-line format
        # Example raw_data string: "[('J. Zirkzee',), ('M. de Ligt',)]"
        if isinstance(raw_data, str):
            raw_data_str = raw_data.strip()
        elif isinstance(raw_data, list):
            # if it ever returns a list of tuples, convert it into text lines
            raw_data_str = "\n".join(
                ", ".join(map(str, row)) if isinstance(row, (list, tuple)) else str(row)
                for row in raw_data
            )
        else:
            raw_data_str = str(raw_data)

        raw_data_text = (
            "Each line below represents ONE ROW of the SQL result.\n"
            "For this query, you MUST create exactly one JSON object in result_set for each line.\n\n"
            f"{raw_data_str}"
        )

        # Extra rules to prevent the model from splitting strings into characters
        extra_mc_rules = """
INTERPRETATION RULES FOR RAW DATA:
- RAW DATA already contains the TRUE result of the SQL query.
- DO NOT split strings into characters.
- Treat EACH LINE in RAW DATA as ONE ROW in the final result_set.
- For a query with a single selected column, each row must be:
  { "<column_name>": "<cell_value>" }.
- Column names MUST match exactly the names in the SELECT clause.
"""

        system_text = (
            SQL_IK_PROMPT
            + SQL_MC_PROMPT.format(schema_info=db_schema, raw_data=raw_data_text)
            + extra_mc_rules
            + format_instructions
        )

        full_prompt = [
            SystemMessage(system_text),
            HumanMessage(build_sql_prompt(sql_query)),
        ]

        result = _invoke_gemini_with_parser(llm, parser, full_prompt)
        LOG.info(f"[Gemini MC] Result: {result}")
        results.append(result)

    return results



# -----------------------------
# Runner functions
# -----------------------------

def run_sql_baseline_gemini(datasets: list[str] | None = None):
    """
    Executes the SQL baseline with Gemini on one or more datasets,
    following the same logic as sql_baseline.py (Watsonx).
    """
    config = Config_Loader().get_config()

    if not datasets:
        datasets = get_dataset_selection(config.database.run)

    datasets = [d.upper() for d in datasets]
    LOG.info(f"[Gemini] Running SQL baseline (Gemini) for datasets: {datasets}")

    for ds in datasets:
        LOG.info(f"[Gemini] Processing dataset: {ds}")
        dataset_path = DATA_DIR / ds

        queries = load_queries_from_folder(dataset_path)
        LOG.info(f"[Gemini] Found {len(queries)} queries in {dataset_path}")

        if ds in IK_DATASETS:
           results = execute_IK_baseline_sql_query_gemini(ds.lower(), queries)
        elif ds in MC_DATASETS:
           results = execute_MC_baseline_sql_query_gemini(ds.lower(), queries)

        else:
            LOG.warning(f"[Gemini] Dataset {ds} not in IK_DATASETS or MC_DATASETS; skipping.")
            continue

        # Save the results to JSON, one file per query
        out_dir = Path(SQL_OUTPUT) / "gemini" / ds
        out_dir.mkdir(parents=True, exist_ok=True)

        for i, res in enumerate(results, start=1):
            out_file = out_dir / f"query{i}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(res, f, ensure_ascii=False, indent=2)
            LOG.info(f"[Gemini] Saved result for {ds} query {i} -> {out_file}")


# -----------------------------
# Entry point CLI
# -----------------------------

def _get_datasets_from_config_or_cli():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("datasets", nargs="*")
    args = parser.parse_args()

    if args.datasets:
        return [d.upper() for d in args.datasets]

    config = Config_Loader().get_config()
    return get_dataset_selection(config.database.run)


if __name__ == "__main__":
    datasets = _get_datasets_from_config_or_cli()
    run_sql_baseline_gemini(datasets)
