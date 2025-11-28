from __future__ import annotations

import ast
import json
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from sqlglot import parse_one, exp
from src.config import AppConfig
from src.llm import get_llm_wrapper
from src.llm.llm_wrappers import LLMBaseWrapper
from src.galois.galois_prompts import (
    build_table_scan_first_prompt,
    build_table_scan_iter_prompt,
)
from src.galois.schema_manager_wo import GaloisWOSchemaManager
from src.utils import sql_query_parser
from src.utils.invoke_with_backoff import invoke_with_backoff
from src.utils.logging_config import LOG
from src.utils import ERR_LLM_PARSING_FAILURE


class GaloisExecutor:
    """
    Core execution engine for the Galois-WO pipeline (Table-Scan).

    Responsibilities:
      - Parse the SQL query.
      - Retrieve schema information for the target table.
      - Build the first and iterative prompts (Algorithm 1 in the paper).
      - Interact with the LLM to collect tuples.
      - Deduplicate tuples across iterations.
    """

    def __init__(self, config: AppConfig, dataset: str, max_iter: int = 15) -> None:
        """
        Parameters
        ----------
        config : AppConfig
            Global application configuration (loaded from config.yaml).
        dataset : str
            Dataset name (e.g. WORLD, GEO, MOVIES, PRESIDENTS, ...).
        max_iter : int, optional
            Maximum number of Table-Scan iterations, used as a default if
            not overridden at call-time.
        """
        self.config = config
        self.dataset = dataset.upper()
        self.max_iter = max_iter

        # LLM wrapper and underlying chat model (Watsonx, Gemini, etc.)
        self.llm_wrapper: LLMBaseWrapper = get_llm_wrapper(config)
        self.llm = self.llm_wrapper.get_llm_instance()

        # Schema manager for Galois-WO, connecting directly to the DuckDB file
        self.schema_mgr = GaloisWOSchemaManager(self.dataset)

    def table_scan(
        self,
        sql_query: str,
        conditions_to_push: Optional[List[str]] = None,
        max_iter: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Execute the Table-Scan operator for a single-table SQL query.
        """
        if max_iter is None:
            max_iter = self.max_iter

        LOG.info(
            f"[GaloisExecutor] Starting Table-Scan for dataset={self.dataset}, "
            f"max_iter={max_iter}"
        )

        #  Parse the SQL query into a structured representation
        parsed = sql_query_parser.parse_sql(sql_query)
        table_name = parsed["from_table"]
        original_where = parsed.get("where_conditions") or []

        # Decide which conditions to actually push down
        if conditions_to_push:
            where_conditions = conditions_to_push
            LOG.info(f"[GaloisExecutor] Using pushed-down conditions from plan: {where_conditions}")
        else:
            where_conditions = original_where
            LOG.info(f"[GaloisExecutor] Using original WHERE conditions: {where_conditions}")

        where_clause = " AND ".join(where_conditions) if where_conditions else None

        #  Resolve the exact table name
        exact_table = self.schema_mgr.get_exact_table_name(table_name) or table_name

        # Retrieve the valid columns
        all_attributes = self.schema_mgr.get_attributes(exact_table)
        if not all_attributes:
            raise ValueError(f"No attributes found for table '{exact_table}'")

        query_attributes = self._get_involved_columns(
            table_name=exact_table,
            sql_query=sql_query,
            conditions=conditions_to_push or []
        )

        # Clever Merging
        # if query_attributes is empy (es. SELECT *), use all_attributes.
        # Otherwise, merge the two lists to be sure that all attributes involved in query are retrieved.
        if not query_attributes:
            attributes = all_attributes
        else:
            # Union avoiding duplicates
            attributes = list(set(all_attributes + query_attributes))

        LOG.info(f"[GaloisExecutor] Fetching ALL attributes to ensure schema consistency: {attributes}")
        if not attributes:
            raise ValueError(f"No attributes identified for table '{exact_table}'")

        # If no PK is defined, the schema manager may return an empty list.
        # Here we only need attributes, so it's fine.
        json_example = self.schema_mgr.get_json_schema_example(exact_table, attributes)

        LOG.debug(f"[GaloisExecutor] Parsed query -> table={exact_table}, where={where_clause}, attrs={attributes}")

        #  Build initial conversation: System + first human prompt
        system_msg = SystemMessage(
            content=(
                "You are a data extraction engine. "
                "Your task is to output ONLY valid JSON objects that represent "
                "rows of a SQL table. Never include explanations or markdown."
            )
        )

        first_prompt = build_table_scan_first_prompt(
            table_name=exact_table,
            attributes=attributes,
            where_clause=where_clause,
            json_example=json_example,
        )
        history: List[BaseMessage] = [system_msg, HumanMessage(content=first_prompt)]

        # Iteratively query the LLM and collect tuples
        all_rows: List[Dict[str, Any]] = []
        seen_rows: Set[Tuple[Tuple[str, Any], ...]] = set()

        for i in range(max_iter):
            LOG.info(f"[GaloisExecutor] Table-Scan iteration {i+1}/{max_iter}")

            # Call the LLM with exponential backoff
            raw_response = invoke_with_backoff(
                self.llm,
                history,
                max_retries=self.config.galois_execution.max_retries,
                base_delay=self.config.galois_execution.backoff_sec,
                )

            # Parse tuples produced in this iteration
            iter_rows = self._parse_table_scan_response(raw_response, exact_table)
            has_new = self._merge_new_rows(iter_rows, all_rows, seen_rows)

            # Append AI message to history (H ← H ∪ {ai})
            if isinstance(raw_response, BaseMessage):
                history.append(raw_response)

            if not has_new:
                LOG.info(
                    "[GaloisExecutor] No new tuples returned by LLM. "
                    "Stopping Table-Scan."
                )
                break

            # Prepare next iteration prompt
            iter_prompt = build_table_scan_iter_prompt()
            history.append(HumanMessage(content=iter_prompt))

        LOG.info(f"[GaloisExecutor] Table-Scan completed. total_rows={len(all_rows)} unique tuples collected.")

        return all_rows


    #method that does an accurate analysis of the columns used in a query and returns it as a list of strings

    def _get_involved_columns(
            self,
            table_name: str,
            sql_query: str,
            conditions: List[str]
    ) -> List[str]:
        """
        Extract ALL columns referenced in the query (SELECT, WHERE, JOIN, ORDER BY).
        NOTE: Does not filter against the DB schema because in some cases the SchemaManager
        might not see all columns, but we know they are needed for the query.
        """
        found_columns = set()

        # Combine query and conditions to give the parser full context
        text_to_analyze = sql_query
        if conditions:
            text_to_analyze += " WHERE " + " AND ".join(conditions)

        try:
            parsed = parse_one(text_to_analyze)
        except Exception as e:
            LOG.warning(f"Sqlglot parsing failed: {e}. Returning empty list.")
            return []

        # If there is a (*), don't guess the columns
        if parsed.find(exp.Star):
            LOG.info("Wildcard (*) detected in query.")
            return []

        # Identify aliases of the target table (e.g. 'world_presidents' -> 'p')
        target_aliases = {table_name.lower()}
        for table in parsed.find_all(exp.Table):
            if table.name.lower() == table_name.lower() and table.alias:
                target_aliases.add(table.alias.lower())

        # Extract the columns
        for col in parsed.find_all(exp.Column):
            col_name = col.name
            table_ref = col.table

            # If there is a prefix (e.g. p.country), check if 'p' refers to the target table
            if table_ref:
                if table_ref.lower() in target_aliases:
                    found_columns.add(col_name)
            # If there is no prefix, assume the column is relevant
            else:
                found_columns.add(col_name)

        # Convert the set to a list
        return list(found_columns)


    def _parse_table_scan_response(
        self,
        raw_response: Any,
        table_name: str,
    ) -> List[Dict[str, Any]]:
        """
        Convert the raw LLM response into a list of dicts representing rows.

        This function is defensive and tries multiple strategies to extract
        and parse JSON from the model output. It supports:

          - Responses as BaseMessage or plain strings.
          - Responses as lists of chunks (e.g. [{"text": "..."}, ...]).
          - Extra whitespace, newlines, or markdown fences (```json ... ```).
          - JSON wrapped in additional prose.
          - Recovery of the *first* complete row object even if the full JSON
            is truncated or invalid (common with long outputs).
        """
        #  Extract content from the LLM response
        if isinstance(raw_response, BaseMessage):
            content = raw_response.content
        else:
            content = str(raw_response)

        # Some providers may return a list of chunks instead of a single string
        if isinstance(content, list):
            chunks: List[str] = []
            for c in content:
                if isinstance(c, dict) and "text" in c:
                    chunks.append(str(c["text"]))
                else:
                    chunks.append(str(c))
            text = " ".join(chunks)
        else:
            text = str(content)

        text = text.strip()

        #  Remove possible markdown fences ```...``` and labels like ```json
        if text.startswith("```"):
            # Strip outer backticks and remove the first line if it is a 'json' label
            text = text.strip("`")
            if "\n" in text:
                first_line, rest = text.split("\n", 1)
                if first_line.strip().lower().startswith("json"):
                    text = rest
                else:
                    text = rest

        def try_load_json(candidate: str) -> Optional[Any]:
            try:
                return json.loads(candidate)
            except Exception:
                pass
            # FALLBACK: Try to parse as literal Python (handle single '': 'key': 'val')
            try:
                candidate_py = candidate.replace("null", "None").replace("true", "True").replace("false", "False")
                return ast.literal_eval(candidate_py)
            except Exception:
                return None

        #  First attempt: parse the whole content as JSON
        obj = try_load_json(text)

        #  If that fails, search for any block that looks like JSON:
        #    a {...} object or a [...] array
        if obj is None:
            matches = re.findall(r"(\{[\s\S]*\}|\[[\s\S]*\])", text)
            for candidate in matches:
                candidate = candidate.strip()
                parsed = try_load_json(candidate)
                if parsed is not None:
                    obj = parsed
                    break

        #  If still None, try to recover the FIRST row object from the array
        #    associated with the given table, e.g.:
        #
        #       {"movie": [ { ... }, { ... (truncated) } ... ]}
        #
        if obj is None:
            key = f'"{table_name}"'
            start_key = text.find(key)
            if start_key != -1:
                # Find the '[' that starts the array after "<table_name>"
                idx_bracket = text.find("[", start_key)
                if idx_bracket != -1:
                    # Find the first '{' that starts the first row object
                    idx_obj_start = text.find("{", idx_bracket)
                    if idx_obj_start != -1:
                        depth = 0
                        in_string = False
                        escape = False
                        end_index = None

                        for i in range(idx_obj_start, len(text)):
                            ch = text[i]

                            if escape:
                                escape = False
                                continue

                            if ch == "\\":
                                escape = True
                                continue

                            if ch == '"':
                                in_string = not in_string
                                continue

                            if not in_string:
                                if ch == "{":
                                    depth += 1
                                elif ch == "}":
                                    depth -= 1
                                    if depth == 0:
                                        end_index = i
                                        break

                        if end_index is not None:
                            candidate = text[idx_obj_start : end_index + 1]
                            parsed = try_load_json(candidate)
                            if parsed is not None:
                                LOG.warning(
                                    "[GaloisExecutor] Recovered first row object "
                                    "from partially invalid/truncated output."
                                )
                                # Return a single-row list here
                                return [parsed]

        #  If still None, log and return no rows
        if obj is None:
            LOG.error(
                ERR_LLM_PARSING_FAILURE.format(
                    f"Table-Scan output for table {table_name}: {text[:400]}"
                )
            )
            return []

        rows: List[Dict[str, Any]] = []

        #  Extract rows depending on the structure:
        #  { "<table_name>": [ {row}, ... ] }
        #  { "result_set": [ {row}, ... ] }
        #  [ {row}, ... ]
        #  { ... } (single row)
        if isinstance(obj, dict):
            if table_name in obj and isinstance(obj[table_name], list):
                for item in obj[table_name]:
                    if isinstance(item, dict):
                        rows.append(item)
            elif "result_set" in obj and isinstance(obj["result_set"], list):
                for item in obj["result_set"]:
                    if isinstance(item, dict):
                        rows.append(item)
            else:
                rows.append(obj)
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, dict):
                    rows.append(item)

        LOG.debug(f"[GaloisExecutor] Parsed {len(rows)} rows from LLM response for table={table_name}")
        return rows

    def _merge_new_rows(
        self,
        new_rows: List[Dict[str, Any]],
        all_rows: List[Dict[str, Any]],
        seen_rows: Set[Tuple[Tuple[str, Any], ...]],
    ) -> bool:
        """
        Merge `new_rows` into `all_rows`, updating `seen_rows` to avoid duplicates.

        Parameters
        ----------
        new_rows : list of dict
            Tuples returned in the current iteration.
        all_rows : list of dict
            Global list of all tuples collected so far.
        seen_rows : set
            Set of hashable keys representing already seen tuples.

        Returns
        -------
        bool
            True if at least one new row was added, False otherwise.
        """
        added_any = False

        for row in new_rows:
            key = self._normalise_row(row)
            if key in seen_rows:
                continue
            seen_rows.add(key)
            all_rows.append(row)
            added_any = True

        return added_any

    @staticmethod
    def _normalise_row(row: Dict[str, Any]) -> Tuple[Tuple[str, Any], ...]:
        """
        Convert a row-dict into a hashable, order-independent key.

        This is used to store rows in a set (seen_rows) and detect
        duplicates regardless of column ordering.
        Excludes columns that look like IDs (e.g., 'id', 'uid') from the
        uniqueness check. This prevents the LLM from creating duplicates
        that only differ by a hallucinated numeric ID.
        """

        ignored_keys = {'id', 'ID', 'Id', '_id', 'uid', 'row_id', 'unique_id'}
        filtered_items = [
            (k, v) for k, v in row.items()
            if k.lower() not in ignored_keys and not k.endswith("_id")
        ]
        return tuple(sorted(filtered_items))
