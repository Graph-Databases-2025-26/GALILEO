from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from langchain_core.messages import HumanMessage, SystemMessage, BaseMessage

from src.config import AppConfig
from src.llm.llm_factory import get_llm_wrapper
from src.llm.llm_wrappers import LLMBaseWrapper
from src.llm.galois_prompts import (
    build_table_scan_first_prompt,
    build_table_scan_iter_prompt,
)
from src.galois_wo.schema_manager_wo import GaloisWOSchemaManager
from src.utils import LOG, ERR_LLM_PARSING_FAILURE
from src.utils.sql_query_parser import parse_sql
from src.utils.invoke_with_backoff import invoke_with_backoff


class GaloisExecutor:
    """
    Core execution engine for the Galois-WO pipeline (Table-Scan).

    - First Prompt -> generates the initial batch of tuples
    - Iterative Prompt -> requests subsequent batches
    - Stops when no new tuples are returned or max_iter is reached.
    """

    def __init__(self, config: AppConfig, dataset: str, max_iter: int = 4) -> None:
        """
        Parameters
        ----------
        config : AppConfig
            AppConfig loaded from Config_Loader().get_config().
        dataset : str
            Name of the dataset (WORLD, GEO, MOVIES, PRESIDENTS, ...).
        max_iter : int, optional
            Maximum number of Table-Scan iterations if not specified per-call.
        """
        self.config = config
        self.dataset = dataset.upper()
        self.max_iter = max_iter

        # LLM wrapper and underlying model (Gemini / Watsonx / ...)
        self.llm_wrapper: LLMBaseWrapper = get_llm_wrapper(config)
        self.llm = self.llm_wrapper.get_llm_instance()

        # Schema manager dedicated to Galois-WO (does NOT use get_db_schema)
        self.schema_mgr = GaloisWOSchemaManager(self.dataset)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def table_scan(
        self,
        sql_query: str,
        max_iter: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Executes the Table-Scan for a single-table SQL query.

        Follows Algorithm 1 from the paper:

            1. genFirstPrompt → first LLM call
            2. loop:
              - genIterativePrompt
              - accumulate new tuples
              - stop if no new tuples or max_iter reached
        """
        if max_iter is None:
            max_iter = self.max_iter

        LOG.info(
            f"[GaloisExecutor] Starting Table-Scan for dataset={self.dataset}, "
            f"max_iter={max_iter}"
        )

        # 1) Parse SQL query into a structured representation
        parsed = parse_sql(sql_query)
        table_name = parsed["from_table"]
        where_conditions = parsed.get("where_conditions") or []
        where_clause = " AND ".join(where_conditions) if where_conditions else None

        # 2) Resolve exact table name and attributes from the schema
        exact_table = self.schema_mgr.get_exact_table_name(table_name) or table_name
        attributes = self.schema_mgr.get_attributes(exact_table)
        if not attributes:
            raise ValueError(f"No attributes found for table '{exact_table}'")

        # If no PK is defined, the schema manager may return an empty list.
        # Here we only need attributes, so it's fine.
        json_example = self.schema_mgr.get_json_schema_example(exact_table, attributes)

        LOG.debug(
            f"[GaloisExecutor] Parsed query -> table={exact_table}, "
            f"where={where_clause}, attrs={attributes}"
        )

        # 3) Build initial conversation (System + first prompt)
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

        # 4) Iterative interaction with the LLM
        all_rows: List[Dict[str, Any]] = []
        seen_rows: Set[Tuple[Tuple[str, Any], ...]] = set()

        for i in range(max_iter):
            LOG.info(f"[GaloisExecutor] Table-Scan iteration {i + 1}/{max_iter}")

            # LLM call with backoff
            raw_response = invoke_with_backoff(
                self.llm,
                history,
                max_retries=self.config.execution.max_retries,
                base_delay=self.config.execution.backoff_sec,
            )

            # Parse the tuples from the current iteration
            iter_rows = self._parse_table_scan_response(raw_response, exact_table)
            has_new = self._merge_new_rows(iter_rows, all_rows, seen_rows)

            # Add the response to the history (H ← H ∪ {ai})
            if isinstance(raw_response, BaseMessage):
                history.append(raw_response)

            if not has_new:
                LOG.info(
                    "[GaloisExecutor] No new tuples returned by LLM. "
                    "Stopping Table-Scan."
                )
                break

            # Iterative prompt for the next iteration
            iter_prompt = build_table_scan_iter_prompt()
            history.append(HumanMessage(content=iter_prompt))

        LOG.info(
            f"[GaloisExecutor] Table-Scan completed. "
            f"total_rows={len(all_rows)} unique tuples collected."
        )

        return all_rows

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _parse_table_scan_response(
        self,
        raw_response: Any,
        table_name: str,
    ) -> List[Dict[str, Any]]:
        """
        Convert the raw LLM response into a list of dicts representing rows.

        This function is defensive and tries multiple strategies to extract
        and parse JSON from the model output. It supports:

          - Responses as BaseMessage or plain strings
          - Responses as lists of chunks (e.g. [{ "text": "..." }, ...])
          - Extra whitespace, newlines, or markdown fences (```json ... ```)
          - JSON wrapped in surrounding text
          - Recovery of the first complete row object even if the full JSON
            is truncated or invalid.
        """
        # 1) Extract content from the LLM response
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

        # 2) Remove possible markdown fences ```...``` and labels like ```json
        if text.startswith("```"):
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
                return None

        # 3) First attempt: parse the whole content as JSON
        obj = try_load_json(text)

        # 4) If that fails, search for any block that looks like JSON:
        #    a {...} object or a [...] array
        if obj is None:
            matches = re.findall(r"(\{[\s\S]*\}|\[[\s\S]*\])", text)
            for candidate in matches:
                candidate = candidate.strip()
                parsed = try_load_json(candidate)
                if parsed is not None:
                    obj = parsed
                    break

        # 5) If still None, try to recover the FIRST row object from the array
        #    associated with the given table, e.g. {"movie": [ { ... }, { ... (truncated) } ... ] }
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

        # 6) If still None, log and return no rows
        if obj is None:
            LOG.error(
                ERR_LLM_PARSING_FAILURE.format(
                    f"Table-Scan output for table {table_name}: {text[:400]}"
                )
            )
            return []

        rows: List[Dict[str, Any]] = []

        # 7) Extract rows depending on the structure:
        #   { "<table_name>": [ {row}, ... ] }
        #   { "result_set": [ {row}, ... ] }
        #   [ {row}, ... ]
        #   { ... } (single row)
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

        LOG.debug(
            f"[GaloisExecutor] Parsed {len(rows)} rows from LLM response "
            f"for table={table_name}"
        )
        return rows

    def _merge_new_rows(
        self,
        new_rows: List[Dict[str, Any]],
        all_rows: List[Dict[str, Any]],
        seen_rows: Set[Tuple[Tuple[str, Any], ...]],
    ) -> bool:
        """
        Merge `new_rows` into `all_rows`, updating `seen_rows` to avoid duplicates.

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

        This is used to store rows in a set (seen_rows) and detect duplicates.
        """
        return tuple(sorted(row.items()))
