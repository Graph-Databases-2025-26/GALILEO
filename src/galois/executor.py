from typing import Any, Dict, List, Optional, Union
from langchain_core.runnables import RunnableLambda
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from langchain_classic.base_memory import BaseMemory
from pydantic import BaseModel, RootModel, Field 

from .galois_prompts import KEY_SCAN_FIRST_PROMPT, KEY_SCAN_ITER_PROMPT, KEY_SCAN_TUPLE_PROMPT, TABLE_SCAN_FIRST_PROMPT, \
    TABLE_SCAN_ITER_PROMPT, build_condition, TABLE_KEY_SCAN_ITER_PROMPT_PAGINATION
from .schema_factory import GaloisSchemaManagerWrapper

from src.config import AppConfig, Config_Loader
from src.llm import get_llm_wrapper

from src.utils import parse_sql, log_init
from src.utils import LOG, ERR_LLM_PARSING_FAILURE

import json, time, re

class NoNewTuplesFound(Exception):
    """Exception raised when no new unique tuple is added to the memory."""
    pass


class Response(RootModel):
    """
    Pydantic RootModel to enforce the expected JSON structure from the LLM.
    """
    root:List[Dict[str, Union[str, int, float, Any]]]= Field(description="LLM Response as a dictionary {column_name: value}.")    


class GaloisExecutor:
    """
    Core execution engine for the Galois-WO pipeline (Table-Scan and Key-Scan).
    """

    class GaloisMemory(BaseMemory, BaseModel):
        """ 
        Custom memory class to store history of prompts and responses and handle deduplication.
        """
        memory: List[Dict[str, Any]] = Field(default_factory=list, description="Internal list of input/output records.")
        logprobs: List[float] = Field(default_factory=list, description="List of mean logprobs for each stored tuple." )
        key_values: set = Field(default_factory=set, description="Set of unique key value tuples for deduplication.")
        tokens: int = Field(default=0, description="Total number of tokens processed." )
        time: float = Field(default=0.0, description="A timestamp or measure of processing time." )
        
        @property
        def memory_variables(self) -> list[str]:
            return ["history"] 

        @property
        def get_logprobs(self) -> List[float]:
            return list(self.logprobs)

        @property
        def get_key_values(self) -> list[tuple]:
            return list(self.key_values)
        
        @property
        def get_memory(self) -> List[Dict[str, Any]]:
            return list(self.memory)

        @property
        def get_time(self) -> List[Dict[str, Any]]:
            return self.time
        
        @property
        def get_tokens(self) -> List[Dict[str, Any]]:
            return self.tokens

        def load_memory_variables(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
            return {"history": json.dumps(self.memory, indent= None)}

        def save_context(self, inputs: Dict[str, Any], outputs: Dict[str, Any]) -> None:
            tmp_set = self.key_values.copy()
            
            if(outputs.get("logprobs")): 
                for (t, t_logp) in zip(outputs["response"], outputs["logprobs"]):
                    tmp_kv = self._get_key_values(t, outputs["key"])
                    if tmp_kv not in self.key_values:
                        self.memory.append(t)
                        self.key_values.add(tmp_kv)
                        self.logprobs.append(t_logp)
            else:
                for resp in outputs["response"]:
                    tmp_kv = self._get_key_values(resp, outputs["key"])
                    if tmp_kv not in self.key_values:
                        self.memory.append(resp)
                        self.key_values.add(tmp_kv)

            self.time += outputs["time"]
            self.tokens += outputs["tokens"]

            if len(tmp_set) == len(self.key_values): 
                raise NoNewTuplesFound("Interrupted Iteration: no new unique tuples were found.")
          
        def clear(self) -> None:
            self.memory.clear()
            self.key_values.clear()
            self.logprobs.clear()
            self.tokens = 0
            self.time = 0
             
        def _get_key_values(self, record: Dict[str, Any], keys: List[str]) -> tuple:
            return tuple(record.get(k) for k in keys)
            
    def __init__(self, config: AppConfig, dataset: str, max_iter: int = 4, baseline_type: str = "GaloisWO") -> None:
        self.schema_mgr = GaloisSchemaManagerWrapper(baseline_type, dataset.upper())
        self.g_memory = self.GaloisMemory()
        self.resp_parser =  PydanticOutputParser(pydantic_object=Response)
        self.llm_wrapper = get_llm_wrapper(config)
        self.dataset = dataset.upper()
        self.max_iter = max_iter

    def _get_context(self, input_d: dict) -> dict:
        output = {
            "history": input_d["history"],
            "prompt_t": input_d["prompt_t"]
        }
        last_val = "START"
        current_memory = self.g_memory.get_memory

        if current_memory and len(current_memory) >0:
            last_record = current_memory[-1]
            if last_record:
                first_key = list(last_record.keys())[0]
                last_val = str(last_record[first_key])

        output["last_val"] = last_val

        if input_d["prompt_t"] not in  ["key_i", "table_i"]:
            parsed_q = parse_sql(input_d["query"])
            output["table"] = parsed_q["from_table"]
            target_cols = input_d.get("columns") if input_d.get("columns") else "all"
            conditions = input_d.get("conditions", "")
            if conditions:
                conditions = build_condition(conditions)
            else:
                conditions = ""
            output["conditions"] =  conditions

            if input_d["prompt_t"] == "table_f":
                if isinstance(target_cols, list):
                    output.update({
                        "attributes": target_cols,
                        "jsonSchema": json.dumps(self.schema_mgr.get_json_schema_from_set(parsed_q["from_table"], target_cols), indent= 4)
                    })
                else:
                    output.update({
                        "attributes": self.schema_mgr.get_attributes(parsed_q["from_table"], target_cols),
                        "jsonSchema": json.dumps(self.schema_mgr.get_json_schema(parsed_q["from_table"], target_cols), indent= 4)
                    })

            if input_d["prompt_t"] == "key_f":
                output.update({
                    "key": self.schema_mgr.get_attributes(parsed_q["from_table"], "key"),
                    "jsonSchema": json.dumps(self.schema_mgr.get_json_schema(parsed_q["from_table"], "key"), indent= 4)
                })

            if input_d["prompt_t"] == "key_t":
                cols_for_tuple = target_cols if target_cols != "all" else "non_key"
                if isinstance(cols_for_tuple, list):
                    output.update({
                        "keyValue" : input_d["keyValue"],
                        "attributes": cols_for_tuple,
                        "jsonSchema": json.dumps(self.schema_mgr.get_json_schema_from_set(parsed_q["from_table"], cols_for_tuple), indent= 4)
                    })
                else:
                    output.update({
                        "keyValue" : input_d["keyValue"],
                        "attributes": self.schema_mgr.get_attributes(parsed_q["from_table"], cols_for_tuple),
                        "jsonSchema": json.dumps(self.schema_mgr.get_json_schema(parsed_q["from_table"], cols_for_tuple), indent= 4)
                    })

        return output
    
    def _select_prompt(self, input_d: dict) -> dict:
        Human_prompt ={
            "key_f": KEY_SCAN_FIRST_PROMPT,
            "key_i": TABLE_KEY_SCAN_ITER_PROMPT_PAGINATION,
            "key_t": KEY_SCAN_TUPLE_PROMPT,
            "table_f": TABLE_SCAN_FIRST_PROMPT,
            "table_i": TABLE_KEY_SCAN_ITER_PROMPT_PAGINATION
        }
        return {
            "history": input_d["history"],
            "human_prompt": Human_prompt[input_d["prompt_t"]].format(**input_d)
        }

    def _build_galois_chain(self, llm_wrapper):
        parser = PydanticOutputParser(pydantic_object=Response)
        format_instructions = parser.get_format_instructions()
        
        g_context = RunnableLambda(self._get_context)
        s_prompt = RunnableLambda(self._select_prompt)
                        
        Syst_Prompt = """
            You are a data extraction engine. 
            Your task is to output ONLY valid JSON objects that represent rows of a SQL table. Never include explanations or markdown.
            If the query results in only one object (row/record), the list must contain that single object: [{{...}}]
            These are the key-value pairs you have generated so far:
            {history}
        """
        
        FULL_PROMPT = ChatPromptTemplate.from_messages([
            ("system", Syst_Prompt),
            ("human", "{human_prompt}")
        ]).partial(format_instructions=format_instructions)

        return g_context | s_prompt | FULL_PROMPT  | llm_wrapper.get_llm_instance()


    def _build_full_response(self, keys: List[str], response: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        key_v: List[tuple] = self.g_memory.get_key_values
        f_response: List[Dict[str, Any]] = []
        for kv, r_attributes in zip(key_v, response):
            key_pair: Dict[str, Any] = dict(zip(keys, kv))
            f_tuples: Dict[str, Any] = {**key_pair, **r_attributes}
            f_response.append(f_tuples)     
        return f_response
    
    def _extract_logprobs_per_tuple(self, raw_response) -> List[float]:
        logprobs_data = self.llm_wrapper.get_logprobs_content(raw_response)
        if not logprobs_data:
            return []

        tuple_averages = []
        current_tuple_probs = []
        in_tuple = False
        is_value_zone = False  
        structural_chars = {'{', '}', '[', ']', ':', ',', '"', "'"}

        for item in logprobs_data:
            token = item.get('token', '')
            clean_token = token.strip() 
            prob = item.get('logprob', 0)

            if '{' in token:
                in_tuple = True
                is_value_zone = False
                current_tuple_probs = []
                continue
            
            if '}' in token:
                if in_tuple and current_tuple_probs:
                    avg = sum(current_tuple_probs) / len(current_tuple_probs)
                    tuple_averages.append(avg)
                in_tuple = False
                is_value_zone = False
                continue

            if not in_tuple:
                continue

            if ':' in token:
                is_value_zone = True
                continue
            
            if ',' in token:
                is_value_zone = False
                continue

            if is_value_zone:
                if clean_token not in structural_chars and clean_token != "":
                    current_tuple_probs.append(prob)

        return tuple_averages
        
    @staticmethod
    def _log_batch_table(iteration: int, rows: list, tokens: int):
        """
        Logs a batch of results using a single line summary.
        """
        count = len(rows)
        # Log minimalista su una riga per l'iterazione
        if count > 0:
            first_key = list(rows[0].keys())[0]
            example_val = str(rows[0][first_key])
            LOG.info(f"FTCH | [ITER {iteration + 1}] Hits: {count} (Tok: {tokens}) -> Ex: {example_val}...")
        else:
            LOG.info(f"FTCH | [ITER {iteration + 1}] Converged. Tokens: {tokens}")

    @staticmethod
    def _log_final_output(response: List[Dict[str, Any]], logprobs: List[float], time_val: float, tokens: int):
        """
        Logs the final output in a clean box-drawing tabular format, indented.
        """
        count = len(response)
        
        LOG.info(f"RSLT | [FINAL] Scan Completed: {count} unique tuples | Time: {time_val:.2f}s | Tokens: {tokens}")

        # Prefisso di allineamento
        p = "     | "

        if not response:
            LOG.info(f"{p}(No data collected)")
            return

        # 1. Determine columns and widths
        columns = list(response[0].keys())
        col_widths = {col: len(col) for col in columns}
        
        # Max width cap per colonna per evitare wrap brutali
        MAX_COL_WIDTH = 30 
        
        for row in response:
            for col in columns:
                val_len = len(str(row.get(col, "")))
                col_widths[col] = max(col_widths[col], val_len)
        
        # Apply cap and padding
        for col in col_widths:
            col_widths[col] = min(col_widths[col], MAX_COL_WIDTH) + 2
        
        lp_width = 15 

        # 2. Draw Table
        # Top Border
        top_border = f"{p}┌" + "┬".join("─" * w for w in col_widths.values()) + f"┬{'─'*lp_width}┐"
        LOG.info(top_border)

        # Header
        header = f"{p}│" + "│".join(f" {col:<{col_widths[col]-1}}" for col in columns) + f"│ {'LOGPROB':<{lp_width-1}}│"
        LOG.info(header)

        # Separator
        separator = f"{p}├" + "┼".join("─" * w for w in col_widths.values()) + f"┼{'─'*lp_width}┤"
        LOG.info(separator)

        # Rows
        for row, lp in zip(response, logprobs):
            row_str = f"{p}│"
            for col in columns:
                raw_val = str(row.get(col, ''))
                if len(raw_val) > MAX_COL_WIDTH - 2:
                    val_fmt = raw_val[:MAX_COL_WIDTH - 5] + "..."
                else:
                    val_fmt = raw_val
                
                row_str += f" {val_fmt:<{col_widths[col]-1}}│"
            
            # Logprob formatting
            row_str += f" {lp:<{lp_width-1}.2e}│"
            LOG.info(row_str)

        # Bottom Border
        bot_border = f"{p}└" + "┴".join("─" * w for w in col_widths.values()) + f"┴{'─'*lp_width}┘"
        LOG.info(bot_border)
    
    def key_scan(self, query: str, columns: Optional[List[str]] = None, conditions_to_push: Optional[List[str]] = None, max_iter: Optional[int] = None) -> Dict[str, Any]:
        if max_iter is None:
            max_iter = self.max_iter
        
        input_d = {"query": query, "columns":columns, "prompt_t": "key_f", "conditions": conditions_to_push, "history": ""}
        chain = self._build_galois_chain(self.llm_wrapper)
        
        f_response = {}
        tuple_logprobs = []
        raw_response = None

        i = 0
        while i < max_iter:
            t_start = time.time()
            try:
                if i == 0:
                    raw_response = chain.invoke(input_d)
                else:
                    input_d.update({"prompt_t": "key_i", **self.g_memory.load_memory_variables({})})
                    raw_response = chain.invoke(input_d)

                t_end = time.time()
                content_fixed = raw_response.content.replace("\\'", "'")
                content_fixed = repair_json_content(content_fixed)
                response = self.resp_parser.parse(content_fixed)                

                self._log_batch_table(i, response.root, raw_response.usage_metadata.get("total_tokens", 0))
                
                self.g_memory.save_context(
                    {}, 
                    {
                        "response": response.root,
                        "key": self.schema_mgr.get_attributes(parse_sql(query)["from_table"], "key"), 
                        "tokens": raw_response.usage_metadata.get("total_tokens", 0),
                        "time" : t_end - t_start   
                    }
                )
                i += 1
            except NoNewTuplesFound:
                LOG.info("FTCH | [INFO] No new unique tuples found.")
                break
        
        input_d.update({"prompt_t": "key_t", "history": "", "keyValue": self.g_memory.get_key_values})
        t_start = time.time()
        try: 
            raw_response = chain.invoke(input_d)
            t_end = time.time()
            content_fixed = raw_response.content.replace("\\'", "'")
            response = self.resp_parser.parse(content_fixed)
            f_response = self._build_full_response(self.schema_mgr.get_attributes(parse_sql(query)["from_table"], "key"), response.root)   
            tuple_logprobs = self._extract_logprobs_per_tuple(raw_response)
            tokens_used = raw_response.usage_metadata.get("total_tokens", 0)
        except Exception as e:
            LOG.error(f"ERR  | [EXEC] Key-Scan Tuple retrieval failed: {e}")
            f_response = []
            tuple_logprobs = []
            tokens_used = 0
            t_end = t_start 

        outputs = {
            "response": f_response,
            "logprobs": tuple_logprobs,
            "time": self.g_memory.get_time + (t_end - t_start),
            "tokens": self.g_memory.get_tokens + tokens_used
        }
        
        self._log_final_output(outputs["response"], outputs["logprobs"], outputs["time"], outputs["tokens"])
        self.g_memory.clear()
        return outputs

    def table_scan(self, query: str, columns: Optional[List[str]] = None, conditions_to_push: Optional[List[str]] = None, max_iter: Optional[int] = None) -> Dict[str, Any]:
        if max_iter is None:
            max_iter = self.max_iter
        
        input_d = {"query": query, "columns":columns, "prompt_t": "table_f", "conditions": conditions_to_push, "history": ""}
        chain = self._build_galois_chain(self.llm_wrapper)
        
        input_tokens_by_iter: List[int] = []
        iters_done = 0
        i = 0
        while i < max_iter:
            t_start =time.time()
            try:
                try:
                    tmp_input = dict(input_d)
                    if i > 0:
                        tmp_input.update({"prompt_t": "table_i", **self.g_memory.load_memory_variables({})})
                    ctx = self._get_context(tmp_input)
                    human_prompt = self._select_prompt(ctx)["human_prompt"]
                    llm = self.llm_wrapper.get_llm_instance()
                    if hasattr(llm, "get_num_tokens"):
                        prompt_input_tokens = int(llm.get_num_tokens(human_prompt))
                    else:
                        prompt_input_tokens = 0
                except Exception:
                    prompt_input_tokens = 0

                if i == 0:
                    raw_response = chain.invoke(input_d)
                else:
                    input_d.update({"prompt_t": "table_i", **self.g_memory.load_memory_variables({})})
                    raw_response = chain.invoke(input_d)
                
                t_end = time.time()
                input_tokens_by_iter.append(prompt_input_tokens)
                iters_done = i + 1

                content_fixed = raw_response.content.replace("\\'", "'")
                content_fixed = repair_json_content(content_fixed)
                content_fixed = re.sub(r"(?<=\d),(?=\d)", "", content_fixed) 
                response = self.resp_parser.parse(content_fixed)

                tuple_logprobs = self._extract_logprobs_per_tuple(raw_response)
                
                self._log_batch_table(i, response.root, raw_response.usage_metadata.get('total_tokens', 0))

                self.g_memory.save_context(
                    {}, 
                    {
                        "response": response.root, 
                        "key": self.schema_mgr.get_attributes(parse_sql(query)["from_table"], "key"), 
                        "tokens": raw_response.usage_metadata.get("total_tokens", 0),
                        "time" : t_end - t_start , 
                        "logprobs": tuple_logprobs
                    }
                )
                i += 1
            except NoNewTuplesFound:
                LOG.info("FTCH | [INFO] No new unique tuples found.")
                break

        outputs ={
            "response": self.g_memory.get_memory,
            "logprobs": self.g_memory.get_logprobs,
            "time": self.g_memory.get_time,
            "tokens": self.g_memory.get_tokens,
            "n_iters": iters_done,
            "input_tokens_by_iter": input_tokens_by_iter,
            "input_tokens_total_all_iters": sum(input_tokens_by_iter),
        }
        
        self._log_final_output(outputs["response"], outputs["logprobs"], outputs["time"], outputs["tokens"])
        self.g_memory.clear()
        return outputs
    
def repair_json_content(json_str: str) -> str:
    original_str = json_str
    json_str = json_str.strip()
    if json_str.startswith("```"):
        json_str = re.sub(r"^```[a-zA-Z]*\n", "", json_str)
        json_str = re.sub(r"\n```$", "", json_str)
        json_str = json_str.strip()

    match = re.search(r'[\[\{]', json_str)
    if match:
        start_idx = match.start()
        if start_idx > 0:
            json_str = json_str[start_idx:]
            last_idx = max(json_str.rfind(']'), json_str.rfind('}'))
            if last_idx != -1:
                json_str = json_str[:last_idx + 1]

    json_str = re.sub(r':\s*,', ': null,', json_str)
    json_str = re.sub(r',\s*([\]}])', r'\1', json_str)
    json_str = re.sub(r'\[\s*\(', '[', json_str)
    json_str = re.sub(r'\)\s*\]', ']', json_str)

    if json_str != original_str:
        LOG.warning("WARN | [JSON] Malformed JSON detected and repaired.")

    return json_str

if __name__ == "__main__":
    config = Config_Loader().get_config()
    log_init()
    
    executor = GaloisExecutor(config, "GEO")

    query = "SELECT DISTINCT usa_state_traversed FROM usa_river"
    
    results = executor.key_scan(query)
    LOG.info("Key-Scan Executed")
    
    results = executor.table_scan(query)
    LOG.info("Table-Scan Executed")