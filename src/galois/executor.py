import re
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

import json, time

#JSON Template for testing prupose
JS_TEMPLATE = """
    {
        "title": "MovieSchema",
        "description": "Schema per l'estrazione dei dettagli di un film e del suo regista.",
        "type": "object",
        "properties": {
            "primarytitle": {
            "type": "string",
            "description": "Il titolo principale o ufficiale del film."
            }
        }
    }
"""

TEST_PROMPT="""
List the  'river_name' of 'usa_river' (where the following condition holds: 'length_in_km > 750').
Respond with JSON only.
Use the following JSON schema: 
{'table_name': 'usa_river', 'type': 'object', 'attributes': {'river_name': {'type': 'VARCHAR', 'key': True}}}
"""

class NoNewTuplesFound(Exception):
    """Exception raised when no new unique tuple is added to the memory."""
    pass


class Response(RootModel):
    """
    Pydantic RootModel to enforce the expected JSON structure from the LLM.
    
    It expects the root element of the JSON response to be a list of dictionaries, where each dictionary represents a row/tuple from the target table.
    """
    
    root:List[Dict[str, Union[str, int, float, Any]]]= Field(description="LLM Response as a dictionary {column_name: value}.")    


class GaloisExecutor:
    """
    Core execution engine for the Galois-WO pipeline (Table-Scan and Key-Scan).

    Responsibilities:
      - Parse the SQL query.
      - Retrieve schema information for the target table using the Schema Manager Wrapper.
      - Build the first and iterative prompts (Algorithm 1 in the paper).
      - Interact with the LLM to collect tuples.
      - Deduplicate tuples across iterations using GaloisMemory.
    
    Attributes:
        schema_mgr (GaloisSchemaManagerWrapper): Wrapper to access schema information for the specific baseline and dataset.
        g_memory (GaloisMemory): Custom memory component used to store previously generated unique tuples and history for the LLM.
        resp_parser (PydanticOutputParser): Parser responsible for validating and structuring the LLM's raw JSON output into the 'Response' Pydantic model.
        llm_wrapper (LLMWrapper): Instance to interact with the underlying LLM (e.g., Watsonx).
        dataset (str): The name of the dataset (uppercase).
        max_iter (int): The maximum number of iterative calls to the LLM allowed before stopping.
    """

    class GaloisMemory(BaseMemory, BaseModel):
        """ 
        Custom memory class to store history of prompts and responses and handle deduplication of generated tuples based on key values.
        
        Attributes:
            memory (List[Dict[str, Any]]): Internal list storing the actual unique records (tuples) generated so far.
            key_values (set): Set of unique key value tuples used for efficient deduplication check.
        """
        
        memory: List[Dict[str, Any]] = Field(default_factory=list, description="Internal list of input/output records.")
        key_values: set = Field(default_factory=set, description="Set of unique key value tuples for deduplication.")
        tokens: int = Field(default=0, description="Total number of tokens processed." )
        time: float = Field(default=0.0, description="A timestamp or measure of processing time." )
        
        
        @property
        def memory_variables(self) -> list[str]:
            """
            Returns the keys for the memory variables, required by BaseMemory.
            
            Returns:
                list[str]: A list containing the string "history".
            """
            
            return ["history"] 


        @property
        def get_key_values(self) -> list[tuple]:
            """
            Returns the list of unique key value tuples found so far.
            
            Returns:
                list[tuple]: List of key tuples used for deduplication.
            """
            
            return list(self.key_values)
        
        
        @property
        def get_memory(self) -> List[Dict[str, Any]]:
            """
            Returns the list of unique records stored in memory.
            
            Returns:
                List[Dict[str, Any]]: The list of stored unique records.
            """
            
            return self.memory

        @property
        def get_time(self) -> List[Dict[str, Any]]:
            """
            Returns the response time of total interaction.
            
            Returns:
                float: Total time.
            """
            
            return self.time
        
        @property
        def get_tokens(self) -> List[Dict[str, Any]]:
            """
            Returns the total token of the LLM response.
            
            Returns:
                int: Total tokens
            """
            
            return self.tokens

        def load_memory_variables(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
            """
            Loads the history in JSON format for inclusion in the next LLM prompt.
            
            Args:
                inputs: Dictionary containing input variables (not used here).
                
            Returns:
                Dict[str, Any]: Dictionary containing the history formatted as a JSON string.
            """
            
            return {"history": json.dumps(self.memory, indent= None)}


        def save_context(self, inputs: Dict[str, Any], outputs: Dict[str, Any]) -> None:
            """
            Saves the generated records to memory and deduplicates them using key values.
            If no new unique tuples are added, it raises a NoNewTuplesFound exception.
            
            Args:
                inputs: Dictionary containing input variables (not used here).
                outputs: Dictionary containing LLM output, must include 'response' (List of records) and 'key' (List of key column names).

            Raises:
                NoNewTuplesFound: If no new unique tuple is added to the memory after processing the output.
            """
            
            tmp_set =self.key_values.copy()
            
            for resp in outputs["response"]:
                tmp_kv = self._get_key_values(resp, outputs["key"])

                if tmp_kv not in self.key_values:
                    self.memory.append(resp)
                    self.key_values.add(tmp_kv)
        
            self.time = self.time + outputs["time"]
            self.tokens = self.tokens + outputs["tokens"]
        
            if len(tmp_set) == len(self.key_values): 
                raise NoNewTuplesFound("Interrupted Iteration: no new unique tuples were found.")
            
            LOG.debug(f"History: {self.memory}")
            
        def clear(self) -> None:
            """
            Clears the memory and the key set, resetting the state.
            """
            
            self.memory = []  
             

        def _get_key_values(self, record: Dict[str, Any], keys: List[str]) -> tuple:
            """
            Helper to extract key values from a record for deduplication.
            
            Args:
                record: The dictionary representing a single record/row.
                keys: A list of column names that constitute the key.
            
            Returns:
                tuple: A tuple containing the key values in consistent order.
            """
            
            return tuple(record.get(k) for k in keys)
          
          
            
            
    def __init__(self, config: AppConfig, dataset: str, max_iter: int = 4, baseline_type: str = "GaloisWO") -> None:
        """
        Initializes the GaloisExecutor instance.

        Args:
            config: Application configuration object.
            dataset: The name of the dataset to operate on (e.g., "MOVIES").
            max_iter: The maximum number of iterations allowed for the key_scan or table_scan process. Default is 4.
            baseline_type: The type of Galois baseline used to select the Schema Manager (e.g., "GaloisWO"). Default is "GaloisWO".
        """
        
        self.schema_mgr = GaloisSchemaManagerWrapper(baseline_type, dataset.upper())
        self.g_memory = self.GaloisMemory()
        
        self.resp_parser =  PydanticOutputParser(pydantic_object=Response)
        self.llm_wrapper = get_llm_wrapper(config)
        
        self.dataset = dataset.upper()
        self.max_iter = max_iter



    def _get_context(self, input_d: dict) -> dict:
        """
        Constructs the context dictionary containing necessary information (table, key, keyValue, attributes, conditions, history) for building the prompt.
        
        Args:
            input_d: Dictionary containing initial input keys like "query", "prompt_t" (prompt type), and "history".
                     
        Returns:
            dict: The context dictionary used by _select_prompt.
        """
        
        #ITER PROMPT
        output = {
            "history": input_d["history"],
            "prompt_t": input_d["prompt_t"]
        }

        """Implementation of pagination for enhancing performance: at each iteration for generate new values, the LLM will start from the last value that it returned in the
        previous iteration , in this way it has a starting point for generating new values.
        """
        last_val = "START"

        #recevoery the actual memory
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

            #TABLE_SCAN FIRST PROMPT
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


            #KEY_SCAN FIRST PROMPT
            if input_d["prompt_t"] == "key_f":

                output.update({
                    "key": self.schema_mgr.get_attributes(parsed_q["from_table"], "key"),
                    "jsonSchema": json.dumps(self.schema_mgr.get_json_schema(parsed_q["from_table"], "key"), indent= 4)
                })


            #KEY_SCAN TUPLE PROMPT 
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
        """
        Selects the appropriate human prompt template based on "prompt_t" and formats it using the context provided in input_d.
        
        Args:
            input_d: Dictionary containing the history and context variables needed to format the specific prompt template.
                     
        Returns:
            dict: Dictionary containing the history and the formatted human prompt.
        """
        
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
        """
        Builds the LangChain Expression Language (LCEL) chain for Galois execution.
        The chain consists of: Context generation -> Prompt selection -> LLM invocation.
        
        Args:
            llm_wrapper: The LLMWrapper instance providing the LLM instance.
            
        Returns:
            Runnable: The executable LCEL chain.
        """
        
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
    
    
    def key_scan(self, query: str, columns: Optional[List[str]] = None, conditions_to_push: Optional[List[str]] = None, max_iter: Optional[int] = None) -> Dict[str, Any]:
        """
        Executes the Key-Scan algorithm iteratively using the LLM.
        
        The Key-Scan first collects a set of unique key values and then requests the remaining non-key attributes for all collected keys in a final step.
        
        Args:
            query: The SQL query used to define the target data extraction task.
            conditions_to_push: Optional list of conditions to push down (currently not fully implemented/used).
            max_iter: Optional override for the maximum number of iterations.
            
        Returns:
            List[Dict[str, Any]]: The final list of complete and unique records collected across all iterations.
        """
        
        if max_iter is None:
            max_iter = self.max_iter
        
        input_d = {"query": query, "columns":columns, "prompt_t": "key_f", "conditions": conditions_to_push, "history": ""}
        chain = self._build_galois_chain(self.llm_wrapper)
        
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
                LOG.info(f"LLM Response Parsed: {response.root}")
                
                self.g_memory.save_context(
                    {}, 
                    {
                        "response": response.root, 
                        "key": self.schema_mgr.get_attributes(parse_sql(query)["from_table"], "key"), 
                        "tokens": raw_response.usage_metadata.get("output_tokens", 0),
                        "time" : t_end - t_start   
                    }
                )

                i += 1
                       
            except NoNewTuplesFound:
                LOG.info("No new unique tuples were added in this iteration.")
                break
        
        
        input_d.update({"prompt_t": "key_t", "history": "", "keyValue": self.g_memory.get_key_values})
        
        t_start = time.time()
            
        raw_response = chain.invoke(input_d)
        
        t_end = time.time()
        
        content_fixed = raw_response.content.replace("\\'", "'")
        response = self.resp_parser.parse(content_fixed)
        
        f_response = self._build_full_response(self.schema_mgr.get_attributes(parse_sql(query)["from_table"], "key"), response.root)   
        
        outputs = {
            "response": f_response,
            "time": self.g_memory.get_time + t_end - t_start,
            "tokens": self.g_memory.get_tokens + raw_response.usage_metadata.get("output_tokens", 0)
        }
        LOG.info(f"LLM Response Parsed: {outputs}")
        
        self.g_memory.clear()
        
        return outputs


    def table_scan(self, query: str, columns: Optional[List[str]] = None, conditions_to_push: Optional[List[str]] = None, max_iter: Optional[int] = None) -> Dict[str, Any]:
        """
        Executes the Table-Scan algorithm iteratively using the LLM.
        
        The Table-Scan collects full unique tuples in each iteration until the maximum iteration limit is reached or the LLM fails to provide new unique tuples.
        
        Args:
            query: The SQL query used to define the target data extraction task.
            conditions_to_push: Optional list of conditions to push down (currently not fully implemented/used).
            max_iter: Optional override for the maximum number of iterations.
            
        Returns:
            List[Dict[str, Any]]: The final list of unique records collected across all iterations, stored in the memory.
        """
        
        if max_iter is None:
            max_iter = self.max_iter
        
        input_d = {"query": query, "columns":columns, "prompt_t": "table_f", "conditions": conditions_to_push, "history": ""}
        chain = self._build_galois_chain(self.llm_wrapper)
        
        i = 0
        while i < max_iter:
            
            t_start =time.time()
            
            try:
                if i == 0:
                    raw_response = chain.invoke(input_d)
                    
                else:
                    input_d.update({"prompt_t": "table_i", **self.g_memory.load_memory_variables({})})
                    
                    raw_response = chain.invoke(input_d)
                
                t_end = time.time()

                content_fixed = raw_response.content.replace("\\'", "'")
                content_fixed = repair_json_content(content_fixed)
                content_fixed = re.sub(r"(?<=\d),(?=\d)", "", content_fixed) #remove commas in numbers
                response = self.resp_parser.parse(content_fixed)
                LOG.info(f"LLM Response Parsed: {response.root}")
                
                self.g_memory.save_context(
                    {}, 
                    {
                        "response": response.root, 
                        "key": self.schema_mgr.get_attributes(parse_sql(query)["from_table"], "key"), 
                        "tokens": raw_response.usage_metadata.get("output_tokens", 0),
                        "time" : t_end - t_start   
                    }
                )
                i += 1
                       
            except NoNewTuplesFound:
                LOG.info("No new unique tuples were added in this iteration.")
                break

        output ={
            "response": self.g_memory.get_memory,
            "time": self.g_memory.get_time,
            "tokens": self.g_memory.get_tokens
        }
        LOG.info(f"LLM Response Parsed: {output}")
        
        self.g_memory.clear()
        
        return output

    import re

def repair_json_content(json_str: str) -> str:
    """
    Attempts to fix common JSON errors produced by LLMs.
    """
    original_str = json_str
    # 1. Remove Markdown code blocks (```json ... ```) if present
    json_str = json_str.strip()
    if json_str.startswith("```"):
        # Remove the first line (e.g. ```json)
        json_str = re.sub(r"^```[a-zA-Z]*\n", "", json_str)
        # Remove the last line (```)
        json_str = re.sub(r"\n```$", "", json_str)
        json_str = json_str.strip()

    match = re.search(r'[\[\{]', json_str)
    if match:
        start_idx = match.start()
        if start_idx > 0:
            # If there is garbage before the JSON, cut it away
            json_str = json_str[start_idx:]

            # Also look for the last valid bracket to clean the tail
            last_idx = max(json_str.rfind(']'), json_str.rfind('}'))
            if last_idx != -1:
                json_str = json_str[:last_idx + 1]


    # 2. CRITICAL FIX FOR THE ERROR: "key": ,  --> "key": null,
    # Look for a colon, optional spaces, then immediately a comma or closing bracket
    json_str = re.sub(r':\s*,', ': null,', json_str)
    #json_str = re.sub(r':\s*}', ': null}', json_str)

    # Remove trailing commas before closing brackets/braces
    json_str = re.sub(r',\s*([\]}])', r'\1', json_str)
    #json has at the beginning, an extra '('
    json_str = re.sub(r'\[\s*\(', '[', json_str)
    # json has at the end, an extra ')'
    json_str = re.sub(r'\)\s*\]', ']', json_str)

    if json_str != original_str:
        LOG.warning("Malformed JSON detected and repaired automatically.")

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