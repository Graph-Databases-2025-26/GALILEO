import re
from typing import List

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from src.utils.get_db_schema_galois import GaloisSchemaManager
from src.utils.logging_config import LOG
from src.llm.llm_wrappers import LLMBaseWrapper


class ConfidenceEstimator:
    """
    Component that implements the estimation logic of the confidence score.
    In practice it check each WHERE clause of the sql query and decide if it is safe to pushdawn to the LLM
    """

    def __init__(self, llm_wrapper: LLMBaseWrapper, dataset: str , confidence_prompt_template_system: str, confidence_prompt_template_human: str):
        self.dataset = dataset
        self.chain = self.build_galois_chain(llm_wrapper, confidence_prompt_template_system, confidence_prompt_template_human)

    def estimate_confidence_conditions(self, table: str, conditions: List[str]) -> List[str]:
        """
        Analyze a list of conditions and returns those with an high confidence for the LLM
        """

        if not conditions:
            return []

        LOG.info(f"Estimating confidence for {conditions} conditions")

        #schema for the context
        schema_manager = GaloisSchemaManager(self.dataset)
        attrs = schema_manager.get_attributes(table)
        schema_summary = f"Table {table} columns: {', '.join(attrs)}"
        schema_manager.close()

        confident_conditions = []
        for cond in conditions:
            chain_input = {
                "table": table,
                "query": cond,
                "schema_summary": schema_summary
            }
                #Build the prompt, GET_CONFIDENCE_ESTIMATION_PROMPT MUST BE IMPLEMENTED IN PROMPT CLASS
                #prompt_context = get_confidence_estimation_prompt(table, schema_summary)

                #Call the LLM
            try:
                #USE OUR STRUCTURE FOR INVOKING THE LLM

                #response = self.llm.invoke(HumanMessage(content=prompt_context))
                #confidence = response.content.strip().upper()
                confidence_response = self.chain.invoke(chain_input)
                confidence = confidence_response.strip().upper()
                LOG.info(f"Condition: '{cond}' -> Confidence: {confidence}")

                if "HIGH" in confidence:
                    LOG.info(f" - Condition '{cond}': HIGH confidence -> PUSH")
                    confident_conditions.append(cond)
                else:
                    LOG.info(f" - Condition '{cond}': LOW confidence -> DENY")

            except Exception as e:
                LOG.error(f"Error while estimating confidence for condition '{cond}': {e}. Dafaulting to LOW")

        return confident_conditions


    def estimate_confidence_query(self, config,  table: str, sql_query: str, num_select_columns: int):
        """
        This method will be used by the execute_variant method for interacting with LLM asking for the confidence score about the query execution plan
        knowing the database schema of that specific dataset
        """
        if not sql_query or not table:
            LOG.error("Cannot estimate confidence for empty query or table")

        if num_select_columns <= 0:
            LOG.error(
                "Number of SELECT columns must be positive for confidence calculation. Dafaulting to TABLE strategy.")
            return "TABLE"

        confidence_threshold = config.galois_execution.confidence_threshold

        schema_manager = GaloisSchemaManager(self.dataset)
        attrs = schema_manager.get_attributes(table)
        schema_summary = f"Table {table} columns: {', '.join(attrs)}"
        schema_manager.close()

        chain_input = {
            "table": table,
            "query": sql_query,
            "schema_summary": schema_summary
        }

        try:
            confidence_response = self.chain.invoke(chain_input)
            LOG.info(f"CONFIDENCE RESPONSE: {confidence_response}")
            matches = re.findall(r"(\d+(?:\.\d+)?)", confidence_response)
            LOG.info(f"SCORE: {matches}")
            llm_raw_confidence = None
            numerical_score = None
            if matches:
                try:
                    llm_raw_confidence = float(matches[0])
                    LOG.info(f"LLM RAW NUMERICAL SCORE: {llm_raw_confidence}")

                    #calculate the score
                    numerical_score = llm_raw_confidence ** num_select_columns
                    LOG.info(f"PROPAGATED CONFIDENCE SCORE (conf(q)): {numerical_score}")
                except Exception as e:
                    LOG.error(f"Error converting score '{matches[0]}' to int: {e}")
            else:
                LOG.error("No numeric score found in confidence response")
                return "TABLE"

            LOG.info(f"Query: '{sql_query}' -> Confidence: {numerical_score}")

            if numerical_score >= confidence_threshold:
                return "KEY"
            else:
                return "TABLE"

        except Exception as e:
            LOG.error(f"Error while estimating confidence for query '{sql_query}': {e}.")
            return None

    @staticmethod
    def build_galois_chain(llm_model: LLMBaseWrapper, system_prompt: str, humanPrompt: str):
        """
        Constructs a Chain lcel Expression Language (LCEL) chain for the GALOIS LLM estimation task.

        Expected Input (dictionary):
        {
            "table": "table_name",
            "condition": "where_condition",
            "schema_summary": "columns_list"
        }

        Expected Output:
        String ("HIGH" or "LOW")
        """

        # Define the prompt template
        system_prompt = system_prompt
        human_prompt = humanPrompt

        confidence_prompt = ChatPromptTemplate.from_messages([
            ("system", system_prompt),
            ("human", human_prompt)
        ])

        # Obtain the llm wrapper instance
        llm = llm_model.get_llm_instance()

        # Using a simple string parser (clean extra spaces)
        parser = StrOutputParser()

        # Embedd the chain LCEL
        chain = confidence_prompt | llm | parser

        return chain

