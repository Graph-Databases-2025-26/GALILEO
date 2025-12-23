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
    It checks each WHERE clause of the sql query and decides if it is safe to push down to the LLM.
    """

    def __init__(self, llm_wrapper: LLMBaseWrapper, dataset: str , confidence_prompt_template_system: str, confidence_prompt_template_human: str):
        self.dataset = dataset
        self.chain = self.build_galois_chain(llm_wrapper, confidence_prompt_template_system, confidence_prompt_template_human)

    def estimate_confidence_conditions(self, table: str, conditions: List[str]) -> List[str]:
        """
        Analyze a list of conditions and returns those with a high confidence for the LLM.
        """

        if not conditions:
            return []

        LOG.info(f"PLAN | [ESTIMATOR] Analyzing {len(conditions)} conditions for table '{table}'")

        # Schema for the context
        schema_manager = None
        try:
            schema_manager = GaloisSchemaManager(self.dataset)
            attrs = schema_manager.get_attributes(table)
            schema_summary = f"Table {table} columns: {', '.join(attrs)}"
        except Exception as e:
            LOG.error(f"ERR  | [ESTIMATOR] Error getting schema: {e}")
            return []
        finally:
            if schema_manager:
                schema_manager.close()

        confident_conditions = []
        for cond in conditions:
            chain_input = {
                "table": table,
                "query": cond,
                "schema_summary": schema_summary
            }
            
            try:
                # Call the LLM
                confidence_response = self.chain.invoke(chain_input)
                confidence = confidence_response.strip().upper()
                
                is_high = "HIGH" in confidence
                action = "PUSH" if is_high else "DENY"
                
                # Log sintetico
                LOG.info(f"PLAN | [ESTIMATOR] Cond: '{cond}' | Conf: {confidence} -> Action: {action}")

                if is_high:
                    confident_conditions.append(cond)

            except Exception as e:
                LOG.error(f"ERR  | [ESTIMATOR] Failed checking '{cond}': {e}. Default: DENY")

        return confident_conditions
    
    
    def estimate_confidence_query(self, config,  table: str, sql_query: str, num_select_columns: int) -> float:
        """
        Returns the confidence score (0.0 - 1.0) for the query execution plan.
        Returns 0.0 in case of errors (fallback to safe TABLE scan).
        """
        if not sql_query or not table:
            LOG.error("ERR  | [ESTIMATOR] Cannot estimate confidence: empty query or table")
            return 0.0

        if num_select_columns <= 0:
            LOG.error("ERR  | [ESTIMATOR] SELECT cols must be positive. Default: 0.0")
            return 0.0

        schema_manager = None
        try:
            schema_manager = GaloisSchemaManager(self.dataset)
            attrs = schema_manager.get_attributes(table)
            schema_summary = f"Table {table} columns: {', '.join(attrs)}"
        except Exception as e:
            LOG.error(f"ERR  | [ESTIMATOR] Schema error: {e}")
            return 0.0
        finally:
            if schema_manager:
                schema_manager.close()

        chain_input = {
            "table": table,
            "query": sql_query,
            "schema_summary": schema_summary
        }

        try:
            confidence_response = self.chain.invoke(chain_input)
            # LOG.debug(f"DBUG | [ESTIMATOR] Raw Response: {confidence_response}") 
            
            matches = re.findall(r"(\d+(?:\.\d+)?)", confidence_response)
            
            numerical_score = 0.0
            if matches:
                try:
                    llm_raw_confidence = float(matches[0])
                    # calculate the score (propagated confidence)
                    numerical_score = llm_raw_confidence ** num_select_columns
                    
                    LOG.info(f"PLAN | [ESTIMATOR] Query Score: {numerical_score:.4f} (Base: {llm_raw_confidence}, Cols: {num_select_columns})")
                except Exception as e:
                    LOG.error(f"ERR  | [ESTIMATOR] Score conversion error: {e}")
                    return 0.0
            else:
                LOG.error("ERR  | [ESTIMATOR] No numeric score found")
                return 0.0

            return numerical_score

        except Exception as e:
            LOG.error(f"ERR  | [ESTIMATOR] Estimation failed: {e}")
            return 0.0

    @staticmethod
    def build_galois_chain(llm_model: LLMBaseWrapper, system_prompt: str, humanPrompt: str):
        """
        Constructs a Chain lcel Expression Language (LCEL) chain.
        """
        system_prompt = system_prompt
        human_prompt = humanPrompt

        confidence_prompt = ChatPromptTemplate.from_messages([
            ("system", system_prompt),
            ("human", human_prompt)
        ])

        llm = llm_model.get_llm_instance()
        parser = StrOutputParser()
        chain = confidence_prompt | llm | parser

        return chain