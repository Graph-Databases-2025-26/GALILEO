import json
from typing import Literal, Any, List, Dict
from src import Config_Loader
from src.db.run_explain_plans import DATA_ROOT
from src.llm import get_llm_wrapper
from src.llm.galois_prompts import system_prompt_galois_confidence, human_prompt_galois_confidence
from src.utils import sql_query_parser, load_queries_from_folder
from src.utils.galois_estimator import ConfidenceEstimator
from src.utils.get_db_schema_galois import GaloisSchemaManager
from src.utils.logging_config import LOG
from src.galois_wo.galois_executor import GaloisExecutor
from src.utils.constants import *


class Galois:
    """
    Main orchestration class for the GALOIS system.

    1. Initialization: Sets up LLM, Schema Manager, and parses the SQL query.
    2. Logical Optimization: Decides WHICH conditions to push down to the LLM (e.g., Push-All, No-Push).
       (Fig. 2)
    3. Physical Optimization: Decides HOW to query the LLM (Table-Scan vs Key-Scan).
       (Section 4 - Physical Optimizations)
    4. Execution: Delegates the actual work to GaloisExecutor.
    """
    ScanStrategy = Literal["table", "key", "auto"]
    def __init__(self,
                 config: Any,
                 dataset: str,
                 sql_query: str,
                 physical_strategy: ScanStrategy = "auto"):
        """
        Args:
            config: Configuration object containing LLM API keys and settings.
            dataset: Name of the dataset (used to load the DuckDB schema).
            sql_query: The raw SQL query string to execute.
            physical_strategy: 'table' (Alg. 1), 'key' (Alg. 2), or 'auto' (dynamic selection).
        """
        self.config = config
        self.dataset = dataset
        self.sql_query = sql_query
        self.physical_strategy = physical_strategy

        #Initialize LLM Wrapper
        self.llm_wrapper = get_llm_wrapper(config)

        #Parse the SQL query
        LOG.info(f"Galois System: Parsing query for dataset {dataset}")
        self.parsed_sql = sql_query_parser.parse_sql(sql_query)

        temp_manager = GaloisSchemaManager(dataset)
        table_name = self.parsed_sql['from_table']
        if not temp_manager.get_attributes(table_name):
            raise ValueError(f"Table '{table_name}' not found in the database schema.")
        temp_manager.close()

    def build_execution_plan(self, conditions_to_push: List[str]) -> Dict[str, Any]:
        """
        Helper method to construct the 'Query Plan' dictionary expected by the Executor.
        This merges the static parsed info (SELECT, FROM) with the
        dynamic logical choice (which WHERE conditions to push).
        """

        return {
            "select_columns": self.parsed_sql['select_columns'],
            "from_table": self.parsed_sql['from_table'],
            "conditions_to_push": conditions_to_push,
            "original_query": self.sql_query
        }

    def  execute_variant(self, plan: Dict[str, Any], variant_name: str):
        """
        Instantiates the Executor and runs the query according to the plan.
        """
        LOG.info(f"\n--- STARTING GALOIS EXECUTION: {variant_name} ---")
        LOG.info(f"Physical Strategy: {self.physical_strategy}")
        LOG.info(f"Pushing Conditions: {plan['conditions_to_push']}")

        #creating an instance of ConfidenceEstimator
        galois_estimator = ConfidenceEstimator(self.llm_wrapper, self.dataset, system_prompt_galois_confidence(), human_prompt_galois_confidence("QUERY"))

        #DEFAULT value of final_strategy
        final_strategy = "TABLE"

        num_select_columns = len(plan['select_columns'])
        if num_select_columns == 0:
            LOG.error("No columns in SELECT clause. Defaulting to TABLE strategy.")
            final_strategy = "TABLE"
        else:
            final_strategy = "TABLE"

        # Determine physical strategy
        if self.physical_strategy == "auto":
            LOG.info("STARTING CONFIDENCE PROCESS FOR TABLE OR KEY SCAN")
            llm_confidence_result = galois_estimator.estimate_confidence_query(self.config, self.parsed_sql["from_table"], plan['original_query'], num_select_columns)
            if llm_confidence_result in ["TABLE", "KEY"]:
                final_strategy = llm_confidence_result
            else:
                LOG.error("LLM  didn't reply with a confidence about  'TABLE' OR 'KEY'")
        else:
            final_strategy = self.physical_strategy.upper()

        LOG.info(f"Final Strategy Selected: {final_strategy}")

        try:

            results= []
            # Decision on table or key scan
            if final_strategy == "TABLE":
                #DO TABLE SCAN
                executor = GaloisExecutor(self.config, self.dataset)
                LOG.info(f"Executing Table Scan using GaloisExecutor instance for query {plan['original_query']}...")
                results = executor.table_scan(plan['original_query'])

            elif final_strategy == "KEY":
                #DO KEY SCAN
                LOG.info("For that query the system will apply KEY SCAN")

            else:
                LOG.info("LLM  didn't reply with a 'TABLE' OR 'KEY'")
            strategy = self.physical_strategy if self.physical_strategy != "auto" else "table"

            return results
        except Exception as e:
            LOG.error(f"Error during the plan selection execution of the query {plan['original_query']} : {e}")
            raise e

    # --- LOGICAL OPTIMIZATION VARIANTS (Fig.2 in the paper)
    def run_no_push(self) -> List[Dict[str, Any]]:
        """
        Variant: GALOIS_WO (Without Optimizations / No-Push)

        Logic:
        - Do NOT push any WHERE conditions to the LLM Scan.
        - Retrieve ALL data (or keys) from the table.
        - Filtering effectively happens in post-processing (or manually later).

        Corresponds to plan (n1) in Fig. 2.
        """
        # We pass an empty list of conditions
        plan = self.build_execution_plan(conditions_to_push=[])
        return self.execute_variant(plan, "GALOIS_WO (No-Push)")

    def run_push_all(self) -> List[Dict[str, Any]]:
        """
        Variant: GALOIS_A (Push-All)

        Logic:
        - Push ALL SQL WHERE conditions into the LLM prompt.
        - Relies on the LLM to understand and filter everything.

        Corresponds to plan (p1) in Fig. 2.
        """
        # We pass all conditions found by the parser
        all_conditions = self.parsed_sql['where_conditions']
        plan = self.build_execution_plan(conditions_to_push=all_conditions)
        return self.execute_variant(plan, "GALOIS_A (Push-All)")

    def run_push_selective(self) -> List[Dict[str, Any]]:
        """
        Variant: GALOIS_S (Push-Selective)

        Logic:
        - Push only the 'most selective' condition.
        - NOTE: True selectivity estimation requires DB statistics or LLM estimation.
        - Simple Heuristic Implementation: Push the FIRST condition only.

        Corresponds to plan (s1) in Fig. 2.
        """
        all_conditions = self.parsed_sql['where_conditions']

        # Heuristic: Take the first condition if available, else push nothing
        selective_conditions = [all_conditions[0]] if all_conditions else []

        plan = self.build_execution_plan(conditions_to_push=selective_conditions)
        return self.execute_variant(plan, "GALOIS_S (Push-Selective)")

    def run_push_confident(self) -> List[Dict[str, Any]]:
        """
        Variant: GALOIS_F (Full / Push-Confident)

        Logic:
        - Push conditions based on LLM confidence.
        - This requires a preliminary "Estimator" step (not yet implemented).
        - Fallback: Currently behaves like Push-All (GALOIS_A) as a placeholder.

        Corresponds to plan (c1) in Fig. 2.
        """

        #Initialize the Estimator
        estimator = ConfidenceEstimator(self.llm_wrapper, self.dataset, system_prompt_galois_confidence(), human_prompt_galois_confidence("CONDITION"))

        #Obtain the original conditions
        all_conditions = self.parsed_sql['where_conditions']
        LOG.info(f"Conditions to Push: {all_conditions}")

        #Filter the conditions based on LLM confidence
        if not all_conditions:
            confidence_conditions = []
        else:
            confidence_conditions = estimator.estimate_confidence_conditions(
                self.parsed_sql['from_table'],
                all_conditions
            )

        num_confident = len(confidence_conditions)
        conditions_to_push = []
        execution_variant = ""

        if num_confident == 0:
            # Case 0: No "HIGH" confidence -> No Pushdown
            conditions_to_push = []
            execution_variant = "GALOIS_F (Push-Confident): No-Push Heuristic"
            LOG.info("Heuristic: 0 confident conditions -> Pushing NONE.")

        elif num_confident == 1:
            # Caso 1: Only one "HIGH" confidence-> Pushdown that condition
            conditions_to_push = confidence_conditions
            execution_variant = "GALOIS_F (Push-Confident): Single-Push Heuristic"
            LOG.info(f"Heuristic: 1 confident condition -> Pushing ONLY: {conditions_to_push}")


        elif num_confident > 1:
            # Caso >1: MOre than one "HIGH" confidence-> Pushdown all original conditions
            conditions_to_push = all_conditions
            execution_variant = "GALOIS_F (Push-Confident): Push-All Heuristic"
            LOG.info("Heuristic: >1 confident conditions -> Pushing ALL original conditions.")

        #Build and execute the plan
        #The conditions that are not involved here will be ignored by the Scan.
        plan = self.build_execution_plan(conditions_to_push=conditions_to_push)

        return self.execute_variant(plan, execution_variant)



def main():
    config = Config_Loader().get_config()

    print("==========================================")
    print("   TEST MINIMALE GALOIS (Python Port)     ")
    print("==========================================")

    # A. Definizione del Test
    # Usiamo il dataset PRESIDENTS perché hai caricato 'presidents.duckdb'
    dataset_name = "movies"
    dataset_path = DATA_ROOT / dataset_name.upper()

    # Una query SQL presa dal tuo file 'queries_presidents.sql' (Query 2)
    # Nota: GALOIS gestirà il parsing di "target.world_presidents"

    #sql_query = "SELECT p.name, p.party FROM target.world_presidents p WHERE p.country='Venezuela' AND p.party='Liberal';"

    queries = load_queries_from_folder(dataset_path)
    for query in queries:

        LOG.info(f"Dataset: {dataset_name}")
        LOG.info(f"Query SQL: {query}")


        try:
            # Initialization of GALOIS SYSTEM
            # Instanciation SchemaManager, the Parser and the LLM Wrapper
            galois_system = Galois(
                config=config,
                dataset=dataset_name,
                sql_query=query,
                physical_strategy="auto"  # Forziamo Algoritmo 1 (Table-Scan) per il primo test
            )

            # C. Esecuzione (Variante: Push-All)
            # Questa variante "spinge" tutti i filtri (Venezuela + Liberal) nel prompt
            print("\n>>> Start Execution: We trust the LLM confidence:")
            results = galois_system.run_push_confident()

            print("\n==========================================")
            print(f"   RESULTS ({len(results)} rows founded)")
            print("==========================================")
            print(json.dumps(results, indent=2))

            print("------------------------------------------")

        except Exception as e:
            LOG.error(f"Error during the test {e}", exc_info=True)

if __name__ == "__main__":
     main()