import json
import re
from pathlib import Path
from typing import Literal, Any, List, Dict, cast
import duckdb
import pandas as pd
from src import Config_Loader
from src.db.run_explain_plans import DATA_ROOT
from src.galois.galois_post_processing import GaloisPostProcessor
from src.llm import get_llm_wrapper
from src.galois.galois_prompts import system_prompt_galois_confidence, human_prompt_galois_confidence
from src.utils import sql_query_parser, load_queries_from_folder
from src.galois.galois_estimator import ConfidenceEstimator
from src.utils.constants import SUBMISSIONS_PATH_GALOIS
from src.utils.get_db_schema_galois import GaloisSchemaManager
from src.utils.logging_config import LOG
from src.galois.galois_executor import GaloisExecutor


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

        #Retrieve the conditions
        all_conditions = self.parsed_sql['where_conditions']
        pushed_conditions = plan['conditions_to_push']

        #Find the residual conditions
        residual_conditions = [c for c in all_conditions if c not in pushed_conditions]

        try:

            ####### HANDLING JOIN ########
            tables_to_scan = []

            from_tbl = self.parsed_sql['from_table']
            from_als = self.parsed_sql.get('from_alias') or from_tbl

            #add the main table
            tables_to_scan.append({
                "name": from_tbl,
                "alias": from_als
            })

            #add the tables involved in join
            if 'joins' in self.parsed_sql:
                for join in self.parsed_sql['joins']:
                    tables_to_scan.append({
                        "name": join['table'],
                        "alias": join['alias']
                    })

            #dictionary for accumulate the data
            data_lake = {}

            #cycle in which the table scan is executed
            executor = GaloisExecutor(self.config, self.dataset)

            for t_info in tables_to_scan:
                t_name = t_info['name']
                t_alias = t_info['alias']
                LOG.info(f"--- FETCHING DATA FOR TABLE: {t_name} (Alias: {t_alias}) ---")

                #retrieve the target columns for that table
                target_cols = plan['select_columns']

                if len(tables_to_scan) == 1 and target_cols:
                    # Clean the columns (remove aggregations like avg(gnp) -> gnp)
                    clean_cols = []
                    for c in target_cols:
                        # If count(*), ignore it
                        if "*" in c: continue
                        # Extract "gnp" from "avg(gnp)"
                        match = re.search(r'\((.*?)\)', c)
                        if match:
                            clean_cols.append(match.group(1))
                        else:
                            clean_cols.append(c)

                    if clean_cols:
                        cols_str = ", ".join(clean_cols)
                        simple_query = f"SELECT {cols_str} FROM {t_name}"
                    else:
                        simple_query = f"SELECT * FROM {t_name}"
                else:
                    # Per i JOIN o casi complessi, per sicurezza lasciamo * per ora
                    # (o l'LLM potrebbe dimenticare le chiavi di join)
                    simple_query = f"SELECT * FROM {t_name}"


                #build a base  query for the extraction
                #simple_query = f"SELECT * FROM {t_name}"

                #filter the WHERE conditions that are addicted to this particular alias
                current_push_conditions = []
                for cond in plan['conditions_to_push']:
                    if f"{t_alias}." in cond or len(tables_to_scan) == 1:
                        current_push_conditions.append(cond)

                LOG.info(f"--- FETCHING DATA FOR TABLE: {t_name} (Alias: {t_alias}) ---")
                if current_push_conditions:
                    LOG.info(f"   -> Pushing filters: {current_push_conditions}")
                else:
                    LOG.info(f"   -> No specific filters identified (Scan Full or Join-only)")

                if final_strategy == "KEY":
                    LOG.info(f"Executing KEY SCAN on {t_name}")

                    ################
                    #DO KEY SCAN
                    ################

                else:
                    #DO TABLE SCAN
                    LOG.info(f"Executing TABLE SCAN on {t_name}")
                    rows = executor.table_scan(sql_query=simple_query, conditions_to_push=current_push_conditions)

                data_lake[t_name] = rows

            #Local join and post processing
            results = self.perform_local_join_and_query(plan['original_query'], data_lake)
            return results

            ##############################

            """
            results= []
            # Decision on table or key scan
            if final_strategy == "TABLE":
                #DO TABLE SCAN
                executor = GaloisExecutor(self.config, self.dataset)
                LOG.info(f"Executing Table Scan using GaloisExecutor instance for query {plan['original_query']}...")
                results = executor.table_scan(sql_query=plan['original_query'],  conditions_to_push=pushed_conditions)

            elif final_strategy == "KEY":
                #DO KEY SCAN
                LOG.info("For that query the system will apply KEY SCAN")

            else:
                LOG.info("LLM  didn't reply with a 'TABLE' OR 'KEY'")

            #POST PROCESSING
            if residual_conditions and results:
                LOG.info(f"Post-processing required for conditions: {residual_conditions}")
                post_processor = GaloisPostProcessor()
                results = post_processor.filter_results(results, residual_conditions)

            return results
            """
        except Exception as e:
            LOG.error(f"Error during the plan selection execution of the query {plan['original_query']} : {e}")
            raise e

    # --- LOGICAL OPTIMIZATION VARIANTS (Fig.2 in the paper)
    def run_no_push(self) -> List[Dict[str, Any]]:
        """
        Variant: GALOIS_WO (Without Optimizations / No-Push)

        Logic:
        - Do NOT push any WHERE conditions to the LLM Scan.
        - Retrieve ALL data (or keys) from the table using KEY-SCAN.
        - Filtering effectively happens in post-processing (or manually later).

        Corresponds to plan (n1) in Fig. 2.
        """
        # We pass an empty list of conditions
        plan = self.build_execution_plan(conditions_to_push=[])
        #Force GaloisWO to use KeyScan as indicated in the former paper
        self.physical_strategy = "key"
        return self.execute_variant(plan, "GALOIS_WO (No-Push)")

    def run_push_all(self) -> List[Dict[str, Any]]:
        """
        Variant: GALOIS_A (Push-All)

        Logic:
        - Push ALL SQL WHERE conditions into the LLM prompt.
        - Relies on the LLM to understand and filter everything.

        Corresponds to plan (p1) in Fig. 2.
        """
        #setting table scan
        self.physical_strategy = "table"
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

        #set table scan
        self.physical_strategy = "table"

        #Instantiate an estimator object
        estimator = ConfidenceEstimator(self.llm_wrapper, self.dataset, system_prompt_galois_confidence(), human_prompt_galois_confidence("CONDITION"))

        all_conditions = self.parsed_sql['where_conditions']
        LOG.info(f"[GALOIS_S] Analyzing selectivity for: {all_conditions}")

        #interacting with LLM for the confidence estimation
        selective_conditions = estimator.estimate_confidence_conditions(self.parsed_sql['from_table'], all_conditions)

        #apply the logic of GALOIS S
        num_high = len(selective_conditions)
        conditions_to_push = []
        variant_desc = ""

        if num_high == 1:
            conditions_to_push = selective_conditions
            variant_desc = "GALOIS_S: Single Selective Push"
        elif num_high > 1:
            conditions_to_push = all_conditions
            variant_desc = "GALOIS_S: Multi-Selective (Push All)"
        else:
            conditions_to_push = []
            variant_desc = "GALOIS_S: Low Selectivity (No Push)"

        LOG.info(f"Selectivity Analysis Result: {num_high} selective conditions found. Action: {variant_desc}")

        plan = self.build_execution_plan(conditions_to_push=conditions_to_push)
        return self.execute_variant(plan, variant_desc)

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

    def perform_local_join_and_query(self, original_sql: str, data_map: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:

        # execute local join using duckdb in-memory on the data provided by LLM
        LOG.info(f"--- ASSEMBLING DATA LOCALLY ({len(data_map)} tables) ---")
        con = duckdb.connect(database=':memory:')

        try:
            # register every dataset as a virtual table
            for table_real_name, rows in data_map.items():
                if not rows:
                    LOG.warning(f"No data found for table {table_real_name}. Join might result in a empty set.")
                    df = pd.DataFrame()
                else:
                    df = pd.DataFrame(rows)

                    # --- SMART AUTO-CASTING ---
                    # The LLM often returns values as strings ("1000", "N/A"), while DuckDB expects numeric types.
                    # Try to convert columns to numeric where possible.
                    for col in df.columns:
                        # Try converting to numeric. If it fails (e.g. "Texas"), leave as-is.
                        numeric_series = pd.to_numeric(df[col], errors='coerce')

                        #counts the NaN values
                        nan_count = numeric_series.isna().sum()
                        total_count = len(df)

                        #if more than 50% of the values are NaN the column is not a numeric one
                        if total_count > 0 and (nan_count / total_count) > 0.5:
                            df[col] = df[col].astype(str)
                        else:
                            # otherwise probably is numeric, using the cionverted one.
                            df[col] = numeric_series

                    # --- STRING CLEANING (Trim whitespace) ---
                    # Important for JOINs to work (e.g. "Arizona " -> "Arizona")
                    # If a column is of type 'object' (string), apply strip()
                    for col in df.select_dtypes(include=['object']).columns:
                        df[col] = df[col].astype(str).str.strip()

                clean_name = table_real_name.strip()
                con.register(clean_name, df)
                LOG.info(f"Registered view: '{clean_name}' ({len(df)} rows)")

            #uncomment this part if you need to see the content of the tables before executing the query locally
            # --- DEBUG: CONTENT OF THE TABLE ---
            #print("\n--- DEBUG DATA DUMP ---")
            #print(con.execute("SELECT * FROM usa_city").df().head())
            #print(con.execute("SELECT * FROM usa_state LIMIT 5").df())
            #print("-----------------------\n")

            clean_sql_query = original_sql

            # Remove any comments from the SQL query
            clean_sql_query = "\n".join([line for line in clean_sql_query.split('\n') if not line.strip().startswith('--')])
            #Remove garbage notes before SELECT
            match = re.search(r'\bSELECT\b', clean_sql_query, re.IGNORECASE)
            if match:
                clean_sql_query = clean_sql_query[match.start():]

            #remove the target. prefix from the original query
            clean_sql_query = clean_sql_query.replace("target.", "")
            LOG.info(f"Executing Local SQL Query: {clean_sql_query}")
            # execute original query
            result_df = con.execute(clean_sql_query).df()

            return cast(List[Dict[str, Any]], result_df.to_dict(orient='records'))
        except Exception as e:
            LOG.error(f"Error during the local join and query execution: {e}")
            try:
                tables = con.execute("SHOW TABLES").fetchall()
                LOG.error(f"Registered tables in DuckDB: {tables}")
            except:
                pass
            return []
        finally:
            con.close()


def save_galois_results(results_list, variant, provider, dataset_name):
    """
    Salva i risultati di GALOIS in JSON per la valutazione.
    """
    variant_key = f"GALOIS_{variant}"

    # Cerca il path corretto in BASELINE_OUTPUT, altrimenti crea un path di default
    try:
        base_dir = SUBMISSIONS_PATH_GALOIS
    except KeyError:
        base_dir = Path(f"./experiments/galois_{variant.lower()}/{provider.lower()}")
        LOG.warning(f"Output path for {variant_key} not found in config. Using default: {base_dir}")

    #output_path = Path(base_dir) / f"{dataset_name}.json"
    variant_dir_name = f"GALOIS_{variant.upper()}"
    dataset_dir_name = dataset_name.upper()

    target_dir = base_dir / variant_dir_name / dataset_dir_name
    target_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for result in results_list:
        query_id = result.get("query_id", "unknown")

        stem_name = Path(str(query_id)).stem

        output_filename = f"query{stem_name}.json"
        output_path = target_dir / output_filename

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=4, ensure_ascii=False)
            count += 1
        except Exception as e:
            LOG.error(f"Failed to save result for {output_filename}: {e}")

    LOG.info(f"Successfully saved {count} files in {target_dir}")
    return str(target_dir)


def main():
    config = Config_Loader().get_config()

    print("==========================================")
    print("   TEST MINIMALE GALOIS (Python Port)     ")
    print("==========================================")

    sql_query_test = """
                     SELECT COUNT ( DISTINCT usa_state_traversed ) 
                     FROM target.usa_river 
                     WHERE length_in_km > 750;

                     """
    dataset_name = "GEO"
    try:
        # Initialize GALOIS
        galois_system = Galois(
            config=config,
            dataset=dataset_name,
            sql_query=sql_query_test,
            physical_strategy="table"  # Forziamo Table Scan per vedere i due scaricamenti
        )

        results = galois_system.run_push_all()

        print("\n==========================================")
        print(f"   RISULTATO JOIN ({len(results)} righe)")
        print("==========================================")
        print(json.dumps(results, indent=2))

    except Exception as e:
        LOG.error(f"Error in the  test: {e}", exc_info=True)

"""
    # A. Definizione del Test
    # Usiamo il dataset PRESIDENTS perché hai caricato 'presidents.duckdb'
    dataset_name = "geo"
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
            """




if __name__ == "__main__":
     main()