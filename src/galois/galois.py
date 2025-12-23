import json
import re
from pathlib import Path
from typing import Literal, Any, List, Dict, cast, Optional
import duckdb
import pandas as pd
from sqlglot import parse_one, exp
from src.config import Config_Loader
from src.llm import get_llm_wrapper
from src.galois.galois_prompts import system_prompt_galois_confidence, human_prompt_galois_confidence
from src.utils import sql_query_parser, load_queries_from_folder
from src.galois.galois_estimator import ConfidenceEstimator
from src.utils.constants import SUBMISSIONS_PATH_GALOIS
from src.utils.get_db_schema_galois import GaloisSchemaManager
from src.utils.logging_config import LOG, log_init
# Alias maintained from your original code
from src.galois.executor import GaloisExecutor as Francesco_Executor


class Galois:
    """
    Main orchestration class for the GALOIS system.

    1. Initialization: Sets up LLM, Schema Manager, and parses the SQL query.
    2. Logical Optimization: Decides WHICH conditions to push down to the LLM.
    3. Physical Optimization: Decides HOW to query the LLM (Table-Scan vs Key-Scan).
    4. Execution: Delegates the actual work to GaloisExecutor.
    """
    ScanStrategy = Literal["table", "key", "auto"]
    
    def __init__(self,
                 config: Any,
                 dataset: str,
                 sql_query: str,
                 physical_strategy: ScanStrategy = "auto",
                 confidence_threshold: float = None): 
        """
        Args:
            config: Configuration object.
            dataset: Name of the dataset.
            sql_query: The raw SQL query string.
            physical_strategy: 'table', 'key', or 'auto'.
            confidence_threshold: (Optional) Threshold for switching strategies (Exp 7).
        """
        self.config = config
        self.dataset = dataset
        self.sql_query = sql_query
        self.physical_strategy = physical_strategy
        
        # Threshold handling (Exp 7 logic)
        if confidence_threshold is not None:
            self.confidence_threshold = confidence_threshold
        else:
            self.confidence_threshold = getattr(config.galois_execution, 'confidence_threshold', 0.8)

        # Initialize LLM Wrapper
        self.llm_wrapper = get_llm_wrapper(config)

        # Parse the SQL query
        LOG.info(f"INFO | [GALOIS] Parsing query for dataset '{dataset}'")
        self.parsed_sql = sql_query_parser.parse_sql(sql_query)

        self.schema_map = {}
        temp_manager = GaloisSchemaManager(dataset)
        try:
            table_name = self.parsed_sql['from_table']
            if not temp_manager.get_attributes(table_name):
                raise ValueError(f"Table '{table_name}' not found in the database schema.")
            real_name = temp_manager.get_exact_table_name(table_name) or table_name
            self.schema_map[real_name] = temp_manager.get_column_types(real_name)

            if 'joins' in self.parsed_sql:
                for join in self.parsed_sql['joins']:
                    j_table = join['table']
                    real_j_name = temp_manager.get_exact_table_name(j_table) or j_table
                    self.schema_map[real_j_name] = temp_manager.get_column_types(real_j_name)

        finally:
            temp_manager.con.close()

    def build_execution_plan(self, conditions_to_push: List[str]) -> Dict[str, Any]:
        """
        Helper method to construct the 'Query Plan' dictionary.
        """
        return {
            "select_columns": self.parsed_sql['select_columns'],
            "from_table": self.parsed_sql['from_table'],
            "conditions_to_push": conditions_to_push,
            "original_query": self.sql_query
        }

    def execute_variant(self, plan: Dict[str, Any], variant_name: str, debug: bool = False):
        """
        Instantiates the Executor and runs the query according to the plan.
        Updated for Exp-7: Uses confidence score vs threshold to select Physical Strategy.
        """
        LOG.info(f"INFO | [GALOIS] Starting Variant: {variant_name}")
        LOG.info(f"PLAN | [GALOIS] Requested Strategy: {self.physical_strategy}")
        LOG.info(f"PLAN | [GALOIS] Conditions to Push: {plan['conditions_to_push']}")

        # Instantiate ConfidenceEstimator
        galois_estimator = ConfidenceEstimator(
            self.llm_wrapper, 
            self.dataset, 
            system_prompt_galois_confidence(), 
            human_prompt_galois_confidence("QUERY")
        )

        final_strategy = "KEY" # Default safe fallback
        num_select_columns = len(plan['select_columns'])
        
        if num_select_columns == 0:
            LOG.warning("WARN | [GALOIS] No columns in SELECT clause. Defaulting to KEY strategy.")

        # --- PHYSICAL OPTIMIZATION LOGIC (Updated) ---
        if self.physical_strategy == "auto":
            LOG.info(f"PLAN | [GALOIS] Auto-Strategy Check (Threshold: {self.confidence_threshold})")
            
            #  Get the numerical score (0.0 - 1.0)
            confidence_score = galois_estimator.estimate_confidence_query(
                self.config, 
                self.parsed_sql["from_table"], 
                plan['original_query'], 
                num_select_columns
            )
            
            if not isinstance(confidence_score, (float, int)):
                LOG.error(f"ERR  | [GALOIS] Invalid confidence score: {confidence_score}. Defaulting to 0.0")
                confidence_score = 0.0

            #  threshold-based-decision
            decision = "KEY SCAN" if confidence_score >= self.confidence_threshold else "TABLE SCAN"
            
            # clean decision log
            LOG.info(f"PLAN | [GALOIS] Score: {confidence_score:.4f} vs {self.confidence_threshold} -> Decision: {decision}")

            if confidence_score >= self.confidence_threshold:
                final_strategy = "KEY"
            else:
                final_strategy = "TABLE"
                
        else:
            final_strategy = self.physical_strategy.upper()
            LOG.info(f"PLAN | [GALOIS] Manual Strategy Selected: {final_strategy}")

        # Retrieve the conditions
        all_conditions = self.parsed_sql['where_conditions']
        pushed_conditions = plan['conditions_to_push']
        residual_conditions = [c for c in all_conditions if c not in pushed_conditions]
        
        per_table_stats = [] 

        try:
            ####### HANDLING JOIN ########
            tables_to_scan = []

            from_tbl = self.parsed_sql['from_table']
            from_als = self.parsed_sql.get('from_alias') or from_tbl
            tables_to_scan.append({"name": from_tbl, "alias": from_als})

            if 'joins' in self.parsed_sql:
                for join in self.parsed_sql['joins']:
                    tables_to_scan.append({"name": join['table'], "alias": join['alias']})

            total_execution_time = 0
            total_tokens_used = 0
            data_lake = {}

            for t_info in tables_to_scan:
                t_name = t_info['name']
                t_alias = t_info['alias']
                
                # retrieve target columns
                cols_to_pass = self._get_involved_columns(
                    sql_query=plan['original_query'],
                    table_name=t_name,
                    alias=t_alias
                )
                
                # simple query construction for display step
                cols_str = ", ".join(cols_to_pass) if cols_to_pass else "*"
                simple_query = f"SELECT {cols_str} FROM {t_name}"

                # Filter specific push conditions
                current_push_conditions = []
                for cond in plan['conditions_to_push']:
                    if f"{t_alias}." in cond or len(tables_to_scan) == 1:
                        current_push_conditions.append(cond)

                # --- NEW LOG STYLE FOR EXECUTION ---
                cols_log = str(cols_to_pass) if cols_to_pass else "ALL (*)"
                LOG.info(f"EXEC | [{self.dataset}] Scanning Table: {t_name} (Alias: {t_alias})")
                LOG.info(f"EXEC | [{self.dataset}] Filters: {current_push_conditions} | Columns: {cols_log}")
                # -----------------------------------

                # EXECUTE SCAN
                if final_strategy == "KEY":
                    key_executor = Francesco_Executor(self.config, self.dataset)
                    try:
                        f_response = key_executor.key_scan(
                            query=simple_query, 
                            columns=cols_to_pass, 
                            conditions_to_push=current_push_conditions, 
                            max_iter=plan.get("max_iter")
                        )
                    finally:
                        if hasattr(key_executor, 'schema_mgr') and hasattr(key_executor.schema_mgr, 'dispose_manager'):
                            key_executor.schema_mgr.dispose_manager()

                else:
                    table_executor = Francesco_Executor(self.config, self.dataset)
                    try:
                        f_response = table_executor.table_scan(
                            query=simple_query, 
                            columns=cols_to_pass, 
                            conditions_to_push=current_push_conditions, 
                            max_iter=plan.get("max_iter")
                        )
                    finally:
                        if hasattr(table_executor, 'schema_mgr'):
                            if hasattr(table_executor.schema_mgr, 'dispose_manager'):
                                table_executor.schema_mgr.dispose_manager()
                            elif hasattr(table_executor.schema_mgr, 'close'):
                                table_executor.schema_mgr.close()
                
                scan_used = "KEY" if final_strategy == "KEY" else "TABLE"
                per_table_stats.append({ 
                    "table": t_name, 
                    "alias": t_alias,
                    "scan": scan_used,
                    "columns": cols_to_pass if cols_to_pass else "*",
                    "pushed_conditions": current_push_conditions,
                    "time": f_response.get("time", 0),
                    "tokens": f_response.get("tokens", 0),
                    "n_iters": f_response.get("n_iters"),
                    "input_tokens_by_iter": f_response.get("input_tokens_by_iter"),
                    "input_tokens_total_all_iters": f_response.get("input_tokens_total_all_iters"),
                }) 

                rows = f_response.get("response", [])
                logprobs = f_response.get("logprobs", [])

                # Injection Logprobs
                if logprobs and len(rows) == len(logprobs):
                    for row, lp in zip(rows, logprobs):
                        row["_galois_logprob"] = lp

                total_execution_time += f_response.get("time", 0)
                total_tokens_used += f_response.get("tokens", 0)
                data_lake[t_name] = rows
            
            # Local join and post processing
            results = self.perform_local_join_and_query(plan['original_query'], data_lake)
            
            stats = {
                "total_time": total_execution_time,
                "total_tokens": total_tokens_used
            }
            
            if not debug:
              return results, stats
            
            debug_info = {
                "variant_name": variant_name,
                "physical_strategy_requested": self.physical_strategy,
                "physical_strategy_final": final_strategy,
                "conditions_to_push": pushed_conditions,
                "residual_conditions": residual_conditions,
                "tables": per_table_stats,
            }
            return results, stats, debug_info

        except Exception as e:
            LOG.error(f"ERR  | [GALOIS] Execution failed for query '{plan['original_query']}': {e}")
            raise e


    # --- LOGICAL OPTIMIZATION VARIANTS ---

    def run_no_push(self, debug:bool =False) -> tuple[list[dict[str, Any]], dict[str, int]]:
        """
        Variant: GALOIS_WO (No-Push). Do NOT push any WHERE conditions.
        """
        plan = self.build_execution_plan(conditions_to_push=[])
        self.physical_strategy = "key"
        return self.execute_variant(plan, "GALOIS_WO (No-Push)", debug=debug)

    def run_push_all(self, debug: bool = False) -> tuple[list[dict[str, Any]], dict[str, int]]:
        """
        Variant: GALOIS_A (Push-All). Push ALL SQL WHERE conditions.
        """
        self.physical_strategy = "table"
        all_conditions = self.parsed_sql['where_conditions']
        plan = self.build_execution_plan(conditions_to_push=all_conditions)
        return self.execute_variant(plan, "GALOIS_A (Push-All)", debug=debug)

    def run_push_selective(self, debug: bool = False) -> tuple[list[dict[str, Any]], dict[str, int]]:
        """
        Variant: GALOIS_S (Push-Selective). Push only 'most selective' conditions.
        """
        self.physical_strategy = "table"
        
        estimator = ConfidenceEstimator(self.llm_wrapper, self.dataset, system_prompt_galois_confidence(), human_prompt_galois_confidence("CONDITION"))
        all_conditions = self.parsed_sql['where_conditions']
        
        LOG.info(f"PLAN | [GALOIS_S] Analyzing selectivity for: {all_conditions}")
        selective_conditions = estimator.estimate_confidence_conditions(self.parsed_sql['from_table'], all_conditions)

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

        LOG.info(f"PLAN | [GALOIS_S] Result: {num_high} selective conditions found. Action: {variant_desc}")
        plan = self.build_execution_plan(conditions_to_push=conditions_to_push)
        return self.execute_variant(plan, variant_desc, debug=debug)

    
    def run_push_confident(self, debug: bool = False) -> tuple[list[dict[str, Any]], dict[str, int]]:
        """
        Variant: GALOIS_F (Push-Confident). Push conditions based on LLM confidence.
        """
        estimator = ConfidenceEstimator(self.llm_wrapper, self.dataset, system_prompt_galois_confidence(), human_prompt_galois_confidence("CONDITION"))

        all_conditions = self.parsed_sql['where_conditions']

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
            conditions_to_push = []
            execution_variant = "GALOIS_F (Push-Confident): No-Push Heuristic"
            LOG.info("PLAN | [GALOIS_F] Heuristic: 0 confident conditions -> Pushing NONE.")
        elif num_confident == 1:
            conditions_to_push = confidence_conditions
            execution_variant = "GALOIS_F (Push-Confident): Single-Push Heuristic"
            LOG.info(f"PLAN | [GALOIS_F] Heuristic: 1 confident condition -> Pushing ONLY: {conditions_to_push}")
        elif num_confident > 1:
            conditions_to_push = all_conditions
            execution_variant = "GALOIS_F (Push-Confident): Push-All Heuristic"
            LOG.info("PLAN | [GALOIS_F] Heuristic: >1 confident conditions -> Pushing ALL original conditions.")

        plan = self.build_execution_plan(conditions_to_push=conditions_to_push)
        return self.execute_variant(plan, execution_variant, debug=debug)


    def perform_local_join_and_query(self, original_sql: str, data_map: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:

        LOG.info(f"DATA | [LOCAL] Assembling data from {len(data_map)} tables...")
        con = duckdb.connect(database=':memory:')
        
        gp_columns_map = {}

        try:
            # register every dataset as a virtual table
            for table_real_name, rows in data_map.items():
                clean_name = table_real_name.strip()
                
                if not rows:
                    LOG.warning(f"WARN | [LOCAL] No data for table '{table_real_name}'. Join might be empty.")
                    df = pd.DataFrame()
                else:
                    df = pd.DataFrame(rows)
                    
                    if "_galois_logprob" not in df.columns:
                        df["_galois_logprob"] = -999.0
                    
                    unique_gp_col = f"_gp_{clean_name}"
                    df.rename(columns={"_galois_logprob": unique_gp_col}, inplace=True)
                    gp_columns_map[clean_name] = unique_gp_col

                    df.replace(r'(?i)^\s*(null|none|nan|n/a)\s*$', None, regex=True, inplace=True)
                    df.replace(["None", "nan", "NaN"], None, inplace=True)

                    schema_cols = self.schema_map.get(table_real_name, {})
                    schema_cols_lower = {k.lower(): v.upper() for k, v in schema_cols.items()}

                    # --- CASTING ---
                    for col in df.columns:
                        if col == unique_gp_col:
                            continue
                        col_lower = col.lower()
                        db_type = schema_cols_lower.get(col_lower, "")
                        numeric_series = pd.to_numeric(df[col], errors='coerce')
                        
                        if any(x in db_type for x in ["INT", "DOUBLE", "FLOAT", "DECIMAL", "REAL", "NUMERIC"]):
                            df[col] = numeric_series
                            df[col] = df[col].astype(object).where(df[col].notnull(), None)
                        elif "VARCHAR" in db_type or "TEXT" in db_type or "STRING" in db_type:
                            df[col] = df[col].astype(object).where(df[col].notnull(), None)
                            mask_str_none = df[col].astype(str).str.strip().str.lower().isin(['none', 'nan', 'null', 'n/a'])
                            df.loc[mask_str_none, col] = None
                        else:
                            non_na_count = df[col].count()
                            numeric_count = numeric_series.count()
                            if numeric_count > 0 and numeric_count >= (non_na_count * 0.8):
                                df[col] = numeric_series
                                df[col] = df[col].astype(object).where(df[col].notnull(), None)
                            else:
                                df[col] = df[col].astype(object).where(df[col].notnull(), None)

                    for col in df.select_dtypes(include=['object']).columns:
                        df[col] = df[col].map(lambda x: x.strip() if isinstance(x, str) else x)

                con.register(clean_name, df)
                # LOG.debug(f"Registered view: '{clean_name}'")

            # --- QUERY CLEANING ---
            clean_sql_query = original_sql
            clean_lines = []
            for line in clean_sql_query.split('\n'):
                stripped = line.strip()
                if not stripped: continue
                if stripped.startswith('--'): continue
                if stripped.startswith('- '): continue 
                clean_lines.append(line)
            clean_sql_query = "\n".join(clean_lines)
            match = re.search(r'\bSELECT\b', clean_sql_query, re.IGNORECASE)
            if match:
                clean_sql_query = clean_sql_query[match.start():]
            clean_sql_query = clean_sql_query.replace("target.", "")

            # --- SMART INJECTION---
            injected_cols = []
            try:
                parsed_expression = parse_one(clean_sql_query)
                
                if isinstance(parsed_expression, exp.Select):
                    query_tables = set(t.name.lower() for t in parsed_expression.find_all(exp.Table))
                    
                    has_agg = False
                    if parsed_expression.args.get("group"): 
                        has_agg = True
                    else:
                        for node in parsed_expression.find_all(exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max):
                            has_agg = True
                            break
                    
                    for t_name, gp_col in gp_columns_map.items():
                        if t_name.lower() in query_tables:
                            col_expr = exp.Column(this=gp_col)
                            if has_agg:
                                inj_expr = exp.Avg(this=col_expr)
                                inj_expr = exp.Alias(this=inj_expr, alias=exp.Identifier(this=gp_col, quoted=False))
                            else:
                                inj_expr = col_expr
                            parsed_expression.select(inj_expr, append=True, copy=False)
                            injected_cols.append(gp_col)
                    
                    clean_sql_query = parsed_expression.sql()
            except Exception as e:
                LOG.warning(f"WARN | [LOCAL] Injection failed: {e}")

            # --- NEW LOG STYLE FOR SQL ---
            # Remove newlines for cleaner logging
            log_query = clean_sql_query.replace('\n', ' ').strip()
            LOG.info(f"SQL  | [LOCAL] Generated Query: {log_query}")
            
            result_df = con.execute(clean_sql_query).df()
            
            # --- POST-PROCESSING ---
            actual_injected = [c for c in injected_cols if c in result_df.columns]
            
            if actual_injected:
                result_df["_galois_logprob"] = result_df[actual_injected].mean(axis=1)
                result_df["_galois_logprob"] = result_df["_galois_logprob"].fillna(0.0)
                result_df.drop(columns=actual_injected, inplace=True)
            else:
                if "_galois_logprob" not in result_df.columns:
                     result_df["_galois_logprob"] = -999.0

            return cast(List[Dict[str, Any]], result_df.to_dict(orient='records'))
            
        except Exception as e:
            LOG.error(f"ERR  | [LOCAL] Execution failed: {e}")
            return []
        finally:
            con.close()


    def _get_involved_columns(self, sql_query: str, table_name: str, alias: str) -> Optional[List[str]]:
        try:
            parsed = parse_one(sql_query, read="duckdb")
        except Exception as e:
            LOG.warning(f"WARN | [SQLGLOT] Parsing failed for table {table_name}: {e}. Fallback to ALL columns.")
            return None

        if parsed.find(exp.Star):
            return None

        found_columns = set()
        target_names = {table_name.lower()}
        if alias:
            target_names.add(alias.lower())

        for col in parsed.find_all(exp.Column):
            col_name = col.name
            table_ref = col.table
            if table_ref:
                if table_ref.lower() in target_names:
                    found_columns.add(col_name)
            else:
                found_columns.add(col_name)

        if not found_columns:
            return None

        return list(found_columns)


def save_galois_results(results_list, variant, provider, dataset_name):
    """
    Saves GALOIS results in JSON for evaluation.
    """
    variant_key = f"GALOIS_{variant}"

    try:
        base_dir = SUBMISSIONS_PATH_GALOIS
    except KeyError:
        base_dir = Path(f"./experiments/galois_{variant.lower()}/{provider.lower()}")
        LOG.warning(f"WARN | [SAVE] Output path for {variant_key} not found. Using default: {base_dir}")

    variant_dir_name = f"GALOIS_{variant.upper()}"
    dataset_dir_name = dataset_name.upper()

    target_dir = base_dir / variant_dir_name / dataset_dir_name
    target_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for result_container in results_list:
        query_id = result_container.get("query_id", "unknown")
        rows = result_container.get("result_set", [])
        
        clean_rows = []
        extracted_logprobs = []
        
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict):
                    row_copy = row.copy()
                    lp = row_copy.pop("_galois_logprob", None)
                    extracted_logprobs.append(lp if lp is not None else -999.0)
                    clean_rows.append(row_copy)
                else:
                    clean_rows.append(row)
                    extracted_logprobs.append(-999.0)
        
        output_obj = {
            "query_id": query_id,
            "sql": result_container.get("sql", ""),
            "result_set": clean_rows,          
            "logprobs": extracted_logprobs,    
            "tokens": result_container.get("tokens", 0),
            "time": result_container.get("time", 0)
        }

        stem_name = Path(str(query_id)).stem
        output_filename = f"query{stem_name}.json"
        output_path = target_dir / output_filename

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(output_obj, f, indent=4, ensure_ascii=False)
            count += 1
        except Exception as e:
            LOG.error(f"ERR  | [SAVE] Failed to save result for {output_filename}: {e}")

    LOG.info(f"INFO | [SAVE] Successfully saved {count} files in {target_dir}")
    return str(target_dir)


def main():
    config = Config_Loader().get_config()
    log_init()
    
    print("==========================================")
    print("   TEST GALOIS (Python Port)     ")
    print("==========================================")

    sql_query_test = """
                     SELECT COUNT ( DISTINCT usa_state_traversed ) 
                     FROM target.usa_river 
                     WHERE length_in_km > 750;

                     """
    dataset_name = "GEO"
    try:
        galois_system = Galois(
            config=config,
            dataset=dataset_name,
            sql_query=sql_query_test,
            physical_strategy="key" 
        )

        results, stats = galois_system.run_push_confident() # Unpacking tuple return

        print("\n==========================================")
        print(f"   JOIN RESULT ({len(results)} rows)")
        print("==========================================")
        print(json.dumps(results, indent=2))

    except Exception as e:
        LOG.error(f"Error in the test: {e}", exc_info=True)


if __name__ == "__main__":
     main()