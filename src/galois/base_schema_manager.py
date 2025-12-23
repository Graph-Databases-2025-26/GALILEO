from sqlalchemy import create_engine, inspect, text
from typing import List
from src.utils import LOG, DATA_DIR, ERR_FILE_READ_FAILURE, ERR_INVALID_JSON_FORMAT, ERR_INVALID_TABLE_NAME
import json
from src.utils.constants import q_get_sample_values

class BaseSchemaManager:
    """
    Manages the database schema information using a minimalist modern logging style.
    It connects to DuckDB and loads key constraints from a companion JSON file.
    """
        
    def __init__(self, dataset: str):
        self.dataset = dataset
        self.engine = create_engine(f"duckdb:///{self.__get_database_path()}", connect_args={'read_only': True})
        self.db_schema = self.__get_schema_info()
        
    def get_attributes(self, table_name: str, attribute_set: str) -> List[str]:
        """
        Retrieves a list of attribute names based on the set ('all', 'key', 'non_key').
        """
        if self.db_schema.get(table_name, "") != "":
            if attribute_set == "all":
                return [col["name"] for col in self.db_schema[table_name]]
            elif attribute_set == "key":
                return [col["name"] for col in self.db_schema[table_name] if col.get("key") is True]
            elif attribute_set == "non_key":
                return [col["name"] for col in self.db_schema[table_name] if col.get("key") is False]
        else:
            LOG.error(f"ERR  | [META] {ERR_INVALID_TABLE_NAME.format(table_name)}")
            return []

    def get_sample_values(self, table_name: str, column_name: str, limit: int = 3):
        """
        Retrieves sample values to help the LLM understand column content.
        """
        try:
            query = q_get_sample_values(table_name, column_name, limit)
            with self.engine.connect() as connection:
                try:
                    connection.execute(text("USE target;"))
                except Exception:
                    pass
                result = connection.execute(text(query)).fetchall()
                return [row[0] for row in result]
        except Exception as e:
            LOG.error(f"ERR  | [META] Sample fetch failed for {table_name}.{column_name}: {e}")
            return []
    
    def get_json_schema(self, table_name: str, attribute_set: str) -> dict:
        """
        Retrieves a JSON-like dictionary schema, injecting sample values for text columns.
        """
        if self.db_schema.get(table_name, "") != "":
            if attribute_set == "all":
                target_cols = self.db_schema[table_name]
            elif attribute_set == "key":
                target_cols = [c for c in self.db_schema[table_name] if c.get("key") is True]
            else: 
                target_cols = [c for c in self.db_schema[table_name] if c.get("key") is False]

            attributes_dict = {}
            for col in target_cols:
                col_name = col["name"]
                col_type = col["type"]
                col_schema = {"type": col_type, "key": col["key"]}

                # Inject examples for text columns
                if "VARCHAR" in col_type.upper() or "TEXT" in col_type.upper():
                    if not col["key"]:
                        samples = self.get_sample_values(table_name, col_name)
                        if samples:
                            col_schema["examples"] = samples
                            col_schema["description"] = f"Examples: {', '.join(samples)}"

                attributes_dict[col_name] = col_schema

            return {
                "table_name": table_name,
                "type": "object",
                "attributes": attributes_dict
            }
        return {}
    
    def get_json_schema_from_set(self, table_name: str, attribute_set: List[str]):
        """
        Retrieves schema for a specific subset of attributes.
        """
        if self.db_schema.get(table_name, "") != "":
            target_attr_names = set(attribute_set)
            attributes_dict = {}
            for col in self.db_schema[table_name]:
                if col["name"] in target_attr_names:
                    col_name = col["name"]
                    col_type = col["type"]
                    col_key = col["key"]
                    col_schema = {"type": col_type, "key": col_key}

                    if "VARCHAR" in col_type.upper() or "TEXT" in col_type.upper():
                        if not col_key:
                            samples = self.get_sample_values(table_name, col_name)
                            if samples:
                                col_schema["examples"] = samples
                                col_schema["description"] = f"Examples: {', '.join(samples)}"
                    attributes_dict[col_name] = col_schema

            return {
                "table_name": table_name,
                "type": "object",
                "attributes": attributes_dict
            }
        return None
      
    def dispose_manager(self) -> None:
        self.db_schema = None
        self.engine.dispose()
     
    def __get_database_path(self) -> str:
        return str(DATA_DIR / self.dataset.upper() / f"{self.dataset.lower()}.duckdb")
    
    def __get_schema_info(self) -> dict:
        """
        Retrieves schema info and logs it in a compact, modern table style.
        """
        schema_info = {}
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        
        LOG.info(f"DATA | [META] Schema Init: Found {len(tables)} tables")
        
        # --- MODERN TABLE LOGGING ---
        if tables:
            w_idx = 3
            w_name = 30
            
            # alignment prefix
            p = "     | "
            
            LOG.info(f"{p}┌{'─'*w_idx}┬{'─'*w_name}┐")
            LOG.info(f"{p}│ {'#':<{w_idx-1}}│ {'Table Name':<{w_name-1}}│")
            LOG.info(f"{p}├{'─'*w_idx}┼{'─'*w_name}┤")
            
            for i, t in enumerate(tables):
                LOG.info(f"{p}│ {i:<{w_idx-1}}│ {t:<{w_name-1}}│")
            
            LOG.info(f"{p}└{'─'*w_idx}┴{'─'*w_name}┘")
        # ----------------------------
        
        key_constraints = self.__get_keys_constraints()
        
        for t in tables:
            columns = inspector.get_columns(t)
            t_keys = key_constraints.get(t, [])
            
            attributes =[]
            for col in columns:
                attributes.append({"name": col['name'], "type": str(col['type']), "key": col['name'] in t_keys})
            
            schema_info[t] = attributes
        
        return schema_info
    
    def __get_keys_constraints(self) -> dict:
        """
        Loads key constraints from JSON.
        """
        json_info = None
        dataset_dir = DATA_DIR / self.dataset.upper()
        
        try:
            json_file = list(dataset_dir.glob("*.json"))
            if json_file:
                target_json_file = json_file[0]
                with open(target_json_file, 'r') as f:
                    json_info = json.load(f)
                    LOG.info(f"DATA | [META] JSON Constraints Loaded")
            else:
                # Silent or debug log if no json found, handled gracefully below
                pass
                
        except Exception as e:
             LOG.warning(f"WARN | [META] Failed loading JSON constraints: {e}")
        
        key_constraints = {}
        if json_info is not None:
            for t in json_info.get("tables",[]):
                key_constraints[t["name"]] = t.get("keys",[])
        return key_constraints

if __name__ == "__main__":
    from src.utils import log_init
    log_init()
    
    gsm = BaseSchemaManager("MOVIES")
    # Test methods...