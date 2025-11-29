from sqlalchemy import create_engine, inspect
from typing import List

from src.utils import LOG, log_init, DATA_DIR, ERR_FILE_READ_FAILURE, ERR_INVALID_JSON_FORMAT, ERR_INVALID_TABLE_NAME

import json


class BaseSchemaManager:
    """
    Manages the database schema information for a given dataset, connecting to a DuckDB file and loading key constraints from a companion JSON file.

    It provides methods to retrieve attribute names and JSON schema representations for tables, categorized by 'all', 'key', or 'non_key' attributes.

    Attributes:
        dataset (str): The name of the dataset (e.g., 'WORLD') currently being managed. It is used to determine file paths.
        engine (Engine): The SQLAlchemy Engine instance connected to the DuckDB file for the specified dataset.
        db_schema (dict): A cached dictionary structure containing the complete schema information (tables, columns, types, and key status) 
                          loaded from the database and the JSON key constraints file.
    """
        
    def __init__(self, dataset: str):
        self.dataset = dataset
        self.engine = create_engine(f"duckdb:///{self.__get_database_path()}")
        self.db_schema = self.__get_schema_info()
        
    
    def get_attributes(self, table_name: str, attribute_set: str) -> List[str]:
        """
        Retrieves a list of attribute (column) names for a specified table based on the desired attribute set.
        
        Args:
            table_name: The name of the table to retrieve attributes from.
            attribute_set: Specifies which set of attributes to return. Must be one of "all", "key", or "non_key".
        
        Returns:
            List[str]: A list of attribute names (strings). Returns an empty list if the table_name is invalid.
        """
        
        if self.db_schema.get(table_name, "") != "":
            
            if attribute_set == "all":
                return [col["name"] for col in self.db_schema[table_name]]
            
            elif attribute_set == "key":
                return [col["name"] for col in self.db_schema[table_name] if col.get("key") is True]
            
            elif attribute_set == "non_key":
                return [col["name"] for col in self.db_schema[table_name] if col.get("key") is False]
        else:
            LOG.error(ERR_INVALID_TABLE_NAME.format(table_name))
            return []
            
    
    def get_json_schema(self, table_name: str, attribute_set: str) -> dict:
        """
        Retrieves a JSON-like dictionary representation of the schema for a specified table and attribute set. The structure includes table_name, type, 
        and an 'attributes' dictionary with column names, types, and key status.
        
        Args:
            table_name: The name of the table to retrieve the JSON schema for.
            attribute_set: Specifies which set of attributes to include. Must be one of "all", "key", or "non_key".
        
        Returns:
            dict: A dictionary representing the JSON schema structure. Returns an empty list if the table_name is invalid.
        """
        
        if self.db_schema.get(table_name, "") != "":
            
            if attribute_set == "all":
                return {
                    "table_name": table_name,
                    "type": "object",
                    "attributes": {
                        col["name"]: {"type": col["type"], "key": col["key"]} for col in self.db_schema[table_name]
                    }
                }
                                
            elif attribute_set == "key":
                return {
                    "table_name": table_name,
                    "type": "object",
                    "attributes": {
                        col["name"]: {"type": col["type"], "key": col["key"]} for col in self.db_schema[table_name] if col.get("key") is True
                    }
                }
            
            elif attribute_set == "non_key":
                return {
                    "table_name": table_name,
                    "type": "object",
                    "attributes": {
                        col["name"]: {"type": col["type"], "key": col["key"]} for col in self.db_schema[table_name] if col.get("key") is False
                    }
                }
                
        else:
            LOG.error(ERR_INVALID_TABLE_NAME.format(table_name))
            return []
    
    
    def get_json_schema_from_set(self, table_name: str, attribute_set: List[str]):
        """
        Retrieves a JSON-like dictionary representation of the schema for a specified table, including only the attributes provided in the input list.
        
        The resulting structure contains the table name, type (always 'object'), and a mapping of attribute names to their data type and key status.

        Args:
            table_name: The name of the table to retrieve the JSON schema for.
            attribute_set: A list of attribute (column) names that should be included in the resulting schema dictionary.
        
        Returns:
            dict: A dictionary representing the JSON schema structure for the specified subset of attributes. Returns None if the table name 
                  is not found in the cached schema (`self.db_schema`).
        """
        
        if self.db_schema.get(table_name, "") != "":
            
            return {
                "table_name": table_name,
                    "type": "object",
                    "attributes": {
                        col["name"]: {"type": col["type"], "key": col["key"]} for col in self.db_schema[table_name] if col["name"] in set(attribute_set)    
                    }
            }     
      
            
    def dispose_manager(self) -> None:
        """
        Clears the cached schema information and disposes of the SQLAlchemy engine to release the underlying database connection.
        """
        
        self.db_schema = None
        self.engine.dispose()
     
        
    def __get_database_path(self) -> str:
        """
        Constructs the file path to the DuckDB database file based on the dataset name.

        Returns:
            str: The absolute path to the DuckDB file.
        """
        
        return str(DATA_DIR / self.dataset.upper() / f"{self.dataset.lower()}.duckdb")
    
    
    def __get_schema_info(self) -> dict:
        """
        Connects to the DuckDB database and uses SQLAlchemy's Inspector to retrieve table and column information.
        It integrates primary/unique key constraints loaded from the accompanying JSON file.
        
        Returns:
            dict: A dictionary where keys are table names and values are lists of attribute dictionaries (containing 'name', 'type', and 'key' status).
        """
        
        schema_info = {}
        
        inspector = inspect(self.engine)
        
        tables = inspector.get_table_names()
        LOG.info(f"Found tables: {tables} in dataset {self.dataset}")
        
        key_constraints = self.__get_keys_constraints()
        
        for t in tables:
            columns = inspector.get_columns(t)
            t_keys = key_constraints.get(t, [])
            
            attributes =[]
            for col in columns:
                attributes.append({"name": col['name'], "type": str(col['type']), "key": col['name'] in t_keys})

            LOG.info(f"Found columns for table {t}: {attributes}")
            schema_info[t] = attributes
        
        return schema_info


    def __get_keys_constraints(self) -> dict:
        """
        Loads the key constraints (primary/unique keys) for all tables from the dataset's accompanying JSON file.
                
        Returns:
            dict: A dictionary where keys are table names and values are lists of column names that are considered keys. 
                  Returns an empty dictionary on file or JSON decoding errors.
        """
        
        json_info = None
        
        try:
            with open(DATA_DIR / self.dataset.upper() / f"{self.dataset.lower()}.json", 'r') as f:
                json_info = json.load(f)
                LOG.info(f"Loaded JSON schema for {self.dataset}")
        
        except FileNotFoundError as e:
            LOG.error(ERR_FILE_READ_FAILURE.format(DATA_DIR / self.dataset.upper() / f"{self.dataset.lower()}.json", str(e)))
            
        except json.JSONDecodeError as e:
            LOG.error(ERR_INVALID_JSON_FORMAT.format(DATA_DIR / self.dataset.upper() / f"{self.dataset.lower()}.json", str(e)))
        
        key_constraints = {}
        if json_info is not None:
            for t in json_info.get("tables",[]):
                key_constraints[t["name"]] = t.get("keys",[])
            
        return key_constraints
    
        

if __name__ == "__main__":
    
    log_init()
    
    gsm = BaseSchemaManager("MOVIES")
    
    attributes = gsm.get_attributes("movies", "all")
    key = gsm.get_attributes("movies", "key")
    non_key = gsm.get_attributes("movies", "non_key")
    
    LOG.info(f"Attributes of table 'movies': {attributes}")
    LOG.info(f"Keys of table 'movies': {key}")
    LOG.info(f"Non-Keys of table 'movies': {non_key}")
    
    fullJ_structure = gsm.get_json_schema("movies", "all")
    keyJ_structure = gsm.get_json_schema("movies", "key")
    non_keyJ_structure = gsm.get_json_schema("movies", "non_key")
    
    d_fullJ_structure = gsm.get_json_schema_from_set("movies", attributes)
    d_keyJ_structure = gsm.get_json_schema_from_set("movies", key)
    d_non_keyJ_structure = gsm.get_json_schema_from_set("movies", non_key)

    LOG.info(f"Full JSON Structure of table 'movies': {fullJ_structure}")
    LOG.info(f"Key JSON Structure of table 'movies': {keyJ_structure}")
    LOG.info(f"Non-Key JSON Structure of table 'movies': {non_keyJ_structure}")
    
    LOG.info(f"DIFFERENT Full JSON Structure of table 'movies': {d_fullJ_structure}")
    LOG.info(f"DIFFERENT Key JSON Structure of table 'movies': {d_keyJ_structure}")
    LOG.info(f"DIFFERENT Non-Key JSON Structure of table 'movies': {d_non_keyJ_structure}")
    
    
    



        
      


 
    

