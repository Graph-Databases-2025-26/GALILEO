from typing import List
from src.utils import GaloisSchemaManager
from src.utils import LOG


class GaloisWOSchemaManager(GaloisSchemaManager):
    """
    Thin wrapper around the core GaloisSchemaManager.

    This class exists only to keep the 'galois' namespace clean and
    to emphasise that this schema manager is used by the Galois-WO
    executor (Table-Scan / Key-Scan).
    The actual logic for connecting to DuckDB and retrieving schema
    information is implemented in `src.utils.get_db_schema_galois.GaloisSchemaManager`.
    """

    def __init__(self, dataset_name: str):
        LOG.info(f"[GaloisWOSchemaManager] Initialising for dataset '{dataset_name}'")
        super().__init__(dataset_name)

    
    def get_key_attributes(self, table_name: str) -> List[str]:
        """
        Retrieve key attributes (Primary Key) for a table.
        Overrides the parent method to add logging specific to Galois-WO.
        """
        keys = super().get_key_attributes(table_name)
        LOG.info(f"[GaloisWOSchemaManager] Retrieved key attributes for table '{table_name}': {keys}")
        return keys
    
    def get_attributes(self, table_name: str) -> List[str]:
        """
        Retrieve all attributes (columns) for a table.
        Overrides the parent method to add logging specific to Galois-WO.
        """
        attrs = super().get_attributes(table_name)
        LOG.info(f"[GaloisWOSchemaManager] Retrieved attributes for table '{table_name}': {attrs}")
        return attrs
    
    def get_exact_table_name(self, table_name: str) -> str:
        """
        Retrieves the exact name of the table in the database, ignoring case sensitivity.
        Overrides the parent method to add logging specific to Galois-WO.
        """
        exact_name = super().get_exact_table_name(table_name)
        LOG.info(f"[GaloisWOSchemaManager] Exact table name for '{table_name}': {exact_name}")
        return exact_name
    
    def close_connection(self) -> None:
        """
        Close the DuckDB connection.
        """
        LOG.info(f"[GaloisWOSchemaManager] Closing DuckDB connection for dataset '{self.database_name}'")
        self.con.close()
        
    def get_json_schema_example(self, table_name, attributes_list):
        return super().get_json_schema_example(table_name, attributes_list)     
    