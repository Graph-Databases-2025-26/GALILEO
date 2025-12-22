from .base_schema_manager import BaseSchemaManager
from src.utils import LOG

from typing import List

class GaloisWOSchemaManager(BaseSchemaManager):
    """
    A specific schema manager for the 'GaloisWO' type of dataset.
    It extends BaseSchemaManager, adding logging within the public methods for attribute and JSON schema retrieval, and a method to close the DB connection.
    """
    
    def __init__(self, dataset: str):
        """
        Initializes the GaloisWOSchemaManager, logging the initialization and calling the parent class constructor.
        """
        
        LOG.info(f"[GaloisWOSchemaManager] Initialising for dataset '{dataset}'")
        super().__init__(dataset)
    
        
    def get_attributes(self, table_name: str, attribute_set: str) -> List[str]:
        """
        Retrieves a list of attribute names, adding logging for the operation. 
        Delegates the core logic to the parent class method.
        
        Args:
            table_name: The name of the table.
            attribute_set: Specifies which set of attributes to include. Must be one of "all", "key", or "non_key".
        
        Returns:
            List[str]: A list of attribute names.
        """
        
        attrs = super().get_attributes(table_name, attribute_set)
        LOG.debug(f"[GaloisWOSchemaManager] Retrieved '{attribute_set}' attributes for table '{table_name}': {attrs}")
        return attrs
    

    def get_json_schema(self, table_name: str, attribute_set: str) -> dict:
        """
        Retrieves the JSON-like schema, adding logging for the operation. 
        Delegates the core logic to the parent class method.
        
        Args:
            table_name: The name of the table.
            attribute_set: Specifies which set of attributes to include. Must be one of "all", "key", or "non_key".
        
        Returns:
            dict: A dictionary representing the JSON schema structure.
        """
        
        schema = super().get_json_schema(table_name, attribute_set)
        LOG.info(f"[GaloisWOSchemaManager] Retrieved JSON schema for table '{table_name}' with '{attribute_set}' attributes: {schema}")
        return schema
    
    
    def get_json_schema_from_set(self, table_name: str, attribute_set: List[str]) -> dict:
        """
        Retrieves the JSON-like schema, adding logging for the operation. 
        Delegates the core logic to the parent class method.
        
        Args:
            table_name: The name of the table.
            attribute_set: A list of attribute (column) names that should be included in the resulting schema dictionary.
        
        Returns:
            dict: A dictionary representing the JSON schema structure.
        """
        
        schema = super().get_json_schema_from_set(table_name, attribute_set)
        LOG.info(f"[GaloisWOSchemaManager] Retrieved JSON schema for table '{table_name}' with '{attribute_set}' attributex set: {schema}")
        return schema
    
    
    def dispose_manager(self) -> None:
        """
        Closes the underlying DuckDB database connection and disposes of the manager.
        """
        
        LOG.info(f"[GaloisWOSchemaManager] Closing DuckDB connection for dataset '{self.dataset}'")
        super().dispose_manager()
    
        

class GaloisASchemaManager(BaseSchemaManager):
    """
    A specific schema manager for the 'GaloisA' type of dataset.
    It extends BaseSchemaManager, adding logging within the public methods for attribute and JSON schema retrieval, and a method to close the DB connection.
    """
    
    def __init__(self, dataset: str):
        """
        Initializes the GaloisASchemaManager, logging the initialization and calling the parent class constructor.
        """
        
        LOG.info(f"[GaloisASchemaManager] Initialising for dataset '{dataset}'")
        super().__init__(dataset)
    
        
    def get_attributes(self, table_name: str, attribute_set: str) -> List[str]:
        """
        Retrieves a list of attribute names, adding logging for the operation. 
        Delegates the core logic to the parent class method.
        
        Args:
            table_name: The name of the table.
            attribute_set: Specifies which set of attributes to include. Must be one of "all", "key", or "non_key".
        
        Returns:
            List[str]: A list of attribute names.
        """
        
        attrs = super().get_attributes(table_name, attribute_set)
        LOG.info(f"[GaloisASchemaManager] Retrieved '{attribute_set}' attributes for table '{table_name}': {attrs}")
        return attrs
    

    def get_json_schema(self, table_name: str, attribute_set: str) -> dict:
        """
        Retrieves the JSON-like schema, adding logging for the operation. 
        Delegates the core logic to the parent class method.
        
        Args:
            table_name: The name of the table.
            attribute_set: Specifies which set of attributes to include. Must be one of "all", "key", or "non_key".
        
        Returns:
            dict: A dictionary representing the JSON schema structure.
        """
        
        schema = super().get_json_schema(table_name, attribute_set)
        LOG.info(f"[GaloisASchemaManager] Retrieved JSON schema for table '{table_name}' with '{attribute_set}' attributex set: {schema}")
        return schema
    
    
    def get_json_schema_from_set(self, table_name: str, attribute_set: List[str]) -> dict:
        """
        Retrieves the JSON-like schema, adding logging for the operation. 
        Delegates the core logic to the parent class method.
        
        Args:
            table_name: The name of the table.
            attribute_set: A list of attribute (column) names that should be included in the resulting schema dictionary.
        
        Returns:
            dict: A dictionary representing the JSON schema structure.
        """
        
        schema = super().get_json_schema_from_set(table_name, attribute_set)
        LOG.info(f"[GaloisASchemaManager] Retrieved JSON schema for table '{table_name}' with '{attribute_set}' attributex set: {schema}")
        return schema
    
    
    def dispose_manager(self) -> None:
        """
        Closes the underlying DuckDB database connection and disposes of the manager.
        """
        
        LOG.info(f"[GaloisASchemaManager] Closing DuckDB connection for dataset '{self.dataset}'")
        super().dispose_manager()


class GaloisSSchemaManager(BaseSchemaManager):
    """
    A specific schema manager for the 'GaloisS' type of dataset.
    It extends BaseSchemaManager, adding logging within the public methods for attribute and JSON schema retrieval, and a method to close the DB connection.
    """
    
    def __init__(self, dataset: str):
        """
        Initializes the GaloisSSchemaManager, logging the initialization and calling the parent class constructor.
        """
        
        LOG.info(f"[GaloisSSchemaManager] Initialising for dataset '{dataset}'")
        super().__init__(dataset)
    
        
    def get_attributes(self, table_name: str, attribute_set: str) -> List[str]:
        """
        Retrieves a list of attribute names, adding logging for the operation. 
        Delegates the core logic to the parent class method.
        
        Args:
            table_name: The name of the table.
            attribute_set: Specifies which set of attributes to include. Must be one of "all", "key", or "non_key".        
        
        Returns:
            List[str]: A list of attribute names.
        """
        
        attrs = super().get_attributes(table_name, attribute_set)
        LOG.info(f"[GaloisSSchemaManager] Retrieved '{attribute_set}' attributes for table '{table_name}': {attrs}")
        return attrs
    

    def get_json_schema(self, table_name: str, attribute_set: str) -> dict:
        """
        Retrieves the JSON-like schema, adding logging for the operation. 
        Delegates the core logic to the parent class method.
        
        Args:
            table_name: The name of the table.
            attribute_set: Specifies which set of attributes to include. Must be one of "all", "key", or "non_key".
        
        Returns:
            dict: A dictionary representing the JSON schema structure.
        """
        
        schema = super().get_json_schema(table_name, attribute_set)
        LOG.info(f"[GaloisSSchemaManager] Retrieved JSON schema for table '{table_name}' with '{attribute_set}' attributex set: {schema}")
        return schema
    
    
    def get_json_schema_from_set(self, table_name: str, attribute_set: List[str]) -> dict:
        """
        Retrieves the JSON-like schema, adding logging for the operation. 
        Delegates the core logic to the parent class method.
        
        Args:
            table_name: The name of the table.
            attribute_set: A list of attribute (column) names that should be included in the resulting schema dictionary.
        
        Returns:
            dict: A dictionary representing the JSON schema structure.
        """
        
        schema = super().get_json_schema_from_set(table_name, attribute_set)
        LOG.info(f"[GaloisSSchemaManager] Retrieved JSON schema for table '{table_name}' with '{attribute_set}' attributex set: {schema}")
        return schema
    
    
    def dispose_manager(self) -> None:
        """
        Closes the underlying DuckDB database connection and disposes of the manager.
        """
        
        LOG.info(f"[GaloisSSchemaManager] Closing DuckDB connection for dataset '{self.dataset}'")
        super().dispose_manager()


class GaloisFSchemaManager(BaseSchemaManager):
    """
    A specific schema manager for the 'GaloisF' type of dataset.
    It extends BaseSchemaManager, adding logging within the public methods for attribute and JSON schema retrieval, and a method to close the DB connection.
    """
    
    def __init__(self, dataset: str):
        """
        Initializes the GaloisFSchemaManager, logging the initialization and calling the parent class constructor.
        """
        
        LOG.info(f"[GaloisFSchemaManager] Initialising for dataset '{dataset}'")
        super().__init__(dataset)
    
        
    def get_attributes(self, table_name: str, attribute_set: str) -> List[str]:
        """
        Retrieves a list of attribute names, adding logging for the operation. 
        Delegates the core logic to the parent class method.
        
        Args:
            table_name: The name of the table.
            attribute_set: Specifies which set of attributes to include. Must be one of "all", "key", or "non_key".
        
        Returns:
            List[str]: A list of attribute names.
        """
       
        attrs = super().get_attributes(table_name, attribute_set)
        LOG.info(f"[GaloisFSchemaManager] Retrieved '{attribute_set}' attributes for table '{table_name}': {attrs}")
        return attrs
    

    def get_json_schema(self, table_name: str, attribute_set: str) -> dict:
        """
        Retrieves the JSON-like schema, adding logging for the operation. 
        Delegates the core logic to the parent class method.
        
        Args:
            table_name: The name of the table.
            attribute_set: Specifies which set of attributes to include. Must be one of "all", "key", or "non_key".
        
        Returns:
            dict: A dictionary representing the JSON schema structure.
        """
        
        schema = super().get_json_schema(table_name, attribute_set)
        LOG.info(f"[GaloisFSchemaManager] Retrieved JSON schema for table '{table_name}' with '{attribute_set}' attributex set: {schema}")
        return schema
    
    
    def get_json_schema_from_set(self, table_name: str, attribute_set: List[str]) -> dict:
        """
        Retrieves the JSON-like schema, adding logging for the operation. 
        Delegates the core logic to the parent class method.
        
        Args:
            table_name: The name of the table.
            attribute_set: A list of attribute (column) names that should be included in the resulting schema dictionary.
        
        Returns:
            dict: A dictionary representing the JSON schema structure.
        """
        
        schema = super().get_json_schema_from_set(table_name, attribute_set)
        LOG.info(f"[GaloisFSchemaManager] Retrieved JSON schema for table '{table_name}' with '{attribute_set}' attributex set: {schema}")
        return schema
    
    
    def dispose_manager(self) -> None:
        """
        Closes the underlying DuckDB database connection and disposes of the manager.
        """
        
        LOG.info(f"[GaloisFSchemaManager] Closing DuckDB connection for dataset '{self.dataset}'")
        super().dispose_manager()