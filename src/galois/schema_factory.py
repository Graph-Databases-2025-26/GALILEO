from .baseline_schema_manager import GaloisWOSchemaManager, GaloisASchemaManager, GaloisFSchemaManager, GaloisSSchemaManager 
from src.utils import ERR_UNSUPPORTED_BASELINE

from typing import List, Literal

BASELINE_TYPE = Literal["GaloisWO", "GaloisA", "GaloisS", "GaloisF"]

class GaloisSchemaManagerWrapper:
    """
    A wrapper class that dynamically selects and instantiates the correct Schema Manager (e.g., GaloisWOSchemaManager) based on the specified 
    baseline type, providing a unified public interface for schema access.
    
    Attributes:
        manager_instance (BaseSchemaManager): The concrete instance of the Schema Manager class that handles the schema operations for the specific baseline type.
    """
    
    def __init__(self, baseline_type: BASELINE_TYPE, dataset_name: str):
        """
        Initializes the wrapper by instantiating the appropriate concrete Schema Manager based on the baseline type.
        
        Args:
            baseline_type: A literal string defining the type of baseline (e.g., "GaloisWO", "GaloisA"). Used to select the correct manager class.
            dataset_name: The name of the dataset to be managed (e.g., 'WORLD').
        
        Raises:
            ValueError: If the provided baseline_type is not supported (i.e., not found in MANAGER_CLASSES).
        """
        
        MANAGER_CLASSES = {
            "GaloisWO": GaloisWOSchemaManager,
            "GaloisA": GaloisASchemaManager,
            "GaloisSS": GaloisSSchemaManager,
            "GaloisF": GaloisFSchemaManager,
        }
        
        ManagerClass = MANAGER_CLASSES.get(baseline_type)
        
        if ManagerClass is None:
            raise ValueError(ERR_UNSUPPORTED_BASELINE.format(baseline_type))
            
        self.manager_instance = ManagerClass(dataset_name)
 
        
    def get_attributes(self, table_name: str, attribute_set: str) -> List[str]:
        """ 
        Retrieves a list of attribute (column) names for a specified table and attribute set ("all", "key", or "non_key"). Delegates to the 
        underlying manager instance's `get_attribute` method, which includes specific logging.
        
        Args:
            table_name: The name of the table to retrieve attributes from.
            attribute_set: Specifies which set of attributes to return. Must be one of "all", "key", or "non_key".
                           
        Returns:
            List[str]: A list of attribute names (strings).
        """
        
        return self.manager_instance.get_attributes(table_name, attribute_set)
    
    
    def get_json_schema(self, table_name: str, attribute_set: str) -> dict:
        """ 
        Retrieves a JSON-like dictionary representation of the schema for a specified table and attribute set ("all", "key", or "non_key"). 
        Delegates to the underlying manager instance.
        
        Args:
            table_name: The name of the table to retrieve the JSON schema for.
            attribute_set: Specifies which set of attributes to include. Must be one of "all", "key", or "non_key".
                           
        Returns:
            dict: A dictionary representing the JSON schema structure.
        """
        
        return self.manager_instance.get_json_schema(table_name, attribute_set)
    
    
    def get_json_schema_from_set(self, table_name: str, attribute_set: List[str]) ->dict:
        """ 
        Retrieves a JSON-like dictionary representation of the schema for a specified table, including only the attributes provided in the input list. 
        Delegates to the underlying manager instance.
        
        Args:
            table_name: The name of the table to retrieve the JSON schema for.
            attribute_set: A list of attribute (column) names that should be included in the resulting schema dictionary.
                           
        Returns:
            dict: A dictionary representing the JSON schema structure for the specified subset of attributes.
        """
        
        return self.manager_instance.get_json_schema(table_name, attribute_set)
    
    
    def dispose_manager(self) -> None:
        """ 
        Disposes of the underlying schema manager, closing any active database connections and releasing resources.
        """
        
        return self.manager_instance.dispose_manager()

    
