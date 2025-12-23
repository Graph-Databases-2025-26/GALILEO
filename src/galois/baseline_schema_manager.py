from .base_schema_manager import BaseSchemaManager
from src.utils import LOG
from typing import List

def _log_schema_flat(prefix_tag: str, table_name: str, schema: dict):
    """
    Logs JSON schema with '     | ' indentation to align with tags.
    """
    if not schema:
        return

    t_name = schema.get('table_name', 'N/A')
    t_type = schema.get('type', 'N/A')

    LOG.info(f"{prefix_tag} JSON Structure: '{t_name}'")

    if 'attributes' in schema:
        w_col = 25
        w_type = 12
        w_key = 6
        w_extra = 40

        # Prefisso di allineamento (5 spazi + pipe + spazio)
        p = "     | "

        # Costruzione Tabella
        LOG.info(f"{p}┌{'─'*w_col}┬{'─'*w_type}┬{'─'*w_key}┬{'─'*w_extra}┐")
        LOG.info(f"{p}│ {'Column Name':<{w_col-1}}│ {'Type':<{w_type-1}}│ {'Key':<{w_key-1}}│ {'Extras':<{w_extra-1}}│")
        LOG.info(f"{p}├{'─'*w_col}┼{'─'*w_type}┼{'─'*w_key}┼{'─'*w_extra}┤")

        for col_name, col_data in schema['attributes'].items():
            c_type = col_data.get('type', 'Unknown')
            is_key = "KEY" if col_data.get('key') else "" 
            
            extras = ""
            if 'examples' in col_data:
                ex_list = col_data['examples']
                if isinstance(ex_list, list):
                    ex_str = ", ".join(str(x) for x in ex_list)
                else:
                    ex_str = str(ex_list)
                extras = f"Ex: [{ex_str}]"
            
            if len(extras) > w_extra - 1:
                extras = extras[:w_extra - 4] + "..."
            
            LOG.info(f"{p}│ {col_name:<{w_col-1}}│ {c_type:<{w_type-1}}│ {is_key:<{w_key-1}}│ {extras:<{w_extra-1}}│")

        LOG.info(f"{p}└{'─'*w_col}┴{'─'*w_type}┴{'─'*w_key}┴{'─'*w_extra}┘")


class GaloisWOSchemaManager(BaseSchemaManager):
    def __init__(self, dataset: str):
        super().__init__(dataset)
    
    def get_json_schema(self, table_name: str, attribute_set: str) -> dict:
        schema = super().get_json_schema(table_name, attribute_set)
        _log_schema_flat("DATA | [SCHEMA]", table_name, schema)
        return schema
    
    def get_json_schema_from_set(self, table_name: str, attribute_set: List[str]) -> dict:
        schema = super().get_json_schema_from_set(table_name, attribute_set)
        _log_schema_flat("DATA | [SCHEMA]", table_name, schema)
        return schema
    
    def dispose_manager(self) -> None:
        LOG.debug(f"DBUG | [SCHEMA] Dispose Manager '{self.dataset}'")
        super().dispose_manager()


class GaloisASchemaManager(BaseSchemaManager):
    def __init__(self, dataset: str):
        super().__init__(dataset)

    def get_json_schema(self, table_name: str, attribute_set: str) -> dict:
        schema = super().get_json_schema(table_name, attribute_set)
        _log_schema_flat("DATA | [SCHEMA]", table_name, schema)
        return schema
    
    def get_json_schema_from_set(self, table_name: str, attribute_set: List[str]) -> dict:
        schema = super().get_json_schema_from_set(table_name, attribute_set)
        _log_schema_flat("DATA | [SCHEMA]", table_name, schema)
        return schema
    
    def dispose_manager(self) -> None:
        super().dispose_manager()


class GaloisSSchemaManager(BaseSchemaManager):
    def __init__(self, dataset: str):
        super().__init__(dataset)

    def get_json_schema(self, table_name: str, attribute_set: str) -> dict:
        schema = super().get_json_schema(table_name, attribute_set)
        _log_schema_flat("DATA | [SCHEMA]", table_name, schema)
        return schema
    
    def get_json_schema_from_set(self, table_name: str, attribute_set: List[str]) -> dict:
        schema = super().get_json_schema_from_set(table_name, attribute_set)
        _log_schema_flat("DATA | [SCHEMA]", table_name, schema)
        return schema
    
    def dispose_manager(self) -> None:
        super().dispose_manager()


class GaloisFSchemaManager(BaseSchemaManager):
    def __init__(self, dataset: str):
        super().__init__(dataset)

    def get_json_schema(self, table_name: str, attribute_set: str) -> dict:
        schema = super().get_json_schema(table_name, attribute_set)
        _log_schema_flat("DATA | [SCHEMA]", table_name, schema)
        return schema
    
    def get_json_schema_from_set(self, table_name: str, attribute_set: List[str]) -> dict:
        schema = super().get_json_schema_from_set(table_name, attribute_set)
        _log_schema_flat("DATA | [SCHEMA]", table_name, schema)
        return schema
    
    def dispose_manager(self) -> None:
        super().dispose_manager()