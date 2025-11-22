from src.utils.get_db_schema_galois import GaloisSchemaManager
from src.utils.logging_config import LOG


class GaloisWOSchemaManager(GaloisSchemaManager):
    """
    Thin wrapper around the core GaloisSchemaManager.

    This class exists only to keep the 'galois_wo' namespace clean and
    to emphasise that this schema manager is used by the Galois-WO
    executor (Table-Scan / Key-Scan).
    The actual logic for connecting to DuckDB and retrieving schema
    information is implemented in `src.utils.get_db_schema_galois.GaloisSchemaManager`.
    """

    def __init__(self, dataset_name: str):
        LOG.info(f"[GaloisWOSchemaManager] Initialising for dataset '{dataset_name}'")
        super().__init__(dataset_name)
