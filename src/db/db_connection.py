# config/db_connection.py

import duckdb
import os
from src.utils import DATA_DIR
from src.utils.logging_config import logger

def connect_to_duckdb(dataset_name: str):
    """
    Create a connection to the DuckDB database for the given dataset.

    Args:
        dataset_name (str): Name of the dataset folder (e.g. "geo", "movies", "world")

    Returns:
        duckdb.DuckDBPyConnection: A DuckDB connection object
    """
    # Compute paths relative to this file (config/)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = DATA_DIR

    dataset_folder = os.path.join(data_dir, dataset_name)

    if not os.path.exists(dataset_folder):
        # try lower-case (e.g., "flight-2")
        dataset_folder_lower = os.path.join(data_dir, dataset_name.lower())
        if os.path.exists(dataset_folder_lower):
            logger.warning(
                f"Dataset folder '{dataset_name}' not found, using lowercase folder '{dataset_name.lower()}'"
            )
            dataset_folder = dataset_folder_lower
        else:
            msg = (
                f"Folder not found for dataset '{dataset_name}' or '{dataset_name.lower()}' in {data_dir}"
            )
            logger.error(msg)
            raise FileNotFoundError(msg)

    db_file_name_lower = dataset_name.lower()
    db_path = os.path.join(dataset_folder, f"{db_file_name_lower}.duckdb")

    if not os.path.exists(db_path):
        msg = f"Database not found: {db_path}"
        logger.error(msg)
        raise FileNotFoundError(msg)

    # Create connection
    con = duckdb.connect(database=db_path, read_only=False)
    logger.info(f"DuckDB connection established → {db_path}")
    return con
