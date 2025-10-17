# config/db_connection.py

import duckdb
import os


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
    data_dir = os.path.abspath(os.path.join(base_dir, "../data"))

    dataset_folder = os.path.join(data_dir, dataset_name)
    db_path = os.path.join(dataset_folder, f"{dataset_name}.duckdb")

    if not os.path.exists(db_path):
        raise FileNotFoundError(f"❌ Database not found: {db_path}")

    # Create connection
    con = duckdb.connect(database=db_path, read_only=False)
    print(f"Connection established with {db_path}")
    return con
