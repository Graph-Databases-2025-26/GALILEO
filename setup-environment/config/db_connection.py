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

    if not os.path.exists(dataset_folder):
        # if the folder doesn't exists, try with lower-case (es. flight-2)
        dataset_folder_lower = os.path.join(data_dir, dataset_name.lower())

        if os.path.exists(dataset_folder_lower):
            dataset_folder = dataset_folder_lower
        else:
            # If both pattern doesn't exists -> error
            raise FileNotFoundError(
                f" Folder not found. Check if the folder exists '{dataset_name}' o '{dataset_name.lower()}' in {data_dir}"
            )

    db_file_name_lower = dataset_name.lower()
    db_path = os.path.join(dataset_folder, f"{db_file_name_lower}.duckdb")

    if not os.path.exists(db_path):
        raise FileNotFoundError(f"❌ Database not found: {db_path}")

    # Create connection
    con = duckdb.connect(database=db_path, read_only=False)
    print(f"Connection established with {db_path}")
    return con
