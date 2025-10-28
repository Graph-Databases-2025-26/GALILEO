from pathlib import Path
import duckdb
import os
import glob
import time
import sys

from src.utils.logging_config import logger
from src.utils.constants import DATASETS

# base path for the project
PROJECT_ROOT = "."

# Folder from which load teh data
DATA_SOURCE_DIR = os.path.join(PROJECT_ROOT, "../data")
# Folder in which save the database instance
DB_PATH = os.path.join(PROJECT_ROOT, "project.duckdb")

HERE = Path(__file__).resolve().parent               # .../setup-environment/config
SETUP_ENV = HERE.parent                              # .../setup-environment
DATA_SOURCE_DIR = SETUP_ENV / "data"                 # default: .../setup-environment/data

# Optional fallback if your team keeps datasets at repo_root/data
PROJECT_ROOT = SETUP_ENV.parent                      # repo root
ALT_DATA = PROJECT_ROOT / "data"                     # .../data
if not DATA_SOURCE_DIR.exists() and ALT_DATA.exists():
    DATA_SOURCE_DIR = ALT_DATA

""""""
# Function for creating tables and loading data using the "ingest_'foldername'.sql"
def execute_ingest_sql(con, folder_path: str):
    """
    Execute the ingest_<name>.sql file in the specified folder.
    The ingest_<name>.sql file should contain the SQL query to create the table and load the data.
    """

    cwd = os.getcwd()  # save original cwd
    os.chdir(folder_path)  # move cwd in the sql file folder

    try:
        # Searching for the ingest_<name>.sql file
        ingest_sql_files = sorted(glob.glob( "ingest_*.sql"))

        if not ingest_sql_files:
            logger.error(f"ingest_<name>.sql file not found in {folder_path}")
            return

        print(f"\n Folder: {folder_path}")
        for ingest_file in ingest_sql_files:
            logger.info(f"Run: {ingest_file}")
            try:
                with open(ingest_file, "r", encoding="utf-8") as f:
                    sql_script = f.read()
                con.execute(sql_script)
                con.commit()
                logger.info(f"Execution done: {os.path.basename(ingest_file)}")
            except Exception as e:
                logger.error(f"Error in {ingest_file}: {e}")
    finally:
        os.chdir(cwd)

# Select which datasets to process
def get_selected_datasets() -> list[str]:
    if len(sys.argv) > 1:
        arg = sys.argv[1].upper()
        logger.info(f"Input parameter: {arg}")
    else:
        arg = "ALL"
        logger.info("No provided parameter, dafault: ALL")

    if arg == "ALL":
        return DATASETS
    elif arg in DATASETS:
        return [arg]
    else:
        logger.warning(f"Dataset '{arg}' not valid. The available datasets are: {DATASETS}")
        return []

# --- Main ---
if __name__ == "__main__":
    logger.info(f"Starting database creation from: {DATA_SOURCE_DIR}")

    if not DATA_SOURCE_DIR.exists():
       raise FileNotFoundError(f"Data folder not found at {DATA_SOURCE_DIR}")

    selected_datasets = get_selected_datasets()
    if not selected_datasets:
        logger.warning("Any valid dataset provided")
        sys.exit(1)
    # Scan all subfolders inside ../data/
    subfolders = [p for p in DATA_SOURCE_DIR.iterdir() if p.is_dir() and p.name in selected_datasets]

    # Process each dataset folder independently
    for folder in sorted(subfolders, key =lambda p: p.name.lower()):
        dataset_name = folder.name.lower()
        db_path = folder / f"{dataset_name}.duckdb"

        logger.info(f"Creating database for dataset: {dataset_name}")
        logger.info(f"Path: {db_path}")

        # Remove existing database if present
        if db_path.exists():
            try:
                db_path.unlink()
                logger.info(f"Removed old database: {db_path}")
            except Exception as e:
                logger.error(f"Unable to remove old database {db_path}: {e}")
                continue


        # Create new database
        t0 = time.time()
        con = duckdb.connect(db_path)
        logger.info("Connection established")

        # Execute ingest SQL scripts
        execute_ingest_sql(con, folder)

        # Show created tables
        logger.info("Listing created tables...")
        try:
            tables_df = con.execute("""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                ORDER BY table_schema, table_name
            """).fetchdf()

            if tables_df.empty:
                logger.warning("No tables found.")
            else:
                logger.info(f"Created tables ({len(tables_df)}):\n{tables_df}")
        except Exception as e:
            logger.error(f"Unable to list tables: {e}")

        # Close connection
        con.close()
        elapsed_ms = (time.time() - t0) * 1000.0
        logger.info(f"Connection closed. Database saved at: {db_path} | latency_ms={elapsed_ms:.1f}")

    logger.info("All dataset databases have been created successfully!")