from src.utils import LOG, DATASETS, DATA_DIR
from pathlib import Path

import duckdb, os, glob, time, sys

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
            LOG.error(f"ingest_<name>.sql file not found in {folder_path}")
            return

        LOG.trace(f"Folder: {folder_path}")
        for ingest_file in ingest_sql_files:
            LOG.info(f"Run: {ingest_file}")
            try:
                with open(ingest_file, "r", encoding="utf-8") as f:
                    sql_script = f.read()
                con.execute(sql_script)
                con.commit()
                LOG.info(f"Execution done: {os.path.basename(ingest_file)}")
            except Exception as e:
                LOG.error(f"Error in {ingest_file}: {e}")
    finally:
        os.chdir(cwd)

# Select which datasets to process
def get_selected_datasets(dataset_name: str) -> list[str]:
    if dataset_name is None:
        LOG.info("No provided parameter, default: ALL")
        return DATASETS
    
    if dataset_name == "ALL":
        LOG.info("Parameter ALL provided, processing all datasets")
        return DATASETS
    elif dataset_name in DATASETS:
        LOG.info(f"Parameter {dataset_name} provided, processing only this dataset")
        return [dataset_name]
    else:
        LOG.warning(f"Dataset '{dataset_name}' not valid. The available datasets are: {DATASETS}")
        return []
    

def db_creation(dataset_name: str) -> None:
    LOG.info(f"Starting database creation for dataset: {dataset_name}")

    if not DATA_DIR.exists():
       raise FileNotFoundError(f"Data folder not found at {DATA_DIR}")

    selected_datasets = get_selected_datasets(dataset_name)
    
    if not selected_datasets:
        LOG.warning("Any valid dataset provided")
        sys.exit(1)
        
    # Scan all subfolders inside ../data/
    subfolders = [p for p in DATA_DIR.iterdir() if p.is_dir() and p.name in selected_datasets]

    # Process each dataset folder independently
    for folder in sorted(subfolders, key =lambda p: p.name.lower()):
        dataset_name = folder.name.lower()
        db_path = folder / f"{dataset_name}.duckdb"

        LOG.info(f"Creating database for dataset: {dataset_name}")
        LOG.info(f"Path: {db_path}")

        # Remove existing database if present
        if db_path.exists():
            try:
                db_path.unlink()
                LOG.info(f"Removed old database: {db_path}")
            except Exception as e:
                LOG.error(f"Unable to remove old database {db_path}: {e}")
                continue


        # Create new database
        t0 = time.time()
        con = duckdb.connect(db_path)
        LOG.info("Connection established")

        # Execute ingest SQL scripts
        execute_ingest_sql(con, folder)

        # Show created tables
        LOG.info("Listing created tables...")
        try:
            table_names_tuples = con.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
            ORDER BY table_name
            """).fetchall()

            table_names = [name[0] for name in table_names_tuples]
            
            if not table_names: 
                LOG.warning("No tables found.")
            else:
                LOG.info(f"Created tables ({len(table_names)}):")
                for tbl in table_names:
                    LOG.info(f" - {tbl}")
        except Exception as e:
            LOG.error(f"Unable to list tables: {e}")    

        # Close connection
        con.close()
        elapsed_ms = (time.time() - t0) * 1000.0
        LOG.info(f"Connection closed. Database saved at: {db_path} | latency_ms={elapsed_ms:.1f}")

    LOG.info("All dataset databases have been created successfully!")



# --- Main ---
if __name__ == "__main__":
    db_creation("ALL")
