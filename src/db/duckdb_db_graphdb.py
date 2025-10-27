from pathlib import Path
import duckdb
import os
import glob

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
            print(f"ERROR: ingest_<name>.sql file not found in {folder_path}")
            return

        print(f"\n Folder: {folder_path}")
        for ingest_file in ingest_sql_files:
            print(f"  Run: {ingest_file}")
            try:
                with open(ingest_file, "r", encoding="utf-8") as f:
                    sql_script = f.read()
                con.execute(sql_script)
                con.commit()
                print(f" Execution done: {os.path.basename(ingest_file)}")
            except Exception as e:
                print(f" Error in {ingest_file}: {e}")
    finally:
        os.chdir(cwd)

# --- Main ---
if __name__ == "__main__":
    print(f" Starting database creation from: {DATA_SOURCE_DIR}")

    if not DATA_SOURCE_DIR.exists():
       raise FileNotFoundError(f"Data folder not found at {DATA_SOURCE_DIR}")

    # Datasets
    TARGET_DATASETS = {"FLIGHT-2", "FLIGHT-4", "FORTUNE", "GEO", "MOVIES", "PRESIDENTS", "PREMIER", "WORLD"}
    # Scan all subfolders inside ../data/
    subfolders = [p for p in DATA_SOURCE_DIR.iterdir() if p.is_dir() and p.name in TARGET_DATASETS]

    # Process each dataset folder independently
    for folder in sorted(subfolders, key =lambda p: p.name.lower()):
        dataset_name = folder.name.lower()
        db_path = folder / f"{dataset_name}.duckdb"

        print(f"\n Creating database for dataset: {dataset_name}")
        print(f" Path: {db_path}")

        # Remove existing database if present
        if db_path.exists():
            db_path.unlink()
            print(f" Removed old database: {db_path}")

        # Create new database
        con = duckdb.connect(db_path)
        print(f" Connection established")

        # Execute ingest SQL scripts
        execute_ingest_sql(con, folder)

        # Show created tables
        print("\n  Tables created in this database:")
        try:
            tables_df = con.execute("""
                                    SELECT table_schema, table_name
                                    FROM information_schema.tables
                                    WHERE table_type = 'BASE TABLE'
                                    ORDER BY table_schema, table_name
                                    """).fetchdf()

            if tables_df.empty:
                print(" No tables found.")
            else:
                print(tables_df)
        except Exception as e:
            print(f" Unable to list tables: {e}")

        # Close connection
        con.close()
        print(f" Connection closed. Database saved at: {db_path}")

    print("\n All dataset databases have been created successfully!")