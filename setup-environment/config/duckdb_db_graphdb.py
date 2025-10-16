import duckdb
import os
import glob

# base path for the project
PROJECT_ROOT = "."

# Folder from which load teh data
DATA_SOURCE_DIR = os.path.join(PROJECT_ROOT, "../data")
# Folder in which save the database instance
DB_PATH = os.path.join(PROJECT_ROOT, "project.duckdb")

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
        # Searchinf for the ingest_<name>.sql file
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

    # recreate the db from scratch for avoiding conflicts
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    # Create or open the DuckDB database
    con = duckdb.connect(DB_PATH)
    print(f" Connection established with {DB_PATH}")

    # Scan all subfolders inside ../data/
    subfolders = [
        os.path.join(DATA_SOURCE_DIR, d)
        for d in os.listdir(DATA_SOURCE_DIR)
        if os.path.isdir(os.path.join(DATA_SOURCE_DIR, d))
    ]

    # Execute all ingest_*.sql files found in each subfolder
    for folder in sorted(subfolders):
        execute_ingest_sql(con, folder)

    # Show all tables created
    print("\n Tables created in the database:")
    try:
        tables_df = con.execute("""
                                SELECT table_schema, table_name
                                FROM information_schema.tables
                                WHERE table_type = 'BASE TABLE'
                                ORDER BY table_schema, table_name
                                """).fetchdf()

        if tables_df.empty:
            print("No tables found in the database.")
        else:
            print(tables_df)

    except Exception as e:
        print(f"Unable to list tables: {e}")

    # Close the connection
    con.close()
    print(f"\n Connection closed. Database saved at: {DB_PATH}")