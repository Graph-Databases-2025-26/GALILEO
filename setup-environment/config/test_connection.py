import duckdb
import os
from typing import List, Dict, Any

# IMPORTANT: Replace 'path/to/your/project_db.duckdb' with the actual path to your file
DB_FILE_PATH = 'project.duckdb'


def test_duckdb_connection(db_path: str) -> None:
    """
    Establishes a connection to the DuckDB file, runs a test query,
    and checks if the database is accessible.
    """
    print(f"--- Testing Connection to: {db_path} ---")

    # 1. Check if the file exists (A good practice for reproducibility)
    if not os.path.exists(db_path):
        print(f"ERROR: Database file not found at {db_path}")
        print("Please check the DB_FILE_PATH variable and run your setup script.")
        return

    try:
        # 2. Establish connection
        # duckdb.connect() opens the file and returns a connection object
        con = duckdb.connect(database=db_path, read_only=True)
        print("SUCCESS: Connection established to DuckDB file.")


        # 3. Define a simple, robust test query
        # This query checks how many tables are in the main schema
        test_query = "SELECT region FROM country WHERE continent='Europe';"

        # 4. Execute the query
        result = con.execute(test_query).fetchall()

        # 5. Check results and display output
        if result:
            table_count = len(result)
            print(f"SUCCESS: Test query executed.")
            print(f"Found at least {table_count} tables. Example tables: {', '.join([t[0] for t in result])}")
        else:
            print("WARNING: Database file opened, but no tables found in the main schema.")

        # 6. Close the connection
        con.close()
        print("Connection closed.")



    except Exception as e:
        print(f"FATAL ERROR: Failed to connect or query the database.")
        print(f"Details: {e}")
        # Make sure to handle potential file locks or corruption issues


# --- Main execution block ---
if __name__ == "__main__":
    test_duckdb_connection(DB_FILE_PATH)