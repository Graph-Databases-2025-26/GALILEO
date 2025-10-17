import os
import json
import glob
import sys
from db_connection import connect_to_duckdb

# --- Paths ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) #current script path
DB_FILE_PATH = os.path.abspath(os.path.join(BASE_DIR, "../data"))  # Path to DuckDB file
OUTPUT_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../verification-test-tools/tests" )) # Folder to save JSON results

os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_queries_from_folder(folder_path: str):
    """
    Read all .sql files in the folder and return a list of queries.
    Returns a list of tuples: (filename, query_string)
    """
    queries = []

    for file_path in sorted(glob.glob(os.path.join(folder_path, "queries_*.sql"))):
        print("ciao2")
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            # Split multiple queries in a single file by ";"
            for q in content.split(";"):
                q = q.strip()
                if q:  # Skip empty strings
                    print(f"Query: {q}")
                    queries.append((os.path.basename(file_path), q))

    print(f"Found {len(queries)} queries in 'queries_*.sql' file")
    return queries


def execute_queries_and_save_json(con, queries, output_dir):
    """
    Execute each query on the DuckDB connection and save results as JSON files.
    """
    for i, (filename, query) in enumerate(queries, start=1):
        print(f"\n▶️  Executing query {i} from file {filename}")
        try:
            result = con.execute(query)
            rows = result.fetchall()
            columns = [desc[0] for desc in result.description]

            # Convert result to list of dictionaries
            data = [dict(zip(columns, row)) for row in rows]

            # Create JSON filename based on SQL file name
            json_name = os.path.splitext(filename)[0] + ".json"
            output_path = os.path.join(output_dir, json_name)

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            print(f"✅ Result saved in: {output_path}")
        except Exception as e:
            print(f"❌ Error executing query from {filename}: {e}")


if __name__ == "__main__":
    # input parameter handling
    if len(sys.argv) < 2:
        print(" Error: Specify the name of the query sub-directory")
        print(f"Example: python nome_script.py folder_name")
        sys.exit(1)  # break the script with an error

    # the name of the sub-directory is the first input parameter
    dataset_name = sys.argv[1]

    # Building the right path
    dataset_folder = os.path.join(DB_FILE_PATH, dataset_name)

    # Store the result in the right folder
    output_per_table = os.path.join(OUTPUT_DIR, dataset_name)
    os.makedirs(output_per_table, exist_ok=True)

    # Test DB connection
    con = connect_to_duckdb(dataset_name)

    queries = load_queries_from_folder(dataset_folder)

    if not queries:
        print(f"No query founded in {dataset_folder}")
    else:
        print(f"Founded {len(queries)} query for the table '{dataset_name}'. Start execution...")
        # execute_queries_and_save_json(con, queries, output_dir=output_per_table)
        execute_queries_and_save_json(con, queries, output_per_table)

    # ... (closing the connection)
    con.close()
    print("\n🏁 All queries executed and results saved in JSON files.")
