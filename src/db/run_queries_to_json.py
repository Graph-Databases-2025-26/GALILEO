import os, json, glob, sys
from pathlib import Path
from .db_connection import connect_to_duckdb
from src.utils import DATA, SUBMISSIONS_PATH


os.makedirs(SUBMISSIONS_PATH, exist_ok=True)


def load_queries_from_folder(folder_path: str):
    """
    Read all .sql files in the folder and return a list of queries.
    Returns a list of tuples: (filename, query_string)
    """
    queries = []

    for file_path in sorted(glob.glob(os.path.join(folder_path, "queries_*.sql"))):
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            # Split multiple queries in a single file by ";"
            for q in content.split(";"):
                q = q.strip()
                if q:  # Skip empty strings
                    #print(f"Query: {q}")
                    queries.append((os.path.basename(file_path), q))

    print(f"Found {len(queries)} queries in 'queries_*.sql' file \n")
    return queries


def execute_queries_and_save_json(con, queries, output_dir):
    """
    Execute each query on the DuckDB connection and save results as JSON files.
    """
    for i, (filename, query) in enumerate(queries, start=1):
        #print(f"\n▶️  Executing query {i} from file {filename}")
        try:
            result = con.execute(query)
            rows = result.fetchall()
            columns = [desc[0] for desc in result.description]

            # Convert result to list of dictionaries
            data = []
            for row in rows:
                row_dict = dict(zip(columns, row))

                for key, value in row_dict.items():
                    if ("sum(" in key or "avg(" in key) and isinstance(value, int):
                        row_dict[key] = float(value)

                data.append(row_dict)

            # Create JSON filename based on SQL file name
            json_name = f"query{i}.json"
            output_path = os.path.join(output_dir, json_name)

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            #print(f"✅ Result saved in: {output_path}")
        except Exception as e:
            print(f"❌ Error executing query from {filename}: {e}")

def run_queries_to_json(dataset_name: str) -> None:
    data_dir = DATA / dataset_name
    data_dir.mkdir(parents=True, exist_ok=True)
    
    con = connect_to_duckdb(dataset_name)
    qrs = load_queries_from_folder(data_dir)

    if qrs:
        complete_path = SUBMISSIONS_PATH / f"{dataset_name}"
        execute_queries_and_save_json(con, qrs, complete_path)
    else:
        print(f"No queries found for dataset '{dataset_name}'")



def main():
    if len(sys.argv) < 2:
        print("❌ Specifica il nome del dataset come argomento.")
        print("Esempio: python3 main.py test_dataset")
        return

    dataset_name = sys.argv[1]
    print(f"▶️ Avvio test per il dataset: {dataset_name}")
    run_queries_to_json(dataset_name)
    print(f"✅ Test completato per il dataset: {dataset_name}")

if __name__ == "__main__":
    main()