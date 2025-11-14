import os, duckdb, psycopg 

ENGINE = os.getenv("ENGINE", "duckdb").lower() # "duckdb" or "postgres"

def connect_duck(path: str | None = None, read_only: bool = False): 
    db_path = path or os.getenv("DUCKDB_PATH", "project.duckdb")
    # create file if missing and not read_only
    if read_only and not os.path.exists(db_path):
        raise FileNotFoundError(f"DuckDB file not found: {db_path}")
    return duckdb.connect(path or "galois.duckdb") # default to galois.duckdb in cwd


def get_connection(): # factory to get the right connection based on ENGINE
    return  connect_duck()
