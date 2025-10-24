import os, duckdb, psycopg 

ENGINE = os.getenv("ENGINE", "duckdb").lower() # "duckdb" or "postgres"

def connect_duck(path: str | None = None, read_only: bool = False): 
    db_path = path or os.getenv("DUCKDB_PATH", "project.duckdb")
    # create file if missing and not read_only
    if read_only and not os.path.exists(db_path):
        raise FileNotFoundError(f"DuckDB file not found: {db_path}")
    return duckdb.connect(path or "galois.duckdb") # default to galois.duckdb in cwd
    

def connect_pg(url: str | None = None): # use env vars or default to local postgres
    url = url or (
        f"postgresql://{os.getenv('PGUSER','postgres')}:{os.getenv('PGPASSWORD','')}" 
        f"@{os.getenv('PGHOST','localhost')}:{os.getenv('PGPORT','5432')}/{os.getenv('PGDATABASE','postgres')}" # default db is 'postgres'
    ) 
    return psycopg.connect(url) 

def get_connection(): # factory to get the right connection based on ENGINE
    return connect_pg() if ENGINE == "postgres" else connect_duck() 
