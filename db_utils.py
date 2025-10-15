import os, duckdb, psycopg

ENGINE = os.getenv("ENGINE", "duckdb").lower()

def connect_duck(path: str | None = None):
    return duckdb.connect(path or "galois.duckdb")

def connect_pg(url: str | None = None):
    url = url or (
        f"postgresql://{os.getenv('PGUSER','postgres')}:{os.getenv('PGPASSWORD','')}"
        f"@{os.getenv('PGHOST','localhost')}:{os.getenv('PGPORT','5432')}/{os.getenv('PGDATABASE','postgres')}"
    )
    return psycopg.connect(url)

def get_connection():
    return connect_pg() if ENGINE == "postgres" else connect_duck()
