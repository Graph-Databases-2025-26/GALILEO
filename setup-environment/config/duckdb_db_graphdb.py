import duckdb
import os
import glob

# base path for the project
PROJECT_ROOT = "."

# Paths definitions
# Folder from which load teh data
DATA_SOURCE_DIR = os.path.join(PROJECT_ROOT, "../data")
# Folder in which save the database instance
DB_PATH = os.path.join(PROJECT_ROOT, "project.duckdb")

# Connection to DuckDB
# Connection: if the file doesn't exist, create it
con = duckdb.connect(DB_PATH)


# Function for creating tables from CSV and JSON files
def load_files_from_folder(folder):
    """Scan a folder and create tables DuckDB from CSV and JSON files."""
    for file_path in glob.glob(os.path.join(folder, "*")):
        name, ext = os.path.splitext(os.path.basename(file_path))

        # Make the name compatible with SQL (substitute - and whitespaces with _)
        name = name.replace("-", "_").replace(" ", "_").replace(".", "_")

        # Ignore .txt, .sql, .json files
        if ext.lower() in [".sql", ".txt"]:
            continue

        if ext.lower() == ".csv":
            print(f"Import CSV: {file_path} -> Table: {name}")
            # read_csv_auto for CSV reading process
            con.execute(f'CREATE OR REPLACE TABLE "{name}" AS SELECT * FROM read_csv_auto(\'{file_path}\');')
        elif ext.lower() == ".json":
            print(f"Importo JSON: {file_path} -> Tabella: {name}")
            # read_json_auto for JSON reading process
            con.execute(f'CREATE OR REPLACE TABLE "{name}" AS SELECT * FROM read_json_auto(\'{file_path}\');')


# Subfolders scanning
print(f"Inizio importazione da: {DATA_SOURCE_DIR}\n")

for subdir in os.listdir(DATA_SOURCE_DIR):
    path = os.path.join(DATA_SOURCE_DIR, subdir)
    if os.path.isdir(path):
        load_files_from_folder(path)

con.commit()

print("\n" + "=" * 50)
print("✅ Import completed! Tables are available:")
print(con.execute("SHOW TABLES").fetchdf())
print("=" * 50)

# Close the connection and write the .duckdb file definitely
con.close()

print(f"\nDuckDB connection closed. The DB file is in: {DB_PATH}")