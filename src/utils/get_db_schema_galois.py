import duckdb
from typing import List, Optional, Dict

from src.utils.constants import *
from src.utils.logging_config import LOG
from src.db.duckdb_db_graphdb import get_duckdb_path


class GaloisSchemaManager:
    """
    Connects to a DuckDB database and dynamically extracts its schema,
    including columns and primary keys.
    Corresponds to 'LLMDB.java' and 'LLMTable.java' in the Java repo.
    """

    def __init__(self, database_name: str):
        self.database_name = database_name
        try:
            #db_object = get_duckdb_path(database_name)
            #uri_string = str(db_object._engine.url)
            #db_object._engine.dispose()
            #db_path = uri_string.removeprefix("duckdb:///")
            db_path = DATA_DIR / self.database_name.upper() / f"{self.database_name.lower()}.duckdb"
            self.con = duckdb.connect(database=db_path, read_only=True)
            self.con.execute("USE target;")
        except Exception as e:
            LOG.info(f"Error connecting to DuckDB for {database_name}: {e}")
            raise


    def get_attributes(self, table_name: str) -> List[str]:
        """ Retrieve all attributes (columns) for a table. """
        try:
            result = self.con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            # The 'name' column is at index 1
            return [row[1] for row in result]
        except duckdb.CatalogException:
            LOG.info(f"Error: Table '{table_name}' not found in {self.database_name}")
            return []

    def get_column_types(self, table_name: str) -> Dict[str, str]:
        """ Retrieve a dictionary {column_name: type_name} for a table. """
        try:
            # row[1] è il nome, row[2] è il tipo (es. 'VARCHAR', 'INTEGER')
            result = self.con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            return {row[1]: row[2].upper() for row in result}
        except Exception as e:
            LOG.warning(f"Could not retrieve types for table {table_name}: {e}")
            return {}

    def get_exact_table_name(self, table_name: str) -> Optional[str]:
        """
        Retrieves the exact name of the table in the database, ignoring case sensitivity.
        """
        try:
            query = """
SELECT table_name
FROM information_schema.tables
WHERE LOWER(table_name) = '{table_name}.lower()'
"""
            result = self.con.execute(query).fetchone()
            return result[0] if result else None
        except duckdb.Error as e:
            LOG.info(f"Error while searching for the exact table name '{table_name}': {e}")
            return None


    def get_key_attributes(self, table_name: str) -> List[str]:
        """
        Retrieve key attributes (Primary Key) for a table.
        """
        try:
            query = """
SELECT cols
FROM (
    SELECT unnest(constraint_column_names) AS cols, constraint_type, table_name
    FROM duckdb_constraints()
)
WHERE table_name = '{table_name}'
  AND constraint_type = 'PRIMARY KEY';
"""
            result = self.con.execute(query).fetchall()
            keys = [row[0] for row in result]

            # Fallback: if no PK is defined, use the first column
            if not keys:
                all_attrs = self.get_attributes(table_name)
                if all_attrs:
                    LOG.info(f"Warning: No PK defined for '{table_name}'. Assuming '{all_attrs[0]}' as key.")
                    return [all_attrs[0]]
            return keys

        except duckdb.Error as e:
            LOG.info(f"Error querying duckdb_constraints (may be normal if there are no PKs): {e}")
            # Fallback
            all_attrs = self.get_attributes(table_name)
            return [all_attrs[0]] if all_attrs else []

    def get_json_schema_example(self, table_name: str, attributes_list: List[str]) -> str:
        """
        Create a JSON example string for the prompt.
        """
        if not attributes_list:
            return "{}"

        full_schema_attrs = self.get_attributes(table_name)
        key_schema_attrs = self.get_key_attributes(table_name)

        # Case 1: Table-Scan (all attributes)
        if set(attributes_list) == set(full_schema_attrs):
            example_record = {attr: "value" for attr in attributes_list}
            return str({table_name: [example_record]})

        # Case 2: Key-Scan (only keys)
        elif set(attributes_list) == set(key_schema_attrs):
            example_record = {attr: "value" for attr in attributes_list}
            return str({table_name: [example_record]})

        # Case 3: Key-Scan (tuple-by-key, only non-keys)
        else:
            return str({attr: "value" for attr in attributes_list})

    def close(self):
        self.con.close()


if __name__ == "__main__":

        mgr = GaloisSchemaManager("MOVIES")

        try:
            attrs = mgr.get_attributes("movies")
            keys = mgr.get_key_attributes("movies")

            print(f"Attributes for movies: {attrs}")
            print(f"Primary keys for movies: {keys}")
            print("JSON example (all attributes):", mgr.get_json_schema_example("movies", attrs))
            print("JSON example (keys):", mgr.get_json_schema_example("movies", keys))

            # Esempio subset (primi 2 non-key)
            subset = [a for a in attrs if a not in keys][:2]
            print("JSON example (subset):", mgr.get_json_schema_example("movies", subset))

        finally:
            mgr.close()