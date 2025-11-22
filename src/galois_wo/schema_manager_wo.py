import json
from typing import List, Dict, Optional

import duckdb

from src.utils import LOG, DATA_DIR


class GaloisWOSchemaManager:
    """
    Schema manager dedicated to Galois-WO.

    Connects directly to the .duckdb file in the data/<dataset>/ folder
    """

    def __init__(self, dataset_name: str):
        self.dataset_name = dataset_name.upper()

        folder = self.dataset_name.lower()          # "MOVIES" -> "movies"
        db_path = DATA_DIR / folder / f"{folder}.duckdb"

        LOG.info(f"[GaloisWOSchemaManager] Connecting to DuckDB at '{db_path}'")

        # Connection (read_only True for safety)
        self.conn = duckdb.connect(str(db_path), read_only=True)

        # Usually you use the "target" schema in your ingests
        try:
            self.conn.execute("USE target;")
        except Exception:
            LOG.info(
                "[GaloisWOSchemaManager] Schema 'target' not found, using default schema."
            )

    # ------------------------------------------------------------------
    # Tables and columns
    # ------------------------------------------------------------------
    def get_tables(self) -> List[str]:
        """
        Returns the list of tables in the current schema.
        """
        query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = current_schema()
        ORDER BY table_name;
        """
        rows = self.conn.execute(query).fetchall()
        return [r[0] for r in rows]

    def get_exact_table_name(self, table_name: str) -> Optional[str]:
        """
        Given a table_name (even with different case),
        returns the exact name in the DB or None.
        If no exact match is found, also tries:
          - version without final 's' (movies -> movie)
        this way we handle cases where the user uses the plural form.
        """
        wanted = table_name.lower()
        tables = self.get_tables()

        # 1) exact match case-insensitive
        for t in tables:
            if t.lower() == wanted:
                return t

        # 2) if it ends with 's', try removing the 's' (movies -> movie)
        if wanted.endswith("s"):
            singular = wanted[:-1]
            for t in tables:
                if t.lower() == singular:LOG.info(
                    "[GaloisWOSchemaManager] Inferred table '%s' from plural '%s'",t,table_name,)
                return t

        # 3) nothing found
        LOG.warning(
            "[GaloisWOSchemaManager] Table '%s' not found. Available tables: %s",
            table_name,
            tables,
        )
        return None


    def _get_table_info(self, table_name: str):
        """
        PRAGMA table_info(...) for a table.
        """
        query = f"PRAGMA table_info('{table_name}');"
        return self.conn.execute(query).fetchall()

    def get_attributes(self, table_name: str) -> List[str]:
        """
        Returns the list of column names (in order) for the table.
        """
        info = self._get_table_info(table_name)
        # In DuckDB: column "name" is index 1
        return [row[1] for row in info]

    def get_key_attributes(self, table_name: str) -> List[str]:
        """
        Returns the list of primary key columns, if defined.
        Uses the 'pk' field of PRAGMA table_info (index 5).
        """
        info = self._get_table_info(table_name)
        keys: List[str] = []

        # row: [cid, name, type, notnull, dflt_value, pk]
        for row in info:
            if len(row) > 5 and row[5]:  # pk != 0
                keys.append(row[1])
                return keys

        # ------------------------------------------------------------------
        # Example JSON for the prompt
        # ------------------------------------------------------------------
    def get_json_schema_example(
        self,
        table_name: str,
        attributes_list: List[str],
        ) -> str:
        """
        Creates an example JSON string to insert in the prompt.

        Structure:

            {
              "<table_name>": [
            { "<attr1>": "<value>", "<attr2>": "<value>", ... }
              ]
            }
        """
        if not attributes_list:
            return "{}"

        example_record: Dict[str, str] = {
            attr: f"example_{attr}" for attr in attributes_list
        }

        example_obj: Dict[str, object] = {
            table_name: [example_record]
        }

        return json.dumps(example_obj, indent=2)

        # ------------------------------------------------------------------
        # Cleanup
        # ------------------------------------------------------------------
    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass
