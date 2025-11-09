from __future__ import annotations
from pathlib import Path
from typing import List
import duckdb
from src.utils.constants import DATA_DIR, DATASETS, IK_DATASETS, MC_DATASETS
from src.utils.logging_config import logger


FORTUNE_SCHEMA = """
Dataset: Fortune 1000 companies (2024).
Tables:
    - target.fortune_2024(
        Rank, Company, Ticker, Sector, Industry, Profitable, Founder_is_CEO, FemaleCEO,
        Growth_in_Jobs, Change_in_Rank, Gained_in_Rank, Dropped_in_Rank,
        Newcomer_to_the_Fortune500, Global500, Worlds_Most_Admired_Companies,
        Best_Companies_to_Work_For, Number_of_employees, MarketCap_March28_M,
        Revenues_M, RevenuePercentChange, Profits_M, ProfitsPercentChange,
        Assets_M, CEO, Country, HeadquartersCity, HeadquartersState, Website,
        CompanyType, Footnote, MarketCap_Updated_M, Updated
    )
""".strip()


PREMIER_SCHEMA = """
Dataset: Premier League 2024–2025 season information.
Tables:
    - target.premier_league_2024_2025_arsenal_matches(
        day_of_the_week, opponent_team, match_date_month, match_date_year, match_date_day
    )
    - target.premier_league_2024_2025_key_events(
        player_name, team, goal_scored
    )
    - target.premier_league_2024_2025_match_result(
        oid, date, home_team, away_team, home_goals, away_goals,
        player_of_the_match, player_of_the_match_team
    )
""".strip()


# --------------------------- #
#  Helpers su DuckDB         #
# --------------------------- #



def _load_schema_from_duckdb(db_path: Path) -> str:
    
    if not db_path.exists():
        logger.warning(f"DuckDB file not found: {db_path}")
        return f"[WARN] DuckDB file not found: {db_path}"

    try:
        conn = duckdb.connect(str(db_path), read_only=True)
        tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
        lines: List[str] = []
        for table in tables:
            desc = conn.execute(f"DESCRIBE {table}").fetchall()
            cols = ", ".join(f"{name} {dtype}" for name, dtype, *_ in desc)
            lines.append(f"TABLE {table}: {cols}")
        conn.close()
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"Error while reading schema from {db_path}: {e}")
        return f"[ERROR] Unable to read schema from {db_path}: {e}"


def _load_sample_rows_from_duckdb(
    db_path: Path, max_rows_per_table: int = 10
) -> str:
   
    if not db_path.exists():
        logger.warning(f"DuckDB file not found for samples: {db_path}")
        return ""

    try:
        conn = duckdb.connect(str(db_path), read_only=True)
        tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
        lines: List[str] = []
        for table in tables:
            desc = conn.execute(f"DESCRIBE {table}").fetchall()
            col_names = [c[0] for c in desc]
            rows = conn.execute(
                f"SELECT * FROM {table} LIMIT {max_rows_per_table}"
            ).fetchall()

            lines.append(f"Sample rows for table {table}:")
            lines.append(" | ".join(col_names))
            for row in rows:
                lines.append(" | ".join(str(v) for v in row))
            lines.append("")  # Empty line between tables

        conn.close()
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"Error while reading sample rows from {db_path}: {e}")
        return f"[ERROR] Unable to read sample rows from {db_path}: {e}"


def build_prompt_context(dataset_name: str):

    dataset_name = dataset_name.upper()
    if(dataset_name in MC_DATASETS):
        if  dataset_name == "FORTUNE":
            schema = """
            Dataset: Fortune 1000 companies (2024).
            Tables:
                - target.fortune_2024(
                    Rank, Company, Ticker, Sector, Industry, Profitable, Founder_is_CEO, FemaleCEO,
                    Growth_in_Jobs, Change_in_Rank, Gained_in_Rank, Dropped_in_Rank,
                    Newcomer_to_the_Fortune500, Global500, Worlds_Most_Admired_Companies,
                    Best_Companies_to_Work_For, Number_of_employees, MarketCap_March28_M,
                    Revenues_M, RevenuePercentChange, Profits_M, ProfitsPercentChange,
                    Assets_M, CEO, Country, HeadquartersCity, HeadquartersState, Website,
                    CompanyType, Footnote, MarketCap_Updated_M, Updated
                )
            """
        elif dataset_name == "PREMIER":
            schema = """
            Dataset: Premier League 2024–2025 season information.
            Tables:
                - target.premier_league_2024_2025_arsenal_matches(
                    day_of_the_week, opponent_team, match_date_month, match_date_year, match_date_day
                )
                - target.premier_league_2024_2025_key_events(
                    player_name, team, goal_scored
                )
                - target.premier_league_2024_2025_match_result(
                    oid, date, home_team, away_team, home_goals, away_goals,
                    player_of_the_match, player_of_the_match_team
                )
            """

    elif dataset_name in IK_DATASETS:
        schema = """
        You must analyze, interpret and answer the questions asked by means the knowledge that you have about the topic.
        Your goal is to provide coherent, verifiable, and well-organized responses based on the question asked by the user.
"""
    context = f"""
    {schema.strip()}
    Now answer the following question:
"""

    return context.strip()


def build_sql_prompt(query: str) -> str:
    prompt = f"""
    Now process this SQL query: {query}
    """
    return prompt.strip()
