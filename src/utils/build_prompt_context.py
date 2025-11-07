from src.utils.constants import *


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
        You must analyze, interpret and answer about general questions.
        Your goal is to provide coherent, verifiable, and well-organized responses based on the question asked, by the user.
"""


    context = f"""
    You are an AI assistant that answers questions based on the following dataset schema:

    {schema.strip()}
    
    """

    return context.strip()
#-------------------------------------------------------------------------------------------------------------

"""
{{
        "result_set": [
            {{ "column_name": "value" }},
            {{ "column_name": "value" }}
        ],
        "time": 0.0,
        "tokens": 0
}}

    Rules:
    - The JSON must be syntactically correct and complete.
    - Do not include explanations, SQL code, or text outside the JSON.
    - If the answer is not found, return "result_set": [].
"""

def build_sql_prompt(query: str) -> str:
    
    prompt = f"""
    Now process this SQL query: {query}
    """

    return prompt.strip()