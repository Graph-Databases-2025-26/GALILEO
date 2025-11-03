def build_prompt_context(dataset_name: str):
    dataset_name = dataset_name.upper()

    if dataset_name == "FLIGHT-2":
        schema = """
        Dataset: flight information.
        Tables:
            - target.usa_airline_companies(uid, airline, call_sign, country)
            - target.usa_airports(city, airportcode, airportname, country, countryabbrev)
            - target.usa_flights(airline, flightno, sourceairport, destairport)
        """
    elif dataset_name == "FLIGHT-4":
        schema = """
        Dataset: flight information.
        Tables:
            - target.airlines(alid, name, iata, icao, callsign, country, active)
            - target.airports(apid, name, city, country, x, y, elevation_in_ft, iata, icao)
            - target.routes(rid, dst_apid, dst_ap, src_apid, src_ap, alid, airline, codeshare)
        """
    elif dataset_name == "FORTUNE":
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
    elif dataset_name == "GEO":
        schema = """
        Dataset: geographical information about the United States.
        Tables:
            - target.usa_border_info(state_name, border)
            - target.usa_city(city_name, population, country_name, state_name)
            - target.usa_highlow(state_name, highest_elevation_in_meters, lowest_point, highest_point, lowest_elevation_in_meters)
            - target.usa_lake(lake_name, area_squared_km, country_name, state_name)
            - target.usa_mountain(mountain_name, mountain_altitude_in_meters, country_name, state_name)
            - target.usa_river(river_name, length_in_km, country_name, usa_state_traversed)
            - target.usa_state(state_name, population, area_squared_miles, country_name, capital, density)
        """
    elif dataset_name == "MOVIES":
        schema = """
        Dataset: movie information including titles, years, genres, and directors.
        Tables:
            - target.movies(
                primarytitle, originaltitle, startyear, endyear, runtimeminutes,
                genres, director, birthyear, deathyear
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
    elif dataset_name == "PRESIDENTS":
        schema = """
        Dataset: world presidents information.
        Tables:
            - target.world_presidents(
                name, start_year, end_year, cardinal_number, party, country
            )
        """
    elif dataset_name == "WORLD":
        schema = """
        Dataset: world countries, cities, and languages information.
        Tables:
            - target.city(id, name, country_code_3_letters, district, population)
            - target.country(
                code_3_letters, name, continent, region, surface_area_in_km2,
                independence_year, population, life_expectancy, gnp, gnp_old,
                local_name, government_form, head_of_state, capital, code_2_letters
            )
            - target.country_language(
                country_code_3_letters, language, is_official, percentage
            )
        """
    else:
        schema = "Generic dataset with unspecified schema."

    prompt = f"""
    You are an AI assistant that answers questions based on the following dataset schema:

    {schema.strip()}

    You have full access to this dataset. Use it to answer the user's question accurately.

    When writing SQL queries, always:
    - Use single quotes (' ') for string literals (e.g. WHERE airline = 'JetBlue Airways')
    - Do not use double quotes (") for strings, since they are used for identifiers in SQL.
    
    IMPORTANT: When referencing tables in SQL queries, "
    "always prefix them with 'target.' (e.g., target.airports). "
    "Do not use unqualified table names.

    Respond **only** in valid JSON format, following this example:

    

    Question:
    """

    return prompt.strip()

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