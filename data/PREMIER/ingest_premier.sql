CREATE SCHEMA IF NOT EXISTS target;
CREATE TABLE target.premier_league_2024_2025_arsenal_matches(day_of_the_week VARCHAR, opponent_team VARCHAR, match_date_month BIGINT, match_date_year BIGINT, match_date_day BIGINT);
CREATE TABLE target.premier_league_2024_2025_key_events(player_name VARCHAR, team VARCHAR, goal_scored BIGINT);;
CREATE TABLE target.premier_league_2024_2025_match_result(oid BIGINT, date DATE, home_team VARCHAR, away_team VARCHAR, home_goals BIGINT, away_goals BIGINT, player_of_the_match VARCHAR, player_of_the_match_team VARCHAR);

COPY target.premier_league_2024_2025_arsenal_matches FROM 'premier_league_2024_2025_arsenal_matches.csv';
COPY target.premier_league_2024_2025_key_events FROM 'premier_league_2024_2025_key_events.csv';
COPY target.premier_league_2024_2025_match_result FROM 'premier_league_2024_2025_match_result.csv';
