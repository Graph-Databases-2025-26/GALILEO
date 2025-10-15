--query1
select m.opponent_team, m.match_date_year, m.match_date_month, m.match_date_day from target.premier_league_2024_2025_arsenal_matches m;

--query2
select count(*) from target.premier_league_2024_2025_arsenal_matches m where m.match_date_month = 8;

--query3
select m.player_of_the_match from target.premier_league_2024_2025_match_result m;

--query4
select m.player_of_the_match from target.premier_league_2024_2025_match_result m where m.player_of_the_match_team = 'Manchester United';

--query5
select home_team,away_team,home_goals,away_goals from target.premier_league_2024_2025_match_result;
