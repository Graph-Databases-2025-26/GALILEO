--query1
SELECT area_squared_miles FROM target.usa_state WHERE state_name = 'new mexico';

--query2
SELECT city_name FROM target.usa_city WHERE population > 150000;

--query3
SELECT count(capital) FROM target.usa_state WHERE state_name = 'rhode island';

--query4
SELECT population FROM target.usa_city WHERE city_name = 'tempe';

--query5
SELECT count(state_name) FROM target.usa_state;

--query6
SELECT lake_name FROM target.usa_lake WHERE state_name = 'california' and area_squared_km > 450;

--query7
SELECT avg(population) FROM target.usa_state;

--query8
SELECT count(distinct river_name) FROM target.usa_river where length_in_km > 400;

--query9
SELECT DISTINCT r.length_in_km FROM target.usa_river r WHERE r.river_name = 'rio grande';

--query10
SELECT DISTINCT lake_name FROM target.usa_lake WHERE area_squared_km > 450;

--query11
SELECT DISTINCT usa_state_traversed FROM target.usa_river WHERE length_in_km > 750;

--query12
SELECT state_name FROM target.usa_city WHERE city_name = 'springfield' AND population > 62000;

--query13
SELECT lake_name FROM target.usa_lake WHERE area_squared_km > 750 AND state_name = 'michigan';

--query14
SELECT DISTINCT lake_name FROM target.usa_lake WHERE area_squared_km > 750;

--query15
SELECT state_name FROM target.usa_city WHERE city_name = 'austin' AND population > 150000;

--query16
SELECT COUNT ( DISTINCT state_name ) FROM target.usa_city WHERE city_name = 'springfield' AND population > 72000;

--query17
SELECT population FROM target.usa_city WHERE city_name = 'seattle' AND state_name = 'washington';

--query18
SELECT COUNT ( DISTINCT usa_state_traversed ) FROM target.usa_river WHERE length_in_km > 750;

--query19
SELECT DISTINCT capital FROM target.usa_state;

--query20
SELECT COUNT ( river_name ) FROM target.usa_river WHERE usa_state_traversed = 'idaho';

--query21
SELECT country_name FROM target.usa_state WHERE state_name = 'massachusetts';

--query22
SELECT t2.capital FROM target.usa_city AS t1 JOIN target.usa_state AS t2 ON t1.state_name = t2.state_name WHERE t1.city_name = 'tempe';

--query23
SELECT t2.capital FROM target.usa_city AS t1 JOIN target.usa_state AS t2 ON t1.city_name=t2.capital WHERE t1.population <= 150000;

--query24
SELECT state_name, population, area_squared_miles FROM target.usa_state;

--query25
SELECT us.state_name, us.capital, us.area_squared_miles FROM target.usa_state us;

--query26
SELECT state_name, population, area_squared_miles FROM target.usa_state WHERE capital = 'frankfort';

--query27
SELECT us.state_name, us.population, us.capital FROM target.usa_state us WHERE us.population > 5000000;

--query28
SELECT us.state_name, us.capital FROM target.usa_state us WHERE us.population > 5000000 AND us.density < 1000;

--query29
SELECT us.state_name, us.capital, us.density, us.population FROM target.usa_state us WHERE us.population > 5000000 AND us.density < 1000 AND us.area_squared_miles < 50000;

--query30
SELECT us.state_name, us.capital FROM target.usa_state us WHERE us.population > 3000000 AND us.area_squared_miles > 50000 order by us.capital;

--query31
SELECT us.state_name, us.capital, us.population FROM target.usa_state us WHERE us.population > 3000000 AND us.population < 8000000 AND us.area_squared_miles > 50000 order by us.population;

--query32
SELECT us.state_name, us.capital, us.population, us.area_squared_miles FROM target.usa_state us WHERE us.population = 4700000 AND us.area_squared_miles=56153;
