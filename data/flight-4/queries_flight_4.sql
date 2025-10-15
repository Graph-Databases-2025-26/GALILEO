--query1
SELECT a.name FROM target.airports a WHERE a.elevation_in_ft >= -50 and a.elevation_in_ft <= 50;

--query2
SELECT max(elevation_in_ft) FROM target.airports WHERE country = 'Iceland';

--query3
SELECT name, city, country, elevation_in_ft FROM target.airports WHERE city = 'New York';
