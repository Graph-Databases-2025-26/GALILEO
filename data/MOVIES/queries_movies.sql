--query1
SELECT m.originaltitle FROM target.movies m WHERE m.director='Richard Thorpe';

--query2
SELECT m.originaltitle FROM target.movies m WHERE m.director='Steven Spielberg';

--query3
SELECT m.originaltitle, m.startyear FROM target.movies m WHERE m.director='Richard Thorpe' AND m.startyear > 1950;

--query4
SELECT m.originaltitle, m.startyear FROM target.movies m WHERE m.director='Steven Spielberg' AND m.startyear > 2000;

--query5
SELECT m.originaltitle, m.startyear, m.genres, m.birthyear FROM target.movies m WHERE m.director='Steven Spielberg' AND m.startyear > 2000;

--query6
SELECT m.originaltitle, m.startyear, m.genres, m.birthyear, m.deathyear, m.runtimeminutes FROM target.movies m WHERE m.director = 'Steven Spielberg' AND m.startyear > 1990 AND m.startyear < 2000;

--query7
SELECT m.startyear, count(*) as numMovies FROM target.movies m WHERE m.director = 'Steven Spielberg' AND m.startyear is not null group by m.startyear;

--query8
SELECT m.startyear, count(*) as count FROM target.movies m WHERE m.director = 'Tim Burton' group by m.startyear order by count desc limit 1;

--query9
SELECT m.director, (m.startyear - m.birthyear) as director_age FROM target.movies m WHERE m.startyear is not null AND m.birthyear is not null order by director_age desc limit 1;
