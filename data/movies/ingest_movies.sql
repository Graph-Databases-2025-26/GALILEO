CREATE SCHEMA IF NOT EXISTS target;
CREATE TABLE target.movie(primarytitle VARCHAR, originaltitle VARCHAR, startyear BIGINT, endyear VARCHAR, runtimeminutes BIGINT, genres VARCHAR, director VARCHAR, birthyear BIGINT, deathyear BIGINT);

COPY target.movie FROM 'movies.csv';