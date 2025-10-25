CREATE SCHEMA IF NOT EXISTS target;
CREATE TABLE target.movies(primarytitle VARCHAR, originaltitle VARCHAR, startyear BIGINT, endyear BIGINT, runtimeminutes BIGINT, genres VARCHAR, director VARCHAR, birthyear BIGINT, deathyear BIGINT);

COPY target.movies FROM 'movies.csv'
  (FORMAT CSV, HEADER TRUE, DELIMITER ',', QUOTE '"', ESCAPE '"', NULL 'null');