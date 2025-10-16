CREATE SCHEMA IF NOT EXISTS target;

CREATE TABLE target.world_presidents (
    name STRING,
    start_year INT,
    end_year INT,
    cardinal_number STRING,
    party STRING,
    country STRING
);

COPY target.world_presidents FROM 'world_presidents.csv';




