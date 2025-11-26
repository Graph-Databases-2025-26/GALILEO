CREATE SCHEMA IF NOT EXISTS target;

CREATE TABLE target.usa_state(
    state_name VARCHAR PRIMARY KEY,
    population BIGINT,
    area_squared_miles DOUBLE PRECISION,
    country_name VARCHAR,
    capital VARCHAR,
    density DOUBLE PRECISION
);

CREATE TABLE target.usa_highlow(
    state_name VARCHAR PRIMARY KEY,
    highest_elevation_in_meters BIGINT,
    lowest_point VARCHAR,
    highest_point VARCHAR,
    lowest_elevation_in_meters BIGINT,
    
);

CREATE TABLE target.usa_city(
    city_name VARCHAR,
    population BIGINT,
    country_name VARCHAR,
    state_name VARCHAR NOT NULL,
    
    PRIMARY KEY(city_name, state_name),
);

CREATE TABLE target.usa_border_info(
    state_name VARCHAR NOT NULL,
    border VARCHAR NOT NULL,
    
    PRIMARY KEY(state_name, border),
);

CREATE TABLE target.usa_lake(
    lake_name VARCHAR,
    area_squared_km DOUBLE PRECISION,
    country_name VARCHAR,
    state_name VARCHAR NOT NULL,
    
    PRIMARY KEY(lake_name, state_name),
);

CREATE TABLE target.usa_mountain(
    mountain_name VARCHAR,
    mountain_altitude_in_meters BIGINT,
    country_name VARCHAR,
    state_name VARCHAR NOT NULL,
    
    PRIMARY KEY(mountain_name, state_name),
);

CREATE TABLE target.usa_river(
    river_name VARCHAR,
    length_in_km BIGINT,
    country_name VARCHAR,
    usa_state_traversed VARCHAR NOT NULL,
    
    PRIMARY KEY(river_name, usa_state_traversed),
);

COPY target.usa_border_info FROM 'usa_border_info.csv';
COPY target.usa_city FROM 'usa_city.csv';
COPY target.usa_highlow FROM 'usa_highlow.csv';
COPY target.usa_lake FROM 'usa_lake.csv';
COPY target.usa_mountain FROM 'usa_mountain.csv';
COPY target.usa_river FROM 'usa_river.csv';
COPY target.usa_state FROM 'usa_state.csv';