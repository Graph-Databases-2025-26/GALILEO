CREATE SCHEMA IF NOT EXISTS target;

CREATE TABLE target.airlines(
    alid BIGINT PRIMARY KEY,
    "name" VARCHAR,
    iata VARCHAR,
    icao VARCHAR,
    callsign VARCHAR,
    country VARCHAR,
    active VARCHAR
);

CREATE TABLE target.airports(
    apid BIGINT PRIMARY KEY,
    "name" VARCHAR,
    city VARCHAR,
    country VARCHAR,
    x DOUBLE,
    y DOUBLE,
    elevation_in_ft BIGINT,
    iata VARCHAR,
    icao VARCHAR
);

CREATE TABLE target.routes(
    rid BIGINT PRIMARY KEY,
    dst_apid BIGINT,
    dst_ap VARCHAR,
    src_apid BIGINT,
    src_ap VARCHAR,
    alid DOUBLE, 
    airline VARCHAR,
    codeshare VARCHAR,
);



COPY target.airlines FROM 'target_airlines.csv';
COPY target.airports FROM 'target_airports.csv';
COPY target.routes FROM 'target_routes.csv';