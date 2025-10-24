CREATE SCHEMA IF NOT EXISTS target;
CREATE TABLE target.airlines(alid BIGINT, "name" VARCHAR, iata VARCHAR, icao VARCHAR, callsign VARCHAR, country VARCHAR, active VARCHAR);
CREATE TABLE target.airports(apid BIGINT, "name" VARCHAR, city VARCHAR, country VARCHAR, x DOUBLE, y DOUBLE, elevation_in_ft BIGINT, iata VARCHAR, icao VARCHAR);
CREATE TABLE target.routes(rid BIGINT, dst_apid BIGINT, dst_ap VARCHAR, src_apid BIGINT, src_ap VARCHAR, alid DOUBLE, airline VARCHAR, codeshare VARCHAR);

COPY target.airlines FROM 'airlines.csv';
COPY target.airports FROM 'airports.csv';
COPY target.routes FROM 'routes.csv';
