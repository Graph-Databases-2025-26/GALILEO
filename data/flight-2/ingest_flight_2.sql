CREATE SCHEMA target;
CREATE TABLE target.usa_airline_companies(uid BIGINT, airline VARCHAR, call_sign VARCHAR, country VARCHAR);
CREATE TABLE target.usa_airports(city VARCHAR, airportcode VARCHAR, airportname VARCHAR, country VARCHAR, countryabbrev VARCHAR);
CREATE TABLE target.usa_flights(airline BIGINT, flightno BIGINT, sourceairport VARCHAR, destairport VARCHAR);

COPY target.usa_airline_companies FROM 'usa_airline_companies.csv';
COPY target.usa_airports FROM 'usa_airports.csv';
COPY target.usa_flights FROM 'usa_flights.csv';
