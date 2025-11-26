CREATE SCHEMA target;

CREATE TABLE target.usa_airline_companies (
    uid INT,
    airline VARCHAR,
    call_sign VARCHAR,
    country VARCHAR,
    
    PRIMARY KEY (uid)
);

CREATE TABLE target.usa_airports (
    city VARCHAR,
    airportcode CHAR(3),
    airportname VARCHAR,
    country VARCHAR,
    countryabbrev VARCHAR,
    
    PRIMARY KEY (airportcode)
);

CREATE TABLE target.usa_flights (
    airline INT NOT NULL,
    flightno INT NOT NULL,
    sourceairport CHAR(3) NOT NULL,
    destairport CHAR(3) NOT NULL,
    
    PRIMARY KEY (airline, flightno),
    
    FOREIGN KEY (airline) 
        REFERENCES target.usa_airline_companies (uid),
);

COPY target.usa_airline_companies FROM 'usa_airline_companies.csv';
COPY target.usa_airports FROM 'usa_airports.csv';
COPY target.usa_flights FROM 'usa_flights.csv';
