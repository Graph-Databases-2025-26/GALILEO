--query1
SELECT call_sign FROM target.usa_airline_companies WHERE airline='jetblue airways';

--query2
SELECT count(*) FROM target.usa_flights f;

--query3
SELECT a.airline FROM target.usa_airline_companies a WHERE a.call_sign='ual';
