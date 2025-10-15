CREATE SCHEMA target;
CREATE TABLE target.city(id BIGINT, "name" VARCHAR, country_code_3_letters VARCHAR, district VARCHAR, population BIGINT);
CREATE TABLE target.country(code_3_letters VARCHAR, "name" VARCHAR, continent VARCHAR, region VARCHAR, surface_area_in_km2 DOUBLE, independence_year VARCHAR, population BIGINT, life_expectancy VARCHAR, gnp DOUBLE, gnp_old VARCHAR, local_name VARCHAR, government_form VARCHAR, head_of_state VARCHAR, capital VARCHAR, code_2_letters VARCHAR);
CREATE TABLE target.country_language(country_code_3_letters VARCHAR, "language" VARCHAR, is_official BOOLEAN, percentage DOUBLE);

COPY target.city FROM 'city.csv';
COPY target.country FROM 'country.csv';
COPY target.country_language FROM 'country_language.csv';
