--query1
SELECT count(distinct government_form) FROM target.country WHERE continent = 'Africa';

--query2 - added TRY_CAST to avoid errors with non-integer values
SELECT name FROM target.country WHERE TRY_CAST(independence_year AS INTEGER) > 1950;

--query3
SELECT avg(gnp), sum(population) FROM target.country WHERE government_form = 'US Territory';

--query4
SELECT distinct t2.region FROM target.country_language as t1 join target.country as t2 on t1.country_code_3_letters = t2.code_3_letters WHERE t1.language = 'English' or t1.language = 'Dutch';
