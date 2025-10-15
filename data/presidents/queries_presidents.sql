--query1
SELECT DISTINCT p.name, p.party FROM target.world_presidents p WHERE p.country='Venezuela';

--query2        
SELECT p.name, p.party FROM target.world_presidents p WHERE p.country='Venezuela' AND p.party='Liberal';

--query3        
SELECT count(p.party) as party FROM target.world_presidents p WHERE p.country='Venezuela' AND p.party='Liberal';

--query4
SELECT p.name FROM target.world_presidents p WHERE p.country='Venezuela' AND p.party='Liberal';

--query5
SELECT p.name FROM target.world_presidents p WHERE p.country='Venezuela' AND p.party='Liberal' AND p.start_year > 1858;

--query6
SELECT p.name, p.start_year, p.end_year, p.cardinal_number, p.party FROM target.world_presidents p WHERE p.country='Venezuela';

--query7
SELECT p.party, count(p.party) num FROM target.world_presidents p WHERE p.country='Venezuela' group by p.party order by num desc limit 1;
                
--query8
SELECT count(*) FROM target.world_presidents p where p.country='Venezuela' AND p.start_year >= 1990  AND p.start_year < 2000;

--query9
SELECT p.name FROM target.world_presidents p where p.country='Venezuela' AND p.party = 'Military' order by p.end_year desc limit 1;

--query10
SELECT p.name, p.party, start_year, end_year, p.cardinal_number FROM target.world_presidents p WHERE p.country ='Venezuela' AND start_year > 1900 AND end_year < 2000 AND party ='Democratic Action';

--query11 
SELECT p.name, p.party, start_year, end_year, p.cardinal_number FROM target.world_presidents p WHERE p.country ='Venezuela' AND start_year > 1800 AND end_year < 1900 AND party ='Conservative';

--query12        
SELECT p.party, count(p.party) num FROM target.world_presidents p WHERE p.country ='Venezuela' AND start_year > 1800 AND end_year < 1900 group by p.party order by num desc;

--query13        
SELECT party, count(p.party) num FROM target.world_presidents p WHERE p.country ='Venezuela' AND start_year > 1900 AND end_year < 2000 group by p.party order by num desc;

--query14
SELECT p.name, p.party FROM target.world_presidents p WHERE p.country='United States';

--query15
SELECT p.name, p.party FROM target.world_presidents p WHERE p.country='United States' AND p.party='Republican';

--query16
SELECT count(p.party) FROM target.world_presidents p WHERE p.country='United States' AND p.party='Republican';

--query17
SELECT p.name FROM target.world_presidents p WHERE p.country='United States' AND p.party='Republican';

--query18
SELECT p.name FROM target.world_presidents p WHERE p.country='United States' AND p.party='Republican' AND p.start_year > 1980;

--query19
SELECT p.name, p.start_year, p.end_year, p.cardinal_number, p.party FROM target.world_presidents p WHERE p.country='United States';

--query20
SELECT p.party, count(p.party) num FROM target.world_presidents p WHERE p.country='United States' group by p.party order by num desc limit 1;

--query21
SELECT count(*) FROM target.world_presidents p WHERE p.country='United States' AND p.start_year >= 1990  AND p.start_year < 2000;

--query22
SELECT p.name FROM target.world_presidents p WHERE p.country='United States' AND p.party='Whig' order by p.end_year desc limit 1;

--query23
SELECT p.name, p.party, start_year, end_year, p.cardinal_number FROM target.world_presidents p WHERE p.country ='United States' AND start_year > 1850 AND end_year < 1900 AND party ='Democratic';

--query24
SELECT p.name, p.party, start_year, end_year, p.cardinal_number FROM target.world_presidents p WHERE p.country ='United States' AND start_year > 1900 AND end_year < 2000 AND party ='Democratic';

--query25
SELECT party, count(*) num FROM target.world_presidents p WHERE p.country ='United States' AND start_year > 1800 AND end_year < 1900 group by p.party order by num desc;

--query26
SELECT party, count(*) num FROM target.world_presidents p WHERE p.country ='United States' AND start_year > 1900 AND end_year < 2000 group by p.party order by num desc;
