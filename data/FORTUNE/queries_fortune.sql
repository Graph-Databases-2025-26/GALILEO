--query1
select f.rank, f.Company from target.fortune_2024 f order by f.rank asc limit 10;

--query2
select Company, ceo from target.fortune_2024 f where f.headquartersstate = 'Oklahoma';

--query3
select Company, headquartersstate from target.fortune_2024 where number_of_employees > 1000000;

--query4
select headquarterscity from target.fortune_2024 f where f.industry = 'Airlines';

--query5
select Company from target.fortune_2024 f where f.sector = 'Technology' and founder_is_ceo = true and profitable = true;

--query6
select ceo from target.fortune_2024 f where femaleceo = true and f.Companytype = 'Private';

--query7
select Company from target.fortune_2024 where best_companies_to_work_for = true and industry = 'Airlines';

--query8
select Company, ceo from target.fortune_2024 f where f.profitable = true and f.femaleceo = true and f.headquartersstate = 'Texas';

--query9
select Company, headquartersstate, ticker, ceo, founder_is_ceo, femaleceo, number_of_employees from target.fortune_2024 where Company = 'Nvidia';

--query10
select Company, headquartersstate, ticker, ceo, founder_is_ceo, femaleceo, number_of_employees from target.fortune_2024 where headquarterscity = 'Santa Clara' and rank < 70;
