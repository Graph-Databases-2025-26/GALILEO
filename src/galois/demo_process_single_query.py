import json
from src.config.loaders import Config_Loader
from src.utils.logging_config import log_init
from src.utils import sql_query_parser
from src.galois.galois import Galois

def main():
    log_init()
    cfg = Config_Loader().get_config() # load default config

    dataset = "WORLD" # use WORLD dataset
    sql_query = """ 
    SELECT distinct t2.region
    FROM target.country_language AS t1
    JOIN target.country AS t2
    ON t1.country_code_3_letters = t2.code_3_letters
    WHERE t1.language = 'English'
    OR t1.language = 'Dutch';
    """
    print("\n=== INPUT QUERY ===") 
    print(sql_query)

    print("\n=== PARSER OUTPUT (sql_query_parser.parse_sql) ===")
    parsed = sql_query_parser.parse_sql(sql_query) # parse the SQL query
    print(json.dumps(parsed, indent=2, ensure_ascii=False)) 

# initialize Galois with the query
    g = Galois(
        config=cfg,
        dataset=dataset,
        sql_query=sql_query,
        physical_strategy="auto",
    ) 

# different physical strategies
    variants = [
        ("WO", g.run_no_push),
        ("A",  g.run_push_all),
        ("S",  g.run_push_selective),
        ("F",  g.run_push_confident),
    ] 

    for tag, fn in variants:
        print(f"\n\n================ {tag} ================\n")

        results, stats, debug_info = fn(debug=True) # execute the query

        print("=== DEBUG INFO ===")
        print(json.dumps(debug_info, indent=2, ensure_ascii=False))

        print("=== STATS ===")
        print(json.dumps(stats, indent=2, ensure_ascii=False))

        print("=== RESULT (first 10 rows) ===")
        print(json.dumps(results[:10], indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
