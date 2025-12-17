import json
from src.config.loaders import Config_Loader
from src.utils.constants import debug_query
from src.utils.logging_config import log_init, LOG
from src.utils import sql_query_parser
from src.galois.galois import Galois

def debug():
    #log_init()
    cfg = Config_Loader().get_config() # load default config

    dataset = "WORLD" # use WORLD dataset
    query_template = debug_query()
    sql_query = query_template.format()
    LOG.info("\n=== INPUT QUERY ===")
    LOG.debug(sql_query)

    LOG.info("\n=== PARSER OUTPUT (sql_query_parser.parse_sql) ===")
    parsed = sql_query_parser.parse_sql(sql_query) # parse the SQL query
    LOG.debug(json.dumps(parsed, indent=2, ensure_ascii=False))

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
        LOG.info(f"\n\n================ {tag} ================\n")

        results, stats, debug_info = fn(debug=True) # execute the query

        LOG.info("=== DEBUG INFO ===")
        LOG.debug(json.dumps(debug_info, indent=2, ensure_ascii=False))

        LOG.info("=== STATS ===")
        LOG.debug(json.dumps(stats, indent=2, ensure_ascii=False))

        LOG.info("=== RESULT (first 10 rows) ===")
        LOG.debug(json.dumps(results[:10], indent=2, ensure_ascii=False))


if __name__ == "__main__":
    debug()
