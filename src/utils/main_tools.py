from src.utils import LOG, DATASETS, ERR_FILE_READ_FAILURE

from pathlib import Path
import sys, re

def get_dataset_selection(database_run: str) -> list[str]:
    """
    Determines the list of datasets to process based on command-line arguments 
    or a configuration parameter.

    Command-line arguments take precedence. If 'ALL' is specified, or if no 
    valid datasets are provided, all available datasets are returned.

    Args:
        database_run: A comma-separated string of dataset names from a configuration file (e.g., "FLIGHT-2, FORTUNE"). Can be None.

    Returns:
        A list of uppercase strings representing the valid datasets to run. 
        It defaults to all datasets if the input is invalid or 'ALL'.
    """
  
    #  Priority to the command line
    if len(sys.argv) > 1:
        args = [arg.upper() for arg in sys.argv[1:]]
        LOG.info(f"Command line args: {args}")
    else:
        #  Otherwise, use the parameter from the .yaml file
        if database_run is None:
            database_run = "ALL"
        else:
            args = [s.strip().upper() for s in database_run.split(",")]

        LOG.info(f"Parameters from YAML file: {args}")

    # Parameters validation
    if "ALL" in args:
        return DATASETS
    valid = [d for d in args if d in DATASETS]
    invalid = [d for d in args if d not in DATASETS]
    if invalid:
        LOG.warning(f"Dataset '{args}' not valid. The available datasets are: {DATASETS}")
    return valid if valid else DATASETS  # fallback with every dataset if any is valid


def load_queries_from_folder(data_folder: Path) -> list[str]:
    """
    Loads and parses SQL queries from a file named 'queries_*.sql' within the 
    specified data folder.

    The function expects queries to be delimited by a regex pattern 
    `--query[N]`.

    Args:
        data_folder: A `pathlib.Path` object pointing to the directory containing the query files.

    Returns:
    A list of strings, where each string is a stripped SQL query.
    Returns an empty list if the file cannot be read or no queries are found.
    """
    
    sql_files = list(data_folder.glob("queries_*.sql"))    
    file_path = sql_files[0] 
    
    f_content = ""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            f_content = f.read().strip()
            
    except Exception as e:
        LOG.error(ERR_FILE_READ_FAILURE.format(file_path, e))
        return {}
    
    qry_regx = re.compile(r'--query(?:\d+)\s*(.*?)(?=--query|\Z)', re.DOTALL | re.IGNORECASE)    
    matches = qry_regx.findall(f_content)

    queries = []
    for qry in matches:
        queries.append(qry.strip())
        
    return queries