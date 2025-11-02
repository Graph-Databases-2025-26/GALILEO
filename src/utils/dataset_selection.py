import sys
from src.utils import LOG
from src.utils import  DATASETS

def get_dataset_selection(database_run: str) -> list[str]:
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