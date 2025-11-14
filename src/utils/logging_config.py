from loguru import logger
from .constants import LOGS_DIR
from pathlib import Path
import sys, warnings

def log_init() -> None:
    """
    Initializes the `loguru` logger with two handlers:
    1. Console output (stdout): Uses a colored format for real-time visibility and logs messages at the 'INFO' level or higher.
    2. File output: Writes persistent logs to 'pipeline.log' within the configured `LOGS_DIR`. Logs messages at 'DEBUG' level or higher.
       It also sets up file rotation (5 MB), retention (10 days), and compression (zip) for old logs.
  
    The function ensures the log directory exists before adding handlers.
    """
    
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    LOG_FILE = LOGS_DIR / "pipeline.log"
    
    logger.remove()  

    #Console output
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{message}</cyan>",
        level="INFO",
        colorize=True,
    )

    #File output
    logger.add(
        LOG_FILE,
        rotation="5 MB",          # start new file every 5 MB
        retention="10 days",      # keep logs for 10 days
        compression="zip",        # compress old logs
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message}",
        level="DEBUG",            # store all messages
    )


def log_query_event(event: str, **kwargs):
    """
    Helper function to log structured events as informational messages.

    This is typically used to record metrics or progress for a query, 
    such as dataset progress, latency, token counts, etc.

    Args:
        event: A string describing the type of event (e.g., "QUERY_START", "LATENCY_RECORD").
        **kwargs: Arbitrary keyword arguments representing the structured context (e.g., `dataset="GEO"`, `tokens=1024`, `latency=0.5`).
    """
  
    context = " ".join(f"{k}={v}" for k, v in kwargs.items())
    logger.info(f"{event} {context}")


def warning_to_loguru(message, category, filename, lineno, file=None, line=None):
    """
    Redirects standard Python warnings (e.g., DeprecationWarning) to the 
    `loguru` logger as 'WARNING' messages.

    This function is assigned to `warnings.showwarning` to centralize 
    warning output with the rest of the application's logging.

    Args:
        message: The warning message string.
        category: The class of the warning (e.g., `DeprecationWarning`).
        filename: The name of the file where the warning occurred.
        lineno: The line number where the warning occurred.
        file: (Ignored) File stream for output.
        line: (Ignored) Source line text.
    """
    
    logger.warning(f"{category.__name__} at {filename}:{lineno} → {message}")

warnings.showwarning = warning_to_loguru

LOG = logger