import sys
import warnings

from loguru import logger

from .constants import LOGS_DIR


def log_init() -> None:
    """
    Initializes the `loguru` logger with two handlers:
    1. Console output (stdout): Includes Timestamp (Green) and Message (Cyan).
    2. File output: Full verbose logs (time, level) for persistent debugging.
    """
    
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    LOG_FILE = LOGS_DIR / "pipeline.log"
    
    # Rimuove il logger di default per evitare duplicati
    logger.remove()  

    # 1. Console output (Hybrid Style: Timestamp + Cyan Message)
    logger.add(
        sys.stdout,
        # Data/Ora in Verde | Messaggio in Azzurro (Cyan)
        # Rimuoviamo {level} per risparmiare spazio e mantenere i tuoi tag personalizzati (PLAN, EXEC) puliti
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <cyan>{message}</cyan>",
        level="INFO",
        colorize=True,
    )

    # 2. File output (Verbose / Debug Style)
    # Mantiene timestamp e livelli per tracciabilità storica su file
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
    """
    context = " ".join(f"{k}={v}" for k, v in kwargs.items())
    logger.info(f"EVNT | [METRICS] {event} {context}")


def warning_to_loguru(message, category, filename, lineno, file=None, line=None):
    """
    Redirects standard Python warnings to loguru.
    """
    logger.warning(f"WARN | [SYSTEM] {category.__name__} at {filename}:{lineno} -> {message}")

# Intercept standard warnings
warnings.showwarning = warning_to_loguru

# Export logger alias
LOG = logger