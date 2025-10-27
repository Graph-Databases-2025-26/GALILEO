from loguru import logger
import sys
from pathlib import Path
import warnings

# === Create /logs directory if it doesn't exist ===
LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "pipeline.log"

# === Configure loguru ===
logger.remove()  # remove default handler

# 1️⃣ Console output (nice, colored)
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
           "<level>{level: <8}</level> | "
           "<cyan>{message}</cyan>",
    level="INFO",
    colorize=True,
)

# 2️⃣ File output (persistent logs)
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
    Helper to log structured events, e.g. dataset progress, latency, tokens, etc.
    """
    context = " ".join(f"{k}={v}" for k, v in kwargs.items())
    logger.info(f"{event} {context}")

def warning_to_loguru(message, category, filename, lineno, file=None, line=None):
    """Redirect Python warnings (e.g., DeprecationWarning) to loguru"""
    logger.warning(f"{category.__name__} at {filename}:{lineno} → {message}")

warnings.showwarning = warning_to_loguru
