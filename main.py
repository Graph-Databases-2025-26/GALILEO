from src import VENV, REQS
from src.utils import PY
import sys, subprocess
from pathlib import Path
from src.utils.logging_config import logger


def run(cmd: list[str | Path], *, check: bool = True) -> None: #helper to run a command
    printable = " ".join(map(str, cmd)) #convert command to string for printing
    logger.info(f"\n Running: {printable}")
    subprocess.run([str(c) for c in cmd], check=check) #execute command

def main():
    # Create venv if missing
    if not VENV.exists():
        logger.info(" Creating virtual environment...")
        run([sys.executable, "-m", "venv", str(VENV)])
    else:
        logger.info(" Virtual environment already exists")

        # Install dependencies
    if REQS.exists():
        logger.info("\n Installing dependencies from requirements.txt...")

        # Always use 'python -m pip'
        run([str(PY), "-m", "pip", "install", "--upgrade", "pip"], check=False)
        run([str(PY), "-m", "pip", "install", "-r", str(REQS)])
    else:
        logger.warning(f"  requirements.txt not found at {REQS}; skipping installs")
        
if __name__ == "__main__":
    main()
