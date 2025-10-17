# setup-environment/config/setup_project.py
from __future__ import annotations
import os
import sys
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]  # project root (2 folder up C:\memoria\galileo_proj)
VENV = ROOT / ".venv" #python virtual 
PIP  = VENV / ("Scripts/pip" if os.name == "nt" else "bin/pip") #pip executable in venv
PY   = VENV / ("Scripts/python" if os.name == "nt" else "bin/python") 

REQS = ROOT / "requirements.txt" #requirements file path
TEST = ROOT / "setup-environment" / "config" / "db_connection.py" #test connection script path

def run(cmd: list[str | Path], *, check: bool = True) -> None: #helper to run a command
    printable = " ".join(map(str, cmd)) #convert command to string for printing
    print(f"\n Running: {printable}")
    subprocess.run([str(c) for c in cmd], check=check) #execute command

def main() -> None:
    print(f" setup_project.py starting at ROOT = {ROOT}")

    # Create venv if missing
    if not VENV.exists():
        print(" Creating virtual environment...")
        run([sys.executable, "-m", "venv", str(VENV)])
    else:
        print(" Virtual environment already exists")

    # Install dependencies
    if REQS.exists():
        print("\n Installing dependencies from requirements.txt...")

    # Always use 'python -m pip' 
        run([str(PY), "-m", "pip", "install", "--upgrade", "pip"], check=False)
        run([str(PY), "-m", "pip", "install", "-r", str(REQS)])
    else:
        print(f"  requirements.txt not found at {REQS}; skipping installs")

    #  Run the connection test
    if TEST.exists():
        print("\n Testing connection...")
        run([str(PY), str(TEST)])
    else:
        print(f" Test file not found: {TEST}")

    print("\n Setup completed.")

if __name__ == "__main__":
    main()


