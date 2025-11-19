from pathlib import Path
import sys, subprocess, os

ROOT = Path(__file__).resolve().parent
VENV = ROOT / ".venv"
REQS_PATH = ROOT / "requirements.txt"

if(os.name == "nt"):
    PIP = VENV / "Scripts" / "pip"
    PY  = VENV / "Scripts" / "python"
else:
    PIP = VENV / "bin" / "pip"
    PY  = VENV / "bin" / "python"

def run(cmd: list[str | Path], *, check: bool = True) -> None: #helper to run a command
    printable = " ".join(map(str, cmd)) #convert command to string for printing
    print(f"\n Running: {printable}")
    subprocess.run([str(c) for c in cmd], check=check) #execute command

def main():
    # Create venv if missing
    if not VENV.exists():
        print(" Creating virtual environment...")
        run([sys.executable, "-m", "venv", str(VENV)])
    else:
        print(" Virtual environment already exists")

        # Install dependencies
    if REQS_PATH.exists():
        print("\n Installing dependencies from requirements.txt...")

        # Always use 'python -m pip'
        run([str(PY), "-m", "pip", "install", "--upgrade", "pip"], check=False)

        print(" Installing CPU-only version of PyTorch (Lightweight)...")
        # Forziamo l'installazione di torch versione CPU prima di tutto il resto
        run([
            str(PY), "-m", "pip", "install",
            "torch", "torchvision",
            "--index-url", "https://download.pytorch.org/whl/cpu"
        ], check=True)

        run([str(PY), "-m", "pip", "install", "-r", str(REQS_PATH)])
    else:
        print(f"  requirements.txt not found at {REQS_PATH}; skipping installs")

if __name__ == "__main__":
    main()
