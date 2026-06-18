import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = PROJECT_ROOT / "src"

if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))
