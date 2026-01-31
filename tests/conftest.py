import sys
import pathlib

# Ensure the repository root is on sys.path so tests can import project modules
ROOT = pathlib.Path(__file__).parent.parent.resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
