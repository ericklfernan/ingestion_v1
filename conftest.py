import sys
import os

sys.dont_write_bytecode = True

_ROOT = os.path.dirname(os.path.abspath(__file__))
_SRC = os.path.join(_ROOT, "src")

for p in [_SRC, _ROOT]:
    if p not in sys.path:
        sys.path.insert(0, p)
