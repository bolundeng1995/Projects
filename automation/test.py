import sys

if sys.prefix != sys.base_prefix:
    print("In a virtual environment")
else:
    print("Not in a venv")