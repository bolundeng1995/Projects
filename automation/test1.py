import sys

if sys.prefix != sys.base_prefix:
    print("Hello!")
else:
    print("Not good!")