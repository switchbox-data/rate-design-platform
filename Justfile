default: help

help:
\t@echo "Available recipes:"
\t@just -l

install:
\tpip install --upgrade pip
\tpip install -e .[dev]

test:
\tpytest -q
