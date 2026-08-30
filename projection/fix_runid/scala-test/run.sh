#!/usr/bin/env bash
# Generate the cell wrappers, then compile and run them offline.
set -e
cd "$(dirname "$0")"
python generate_wrappers.py
mvn -o test
