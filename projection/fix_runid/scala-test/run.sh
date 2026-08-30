#!/usr/bin/env bash
# Compile and run the cell tests offline. Wrapper generation is bound to the
# build's generate-sources phase, so `mvn -o test` re-wraps the cells itself.
set -e
cd "$(dirname "$0")"
mvn -o test
