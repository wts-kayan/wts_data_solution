#!/usr/bin/env bash
# Compile both versions of the step and compare their output field by field.
set -e
cd "$(dirname "$0")"
javac -d out *.java
java -cp out Compare "$@"
