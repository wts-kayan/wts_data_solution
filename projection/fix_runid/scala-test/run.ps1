# Generate the cell wrappers, then compile and run them offline.
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
python generate_wrappers.py
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
mvn -o test
exit $LASTEXITCODE
