# Compile and run the cell tests offline. Wrapper generation is bound to the
# build's generate-sources phase, so `mvn -o test` re-wraps the cells itself.
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
mvn -o test
exit $LASTEXITCODE
