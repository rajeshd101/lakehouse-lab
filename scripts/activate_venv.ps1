Set-StrictMode -Version Latest

$venvActivate = Join-Path $PSScriptRoot "..\\venv\\Scripts\\Activate.ps1"

if (-not (Test-Path $venvActivate)) {
    Write-Error "venv not found at $venvActivate"
    exit 1
}

& $venvActivate
