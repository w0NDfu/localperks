param([switch]$SkipModels, [string]$ContinueFrom)
$ErrorActionPreference = 'Stop'
$repository = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$python = [IO.Path]::GetFullPath((Join-Path $repository 'agent-service/.venv/Scripts/python.exe'))
$previousBytecode = $env:PYTHONDONTWRITEBYTECODE
$env:PYTHONDONTWRITEBYTECODE = '1'
Push-Location $repository
try {
    $arguments = @('tests/real_e2e/run.py')
    if ($SkipModels) { $arguments += '--skip-models' }
    if ($ContinueFrom) { $arguments += @('--continue-from',$ContinueFrom) }
    & $python @arguments
    $code = $LASTEXITCODE
} finally { $env:PYTHONDONTWRITEBYTECODE = $previousBytecode; Pop-Location }
exit $code
