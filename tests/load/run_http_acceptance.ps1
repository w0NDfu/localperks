param([int]$Seconds = 60, [int[]]$Rounds = @(1, 2, 3), [switch]$BurstOnly)
$ErrorActionPreference = 'Stop'
$env:PYTHONDONTWRITEBYTECODE = '1'
$env:PYTHONIOENCODING = 'utf-8'
$python = Join-Path $PSScriptRoot '..\..\agent-service\.venv\Scripts\python.exe'
$jmeter = 'artifacts\test-runs\.tools\apache-jmeter-5.6.3\bin\jmeter.bat'
$baseline = 'artifacts\test-runs\v03-20261107-165630-9f6eb7d9\source-before'
# Alternating variants; each runner creates and drains its own isolated environment.
foreach ($round in $Rounds) {
    foreach ($variant in @('original', 'optimized')) {
        $arguments = @('tests\load\run_http_perf.py', '--jmeter', $jmeter,
            '--threads', '500', '--warmup', '--precise', '--label', "p6-repeat-$round-$variant")
        if ($variant -eq 'original') { $arguments += @('--baseline-source', $baseline) }
        & $python @arguments
        Write-Output "ACCEPTANCE_RESULT variant=$variant round=$round exit=$LASTEXITCODE"
    }
}
if (-not $BurstOnly) {
    foreach ($rate in @(20, 50, 100)) {
        & $python tests\load\run_http_perf.py --jmeter $jmeter --rps $rate --seconds $Seconds --warmup --precise --label "p6-final-$rate-rps-$Seconds-sec"
        Write-Output "ACCEPTANCE_RESULT rate=$rate exit=$LASTEXITCODE"
    }
    & $python tests\load\run_http_faults.py
    Write-Output "ACCEPTANCE_RESULT faults exit=$LASTEXITCODE"
}
