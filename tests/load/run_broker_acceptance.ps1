param([int]$Seconds = 180, [switch]$SkipFaults, [switch]$IncludeControl, [switch]$IncludeReadControl)
$ErrorActionPreference = 'Stop'
$python = 'agent-service\.venv\Scripts\python.exe'
$jmeter = 'artifacts\test-runs\.tools\apache-jmeter-5.6.3\bin\jmeter.bat'
$env:PYTHONDONTWRITEBYTECODE = '1'
$env:PYTHONIOENCODING = 'utf-8'
function Invoke-Tier([string[]]$RunArguments) {
    & $python @RunArguments 2>&1 | Tee-Object -Variable tierOutput
    if ($LASTEXITCODE -ne 0) { throw 'Performance runner failed' }
    $line = @($tierOutput | ForEach-Object { $_.ToString() } | Where-Object { $_.StartsWith('HTTP_PERF ') })[-1]
    if (-not $line) { throw 'Missing run identity' }
    $runDirectory = $line.Substring('HTTP_PERF '.Length)
    & $python tests/load/summarize_p7.py $runDirectory
    if ($LASTEXITCODE -ne 0) { throw 'Could not summarize evidence' }
    $summary = Get-Content -LiteralPath (Join-Path $runDirectory 'p7-summary.json') -Raw | ConvertFrom-Json
    if (-not $summary.correctness_pass) { throw "Consistency gate failed: $runDirectory" }
    if ($summary.result.admission_sql_rows -ne 0) { throw "New admission SQL rows found: $runDirectory" }
    if ($summary.result.group -eq 'arrival' -and -not $summary.steady_window_pass) { throw "Queue growth gate failed: $runDirectory" }
}
# Sequential fresh disk stacks; never run the tiers concurrently on this host.
if ($IncludeReadControl) {
    Invoke-Tier -RunArguments @('tests/load/run_http_perf.py','--jmeter',$jmeter,'--threads','500','--warmup','--precise','--storage','disk','--disable-eligibility-batch','--label','single-eligibility-read-control-500')
}
if ($IncludeControl) {
    Invoke-Tier -RunArguments @('tests/load/run_http_perf.py','--jmeter',$jmeter,'--threads','500','--warmup','--precise','--storage','disk','--disable-broker-batch','--label','broker-single-control-500')
}
Invoke-Tier -RunArguments @('tests/load/run_http_perf.py','--jmeter',$jmeter,'--threads','500','--warmup','--precise','--storage','disk','--label','broker-confirmed-500')
Invoke-Tier -RunArguments @('tests/load/run_http_perf.py','--jmeter',$jmeter,'--rps','100','--seconds',"$Seconds",'--warmup','--precise','--storage','disk','--label',"broker-confirmed-steady-$Seconds")
if (-not $SkipFaults) {
    & $python tests/load/run_http_faults.py
    if ($LASTEXITCODE -ne 0) { throw 'Fault regression failed' }
}
