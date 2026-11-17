param([int]$Seconds = 180, [switch]$SkipFaults)
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
    if ($summary.result.group -eq 'arrival' -and -not $summary.steady_window_pass) { throw "Queue growth gate failed: $runDirectory" }
}
# Sequential fresh stacks, fixed database/CPU budgets and disk synchronization.
if (Select-String -LiteralPath 'src/main/java/com/hmdp/mq/SeckillOrderMessageService.java' -SimpleMatch 'private SendResult sendConfirmed' -Quiet) {
    Write-Output 'Admission Outbox removed; running broker-confirmed acceptance instead of archived P7 variants.'
    & (Join-Path $PSScriptRoot 'run_broker_acceptance.ps1') @PSBoundParameters
    return
}
foreach ($variant in @('single', 'batched')) {
    $arguments = @('tests/load/run_http_perf.py', '--jmeter', $jmeter, '--threads', '500',
        '--warmup', '--precise', '--storage', 'disk', '--label', "p7-disk-$variant")
    if ($variant -eq 'single') { $arguments += '--disable-outbox-batch' }
    Invoke-Tier -RunArguments $arguments
}
Invoke-Tier -RunArguments @('tests/load/run_http_perf.py','--jmeter',$jmeter,'--rps','100','--seconds',"$Seconds",'--warmup','--precise','--storage','disk','--label',"p7-disk-steady-$Seconds")
if (-not $SkipFaults) {
    & $python tests/load/run_http_faults.py
    if ($LASTEXITCODE -ne 0) { throw 'Fault regression failed' }
}
# Inspect each perf-result.json and p7-summary.json. Process exit alone is not the SLO verdict.
