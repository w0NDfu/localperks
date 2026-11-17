param(
    [string]$RunId = ('v03-' + (Get-Date -Format 'yyyyMMdd-HHmmss') + '-' + [guid]::NewGuid().ToString('N').Substring(0,8)),
    [switch]$SkipDocker,
    [switch]$RealModel,
    [string]$PriorModelRun,
    [switch]$InjectFailureAfterIsolation,
    [int]$MaxModelRequests = 40,
    [int]$MaxModelTokens = 24000
)
$ErrorActionPreference = 'Stop'
$repository = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$agent = [IO.Path]::GetFullPath((Join-Path $repository 'agent-service'))
if ($RunId -notmatch '^v03-\d{8}-\d{6}-[a-f0-9]{8}$') { throw 'Invalid run ID' }
if ($MaxModelRequests -lt 1 -or $MaxModelRequests -gt 40 -or $MaxModelTokens -lt 1024 -or $MaxModelTokens -gt 24000) { throw 'Invalid model budget' }
$run = Join-Path $repository "artifacts/test-runs/$RunId"
if (Test-Path -LiteralPath $run) { throw 'Run already exists; refusing to reuse or delete it' }
New-Item -ItemType Directory -Path (Join-Path $run 'temp') -Force | Out-Null
$RunId | Set-Content -LiteralPath (Join-Path $run '.run-owner')
$manifest = [ordered]@{ run_id = $RunId; created_at = [DateTimeOffset]::Now.ToString('o'); compose_project = "sltest-$RunId"; resources = @() }
function Save-Manifest { $manifest | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $run 'resource_manifest.json') -Encoding utf8 }
function Register-Resource($Type, $Identifier, $Method) {
    $manifest.resources += [ordered]@{ run_id = $RunId; created_at = [DateTimeOffset]::Now.ToString('o'); type = $Type; name = $Identifier; identifier = $Identifier; compose_project = $manifest.compose_project; creation_method = $Method; isolated = $true; auto_cleanup = $true; cleanup_status = 'pending' }
    Save-Manifest
}
Register-Resource 'directory' (Join-Path $run 'temp') 'New-Item; all disposable output under this subtree'
$results = @(); $exitCode = 0; $environmentBackup = @{}
$python = Join-Path $agent '.venv/Scripts/python.exe'
function Set-RunEnvironment($Name, $Value) {
    if (-not $environmentBackup.ContainsKey($Name)) { $environmentBackup[$Name] = [Environment]::GetEnvironmentVariable($Name, 'Process') }
    [Environment]::SetEnvironmentVariable($Name, $Value, 'Process')
}
function Sample($Phase, [switch]$Quick) {
    $sample = & (Join-Path $PSScriptRoot 'test_resources.ps1') -RunDirectory $run -Phase $Phase -Quick:$Quick
    if ($sample.run_bytes -gt 1GB) { throw 'Run artifact budget exceeded (1 GiB)' }
    if ($null -ne $sample.available_memory_bytes -and $sample.available_memory_bytes -lt 2GB) { throw 'Available Windows memory below 2 GiB; stopping load' }
    $drive = @($sample.drives | Where-Object drive -eq 'D')
    if ($drive.Count -and $drive[0].free_bytes -lt 5GB) { throw 'D drive free space below 5 GiB' }
    $dockerBytes = 0.0
    foreach ($line in $sample.docker_stats) {
        try {
            $stats = $line | ConvertFrom-Json
            $value = ($stats.MemUsage -split '/')[0].Trim()
            if ($value -match '^([0-9.]+)([KMGT]?i?B)$') {
                $units = @{ B=1; kB=1000; MB=1e6; GB=1e9; KiB=1024; MiB=1MB; GiB=1GB }
                $dockerBytes += [double]$Matches[1] * $units[$Matches[2]]
            }
        } catch { }
    }
    if ($dockerBytes -gt 6GB) { throw 'Observed Docker aggregate memory above 6 GiB; stopping current workload' }
}
function Run-Step([string]$Name, [string]$Executable, [string[]]$Arguments, [string]$WorkingDirectory, [int]$TimeoutSeconds = 240) {
    Sample "before-$Name" -Quick
    $out = Join-Path $run "$Name.stdout.log"; $err = Join-Path $run "$Name.stderr.log"
    $clock = [Diagnostics.Stopwatch]::StartNew()
    # Paths are generated inside this workspace. Quote each argv without invoking a command shell.
    $quoted = @($Arguments | ForEach-Object { '"' + $_.Replace('"','\"') + '"' })
    $process = Start-Process -FilePath $Executable -ArgumentList $quoted -WorkingDirectory $WorkingDirectory -WindowStyle Hidden -PassThru -RedirectStandardOutput $out -RedirectStandardError $err
    $code = 1; $reason = $null
    try {
        while (-not $process.WaitForExit(3000)) {
            Sample "running-$Name" -Quick
            if ($clock.Elapsed.TotalSeconds -gt $TimeoutSeconds) { throw "Step timeout: $TimeoutSeconds seconds" }
        }
        $process.Refresh(); $code = $process.ExitCode
    } catch {
        $reason = $_.Exception.Message
        # Only the process tree launched by this step is terminated.
        & taskkill /PID $process.Id /T /F 2>$null | Out-Null
        $code = 1
    } finally { $process.Dispose() }
    $script:results += [ordered]@{ name = $Name; command = "$Executable $($Arguments -join ' ')"; exit_code = $code; duration_seconds = $clock.Elapsed.TotalSeconds; status = $(if ($code -eq 0) {'PASS'} else {'FAIL'}); reason = $reason }
    $script:results | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $run 'steps.json') -Encoding utf8
    if ($code -ne 0) { $script:exitCode = 1 }
    Write-Host "$Name exit=$code seconds=$([math]::Round($clock.Elapsed.TotalSeconds,2))"
}
function Create-TestContainer([string]$Image, [string]$Role, [string[]]$Options, [string[]]$Command) {
    & docker image inspect $Image 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Cached image missing: $Image; pulls are disabled" }
    $containerArgs = @('create', '--name', "$($manifest.compose_project)-$Role", '--label', "localperks.validation.run=$RunId", '--label', "com.docker.compose.project=$($manifest.compose_project)", '--cpus', '0.5') + $Options + @($Image) + $Command
    $identifier = & docker @containerArgs
    if ($LASTEXITCODE -ne 0) { throw "Could not create isolated $Role" }
    Register-Resource 'container' $identifier.Trim() 'docker create, cached image, tmpfs only, bounded CPU/memory'
    & docker start $identifier | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Could not start $Role" }
    return $identifier.Trim()
}
Push-Location $repository
try {
    Set-RunEnvironment 'VALIDATION_RUN_DIR' $run
    Set-RunEnvironment 'PYTHONPATH' "$repository/tests;$agent;$agent/tests"
    Set-RunEnvironment 'PYTHONDONTWRITEBYTECODE' '1'
    Set-RunEnvironment 'PYTHONIOENCODING' 'utf-8'
    Set-RunEnvironment 'TEMP' (Join-Path $run 'temp')
    Set-RunEnvironment 'TMP' (Join-Path $run 'temp')
    Set-RunEnvironment 'AGENT_STATE_SCHEMA' 'test_validation'
    Set-RunEnvironment 'AGENT_TEST_PGVECTOR' '0'
    Set-RunEnvironment 'AGENT_TEST_REDIS_URL' ''
    Set-RunEnvironment 'PLAYWRIGHT_CHANNEL' 'chrome'
    Set-RunEnvironment 'PLAYWRIGHT_PORT' '5175'
    Set-RunEnvironment 'PLAYWRIGHT_HTML_OUTPUT_DIR' (Join-Path $run 'playwright-report')
    Set-RunEnvironment 'PLAYWRIGHT_JSON_OUTPUT_NAME' (Join-Path $run 'playwright.json')
    Set-RunEnvironment 'MAVEN_OPTS' '-Xms64m -Xmx512m'
    Set-RunEnvironment 'OPENAI_MAX_REQUESTS' "$MaxModelRequests"
    Set-RunEnvironment 'OPENAI_MAX_TOKENS_BUDGET' "$MaxModelTokens"
    Sample 'before'
    & $python (Join-Path $repository 'tests/load/validation_workload.py') environment --output $run
    if ($LASTEXITCODE -ne 0) { throw 'Environment capture failed' }
    if ($PriorModelRun) {
        $prior = [IO.Path]::GetFullPath($PriorModelRun).TrimEnd('\','/')
        if ((Split-Path -Parent $prior) -ne (Join-Path $repository 'artifacts/test-runs')) { throw 'Prior evidence must be inside this workspace test-runs' }
        $priorManifest = Get-Content (Join-Path $prior 'resource_manifest.json') -Raw | ConvertFrom-Json
        if ($priorManifest.run_id -ne (Split-Path -Leaf $prior)) { throw 'Prior run ownership mismatch' }
        $evidence = Join-Path $run 'prior-real-evidence'
        New-Item -ItemType Directory -Path $evidence | Out-Null
        foreach ($name in @('model_budget.json','baseline_real_comparison.csv','rag_unverified.json','real-model-rag.stdout.log','real-model-rag.stderr.log')) {
            if (Test-Path (Join-Path $prior $name)) { Copy-Item -LiteralPath (Join-Path $prior $name) -Destination (Join-Path $evidence $name) }
        }
        @{ run_id = $priorManifest.run_id; source_directory = $prior; reason = 'Preserve earlier real-model evidence without additional paid requests'; ledger = (Get-Content (Join-Path $evidence 'model_budget.json') -Raw | ConvertFrom-Json) } | ConvertTo-Json -Depth 12 | Set-Content (Join-Path $run 'prior_real_evidence.json') -Encoding utf8
    }
    if (-not $SkipDocker) {
        try {
            $redisId = Create-TestContainer 'redis:7.2-alpine' 'redis' @('--memory','128m','--memory-swap','128m','--tmpfs','/data:rw,size=64m','-p','127.0.0.1::6379') @('redis-server','--save','','--appendonly','no','--maxmemory','64mb','--maxmemory-policy','noeviction')
            $redisPort = ((& docker port $redisId '6379/tcp').Trim() -split ':')[-1]
            Set-RunEnvironment 'AGENT_TEST_REDIS_URL' "redis://127.0.0.1:$redisPort/0"
            Set-RunEnvironment 'REDIS_URL' "redis://127.0.0.1:$redisPort/0"
            Set-RunEnvironment 'VALIDATION_REDIS_PORT' $redisPort
            $pgId = Create-TestContainer 'pgvector/pgvector:pg16' 'pgvector' @('--memory','384m','--memory-swap','384m','--tmpfs','/var/lib/postgresql/data:rw,size=128m','-p','127.0.0.1::5432','-e','POSTGRES_PASSWORD=isolated-test-only','-e','POSTGRES_DB=validation') @('postgres','-c','shared_buffers=32MB','-c','max_connections=20')
            $pgPort = ((& docker port $pgId '5432/tcp').Trim() -split ':')[-1]
            Set-RunEnvironment 'PGVECTOR_DSN' "postgresql://postgres:isolated-test-only@127.0.0.1:$pgPort/validation"
            Set-RunEnvironment 'AGENT_STATE_DSN' "postgresql://postgres:isolated-test-only@127.0.0.1:$pgPort/validation"
            Set-RunEnvironment 'AGENT_TEST_STATE_DSN' "postgresql://postgres:isolated-test-only@127.0.0.1:$pgPort/validation"
            Set-RunEnvironment 'EMBEDDING_DIMENSIONS' '3'
            Run-Step 'isolation-seed' $python @('tests/load/validation_workload.py','seed','--output',$run) $repository 45
            if ($results[-1].exit_code -eq 0) { Set-RunEnvironment 'AGENT_TEST_PGVECTOR' '1' }
        } catch {
            $results += @{ name = 'isolated-dependencies'; status = 'NOT_VERIFIED'; reason = $_.Exception.Message }
            $exitCode = 1
        }
    }
    if ($InjectFailureAfterIsolation) { throw 'INJECTED_RUNNER_FAILURE: verifying finally cleanup and failure exit preservation' }
    Run-Step 'python' $python @('-m','pytest','-p','validation_plugin',"$agent/tests","$repository/tests/integration","$repository/tests/reliability","$repository/tests/security",'-q','--basetemp',"$run/temp/pytest",'-o',"cache_dir=$run/temp/pytest-cache",'--junitxml',"$run/pytest.xml") $repository
    $maven = (Get-Command mvn.cmd).Source
    $javaArgs = @('-o','-B','-Dfile.encoding=UTF-8','-DargLine=-Xmx384m','test',"-Dvalidation.redis.port=$env:VALIDATION_REDIS_PORT")
    Run-Step 'java' $maven $javaArgs $repository
    $npm = (Get-Command npm.cmd).Source
    Run-Step 'frontend-test' $npm @('run','test','--','--reporter=default','--reporter=json',"--outputFile=$run/vitest.json") (Join-Path $repository 'frontend')
    Run-Step 'frontend-lint' $npm @('run','lint') (Join-Path $repository 'frontend')
    Run-Step 'frontend-build' $npm @('run','build') (Join-Path $repository 'frontend')
    Run-Step 'browser' $npm @('run','e2e','--','--workers=1',"--output=$run/browser-evidence",'--reporter=json') (Join-Path $repository 'frontend')
    Run-Step 'mock-load' $python @('tests/load/validation_workload.py','load','--output',$run) $repository
    Run-Step 'offline-baseline' $python @('tests/load/validation_workload.py','compare','--output',$run) $repository
    Run-Step 'fixture-rag' $python @('tests/load/validation_workload.py','rag-fixture','--output',$run) $repository
    if ($RealModel) { Run-Step 'real-model-rag' $python @('tests/load/validation_workload.py','real','--output',$run) $repository 600 }
    Sample 'end'
} catch {
    $results += @{ name = 'validation-runner'; status = 'FAIL'; reason = $_.Exception.Message }
    $exitCode = 1
} finally {
    $results | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $run 'steps.json') -Encoding utf8
    try { & (Join-Path $PSScriptRoot 'test_cleanup.ps1') -RunDirectory $run -DryRun | Out-File (Join-Path $run 'cleanup-dry-run.log') }
    catch { $_.Exception.Message | Set-Content (Join-Path $run 'cleanup-dry-run-error.log'); $exitCode = 1 }
    try { & (Join-Path $PSScriptRoot 'test_cleanup.ps1') -RunDirectory $run -Apply | Out-File (Join-Path $run 'cleanup-apply.log') }
    catch { $_.Exception.Message | Set-Content (Join-Path $run 'cleanup-error.log'); $exitCode = 1 }
    # Per repository policy, stop this project's existing Compose services, preserving containers and volumes.
    & docker compose --profile agent --profile dashboard stop 2>&1 | Out-File (Join-Path $run 'project-stop.log')
    if ($LASTEXITCODE -ne 0) { $exitCode = 1 }
    try {
        $stopped = @()
        $owned = @(Get-CimInstance Win32_Process | Where-Object {
            ($_.Name -eq 'node.exe' -and $_.CommandLine -like "*$repository*" -and $_.CommandLine -match '[\\/]frontend[\\/].*[\\/]vite[\\/]') -or
            ($_.Name -eq 'java.exe' -and $_.CommandLine -like "*$repository*" -and $_.CommandLine -match 'HmDianPingApplication|hm-dianping[^\s]*\.jar|spring-boot:run') -or
            ($_.Name -match '^pythonw?\.exe$' -and $_.CommandLine -like "*$agent*" -and $_.CommandLine -match 'uvicorn|[\\/]main\.py')
        })
        foreach ($process in $owned) {
            Stop-Process -Id $process.ProcessId -Force -ErrorAction Stop
            $stopped += @{ pid = $process.ProcessId; executable = $process.Name }
        }
        $listeners = @(Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Where-Object LocalPort -in @(5173,5175,8081,8090,3307,6379,5433,9876,10909,10911,10912,8088) | Select-Object LocalAddress,LocalPort,OwningProcess)
        @{ stopped_owned_processes = $stopped; remaining_listeners = $listeners; note = 'Only full project paths in command lines prove process ownership; unknown listeners are retained.' } | ConvertTo-Json -Depth 6 | Set-Content (Join-Path $run 'service_cleanup.json') -Encoding utf8
    } catch { $_.Exception.Message | Set-Content (Join-Path $run 'service-cleanup-error.log'); $exitCode = 1 }
    try { & (Join-Path $PSScriptRoot 'test_resources.ps1') -RunDirectory $run -Phase 'after-cleanup' | Out-Null }
    catch { $_.Exception.Message | Set-Content (Join-Path $run 'resource-final-error.log') }
    & $python (Join-Path $repository 'tests/load/validation_workload.py') report --output $run
    if ($LASTEXITCODE -ne 0) { $exitCode = 1 }
    foreach ($name in $environmentBackup.Keys) { [Environment]::SetEnvironmentVariable($name, $environmentBackup[$name], 'Process') }
    Pop-Location
}
Write-Host "Run artifacts: $run"
exit $exitCode
