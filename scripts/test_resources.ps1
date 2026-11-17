param(
    [Parameter(Mandatory = $true)][string]$RunDirectory,
    [string]$Phase = 'snapshot',
    [switch]$Quick
)
$ErrorActionPreference = 'Stop'
$run = [IO.Path]::GetFullPath($RunDirectory)
if (-not (Test-Path -LiteralPath (Join-Path $run 'resource_manifest.json'))) { throw 'Missing run manifest' }
$errors = @()
$available = $null; $total = $null; $cpu = $null
try {
    $os = Get-CimInstance Win32_OperatingSystem -ErrorAction Stop
    $available = [long]$os.FreePhysicalMemory * 1024
    $total = [long]$os.TotalVisibleMemorySize * 1024
    $cpu = (Get-CimInstance Win32_Processor | Measure-Object LoadPercentage -Average).Average
} catch { $errors += 'Windows CIM unavailable' }
$drives = @(Get-PSDrive -PSProvider FileSystem | Where-Object Name -in @('C','D','F') | ForEach-Object {
    @{ drive = $_.Name; free_bytes = $_.Free; used_bytes = $_.Used }
})
$bytes = (Get-ChildItem -LiteralPath $run -File -Recurse -ErrorAction SilentlyContinue | Measure-Object Length -Sum).Sum
$dockerStats = @(); $dockerDisk = @(); $dockerDetails = @()
if (Get-Command docker -ErrorAction SilentlyContinue) {
    $dockerStats = @(& docker stats --no-stream --format '{{json .}}' 2>$null)
    if ($LASTEXITCODE -ne 0) { $errors += 'Docker stats unavailable' }
    if (-not $Quick) {
        $dockerDisk = @(& docker system df --format '{{json .}}' 2>$null)
        $dockerDetails = @(& docker system df -v 2>$null)
        if ($LASTEXITCODE -ne 0) { $errors += 'Docker disk accounting unavailable' }
    }
}
$snapshot = [ordered]@{
    timestamp = [DateTimeOffset]::Now.ToString('o'); phase = $Phase
    available_memory_bytes = $available; total_memory_bytes = $total; cpu_percent = $cpu
    drives = $drives; f_drive_present = [bool]($drives | Where-Object drive -eq 'F')
    run_bytes = [long]$bytes; docker_stats = $dockerStats; docker_disk = $dockerDisk
    docker_details = $dockerDetails; errors = $errors
}
$path = Join-Path $run 'resource_usage.json'
$history = @()
if (Test-Path -LiteralPath $path) { $history = @((Get-Content -LiteralPath $path -Raw | ConvertFrom-Json).snapshots) }
$history += $snapshot
$knownMemory = @($history | Where-Object { $null -ne $_.available_memory_bytes })
$summary = [ordered]@{
    snapshots = $history
    minimum_available_memory_bytes = ($knownMemory | Measure-Object available_memory_bytes -Minimum).Minimum
    peak_run_bytes = ($history | Measure-Object run_bytes -Maximum).Maximum
    note = 'Sampled peaks, not continuous profiler peaks. Docker internal reclaim and Windows free space are separate measurements. F drive absence is explicit.'
}
$summary | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $path -Encoding utf8
@('# Resource samples', '', '| Time | Phase | Available RAM bytes | Run bytes |', '|---|---|---:|---:|') + @(
    $history | ForEach-Object { "| $($_.timestamp) | $($_.phase) | $($_.available_memory_bytes) | $($_.run_bytes) |" }
) | Set-Content -LiteralPath (Join-Path $run 'resource_usage.md') -Encoding utf8
return [pscustomobject]$snapshot
