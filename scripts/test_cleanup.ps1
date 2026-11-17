param(
    [Parameter(Mandatory = $true)][string]$RunDirectory,
    [switch]$DryRun,
    [switch]$Apply
)
$ErrorActionPreference = 'Stop'
if ($DryRun -and $Apply) { throw 'Choose DryRun or Apply' }
$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../artifacts/test-runs'))
$run = [IO.Path]::GetFullPath($RunDirectory).TrimEnd('\','/')
if (-not $run.StartsWith($root + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) { throw 'Run outside allowed root' }
if ((Split-Path -Parent $run) -ne $root) { throw 'Run must be a direct child of test-runs' }
$manifestPath = Join-Path $run 'resource_manifest.json'
$manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
if ($manifest.run_id -ne (Split-Path -Leaf $run) -or $manifest.run_id -notmatch '^v03-\d{8}-\d{6}-[a-f0-9]{8}$') { throw 'Invalid run identity' }
if ((Get-Content -LiteralPath (Join-Path $run '.run-owner') -Raw).Trim() -ne $manifest.run_id) { throw 'Ownership marker mismatch' }
function Assert-NoLinks([string]$Path) {
    $cursor = Get-Item -LiteralPath $Path -Force
    while ($cursor) {
        if ($cursor.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'Reparse point is not allowed' }
        $cursor = $cursor.Parent
    }
    if (Get-ChildItem -LiteralPath $Path -Recurse -Force | Where-Object { $_.Attributes -band [IO.FileAttributes]::ReparsePoint }) { throw 'Linked descendants are not allowed' }
}
Assert-NoLinks $run
$actions = @(); $reclaimed = 0L
function Assert-TestVolume([string]$Name) {
    $prefix = 'sltest-' + $manifest.run_id
    if ($Name -notmatch ('^' + [regex]::Escape($prefix) + '-(mysql|redis|broker)-data$')) { throw 'Volume is outside the dedicated test namespace' }
    $registered = @($manifest.resources | Where-Object { $_.type -eq 'volume' -and $_.identifier -eq $Name -and
        $_.run_id -eq $manifest.run_id -and $_.compose_project -eq $prefix -and $_.isolated -and $_.auto_cleanup })
    if ($registered.Count -ne 1) { throw 'Volume was not uniquely registered by this test' }
    $rawVolume = & docker volume inspect $Name 2>$null
    if ($LASTEXITCODE -ne 0) { return $null }
    $volume = @($rawVolume | ConvertFrom-Json)[0]
    if ($volume.Name -ne $Name -or $volume.Labels.'localperks.validation.run' -ne $manifest.run_id -or
        $volume.Labels.'com.docker.compose.project' -ne $prefix) { throw 'Volume ownership labels mismatch' }
    return $volume
}
$orderedResources = @($manifest.resources | Where-Object { $_.type -notin @('network','volume') }) +
    @($manifest.resources | Where-Object type -eq 'volume') + @($manifest.resources | Where-Object type -eq 'network')
foreach ($resource in $orderedResources) {
    $entry = [ordered]@{ type = $resource.type; identifier = $resource.identifier; status = 'retained'; reason = '' }
    try {
        if (-not $resource.auto_cleanup -or $resource.run_id -ne $manifest.run_id -or -not $resource.isolated) { throw 'Not registered for isolated automatic cleanup' }
        if ($resource.type -eq 'directory') {
            $target = [IO.Path]::GetFullPath($resource.identifier).TrimEnd('\','/')
            # Only this one disposable subtree is supported. Reports and evidence cannot be registered for deletion.
            if ($target -ne (Join-Path $run 'temp')) { throw 'Only run/temp is disposable' }
            if (-not (Test-Path -LiteralPath $target)) { $entry.status = 'absent' }
            else {
                Assert-NoLinks $target
                $size = [long](Get-ChildItem -LiteralPath $target -File -Recurse -Force | Measure-Object Length -Sum).Sum
                if ($Apply) { Remove-Item -LiteralPath $target -Recurse -Force; $reclaimed += $size; $entry.status = 'removed' }
                else { $entry.status = 'would_remove' }
                $entry.bytes = $size
            }
        } elseif ($resource.type -eq 'container') {
            if ($resource.compose_project -ne ('sltest-' + $manifest.run_id)) { throw 'Test Compose project mismatch' }
            $raw = & docker inspect $resource.identifier 2>$null
            if ($LASTEXITCODE -ne 0) { $entry.status = 'absent' }
            else {
                $item = @($raw | ConvertFrom-Json)[0]
                if ($item.Id -ne $resource.identifier -or $item.Config.Labels.'localperks.validation.run' -ne $manifest.run_id -or
                    $item.Config.Labels.'com.docker.compose.project' -ne $resource.compose_project) { throw 'Container identity or ownership mismatch' }
                foreach ($mount in @($item.Mounts | Where-Object Type -in @('volume','bind'))) {
                    if ($mount.Type -eq 'bind') { throw 'Shared bind mount; retaining container' }
                    if ($null -eq (Assert-TestVolume $mount.Name)) { throw 'Unverified persistent volume; retaining container' }
                }
                if ($Apply) {
                    & docker stop --time 10 $item.Id | Out-Null
                    if ($LASTEXITCODE -ne 0) { throw 'Stop failed' }
                    & docker rm $item.Id | Out-Null
                    if ($LASTEXITCODE -ne 0) { throw 'Remove failed' }
                    $entry.status = 'removed'
                } else { $entry.status = 'would_remove' }
            }
        } elseif ($resource.type -eq 'volume') {
            $volume = Assert-TestVolume $resource.identifier
            if ($null -eq $volume) { $entry.status = 'absent' }
            elseif ($Apply) {
                & docker volume rm $volume.Name | Out-Null
                if ($LASTEXITCODE -ne 0) { throw 'Owned test volume is still referenced or could not be removed' }
                $entry.status = 'removed'
            } else { $entry.status = 'would_remove' }
        } elseif ($resource.type -eq 'network') {
            $raw = & docker network inspect $resource.identifier 2>$null
            if ($LASTEXITCODE -ne 0) { $entry.status = 'absent' }
            else {
                $item = @($raw | ConvertFrom-Json)[0]
                if ($item.Id -ne $resource.identifier -or $item.Labels.'localperks.validation.run' -ne $manifest.run_id -or
                    $item.Labels.'com.docker.compose.project' -ne $resource.compose_project -or
                    $resource.compose_project -ne ('sltest-' + $manifest.run_id)) { throw 'Network ownership mismatch' }
                if (@($item.Containers.PSObject.Properties).Count) { throw 'Network still referenced; retaining it' }
                if ($Apply) {
                    & docker network rm $item.Id | Out-Null
                    if ($LASTEXITCODE -ne 0) { throw 'Network removal failed' }
                    $entry.status = 'removed'
                } else { $entry.status = 'would_remove' }
            }
        } else { throw 'Unsupported resource type; manual verification required' }
    } catch { $entry.reason = $_.Exception.Message }
    $resource.cleanup_status = $entry.status
    $actions += $entry
}
$manifest | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $manifestPath -Encoding utf8
$report = [ordered]@{ run_id = $manifest.run_id; timestamp = [DateTimeOffset]::Now.ToString('o'); mode = $(if ($Apply) {'Apply'} else {'DryRun'}); reclaimed_file_bytes = $reclaimed; actions = $actions }
$outputName = if ($Apply) { 'cleanup_report.json' } else { 'cleanup_dry_run.json' }
$report | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $run $outputName) -Encoding utf8
$report | ConvertTo-Json -Depth 12
if ($Apply -and @($actions | Where-Object status -eq 'retained').Count) { throw 'Some resources were retained; inspect cleanup_report.json' }
