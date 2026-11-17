param([int]$AppPid,[int]$Port)
$load = Get-CimInstance Win32_Process -Filter "Name='java.exe'" | Where-Object {
    $_.CommandLine -match 'ApacheJMeter\.jar' -and $_.CommandLine -like "*-Jport=$Port *"
} | Select-Object -First 1
$roles=@{ $AppPid='app' }
if ($load) { $roles[[int]$load.ProcessId]='jmeter' }
$network=@{}
$endpoint=@{}
netstat -ano -p tcp | ForEach-Object {
    if ($_ -match '^\s*TCP\s+(\S+)\s+(\S+)\s+(\S+)\s+(\d+)\s*$') {
        $owner=[int]$Matches[4]
        if ($Matches[1].EndsWith(':' + $Port) -or $Matches[2].EndsWith(':' + $Port)) {
            $state=$Matches[3]
            if (-not $endpoint.ContainsKey($state)) { $endpoint[$state]=0 }
            $endpoint[$state]++
        }
        if ($roles.ContainsKey($owner)) {
            if (-not $network.ContainsKey($owner)) { $network[$owner]=@{} }
            $kind=$Matches[3]
            if (-not $network[$owner].ContainsKey($kind)) { $network[$owner][$kind]=0 }
            $network[$owner][$kind]++
        }
    }
}
$sample=@(foreach ($id in $roles.Keys) {
    $p=Get-Process -Id $id -ErrorAction SilentlyContinue
    if ($p) {
        @{role=$roles[$id]; pid=$id; cpu_s=$p.CPU; rss_bytes=$p.WorkingSet64; threads=$p.Threads.Count; tcp=$network[$id]}
    }
})
@{at=[DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds(); processes=$sample; endpoint_tcp=$endpoint} | ConvertTo-Json -Compress -Depth 5
