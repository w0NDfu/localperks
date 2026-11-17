param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$MavenArguments = @('test')
)

$ErrorActionPreference = 'Stop'
$repositoryPath = Split-Path -Parent $PSScriptRoot
$configPath = Join-Path $repositoryPath '.env'

if (-not (Test-Path -LiteralPath $configPath)) {
    throw 'Create .env from .env.example and set HMDP_DB_PASSWORD first.'
}

# Read only database settings; never execute .env contents as PowerShell.
foreach ($line in Get-Content -LiteralPath $configPath) {
    if ($line -match '^\s*(HMDP_DB_(?:URL|USERNAME|PASSWORD))\s*=(.*)$') {
        $settingName = $Matches[1]
        $settingValue = $Matches[2].Trim()
        if ($settingValue.Length -ge 2 -and (
            ($settingValue.StartsWith('"') -and $settingValue.EndsWith('"')) -or
            ($settingValue.StartsWith("'") -and $settingValue.EndsWith("'"))
        )) {
            $settingValue = $settingValue.Substring(1, $settingValue.Length - 2)
        }
        [Environment]::SetEnvironmentVariable($settingName, $settingValue, 'Process')
    }
}

if (-not $env:HMDP_DB_PASSWORD) {
    throw 'Set HMDP_DB_PASSWORD in .env first.'
}

Push-Location $repositoryPath
try {
    & mvn @MavenArguments
    if ($LASTEXITCODE -ne 0) {
        throw "Maven exited with code $LASTEXITCODE."
    }
} finally {
    Pop-Location
}
