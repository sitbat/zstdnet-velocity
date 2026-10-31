$ErrorActionPreference = 'Stop'
$suiteRoot = Split-Path -Parent $PSScriptRoot
$runtimeDir = Join-Path $suiteRoot 'runtime'
$runtimeJar = Join-Path $runtimeDir 'velocity-4.3.0-44.jar'
$expectedHash = '85be73b90f295acd8d5d45a00ad10bac791d4b72b7554172612a89565ebaaa8f'
New-Item -ItemType Directory -Force -Path $runtimeDir | Out-Null
if (Test-Path -LiteralPath $runtimeJar) {
    if ((Get-FileHash -LiteralPath $runtimeJar -Algorithm SHA256).Hash.ToLowerInvariant() -eq $expectedHash) {
        Write-Host 'Verified Velocity 4.3.0 build 44 is already available.'
        exit 0
    }
    throw "Existing runtime JAR has an unexpected checksum: $runtimeJar"
}
$headers = @{ 'User-Agent' = 'ZstdNetProxyBuild/2.1.0 (local reproducible build)' }
$build = Invoke-RestMethod -Uri 'https://fill.papermc.io/v3/projects/velocity/versions/4.3.0/builds/44' -Headers $headers
$download = $build.downloads.'server:default'
if (-not $download.url) { throw 'The official download API did not return a server download.' }
if ([Uri]::new($download.url).Scheme -ne 'https') { throw 'Expected an HTTPS download URL.' }
Invoke-WebRequest -Uri $download.url -Headers $headers -OutFile $runtimeJar
if ((Get-FileHash -LiteralPath $runtimeJar -Algorithm SHA256).Hash.ToLowerInvariant() -ne $expectedHash) {
    throw 'The downloaded runtime checksum does not match the verified 4.3.0 build 44 distribution.'
}
Write-Host "Verified: $runtimeJar"
