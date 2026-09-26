[CmdletBinding()]
param([string]$ServerUrl)

$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
foreach ($tool in @('node', 'npm', 'cargo')) {
    if (!(Get-Command $tool -ErrorAction SilentlyContinue)) {
        throw "Missing build tool: $tool. See docs/WINDOWS_TAURI.md. Staff PCs do not need build tools."
    }
}
if ($PSBoundParameters.ContainsKey('ServerUrl')) {
    $env:VOTING_STUDIO_SERVER_URL = $ServerUrl
}
npm ci
if ($LASTEXITCODE -ne 0) { throw 'Dependency installation failed.' }
npm run desktop:check
if ($LASTEXITCODE -ne 0) { throw 'Desktop configuration check failed.' }
cargo test --locked --manifest-path src-tauri/Cargo.toml
if ($LASTEXITCODE -ne 0) { throw 'Desktop policy tests failed.' }
npm run desktop:build
if ($LASTEXITCODE -ne 0) { throw 'Windows installer build failed.' }
Write-Host 'Installer: src-tauri/target/release/bundle/nsis/*-setup.exe'
