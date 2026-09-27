# Runs only on a disposable Windows CI/build machine.
$ErrorActionPreference = 'Stop'
$installer = Get-ChildItem "$PSScriptRoot/../src-tauri/target/release/bundle/nsis/*-setup.exe" | Select-Object -First 1
if (!$installer) { throw 'No Windows installer was produced.' }
$setup = Start-Process $installer.FullName -ArgumentList '/S' -PassThru
if (!$setup.WaitForExit(180000)) { throw 'Installer timed out.' }
if ($setup.ExitCode -notin @(0, 3010)) { throw "Installation failed: $($setup.ExitCode)" }
$executable = Join-Path $env:LOCALAPPDATA 'AirVote/airvote.exe'
if (!(Test-Path $executable)) { throw 'Installed application was not found.' }
$app = $null
$server = $null
$root = (Resolve-Path "$PSScriptRoot/..").Path
$python = (Get-Command python).Source
$stdout = Join-Path $root 'local-smoke-output.log'
$stderr = Join-Path $root 'local-smoke-error.log'
try {
    & $python "$root/scripts/local_server.py" setup --no-account
    if ($LASTEXITCODE -ne 0) { throw 'Local server setup failed.' }
    $server = Start-Process $python -ArgumentList @('scripts/local_server.py', 'start') -WorkingDirectory $root -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru
    $ready = $false
    foreach ($attempt in 1..60) {
        if ($server.HasExited) { throw 'Local server exited before becoming ready.' }
        try {
            $status = Invoke-RestMethod 'http://127.0.0.1:8000/api/desktop/status' -TimeoutSec 1
            if ($status.desktop_api -eq 1) { $ready = $true; break }
        } catch { Start-Sleep -Milliseconds 500 }
    }
    if (!$ready) { throw 'Local server did not become ready.' }
    $configDirectory = Join-Path $env:APPDATA 'zw.co.radiozimbabwe.votingstudio'
    New-Item -ItemType Directory -Force $configDirectory | Out-Null
    '{"server_url":"http://127.0.0.1:8000/"}' | Set-Content -Encoding utf8NoBOM (Join-Path $configDirectory 'config.json')
    $app = Start-Process $executable -PassThru
    $deadline = (Get-Date).AddSeconds(45)
    do {
        Start-Sleep -Milliseconds 500
        $app.Refresh()
        if ($app.HasExited) { throw "Application exited before showing a window: $($app.ExitCode)" }
    } while ($app.MainWindowHandle -eq 0 -and (Get-Date) -lt $deadline)
    if ($app.MainWindowHandle -eq 0) { throw 'No application window appeared.' }
    if ($app.MainWindowTitle -notlike '*AirVote*') { throw 'Unexpected application window.' }
    $connected = $false
    foreach ($attempt in 1..60) {
        if ((Test-Path $stderr) -and (Select-String -Path $stderr -Pattern 'GET /accounts/login/' -Quiet)) {
            $connected = $true
            break
        }
        Start-Sleep -Milliseconds 500
    }
    if (!$connected) { throw 'Native app did not load the local Django sign-in page.' }
    $second = Start-Process $executable -PassThru
    if (!$second.WaitForExit(15000)) {
        Stop-Process -Id $second.Id -Force
        throw 'The second launch did not return to the running instance.'
    }
    $app.Refresh()
    if ($app.HasExited) { throw 'The original application unexpectedly exited.' }
    Write-Host 'Passed: silent installation, native window startup, local Django sign-in page, and single-instance launch.'
} finally {
    if ($server -and !$server.HasExited) { taskkill /PID $server.Id /T /F | Out-Null }
    if ($app -and !$app.HasExited) { Stop-Process -Id $app.Id -Force }
}
