# Runs only on a disposable Windows CI/build machine.
$ErrorActionPreference = 'Stop'
$installer = Get-ChildItem "$PSScriptRoot/../src-tauri/target/release/bundle/nsis/*-setup.exe" | Select-Object -First 1
if (!$installer) { throw 'No Windows installer was produced.' }
$setup = Start-Process $installer.FullName -ArgumentList '/S' -PassThru
if (!$setup.WaitForExit(180000)) { throw 'Installer timed out.' }
if ($setup.ExitCode -notin @(0, 3010)) { throw "Installation failed: $($setup.ExitCode)" }
$executable = Join-Path $env:LOCALAPPDATA 'Radio Zimbabwe Voting Studio/voting-studio.exe'
if (!(Test-Path $executable)) { throw 'Installed application was not found.' }
$app = $null
try {
    $app = Start-Process $executable -PassThru
    $deadline = (Get-Date).AddSeconds(45)
    do {
        Start-Sleep -Milliseconds 500
        $app.Refresh()
        if ($app.HasExited) { throw "Application exited before showing a window: $($app.ExitCode)" }
    } while ($app.MainWindowHandle -eq 0 -and (Get-Date) -lt $deadline)
    if ($app.MainWindowHandle -eq 0) { throw 'No application window appeared.' }
    if ($app.MainWindowTitle -notlike '*Voting Studio*') { throw 'Unexpected application window.' }
    $second = Start-Process $executable -PassThru
    if (!$second.WaitForExit(15000)) {
        Stop-Process -Id $second.Id -Force
        throw 'The second launch did not return to the running instance.'
    }
    $app.Refresh()
    if ($app.HasExited) { throw 'The original application unexpectedly exited.' }
    Write-Host 'Passed: silent installation, native window startup, and single-instance launch.'
} finally {
    if ($app -and !$app.HasExited) { Stop-Process -Id $app.Id -Force }
}
