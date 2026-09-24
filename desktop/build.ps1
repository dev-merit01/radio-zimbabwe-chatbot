$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
py -3.12 -m venv .venv-desktop
if ($LASTEXITCODE -ne 0) { throw 'Python 3.12 is required.' }
& .\.venv-desktop\Scripts\python.exe -m pip install --upgrade pip
if ($LASTEXITCODE -ne 0) { throw 'Build tooling update failed.' }
& .\.venv-desktop\Scripts\python.exe -m pip install -r desktop/requirements.txt
if ($LASTEXITCODE -ne 0) { throw 'Dependency installation failed.' }
& .\.venv-desktop\Scripts\python.exe -m PyInstaller --noconfirm --clean --windowed --name VotingStudio --collect-all webview desktop/launcher.py
if ($LASTEXITCODE -ne 0) { throw 'Application build failed.' }
$iscc = "${env:ProgramFiles(x86)}\Inno Setup 6\ISCC.exe"
if (!(Test-Path $iscc)) { throw 'Install Inno Setup 6, then run this script again.' }
& $iscc desktop/installer.iss
if ($LASTEXITCODE -ne 0) { throw 'Installer build failed.' }
