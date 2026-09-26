@echo off
setlocal
cd /d "%~dp0"
if not exist ".venv-local\Scripts\python.exe" (
    echo Run setup-local.cmd first.
    pause
    exit /b 1
)
".venv-local\Scripts\python.exe" scripts\local_server.py start
if errorlevel 1 pause
