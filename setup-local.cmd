@echo off
setlocal
cd /d "%~dp0"
if not exist ".venv-local\Scripts\python.exe" (
    py -3.12 -m venv .venv-local
    if errorlevel 1 goto fail
)
".venv-local\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 goto fail
".venv-local\Scripts\python.exe" scripts\local_server.py setup
if errorlevel 1 goto fail
echo.
echo Setup complete. Double-click start-local.cmd next.
pause
exit /b 0
:fail
echo.
echo Setup failed. Check the error above. Python 3.12 with the Windows py launcher is required.
pause
exit /b 1
