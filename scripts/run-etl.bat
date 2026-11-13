@echo off
rem Runs ONE ETL pass and exits. Point Windows Task Scheduler at this file.
setlocal
cd /d "%~dp0.."
set "PYTHONPATH=%CD%\src"
if not exist "logs" mkdir "logs"
".venv\Scripts\python.exe" -m ekal_pipeline.run_once >> "logs\scheduler.log" 2>&1
exit /b %ERRORLEVEL%
