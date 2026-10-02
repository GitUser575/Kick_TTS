@echo off
cd /d "%~dp0"
TITLE Chatterbox Turbo Launcher
color 0B
echo.
echo ===================================================
echo     Starting Chatterbox Turbo... please wait!
echo ===================================================
echo.

:: Include portable MinGit in PATH if installed
if exist "%~dp0bin\git\cmd" set "PATH=%~dp0bin\git\cmd;%PATH%"

:: Suppress non-critical library deprecation warnings from command prompt
set PYTHONWARNINGS=ignore::FutureWarning

:: Run the python app
python main.py

:: If the app crashes, pause so the user can read the error
if %ERRORLEVEL% neq 0 (
    echo.
    echo [ERROR] The application closed unexpectedly.
    pause
)
