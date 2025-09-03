@echo off
echo ========================================
echo ENHANCED CELERY WORKER MANAGER
echo ========================================
echo.
echo This script starts Celery with enhanced reliability
echo and automatic restart capabilities.
echo.
echo Press Ctrl+C to stop
echo.

REM Activate virtual environment if it exists
if exist "venv\Scripts\activate.bat" (
    echo Activating virtual environment...
    call venv\Scripts\activate.bat
)

REM Start the enhanced Celery manager
echo Starting Enhanced Celery Manager...
python start_celery_enhanced.py

echo.
echo Celery Manager stopped.
pause

