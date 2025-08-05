@echo off
echo ========================================
echo CELERY WORKER STARTUP SCRIPT
echo ========================================

echo Setting up environment...
set DJANGO_SETTINGS_MODULE=analytics.settings

echo Starting Celery worker...
echo.
echo Celery worker will start with the following configuration:
echo - App: analytics
echo - Log Level: info
echo - Concurrency: 4
echo - Pool: prefork
echo - Broker: memory://
echo.

celery -A analytics worker --loglevel=info --concurrency=4 --pool=prefork

echo.
echo Celery worker stopped.
pause 