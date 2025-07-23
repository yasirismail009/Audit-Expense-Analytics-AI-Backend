@echo off
echo Starting Celery Worker for Analytics Project...
echo.

REM Set the Django settings module
set DJANGO_SETTINGS_MODULE=analytics.settings

REM Start the Celery worker
celery -A analytics worker --loglevel=info --pool=solo --concurrency=4

pause 