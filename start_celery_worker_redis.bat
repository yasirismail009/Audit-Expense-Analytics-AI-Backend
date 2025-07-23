@echo off
echo Starting Celery Worker with Redis Configuration...
echo.

REM Set the Django settings module
set DJANGO_SETTINGS_MODULE=analytics.settings

REM Start the Celery worker with multiple queues
celery -A analytics worker --loglevel=info --pool=solo --concurrency=4 --queues=analytics,ml_training,maintenance

pause 