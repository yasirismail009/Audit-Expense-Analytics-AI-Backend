@echo off
echo Checking User Analysis Data in Docker Database...
echo ================================================

REM Check if Docker containers are running
echo Checking if Docker containers are running...
docker ps | findstr "analytics_db" >nul
if errorlevel 1 (
    echo ❌ Database container not running. Starting Docker services...
    docker-compose up -d db
    timeout /t 5 /nobreak >nul
)

REM Run the Django script through Docker
echo Running database check script...
docker-compose exec web python check_docker_django_db.py

echo.
echo Database check completed!
pause 