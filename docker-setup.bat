@echo off
setlocal enabledelayedexpansion

echo ==========================================
echo Analytics Application Docker Setup
echo ==========================================

if "%1"=="" set "command=start"
if "%1"=="start" set "command=start"
if "%1"=="stop" set "command=stop"
if "%1"=="restart" set "command=restart"
if "%1"=="logs" set "command=logs"
if "%1"=="status" set "command=status"
if "%1"=="migrate" set "command=migrate"
if "%1"=="superuser" set "command=superuser"
if "%1"=="cleanup" set "command=cleanup"
if "%1"=="shell" set "command=shell"
if "%1"=="test" set "command=test"

if "%command%"=="start" goto :start
if "%command%"=="stop" goto :stop
if "%command%"=="restart" goto :restart
if "%command%"=="logs" goto :logs
if "%command%"=="status" goto :status
if "%command%"=="migrate" goto :migrate
if "%command%"=="superuser" goto :superuser
if "%command%"=="cleanup" goto :cleanup
if "%command%"=="shell" goto :shell
if "%command%"=="test" goto :test

echo Usage: %0 {start^|stop^|restart^|logs^|status^|migrate^|superuser^|cleanup^|shell^|test}
echo.
echo Commands:
echo   start     - Build and start all services
echo   stop      - Stop all services
echo   restart   - Restart all services
echo   logs      - Show recent logs
echo   status    - Show service status
echo   migrate   - Run database migrations
echo   superuser - Create superuser
echo   cleanup   - Clean up Docker resources
echo   shell     - Open Django shell
echo   test      - Run tests
exit /b 1

:start
echo 🚀 Starting services...
docker-compose up -d --build
echo ⏳ Waiting for services to be ready...
timeout /t 10 /nobreak >nul
echo ✅ Services started successfully!
call :migrate
call :status
goto :eof

:stop
echo 🛑 Stopping services...
docker-compose down
echo ✅ Services stopped!
goto :eof

:restart
echo 🔄 Restarting services...
docker-compose restart
echo ✅ Services restarted!
goto :eof

:logs
echo 📋 Recent logs:
docker-compose logs --tail=50
goto :eof

:status
echo 📊 Service Status:
docker-compose ps
echo.
echo 🌐 Web Application: http://localhost:8000
echo 🔍 Celery Flower (Monitoring): http://localhost:5555
echo 🗄️  PostgreSQL Database: localhost:5432
echo 🔴 Redis: localhost:6379
goto :eof

:migrate
echo 🔄 Running database migrations...
docker-compose exec web python manage.py migrate
echo ✅ Migrations completed!
goto :eof

:superuser
echo 👤 Creating superuser...
docker-compose exec web python manage.py createsuperuser --noinput
echo ✅ Superuser created!
goto :eof

:cleanup
echo 🧹 Cleaning up Docker resources...
docker-compose down -v
docker system prune -f
echo ✅ Cleanup completed!
goto :eof

:shell
echo 🐚 Opening Django shell...
docker-compose exec web python manage.py shell
goto :eof

:test
echo 🧪 Running tests...
docker-compose exec web python manage.py test
goto :eof 