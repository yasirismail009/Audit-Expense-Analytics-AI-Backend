@echo off
echo Starting Docker Compose with Windows-compatible configuration...
echo.

REM Stop any existing containers
echo Stopping existing containers...
docker-compose -f docker-compose-windows.yml down

REM Remove any existing volumes to start fresh
echo Removing existing volumes...
docker volume prune -f

REM Start the services
echo Starting services with docker-compose-windows.yml...
docker-compose -f docker-compose-windows.yml up -d

echo.
echo Docker services are starting up...
echo.
echo Services:
echo - Web: http://localhost:8000
echo - Database: localhost:5433
echo - Redis: localhost:6380
echo - Flower: http://localhost:5555
echo.
echo To view logs: docker-compose -f docker-compose-windows.yml logs -f
echo To stop: docker-compose -f docker-compose-windows.yml down
echo.
pause
