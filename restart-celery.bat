@echo off
echo ==========================================
echo Restarting Celery Services
echo ==========================================

echo 🛑 Stopping Celery services...
docker-compose -f docker-compose-celery-only.yml down

echo ⏳ Waiting for services to stop...
timeout /t 5 /nobreak >nul

echo 🚀 Starting Celery services...
docker-compose -f docker-compose-celery-only.yml up -d --build

echo ⏳ Waiting for services to be ready...
timeout /t 15 /nobreak >nul

echo ✅ Celery services restarted!
echo.
echo 📊 Service Status:
docker-compose -f docker-compose-celery-only.yml ps
echo.
echo 🔴 Redis: localhost:6379
echo 🔄 Celery Worker: Running in Docker
echo ⏰ Celery Beat: Running in Docker
echo.
echo 💡 Test Celery connection: python test_celery_connection.py

pause 