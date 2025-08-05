@echo off
echo ==========================================
echo Starting Celery Services Only (Docker)
echo ==========================================

echo 🚀 Starting Redis and Celery services...
docker-compose -f docker-compose-celery-only.yml up -d --build

echo ⏳ Waiting for Redis to be ready...
timeout /t 10 /nobreak >nul

echo ✅ Celery services started successfully!
echo.
echo 🔴 Redis: localhost:6379
echo 🔄 Celery Worker: Running in Docker
echo ⏰ Celery Beat: Running in Docker
echo.
echo 📊 Service Status:
docker-compose -f docker-compose-celery-only.yml ps
echo.
echo 🌐 Django Web: Continue using your local setup
echo 🗄️  Database: Continue using your local PostgreSQL
echo.
echo 💡 To test Celery, run your analysis tasks locally
echo 💡 They will be processed by the Docker Celery workers

pause 