@echo off
echo Starting Docker services without volume mounts...

REM Stop any existing containers
docker-compose down

REM Start only database and redis first
docker-compose up -d db redis

REM Wait for services to be ready
echo Waiting for database and redis to be ready...
timeout /t 30 /nobreak

REM Build the web image
echo Building web image...
docker build -t analytics-web .

REM Run web container without volume mounts
echo Starting web container...
docker run -d --name analytics-web-temp --network analytics_default -p 8000:8000 -e POSTGRES_DB=analytics_db -e POSTGRES_USER=analytics_user -e POSTGRES_PASSWORD=analytics_password -e POSTGRES_HOST=db -e POSTGRES_PORT=5432 -e REDIS_URL=redis://redis:6379/0 -e CELERY_BROKER_URL=redis://redis:6379/0 -e CELERY_RESULT_BACKEND=redis://redis:6379/0 analytics-web python manage.py runserver 0.0.0.0:8000

REM Run celery worker
echo Starting celery worker...
docker run -d --name analytics-celery-temp --network analytics_default -e POSTGRES_DB=analytics_db -e POSTGRES_USER=analytics_user -e POSTGRES_PASSWORD=analytics_password -e POSTGRES_HOST=db -e POSTGRES_PORT=5432 -e REDIS_URL=redis://redis:6379/0 -e CELERY_BROKER_URL=redis://redis:6379/0 -e CELERY_RESULT_BACKEND=redis://redis:6379/0 analytics-web celery -A analytics worker --loglevel=info --concurrency=2

echo Services started successfully!
echo Web app: http://localhost:8000
echo Database: localhost:5433
echo Redis: localhost:6380
