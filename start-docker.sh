#!/bin/bash

echo "=========================================="
echo "Starting Analytics Application (Simple)"
echo "=========================================="

# Start services without Flower
echo "🚀 Starting services..."
docker-compose -f docker-compose-simple.yml up -d --build

echo "⏳ Waiting for services to be ready..."
sleep 15

echo "🔄 Running database migrations..."
docker-compose -f docker-compose-simple.yml exec web python manage.py migrate

echo "📦 Collecting static files..."
docker-compose -f docker-compose-simple.yml exec web python manage.py collectstatic --noinput

echo "✅ Services started successfully!"
echo ""
echo "🌐 Web Application: http://localhost:8000"
echo "🗄️  PostgreSQL Database: localhost:5432"
echo "🔴 Redis: localhost:6379"
echo ""
echo "📊 Service Status:"
docker-compose -f docker-compose-simple.yml ps 