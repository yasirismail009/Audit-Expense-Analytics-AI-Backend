#!/bin/bash

echo "Checking User Analysis Data in Docker Database..."
echo "================================================"

# Check if Docker containers are running
echo "Checking if Docker containers are running..."
if ! docker ps | grep -q "analytics_db"; then
    echo "❌ Database container not running. Starting Docker services..."
    docker-compose up -d db
    sleep 5
fi

# Run the Django script through Docker
echo "Running database check script..."
docker-compose exec web python check_docker_django_db.py

echo ""
echo "Database check completed!" 