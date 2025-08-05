#!/bin/bash

# Docker Setup Script for Analytics Application
# This script helps set up and manage the Docker environment

set -e

echo "=========================================="
echo "Analytics Application Docker Setup"
echo "=========================================="

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        echo "❌ Docker is not running. Please start Docker and try again."
        exit 1
    fi
    echo "✅ Docker is running"
}

# Function to build and start services
start_services() {
    echo "🚀 Starting services..."
    docker-compose up -d --build
    
    echo "⏳ Waiting for services to be ready..."
    sleep 10
    
    echo "✅ Services started successfully!"
}

# Function to run migrations
run_migrations() {
    echo "🔄 Running database migrations..."
    docker-compose exec web python manage.py migrate
    
    echo "✅ Migrations completed!"
}

# Function to create superuser
create_superuser() {
    echo "👤 Creating superuser..."
    docker-compose exec web python manage.py createsuperuser --noinput || true
    
    echo "✅ Superuser created (if not already exists)!"
}

# Function to collect static files
collect_static() {
    echo "📦 Collecting static files..."
    docker-compose exec web python manage.py collectstatic --noinput
    
    echo "✅ Static files collected!"
}

# Function to show service status
show_status() {
    echo "📊 Service Status:"
    docker-compose ps
    
    echo ""
    echo "🌐 Web Application: http://localhost:8000"
    echo "🔍 Celery Flower (Monitoring): http://localhost:5555"
    echo "🗄️  PostgreSQL Database: localhost:5432"
    echo "🔴 Redis: localhost:6379"
}

# Function to show logs
show_logs() {
    echo "📋 Recent logs:"
    docker-compose logs --tail=50
}

# Function to stop services
stop_services() {
    echo "🛑 Stopping services..."
    docker-compose down
    
    echo "✅ Services stopped!"
}

# Function to restart services
restart_services() {
    echo "🔄 Restarting services..."
    docker-compose restart
    
    echo "✅ Services restarted!"
}

# Function to clean up
cleanup() {
    echo "🧹 Cleaning up Docker resources..."
    docker-compose down -v
    docker system prune -f
    
    echo "✅ Cleanup completed!"
}

# Main script logic
case "${1:-start}" in
    "start")
        check_docker
        start_services
        run_migrations
        collect_static
        show_status
        ;;
    "stop")
        stop_services
        ;;
    "restart")
        restart_services
        ;;
    "logs")
        show_logs
        ;;
    "status")
        show_status
        ;;
    "migrate")
        run_migrations
        ;;
    "superuser")
        create_superuser
        ;;
    "cleanup")
        cleanup
        ;;
    "shell")
        echo "🐚 Opening Django shell..."
        docker-compose exec web python manage.py shell
        ;;
    "test")
        echo "🧪 Running tests..."
        docker-compose exec web python manage.py test
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|logs|status|migrate|superuser|cleanup|shell|test}"
        echo ""
        echo "Commands:"
        echo "  start     - Build and start all services"
        echo "  stop      - Stop all services"
        echo "  restart   - Restart all services"
        echo "  logs      - Show recent logs"
        echo "  status    - Show service status"
        echo "  migrate   - Run database migrations"
        echo "  superuser - Create superuser"
        echo "  cleanup   - Clean up Docker resources"
        echo "  shell     - Open Django shell"
        echo "  test      - Run tests"
        exit 1
        ;;
esac 