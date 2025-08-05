# Analytics Application - Docker Setup

This document provides instructions for setting up and running the Analytics Application using Docker.

## 🐳 Docker Architecture

The application uses the following Docker services:

- **PostgreSQL 15** - Database
- **Redis 7** - Celery broker and result backend
- **Django Web** - Main web application
- **Celery Worker** - Background task processing
- **Celery Beat** - Scheduled tasks
- **Celery Flower** - Task monitoring and management

## 📋 Prerequisites

1. **Docker Desktop** installed and running
2. **Docker Compose** (usually included with Docker Desktop)
3. At least **4GB RAM** available for Docker

## 🚀 Quick Start

### 1. Start All Services

**Windows:**
```bash
docker-setup.bat start
```

**Linux/Mac:**
```bash
chmod +x docker-setup.sh
./docker-setup.sh start
```

**Manual:**
```bash
docker-compose up -d --build
```

### 2. Run Database Migrations

**Windows:**
```bash
docker-setup.bat migrate
```

**Linux/Mac:**
```bash
./docker-setup.sh migrate
```

**Manual:**
```bash
docker-compose exec web python manage.py migrate
```

### 3. Create Superuser (Optional)

**Windows:**
```bash
docker-setup.bat superuser
```

**Linux/Mac:**
```bash
./docker-setup.sh superuser
```

**Manual:**
```bash
docker-compose exec web python manage.py createsuperuser
```

## 🌐 Access Points

Once all services are running, you can access:

- **Web Application**: http://localhost:8000
- **Celery Flower (Monitoring)**: http://localhost:5555
- **PostgreSQL Database**: localhost:5432
- **Redis**: localhost:6379

## 📊 Service Management

### Check Service Status

**Windows:**
```bash
docker-setup.bat status
```

**Linux/Mac:**
```bash
./docker-setup.sh status
```

**Manual:**
```bash
docker-compose ps
```

### View Logs

**Windows:**
```bash
docker-setup.bat logs
```

**Linux/Mac:**
```bash
./docker-setup.sh logs
```

**Manual:**
```bash
docker-compose logs -f
```

### Stop Services

**Windows:**
```bash
docker-setup.bat stop
```

**Linux/Mac:**
```bash
./docker-setup.sh stop
```

**Manual:**
```bash
docker-compose down
```

### Restart Services

**Windows:**
```bash
docker-setup.bat restart
```

**Linux/Mac:**
```bash
./docker-setup.sh restart
```

**Manual:**
```bash
docker-compose restart
```

## 🧪 Testing

### Run the Complete Analysis Test

**Inside Docker container:**
```bash
docker-compose exec web python test_docker_analysis.py
```

### Run Django Tests

**Windows:**
```bash
docker-setup.bat test
```

**Linux/Mac:**
```bash
./docker-setup.sh test
```

**Manual:**
```bash
docker-compose exec web python manage.py test
```

## 🔧 Development

### Access Django Shell

**Windows:**
```bash
docker-setup.bat shell
```

**Linux/Mac:**
```bash
./docker-setup.sh shell
```

**Manual:**
```bash
docker-compose exec web python manage.py shell
```

### View Celery Tasks

1. Open http://localhost:5555 in your browser
2. Monitor task execution, worker status, and performance

## 🗄️ Database

### Database Credentials

- **Database**: analytics_db
- **Username**: analytics_user
- **Password**: analytics_password
- **Host**: localhost (or db service name in Docker)
- **Port**: 5432

### Connect to Database

**Using psql:**
```bash
docker-compose exec db psql -U analytics_user -d analytics_db
```

**Using pgAdmin or other tools:**
- Host: localhost
- Port: 5432
- Database: analytics_db
- Username: analytics_user
- Password: analytics_password

## 🔄 Celery Configuration

The application uses Redis as the Celery broker and result backend:

- **Broker URL**: redis://redis:6379/0
- **Result Backend**: redis://redis:6379/0
- **Worker Concurrency**: 4
- **Task Timeout**: 5 minutes
- **Soft Timeout**: 4 minutes

## 📁 File Structure

```
analytics/
├── Dockerfile                 # Main application container
├── docker-compose.yml         # Service orchestration
├── docker-setup.sh           # Linux/Mac setup script
├── docker-setup.bat          # Windows setup script
├── .dockerignore             # Files to exclude from build
├── requirements.txt          # Python dependencies
├── analytics/                # Django project
├── core/                     # Main application
└── test_docker_analysis.py   # Docker test script
```

## 🧹 Cleanup

### Remove All Data

**Windows:**
```bash
docker-setup.bat cleanup
```

**Linux/Mac:**
```bash
./docker-setup.sh cleanup
```

**Manual:**
```bash
docker-compose down -v
docker system prune -f
```

## 🔍 Troubleshooting

### Common Issues

1. **Port Already in Use**
   ```bash
   # Check what's using the port
   netstat -ano | findstr :8000
   
   # Stop the conflicting service or change ports in docker-compose.yml
   ```

2. **Database Connection Issues**
   ```bash
   # Check if database is running
   docker-compose ps db
   
   # Check database logs
   docker-compose logs db
   ```

3. **Celery Worker Not Starting**
   ```bash
   # Check Celery logs
   docker-compose logs celery_worker
   
   # Restart Celery worker
   docker-compose restart celery_worker
   ```

4. **Memory Issues**
   - Increase Docker memory limit in Docker Desktop settings
   - Reduce worker concurrency in docker-compose.yml

### Logs and Debugging

**View all logs:**
```bash
docker-compose logs -f
```

**View specific service logs:**
```bash
docker-compose logs -f web
docker-compose logs -f celery_worker
docker-compose logs -f db
docker-compose logs -f redis
```

## 📈 Performance

### Recommended Resources

- **CPU**: 4+ cores
- **RAM**: 8GB+ (4GB for Docker)
- **Storage**: 20GB+ free space
- **Network**: Stable internet connection

### Optimization Tips

1. **Increase Worker Concurrency** (if you have more CPU cores):
   ```yaml
   # In docker-compose.yml
   command: celery -A analytics worker --loglevel=info --concurrency=8
   ```

2. **Use Volume Mounts** for development:
   ```yaml
   # In docker-compose.yml
   volumes:
     - .:/app
   ```

3. **Monitor Resource Usage**:
   ```bash
   docker stats
   ```

## 🔐 Security

### Production Considerations

1. **Change Default Passwords**:
   - Update database credentials in docker-compose.yml
   - Use environment variables for sensitive data

2. **Use SSL/TLS**:
   - Configure HTTPS for web application
   - Use SSL for database connections

3. **Network Security**:
   - Restrict port exposure
   - Use internal Docker networks

4. **Regular Updates**:
   - Keep Docker images updated
   - Monitor for security vulnerabilities

## 📞 Support

If you encounter issues:

1. Check the troubleshooting section above
2. Review the logs using `docker-compose logs`
3. Ensure Docker Desktop is running
4. Verify all prerequisites are met

## 🎯 Next Steps

After successful setup:

1. Upload test data files
2. Run the analysis pipeline
3. Monitor task execution in Celery Flower
4. Explore the web interface
5. Review analysis results in the database 