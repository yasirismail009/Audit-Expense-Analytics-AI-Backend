# Complete Analytics Backend Bundle

This repository now includes a **complete backend bundle** that packages all services (Django, Celery, Redis, PostgreSQL) into a single Docker container using supervisor for process management.

## 🚀 Overview

The complete backend bundle provides a **self-contained solution** with all necessary services running in one container:

- **Django Web Server** - Main web application
- **Celery Worker** - Background task processing
- **Celery Beat** - Scheduled task scheduler
- **Celery Flower** - Task monitoring interface
- **PostgreSQL Database** - Primary database
- **Redis Cache** - Message broker and caching

## 🎯 Benefits

1. **Single Container Deployment** - Everything in one place
2. **Simplified Orchestration** - No need for complex multi-container setups
3. **Easier Development** - One command to start everything
4. **Reduced Resource Usage** - Shared system resources
5. **Faster Deployment** - Single image to deploy
6. **Consistent Environment** - All services use the same base

## 📦 Quick Start

### 1. Build the Complete Backend

**Windows (PowerShell):**
```powershell
.\build-complete-backend.ps1
```

**Linux/Mac:**
```bash
docker build -t yasir009/analytics-complete-backend:latest -f Dockerfile.complete-backend .
```

### 2. Run with Docker Compose

```bash
docker-compose -f docker-compose-complete-backend.yml up -d
```

### 3. Run Directly with Docker

```bash
docker run -d \
  --name analytics-complete-backend \
  -p 8000:8000 \
  -p 5555:5555 \
  -p 5432:5432 \
  -p 6379:6379 \
  -v postgres_data:/var/lib/postgresql/data \
  -v redis_data:/var/lib/redis \
  yasir009/analytics-complete-backend:latest
```

### 4. Push to Docker Hub

**Windows (PowerShell):**
```powershell
.\push-complete-backend.ps1
```

**Manual push:**
```bash
docker push yasir009/analytics-complete-backend:latest
docker push yasir009/analytics-complete-backend:v1.0.0
```

## 🔧 Architecture

### Container Structure

```
analytics-complete-backend/
├── Django Web Server (Port 8000)
├── Celery Worker
├── Celery Beat Scheduler
├── Celery Flower (Port 5555)
├── PostgreSQL Database (Port 5432)
├── Redis Cache (Port 6379)
└── Supervisor (Process Manager)
```

### Process Management

The container uses **supervisor** to manage all processes:

- **Priority 100**: PostgreSQL (starts first)
- **Priority 200**: Redis (starts second)
- **Priority 300**: Django (starts after databases)
- **Priority 400**: Celery Worker
- **Priority 500**: Celery Beat
- **Priority 600**: Celery Flower

## 🌐 Service Endpoints

Once running, you can access:

- **Django Web App**: http://localhost:8000
- **Celery Flower**: http://localhost:5555
- **PostgreSQL**: localhost:5432
- **Redis**: localhost:6379

## 📊 Monitoring

### Health Check
The container includes a health check that verifies the Django server is responding:
```bash
curl -f http://localhost:8000/
```

### Logs
All service logs are available through supervisor:
```bash
# View all logs
docker logs analytics-complete-backend

# View specific service logs
docker exec analytics-complete-backend supervisorctl tail -f django
docker exec analytics-complete-backend supervisorctl tail -f celery-worker
docker exec analytics-complete-backend supervisorctl tail -f postgresql
docker exec analytics-complete-backend supervisorctl tail -f redis
```

### Supervisor Commands
```bash
# Check service status
docker exec analytics-complete-backend supervisorctl status

# Restart a service
docker exec analytics-complete-backend supervisorctl restart django

# Stop a service
docker exec analytics-complete-backend supervisorctl stop celery-worker
```

## 🔒 Security Considerations

### Database Security
- PostgreSQL is configured to accept connections from localhost only
- Default user: `analytics_user`
- Default database: `analytics_db`
- Password: `analytics_password`

### Redis Security
- Redis is configured to bind to all interfaces (0.0.0.0)
- Memory limit: 256MB
- Eviction policy: LRU (Least Recently Used)

### Environment Variables
All services use the same environment variables:
- `POSTGRES_DB=analytics_db`
- `POSTGRES_USER=analytics_user`
- `POSTGRES_PASSWORD=analytics_password`
- `POSTGRES_HOST=localhost`
- `REDIS_URL=redis://localhost:6379/0`
- `DJANGO_SETTINGS_MODULE=analytics.settings`

## 🚀 Production Deployment

### Docker Compose (Recommended)
```yaml
version: '3.8'
services:
  analytics-backend:
    image: yasir009/analytics-complete-backend:latest
    ports:
      - "8000:8000"
      - "5555:5555"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - redis_data:/var/lib/redis
    environment:
      - DEBUG=False
    restart: unless-stopped
```

### Kubernetes
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: analytics-backend
spec:
  replicas: 1
  selector:
    matchLabels:
      app: analytics-backend
  template:
    metadata:
      labels:
        app: analytics-backend
    spec:
      containers:
      - name: analytics-backend
        image: yasir009/analytics-complete-backend:latest
        ports:
        - containerPort: 8000
        - containerPort: 5555
        - containerPort: 5432
        - containerPort: 6379
        volumeMounts:
        - name: postgres-data
          mountPath: /var/lib/postgresql/data
        - name: redis-data
          mountPath: /var/lib/redis
```

## 🔧 Customization

### Adding New Services
1. Add the service to the Dockerfile
2. Create a supervisor configuration
3. Update the startup script
4. Rebuild the container

### Modifying Configuration
- **PostgreSQL**: Edit `/etc/postgresql/*/main/postgresql.conf`
- **Redis**: Edit `/etc/redis/redis.conf`
- **Supervisor**: Edit `/etc/supervisor/conf.d/supervisord.conf`

## 🐛 Troubleshooting

### Common Issues

1. **Port Conflicts**
   ```bash
   # Check if ports are in use
   netstat -tulpn | grep :8000
   ```

2. **Database Connection Issues**
   ```bash
   # Check PostgreSQL status
   docker exec analytics-complete-backend supervisorctl status postgresql
   ```

3. **Redis Connection Issues**
   ```bash
   # Check Redis status
   docker exec analytics-complete-backend supervisorctl status redis
   ```

4. **Django Migration Issues**
   ```bash
   # Run migrations manually
   docker exec analytics-complete-backend python manage.py migrate
   ```

### Performance Tuning

1. **Increase Memory Limits**
   ```bash
   docker run --memory=4g yasir009/analytics-complete-backend:latest
   ```

2. **Adjust Celery Concurrency**
   Edit supervisor config to change worker concurrency

3. **Database Optimization**
   Adjust PostgreSQL configuration in postgresql.conf

## 📈 Scaling Considerations

### Horizontal Scaling
For production, consider:
- Using external PostgreSQL and Redis instances
- Running multiple containers behind a load balancer
- Using Kubernetes for orchestration

### Vertical Scaling
- Increase container memory and CPU limits
- Optimize PostgreSQL and Redis configurations
- Adjust Celery worker concurrency

## 🔄 Migration from Multi-Container Setup

1. **Backup your data**
   ```bash
   docker exec old-postgres pg_dump -U analytics_user analytics_db > backup.sql
   ```

2. **Stop old containers**
   ```bash
   docker-compose down
   ```

3. **Start new complete backend**
   ```bash
   docker-compose -f docker-compose-complete-backend.yml up -d
   ```

4. **Restore data**
   ```bash
   docker exec -i analytics-complete-backend psql -U analytics_user analytics_db < backup.sql
   ```

## 📞 Support

For issues or questions:
1. Check the logs: `docker logs analytics-complete-backend`
2. Verify supervisor status: `docker exec analytics-complete-backend supervisorctl status`
3. Check service connectivity: `docker exec analytics-complete-backend netstat -tulpn`
4. Review configuration files in the container
