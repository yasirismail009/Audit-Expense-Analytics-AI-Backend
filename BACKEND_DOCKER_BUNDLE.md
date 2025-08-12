# Analytics Backend Docker Bundle

This repository now includes a unified approach to bundle all backend Docker containers into one repository using multi-stage Docker builds.

## Overview

Instead of maintaining separate Dockerfiles for each service, we now use a single multi-stage Dockerfile (`Dockerfile.multi`) that can build all backend services:

- **Web Service** - Django web application
- **Celery Worker** - Background task processing
- **Celery Beat** - Scheduled task scheduler
- **Celery Flower** - Task monitoring and management
- **Production** - Optimized production deployment

## Benefits of Unified Approach

1. **Single Source of Truth** - All services built from the same base image
2. **Consistent Dependencies** - All services use identical Python packages and system dependencies
3. **Easier Maintenance** - Update dependencies in one place
4. **Reduced Build Time** - Shared layers between services
5. **Simplified Deployment** - One repository to manage all backend services

## Quick Start

### 1. Build All Services

**Windows (PowerShell):**
```powershell
.\build-backend-images.ps1
```

**Linux/Mac:**
```bash
chmod +x build-backend-images.sh
./build-backend-images.sh
```

### 2. Run with Docker Compose

```bash
docker-compose -f docker-compose-unified.yml up -d
```

### 3. Push to Docker Hub

**Windows (PowerShell):**
```powershell
.\push-backend-images.ps1
```

**Manual push:**
```bash
docker push yasir009/analytics-backend:web
docker push yasir009/analytics-backend:worker
docker push yasir009/analytics-backend:beat
docker push yasir009/analytics-backend:flower
docker push yasir009/analytics-backend:production
```

## Available Images

After building, you'll have these images available:

- `yasir009/analytics-backend:web` - Django web server
- `yasir009/analytics-backend:worker` - Celery worker
- `yasir009/analytics-backend:beat` - Celery beat scheduler
- `yasir009/analytics-backend:flower` - Celery monitoring
- `yasir009/analytics-backend:production` - Production optimized

## Docker Compose Configuration

The `docker-compose-unified.yml` file uses the multi-stage builds:

```yaml
web:
  build:
    context: .
    dockerfile: Dockerfile.multi
    target: web
  image: yasir009/analytics-backend:web
  # ... other configuration

celery_worker:
  build:
    context: .
    dockerfile: Dockerfile.multi
    target: celery-worker
  image: yasir009/analytics-backend:worker
  # ... other configuration
```

## Multi-Stage Dockerfile Structure

The `Dockerfile.multi` contains:

1. **Base Stage** - Common dependencies and setup
2. **Web Stage** - Django development server
3. **Celery Worker Stage** - Background task processing
4. **Celery Beat Stage** - Task scheduling
5. **Celery Flower Stage** - Monitoring interface
6. **Production Stage** - Gunicorn production server

## Environment Variables

All services use the same environment variables:

- `POSTGRES_DB` - Database name
- `POSTGRES_USER` - Database user
- `POSTGRES_PASSWORD` - Database password
- `POSTGRES_HOST` - Database host
- `REDIS_URL` - Redis connection string
- `DJANGO_SETTINGS_MODULE` - Django settings module

## Ports

- **Web Service**: 8000
- **Celery Flower**: 5555
- **PostgreSQL**: 5433
- **Redis**: 6380

## Development vs Production

### Development
- Uses Django development server
- Volume mounts for live code changes
- Debug mode enabled

### Production
- Uses Gunicorn WSGI server
- Static files collected
- Optimized for performance

## Migration from Old Setup

If you're migrating from the old separate Dockerfiles:

1. **Backup your current setup**
2. **Build new unified images**: `.\build-backend-images.ps1`
3. **Update docker-compose**: Use `docker-compose-unified.yml`
4. **Test thoroughly** before deploying to production

## Troubleshooting

### Build Issues
- Ensure Docker has enough memory (4GB+ recommended)
- Clear Docker cache: `docker system prune -a`
- Check for port conflicts

### Runtime Issues
- Check service logs: `docker-compose logs [service-name]`
- Verify environment variables
- Ensure database and Redis are accessible

## Contributing

When adding new services:

1. Add a new stage to `Dockerfile.multi`
2. Update build scripts
3. Add service to `docker-compose-unified.yml`
4. Update documentation

## Support

For issues or questions:
1. Check the logs: `docker-compose logs`
2. Verify configuration in docker-compose files
3. Ensure all dependencies are properly installed
