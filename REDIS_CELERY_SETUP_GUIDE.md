# Redis + Celery Setup Guide

## Overview

This guide provides step-by-step instructions for setting up Celery with Redis for the analytics project.

## Prerequisites

- Python 3.8+
- Redis server running on localhost:6379
- Django project with Celery dependencies

## Installation

### 1. Install Dependencies
```bash
pip install redis>=4.6.0
pip install celery>=5.3.0
```

### 2. Install Redis Server

#### Windows:
1. Download Redis for Windows from: https://github.com/microsoftarchive/redis/releases
2. Install and start Redis server
3. Or use WSL2 with Redis

#### Linux/Mac:
```bash
# Ubuntu/Debian
sudo apt-get install redis-server

# macOS
brew install redis
```

## Configuration

### 1. Celery Settings (analytics/settings.py)
```python
# Celery broker settings - Using Redis for better performance
CELERY_BROKER_URL = 'redis://localhost:6379/0'
CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'

# Celery task settings
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TIMEZONE = TIME_ZONE
CELERY_ENABLE_UTC = True

# Celery task routing
CELERY_TASK_ROUTES = {
    'core.tasks.process_file_with_anomalies': {'queue': 'analytics'},
    'core.tasks.train_ml_models': {'queue': 'ml_training'},
    'core.tasks.retrain_ml_models': {'queue': 'ml_training'},
    'core.tasks.train_enhanced_ml_models': {'queue': 'ml_training'},
    'core.tasks.monitor_processing_jobs': {'queue': 'maintenance'},
    'core.tasks.monitor_ml_model_performance': {'queue': 'maintenance'},
}

# Celery task default settings
CELERY_TASK_DEFAULT_QUEUE = 'analytics'
CELERY_TASK_DEFAULT_EXCHANGE = 'analytics'
CELERY_TASK_DEFAULT_ROUTING_KEY = 'analytics'

# Celery worker settings
CELERY_WORKER_CONCURRENCY = 4
CELERY_WORKER_MAX_TASKS_PER_CHILD = 1000
CELERY_WORKER_PREFETCH_MULTIPLIER = 1

# Celery task timeout and retry settings
CELERY_TASK_TIME_LIMIT = 300  # 5 minutes
CELERY_TASK_SOFT_TIME_LIMIT = 240  # 4 minutes
CELERY_TASK_MAX_RETRIES = 3
CELERY_TASK_RETRY_DELAY = 60  # 1 minute
```

### 2. Task Configuration (core/tasks.py)
```python
@shared_task(bind=True, max_retries=3, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def process_file_with_anomalies(self, job_id):
    # Task implementation
```

## Running Celery

### 1. Start Redis Server
```bash
# Windows (if installed as service)
redis-server

# Linux/Mac
sudo systemctl start redis
# or
redis-server
```

### 2. Start Celery Worker

#### Option 1: Using Python Script
```bash
python start_celery_worker_redis.py
```

#### Option 2: Using Batch File (Windows)
```bash
start_celery_worker_redis.bat
```

#### Option 3: Direct Command
```bash
celery -A analytics worker --loglevel=info --pool=solo --concurrency=4 --queues=analytics,ml_training,maintenance
```

### 3. Start Celery Beat (for scheduled tasks)
```bash
celery -A analytics beat --loglevel=info
```

## Testing

### 1. Test Redis Connection
```bash
python test_redis_celery.py
```

### 2. Test Task Submission
```python
from core.tasks import process_file_with_anomalies
result = process_file_with_anomalies.delay(job_id)
print(f"Task ID: {result.id}")
```

## Queue Structure

### Analytics Queue
- File processing tasks
- Analytics generation
- Anomaly detection

### ML Training Queue
- Model training tasks
- Model retraining
- Enhanced model training

### Maintenance Queue
- Monitoring tasks
- System maintenance

## Monitoring

### 1. Check Worker Status
```bash
celery -A analytics inspect stats
```

### 2. Check Active Tasks
```bash
celery -A analytics inspect active
```

### 3. Check Queue Status
```bash
celery -A analytics inspect active_queues
```

## Troubleshooting

### Common Issues

#### 1. Redis Connection Failed
```
❌ Redis connection failed: Connection refused
```
**Solution:** Make sure Redis server is running on localhost:6379

#### 2. No Celery Workers Running
```
❌ No Celery workers are running!
```
**Solution:** Start the Celery worker using one of the methods above

#### 3. Task Import Errors
```
❌ cannot import name 'task_name' from 'core.tasks'
```
**Solution:** Check that the task is properly defined and imported

#### 4. Windows-Specific Issues
```
❌ Celery doesn't work with default pool on Windows
```
**Solution:** Use `--pool=solo` flag for Windows

### Debug Commands

#### 1. Test Redis Connection
```python
import redis
r = redis.Redis(host='localhost', port=6379, db=0)
r.ping()  # Should return True
```

#### 2. Test Celery Configuration
```python
from analytics.celery import app
print(app.conf.broker_url)
print(app.conf.result_backend)
```

#### 3. Test Worker Status
```python
from celery import current_app
inspect = current_app.control.inspect()
stats = inspect.stats()
print(stats)
```

## Performance Optimization

### 1. Worker Concurrency
- Adjust `CELERY_WORKER_CONCURRENCY` based on CPU cores
- Use `--pool=solo` for Windows, `--pool=prefork` for Linux

### 2. Task Timeouts
- Set appropriate `time_limit` and `soft_time_limit`
- Use retry mechanisms for transient failures

### 3. Queue Prioritization
- Use separate queues for different task types
- Prioritize critical tasks

## Security Considerations

### 1. Redis Security
- Set Redis password in production
- Use SSL/TLS for Redis connections
- Restrict Redis access to localhost

### 2. Task Security
- Validate all task inputs
- Use appropriate timeouts
- Implement proper error handling

## Production Deployment

### 1. Use Redis Cluster
```python
CELERY_BROKER_URL = 'redis://redis-cluster:6379/0'
```

### 2. Use Process Management
- Use supervisor or systemd
- Implement health checks
- Set up monitoring

### 3. Use SSL/TLS
```python
CELERY_BROKER_URL = 'rediss://localhost:6379/0'
```

## Summary

This setup provides:
- ✅ **Redis-based message broker** for better performance
- ✅ **Multiple queues** for task prioritization
- ✅ **Task retry mechanisms** for reliability
- ✅ **Timeout handling** for resource management
- ✅ **Windows compatibility** with solo pool
- ✅ **Comprehensive monitoring** and debugging tools

The system is now ready for production use with proper monitoring and maintenance procedures. 