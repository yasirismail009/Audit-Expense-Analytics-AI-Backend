# Enhanced Celery Setup - Always Listening to Queues

This document describes the enhanced Celery configuration that ensures your Celery workers are always listening to and processing queue tasks reliably.

## 🚀 Features

- **Automatic Queue Monitoring**: Celery workers continuously monitor queues for new tasks
- **Enhanced Reliability**: Automatic restart on failures and connection issues
- **Priority Queues**: Tasks are processed based on priority levels
- **Health Monitoring**: Continuous health checks and status monitoring
- **Auto-Recovery**: Workers automatically recover from failures
- **Real-time Monitoring**: Live monitoring of queue status and worker health

## 📁 Files Overview

### Core Configuration
- `analytics/celery.py` - Enhanced Celery configuration with reliability features
- `docker-compose.yml` - Docker setup with health checks and restart policies

### Management Scripts
- `start_celery_enhanced.py` - Enhanced startup script with auto-restart capabilities
- `start_celery_enhanced.bat` - Windows batch file for easy startup
- `monitor_celery_queues.py` - Queue monitoring and health check script
- `monitor_celery_queues.bat` - Windows batch file for monitoring

## 🛠️ Setup Instructions

### 1. Prerequisites

Ensure you have the following installed:
- Python 3.8+
- Redis server
- Required Python packages (see requirements.txt)

### 2. Start Enhanced Celery

#### Option A: Using the Enhanced Startup Script (Recommended)

```bash
# Windows
start_celery_enhanced.bat

# Linux/Mac
python start_celery_enhanced.py
```

#### Option B: Using Docker Compose

```bash
docker-compose up -d celery_worker celery_beat
```

#### Option C: Manual Start

```bash
# Terminal 1: Start Celery Worker
celery -A analytics worker --loglevel=info --concurrency=4 -Q default,analytics,ml_training,maintenance

# Terminal 2: Start Celery Beat
celery -A analytics beat --loglevel=info --scheduler=django_celery_beat.schedulers:DatabaseScheduler
```

### 3. Monitor Queue Health

```bash
# Windows
monitor_celery_queues.bat

# Linux/Mac
python monitor_celery_queues.py

# Run once
python monitor_celery_queues.py --once

# Custom interval (every 15 seconds)
python monitor_celery_queues.py --interval 15
```

## 🔧 Configuration Details

### Enhanced Celery Settings

The enhanced configuration includes:

```python
# Connection reliability
broker_connection_retry_on_startup=True
broker_connection_max_retries=20
broker_connection_retry=True
broker_connection_retry_delay=1

# Worker settings
worker_concurrency=4
worker_max_tasks_per_child=1000
worker_prefetch_multiplier=1
worker_disable_rate_limits=False
worker_enable_remote_control=True

# Task routing with priorities
task_routes={
    'core.tasks.process_queued_jobs': {'queue': 'analytics', 'priority': 15},  # Highest
    'core.tasks.process_file_with_anomalies': {'queue': 'analytics', 'priority': 10},
    'core.tasks.train_ml_models': {'queue': 'ml_training', 'priority': 5},
    'core.tasks.monitor_processing_jobs': {'queue': 'maintenance', 'priority': 1},
}

# Enhanced beat schedule
beat_schedule={
    'process-queued-jobs': {
        'task': 'core.tasks.process_queued_jobs',
        'schedule': 10.0,  # Every 10 seconds
        'options': {'queue': 'analytics', 'priority': 15}
    },
    'monitor-processing-jobs': {
        'task': 'core.tasks.monitor_processing_jobs',
        'schedule': 60.0,  # Every minute
        'options': {'queue': 'maintenance', 'priority': 1}
    },
}
```

### Queue Configuration

Three priority queues are configured:

1. **analytics** (Priority 1-20): Main processing tasks
2. **ml_training** (Priority 1-10): Machine learning model training
3. **maintenance** (Priority 1-5): System maintenance and monitoring

## 📊 Monitoring and Health Checks

### Automatic Health Checks

The system performs automatic health checks every 30 seconds:

- Redis connection status
- Worker responsiveness
- Queue health
- Task processing status
- Scheduled task monitoring

### Health Check Commands

```bash
# Check worker status
celery -A analytics inspect ping

# Check active queues
celery -A analytics inspect active_queues

# Check worker statistics
celery -A analytics inspect stats

# Check active tasks
celery -A analytics inspect active

# Check reserved tasks
celery -A analytics inspect reserved
```

### Monitoring Dashboard

Access Flower monitoring at `http://localhost:5555` (if enabled in Docker):

```bash
docker-compose up -d celery_flower
```

## 🔄 Auto-Recovery Features

### Automatic Restart

- Workers automatically restart on failure
- Maximum 10 restart attempts before stopping
- 5-second delay between restart attempts
- Health check failures trigger automatic restarts

### Connection Recovery

- Infinite broker connection retries
- Exponential backoff for failed connections
- Automatic reconnection on network issues
- Heartbeat monitoring for connection health

### Task Recovery

- Failed tasks are retried up to 5 times
- Exponential backoff between retries
- Maximum retry delay of 10 minutes
- Task results are preserved for 1 hour

## 🚨 Troubleshooting

### Common Issues

#### Worker Not Starting
```bash
# Check Redis connection
redis-cli ping

# Check Django settings
python manage.py check

# Check Celery configuration
python -c "from analytics.celery import app; print(app.conf.broker_url)"
```

#### Tasks Not Processing
```bash
# Check worker status
celery -A analytics inspect ping

# Check queue status
celery -A analytics inspect active_queues

# Check for errors in logs
tail -f celery_worker.log
```

#### Connection Issues
```bash
# Test Redis connection
python -c "import redis; r=redis.Redis(); r.ping()"

# Check network connectivity
telnet localhost 6379

# Restart Redis
redis-cli shutdown
redis-server
```

### Log Files

- `celery_worker.log` - Worker operation logs
- `celery_monitor.log` - Monitoring script logs
- `debug.log` - Django debug logs

## 📈 Performance Optimization

### Worker Tuning

```python
# Optimal settings for most workloads
worker_concurrency = 4  # Number of worker processes
worker_max_tasks_per_child = 1000  # Restart worker after N tasks
worker_prefetch_multiplier = 1  # Don't prefetch too many tasks
```

### Queue Optimization

```python
# Priority-based processing
task_routes = {
    'high_priority_task': {'queue': 'analytics', 'priority': 20},
    'normal_task': {'queue': 'analytics', 'priority': 10},
    'low_priority_task': {'queue': 'analytics', 'priority': 1},
}
```

### Memory Management

```python
# Result expiration
result_expires = 3600  # Results expire after 1 hour

# Cleanup old results
'cleanup-old-results': {
    'task': 'core.tasks.cleanup_old_results',
    'schedule': 3600.0,  # Every hour
}
```

## 🔐 Security Considerations

### Redis Security

- Use authentication if Redis is exposed to network
- Consider using SSL for Redis connections
- Restrict Redis access to localhost in development

### Worker Security

- Workers run with Django's security context
- Task results are stored securely
- Sensitive data is not logged

## 📚 Additional Resources

### Documentation
- [Celery Documentation](https://docs.celeryproject.org/)
- [Redis Documentation](https://redis.io/documentation)
- [Django Celery Beat](https://django-celery-beat.readthedocs.io/)

### Monitoring Tools
- [Flower](https://flower.readthedocs.io/) - Celery monitoring
- [Redis Commander](https://github.com/joeferner/redis-commander) - Redis management
- [Celery Inspect](https://docs.celeryproject.org/en/stable/reference/celery.app.control.html) - Command-line monitoring

## 🤝 Support

If you encounter issues:

1. Check the logs for error messages
2. Run the monitoring script to identify problems
3. Verify Redis and Django configurations
4. Check network connectivity and firewall settings
5. Review the troubleshooting section above

## 📝 Changelog

### Version 2.0 (Current)
- Enhanced reliability and auto-recovery
- Priority queue system
- Comprehensive health monitoring
- Automatic restart capabilities
- Enhanced Docker configuration

### Version 1.0
- Basic Celery setup
- Simple queue processing
- Basic monitoring

---

**Note**: This enhanced setup ensures that Celery workers are always listening to queues and automatically recover from failures, providing a robust and reliable task processing system.





