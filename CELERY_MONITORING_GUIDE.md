# 🚀 Celery Debug Logging and Monitoring Guide

This guide shows you how to enable debug logs in Celery and monitor processing status in real-time.

## 📋 Quick Commands for Monitoring

### 1. Enable Debug Logging
```bash
# Run the debug monitoring script
docker exec analytics-web-1 python enable_celery_debug.py
```

### 2. Monitor Celery Worker Logs (Real-time)
```bash
# Follow Celery worker logs in real-time
docker logs -f analytics-celery_worker-1

# Follow web application logs
docker logs -f analytics-web-1

# Follow Redis logs
docker logs -f analytics-redis-1
```

### 3. Check Processing Status
```bash
# Run real-time monitoring
docker exec analytics-web-1 python monitor_celery_realtime.py

# Check current job status
docker exec analytics-web-1 python docker_data_analysis.py
```

## 🔧 Debug Logging Configuration

### Updated Settings
The following debug logging has been enabled in `analytics/settings.py`:

```python
'celery': {
    'handlers': ['console', 'file'],
    'level': 'DEBUG',  # Changed from INFO to DEBUG
    'propagate': False,
},
'celery.task': {
    'handlers': ['console', 'file'],
    'level': 'DEBUG',
    'propagate': False,
},
'celery.worker': {
    'handlers': ['console', 'file'],
    'level': 'DEBUG',
    'propagate': False,
},
```

### Log Files
- **Console Output**: Real-time debug information
- **debug.log**: Persistent debug logs in project directory
- **celery_monitor.log**: Real-time monitoring logs

## 📊 What You'll See in Debug Logs

### 1. Task Processing Details
```
DEBUG - core.tasks - Starting duplicate analysis for job: 01059bdd-aec3-41fa-802a-5f36fd0aaa2f
DEBUG - core.tasks - Found 10000 transactions to analyze
DEBUG - core.tasks - Processing transaction 1/10000
DEBUG - core.tasks - Duplicate detection completed: 2 duplicates found
DEBUG - core.tasks - Risk assessment: HIGH (score: 85.5)
```

### 2. Worker Status
```
DEBUG - celery.worker - Worker celery@worker-1 ready
DEBUG - celery.worker.consumer - Connected to redis://localhost:6379/0
DEBUG - celery.worker.consumer.tasks - Received task: core.tasks.run_duplicate_analysis
DEBUG - celery.worker.consumer.tasks - Task core.tasks.run_duplicate_analysis succeeded
```

### 3. Queue Information
```
DEBUG - celery.worker.consumer.connection - Connected to broker
DEBUG - celery.worker.consumer.connection - Queue 'analytics' bound to exchange 'analytics'
DEBUG - celery.worker.consumer.tasks - Task accepted: core.tasks.run_backdated_analysis
```

## 🔍 Monitoring Commands

### 1. Check Celery Health
```bash
# Check if Celery workers are running
docker exec analytics-web-1 python -c "
from analytics.celery import app
inspect = app.control.inspect()
active = inspect.active()
print(f'Active workers: {len(active) if active else 0}')
"
```

### 2. Monitor Task Queue
```bash
# Check active tasks
docker exec analytics-web-1 python -c "
from analytics.celery import app
inspect = app.control.inspect()
active = inspect.active()
for worker, tasks in (active or {}).items():
    print(f'Worker {worker}: {len(tasks)} active tasks')
"
```

### 3. Check Processing Jobs
```bash
# Check job status in database
docker exec analytics-web-1 python -c "
import django
django.setup()
from core.models import FileProcessingJob
jobs = FileProcessingJob.objects.all().order_by('-created_at')[:5]
for job in jobs:
    print(f'Job {job.id}: {job.status} - {job.created_at}')
"
```

## 🚨 Troubleshooting

### 1. No Debug Logs Appearing
```bash
# Restart Celery worker with debug logging
docker-compose restart celery_worker

# Check if debug logging is enabled
docker exec analytics-web-1 python -c "
import logging
print('Celery log level:', logging.getLogger('celery').getEffectiveLevel())
print('Core log level:', logging.getLogger('core').getEffectiveLevel())
"
```

### 2. Tasks Not Processing
```bash
# Check Celery connection
docker exec analytics-web-1 python -c "
from analytics.celery import app
try:
    result = app.control.inspect().active()
    print('Celery connection: OK')
except Exception as e:
    print(f'Celery connection failed: {e}')
"
```

### 3. Redis Connection Issues
```bash
# Check Redis connectivity
docker exec analytics-web-1 python -c "
import redis
try:
    r = redis.Redis(host='redis', port=6379, db=0)
    r.ping()
    print('Redis connection: OK')
except Exception as e:
    print(f'Redis connection failed: {e}')
"
```

## 📈 Real-Time Monitoring Scripts

### 1. `enable_celery_debug.py`
- Enables debug logging for all Celery components
- Tests Celery connection
- Shows current configuration
- Runs a test task

### 2. `monitor_celery_realtime.py`
- Continuous monitoring with 10-second intervals
- Shows active tasks, processing jobs, and queue status
- Real-time status updates
- Press Ctrl+C to stop

### 3. `docker_data_analysis.py`
- Comprehensive analysis of data and processing status
- Shows duplicate detection, backdated entries, and risk calculation
- Checks Celery processing status

## 🎯 Key Debug Information to Look For

### Task Processing
- Task start/completion times
- Processing duration
- Number of records processed
- Error messages and stack traces

### Performance Metrics
- Memory usage
- CPU utilization
- Database query performance
- Redis connection status

### Error Detection
- Task failures and retries
- Connection timeouts
- Database errors
- Memory leaks

## 📝 Example Debug Output

```
2025-01-15 10:30:15 - celery.worker.consumer.tasks - DEBUG - Received task: core.tasks.run_duplicate_analysis
2025-01-15 10:30:15 - core.tasks - DEBUG - Starting duplicate analysis for job: 01059bdd-aec3-41fa-802a-5f36fd0aaa2f
2025-01-15 10:30:15 - core.tasks - DEBUG - Loading 10000 transactions from database
2025-01-15 10:30:16 - core.tasks - DEBUG - Building lookup tables for duplicate detection
2025-01-15 10:30:17 - core.tasks - DEBUG - Processing transaction 1/10000
2025-01-15 10:30:17 - core.tasks - DEBUG - Found duplicate type_4 for transaction pair
2025-01-15 10:30:18 - core.tasks - DEBUG - Duplicate detection completed: 2 duplicates found
2025-01-15 10:30:18 - core.tasks - DEBUG - Risk assessment calculated: HIGH (score: 85.5)
2025-01-15 10:30:18 - core.tasks - DEBUG - Saving results to database
2025-01-15 10:30:18 - celery.worker.consumer.tasks - DEBUG - Task core.tasks.run_duplicate_analysis succeeded
```

## ✅ Success Indicators

- Tasks are being received and processed
- No connection errors in logs
- Processing jobs show "COMPLETED" status
- Debug logs show detailed processing steps
- No memory or performance issues

## ❌ Problem Indicators

- "Connection refused" errors
- Tasks stuck in "PENDING" status
- No debug logs appearing
- High memory usage or timeouts
- Database connection errors

## 🚀 Next Steps

1. **Start monitoring**: Run `docker exec analytics-web-1 python enable_celery_debug.py`
2. **Watch logs**: Use `docker logs -f analytics-celery_worker-1`
3. **Upload a file**: Test the processing with debug logs enabled
4. **Monitor real-time**: Use `docker exec analytics-web-1 python monitor_celery_realtime.py`

This will give you complete visibility into Celery processing and help identify any issues with duplicate detection, backdated analysis, or overall risk calculation. 