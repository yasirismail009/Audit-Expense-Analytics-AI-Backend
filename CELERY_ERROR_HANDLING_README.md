# Celery Error Handling and Debugging Solution

## Overview

This solution addresses the `WSAECONNREFUSED` (WinError 10061) error that occurs when Celery workers are not running or not accessible. The system now provides comprehensive error handling, database field updates, and debugging tools to identify and resolve Celery connectivity issues.

## Problem Description

The error `[WinError 10061] No connection could be made because the target machine actively refused it` indicates that:
- No Celery worker is running
- The worker is not listening on the expected port
- Network connectivity issues between the application and Celery broker

## Solution Components

### 1. Enhanced Database Model

**File**: `core/models.py`

Added `'CELERY_ERROR'` status to `FileProcessingJob.STATUS_CHOICES`:

```python
STATUS_CHOICES = [
    ('PENDING', 'Pending'),
    ('PROCESSING', 'Processing'),
    ('COMPLETED', 'Completed'),
    ('FAILED', 'Failed'),
    ('CELERY_ERROR', 'Celery Connection Error'),  # NEW
    ('SKIPPED', 'Skipped - Duplicate Content'),
]
```

### 2. Enhanced Error Handling

**File**: `core/views.py`

#### A. Utility Methods

- `_test_celery_connection()`: Tests Celery worker connectivity
- `_handle_celery_failure()`: Centralized Celery error handling

#### B. Database Field Updates

When Celery fails, the system automatically:
- Updates `processing_job.status` to `'CELERY_ERROR'`
- Sets `processing_job.error_message` with detailed error information
- Sets `processing_job.completed_at` timestamp
- Logs comprehensive error details

#### C. Retry Functionality

Added retry endpoint: `/api/file-processing-jobs/{job_id}/retry/`

### 3. Enhanced Task Logging and Monitoring

**File**: `core/tasks.py`

#### A. Comprehensive Logging

- `log_task_info()`: Structured logging with task/job context
- `get_system_info()`: System resource monitoring
- Full traceback logging for errors

#### B. Debug Tasks

- `debug_task()`: Basic connectivity and system tests
- `worker_health_check()`: Comprehensive health checks
- `monitor_worker_performance()`: Performance metrics collection

### 4. Debug API Endpoints

**File**: `core/views.py` - `CeleryDebugView`

#### Available Endpoints:

- `GET /api/celery-debug/`: Get worker status and health
- `POST /api/celery-debug/`: Trigger debug tasks
- `GET /api/celery-debug/{task_id}/`: Get task results

#### Debug Actions:

```json
{
  "action": "debug"              // Basic connectivity test
  "action": "health_check"       // Comprehensive health check
  "action": "performance_monitor" // Performance metrics
}
```

## Usage Examples

### 1. Check Celery Status

```bash
curl -X GET http://localhost:8000/api/celery-debug/
```

### 2. Trigger Health Check

```bash
curl -X POST http://localhost:8000/api/celery-debug/ \
  -H "Content-Type: application/json" \
  -d '{"action": "health_check"}'
```

### 3. Retry Failed Job

```bash
curl -X POST http://localhost:8000/api/file-processing-jobs/{job_id}/retry/
```

### 4. Check Job Status

```bash
curl -X GET http://localhost:8000/api/file-processing-jobs/{job_id}/status/
```

## Error Response Format

When Celery fails, the API returns:

```json
{
  "job_id": "uuid",
  "status": "CELERY_ERROR",
  "message": "File uploaded but background processing failed due to Celery connection error.",
  "error": "[WinError 10061] No connection could be made...",
  "error_type": "OperationalError",
  "celery_connection_status": {
    "connected": false,
    "error": "Connection refused",
    "workers": [],
    "worker_count": 0
  },
  "retry_endpoint": "/api/file-processing-jobs/{job_id}/retry/",
  "file_info": {...},
  "status_endpoint": "/api/file-processing-jobs/{job_id}/status/"
}
```

## Health Check Results

The health check provides detailed diagnostics:

```json
{
  "task_id": "uuid",
  "worker": "worker@hostname",
  "pid": 12345,
  "timestamp": "2025-07-22T18:41:00.345054+00:00",
  "checks": {
    "system_info": {"status": "OK", "data": {...}},
    "database": {"status": "OK", "data": {...}},
    "file_system": {"status": "OK", "data": {...}},
    "memory": {"status": "OK", "data": {...}},
    "cpu": {"status": "OK", "data": {...}},
    "model_imports": {"status": "OK", "data": {...}}
  },
  "overall_status": "HEALTHY",
  "failed_checks_count": 0
}
```

## Troubleshooting Guide

### 1. Celery Worker Not Running

**Symptoms**: `WSAECONNREFUSED` error
**Solution**: Start Celery worker

```bash
# Start worker with Redis
python start_celery_worker_redis.py

# Or start worker with SQLite
python start_celery_worker.py
```

### 2. Redis Not Running

**Symptoms**: Connection refused to Redis
**Solution**: Start Redis server

```bash
# Windows
redis-server

# Linux/Mac
sudo systemctl start redis
```

### 3. Port Conflicts

**Symptoms**: Port already in use
**Solution**: Check and free ports

```bash
# Check what's using the port
netstat -ano | findstr :6379

# Kill process if needed
taskkill /PID <process_id> /F
```

### 4. Network Issues

**Symptoms**: Connection timeout
**Solution**: Check firewall and network settings

## Monitoring and Alerts

### 1. Automatic Monitoring

The system automatically:
- Logs all Celery errors with full context
- Updates database fields for tracking
- Provides retry mechanisms
- Offers detailed error reporting

### 2. Manual Monitoring

Use the debug endpoints to:
- Check worker health
- Monitor performance
- Test connectivity
- View active tasks

### 3. Log Analysis

Look for these log patterns:
- `[TASK:process_file_with_anomalies] [JOB:uuid]` - Task execution logs
- `Celery failure in context: error` - Error handling logs
- `Job status updated to CELERY_ERROR` - Status update logs

## Best Practices

### 1. Error Handling

- Always check job status after submission
- Use retry endpoints for failed jobs
- Monitor error messages for patterns

### 2. Monitoring

- Regular health checks during development
- Performance monitoring for production
- Log analysis for troubleshooting

### 3. Recovery

- Use retry functionality for transient failures
- Restart workers for persistent issues
- Check system resources during high load

## Testing

Run the test scripts to verify functionality:

```bash
# Test error handling
python test_celery_error_handling.py

# Test debugging features
python test_celery_debugging.py
```

## Dependencies

- `psutil>=7.0.0` - System monitoring
- `celery>=5.3.0` - Task queue
- `redis>=4.6.0` - Message broker (optional)

## Conclusion

This solution provides comprehensive error handling, debugging capabilities, and monitoring tools to address Celery connectivity issues. The system gracefully handles failures, provides detailed error information, and offers recovery mechanisms to ensure robust operation. 