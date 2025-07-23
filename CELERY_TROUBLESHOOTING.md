# Celery Troubleshooting Guide

This guide helps you resolve common Celery issues in the analytics project.

## Quick Start

### 1. Start Celery Worker
```bash
# Option 1: Use the provided script (recommended)
python start_celery_worker.py

# Option 2: Use the batch file (Windows)
start_celery_worker.bat

# Option 3: Direct command
celery -A analytics worker --loglevel=info --pool=solo
```

### 2. Test Connection
```bash
python test_celery_connection.py
```

## Common Issues and Solutions

### Issue 1: "No connection could be made because the target machine actively refused it"

**Symptoms:**
- Error: `[WinError 10061] No connection could be made because the target machine actively refused it`
- Tasks fail to submit
- Worker inspection fails

**Causes:**
1. Celery worker is not running
2. Missing dependencies (sqlalchemy)
3. Incorrect broker configuration

**Solutions:**

#### A. Install Missing Dependencies
```bash
pip install sqlalchemy
```

#### B. Start the Worker
```bash
python start_celery_worker.py
```

#### C. Check Worker Status
```bash
python test_celery_connection.py
```

### Issue 2: "ModuleNotFoundError: No module named 'sqlalchemy'"

**Solution:**
```bash
pip install sqlalchemy>=2.0.0
```

### Issue 3: Worker Starts But Tasks Don't Execute

**Check:**
1. Worker logs for errors
2. Task routing configuration
3. Task registration

**Solution:**
```bash
# Check worker logs
python start_celery_worker.py

# Test task submission
python test_celery_connection.py
```

### Issue 4: Windows-Specific Issues

**Problem:** Celery doesn't work well with default pool on Windows

**Solution:** Use the `solo` pool
```bash
celery -A analytics worker --loglevel=info --pool=solo
```

## Configuration Details

### Broker Configuration
The project uses SQLite as the broker (no Redis required):
```python
CELERY_BROKER_URL = 'sqla+sqlite:///celery_broker.db'
CELERY_RESULT_BACKEND = 'db+sqlite:///celery_results.db'
```

### Worker Settings
```python
CELERY_WORKER_CONCURRENCY = 4
CELERY_WORKER_MAX_TASKS_PER_CHILD = 1000
CELERY_WORKER_PREFETCH_MULTIPLIER = 1
```

## Monitoring and Debugging

### Check Worker Status
```bash
python test_celery_connection.py
```

### View Worker Logs
The worker script provides detailed logging. Look for:
- ✅ Connection successful
- ✅ Task processing
- ❌ Error messages

### Database Files
Celery creates these SQLite files:
- `celery_broker.db` - Message broker
- `celery_results.db` - Task results

## Development Workflow

### 1. Start Development Server
```bash
python manage.py runserver
```

### 2. Start Celery Worker (in separate terminal)
```bash
python start_celery_worker.py
```

### 3. Test File Upload
Upload a file through the API - it should now process successfully.

## Production Considerations

### For Production Deployment:
1. Use Redis instead of SQLite for better performance
2. Configure proper logging
3. Set up monitoring
4. Use process management (supervisor, systemd)

### Redis Configuration (Optional)
```python
CELERY_BROKER_URL = 'redis://localhost:6379/0'
CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'
```

## Troubleshooting Checklist

- [ ] Is sqlalchemy installed? (`pip install sqlalchemy`)
- [ ] Is the worker running? (`python test_celery_connection.py`)
- [ ] Are there any error messages in worker logs?
- [ ] Is the broker database accessible?
- [ ] Are tasks properly registered?
- [ ] Is the Django settings module correct?

## Getting Help

If you're still experiencing issues:

1. Check the worker logs for detailed error messages
2. Run the connection test: `python test_celery_connection.py`
3. Verify all dependencies are installed: `pip list | grep celery`
4. Check Django settings for Celery configuration
5. Ensure the worker is running in a separate terminal

## Files Created for Celery Support

- `start_celery_worker.py` - Worker startup script
- `start_celery_worker.bat` - Windows batch file
- `test_celery_connection.py` - Connection testing script
- `CELERY_TROUBLESHOOTING.md` - This guide 