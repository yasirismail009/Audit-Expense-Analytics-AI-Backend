# Unprocessed Files Analysis Report

## Issue Summary
A file was uploaded but not being processed in the Docker environment. The investigation revealed several issues and provided solutions.

## Initial Status
- **File Status**: 1 file uploaded with status "COMPLETED"
- **Job Status**: 1 processing job with status "QUEUED" (not being processed)
- **Docker Services**: All services running (web, db, redis, celery_worker, celery_beat, celery_flower)

## Root Cause Analysis

### 1. Command Logic Issue
**Problem**: The `process_queued_jobs` management command was only looking for jobs with status 'PENDING' or 'FAILED', but the job had status 'QUEUED'.

**Solution**: Updated the command to include 'QUEUED' status in the job filtering logic.

**Files Modified**:
- `core/management/commands/process_queued_jobs.py`

**Changes Made**:
```python
# Before
jobs = FileProcessingJob.objects.filter(status__in=['PENDING', 'FAILED'])

# After  
jobs = FileProcessingJob.objects.filter(status__in=['PENDING', 'QUEUED', 'FAILED'])
```

### 2. Celery Connection Issue
**Problem**: When trying to process jobs via Celery, the connection was being refused with error "Connection refused".

**Root Cause**: The Celery worker was running but there was a connection issue between the web container and the Celery broker.

**Solution**: Used synchronous processing mode (`--sync` flag) to bypass Celery and process jobs directly.

## Status Flow
1. **Initial**: File uploaded → Status: COMPLETED, Job Status: QUEUED
2. **After Fix**: File Status: COMPLETED, Job Status: COMPLETED

## Tools Created

### 1. Comprehensive Check Script (`check_unprocessed_files.py`)
A full-featured script that provides:
- Docker service status checking
- Celery worker monitoring
- Database health verification
- File processing status analysis
- Processing job status tracking
- Temporary file checking
- Automated fix suggestions

### 2. Simple Check Script (`check_unprocessed.py`)
A lightweight script for quick status checks:
- Docker container status
- Service health verification
- Error log monitoring
- Basic troubleshooting suggestions

### 3. Django Management Command (`check_unprocessed_files.py`)
A Django management command that can be run within the Docker environment:
- Detailed file status reporting
- Processing job analysis
- Automatic fix capabilities
- Retry failed jobs functionality

## Commands for Monitoring

### Quick Status Check
```bash
# Simple external check
python check_unprocessed.py

# Django management command
docker-compose exec web python manage.py check_unprocessed_files

# Detailed analysis
docker-compose exec web python manage.py check_unprocessed_files --detailed
```

### Processing Jobs
```bash
# Process pending/queued jobs
docker-compose exec web python manage.py process_queued_jobs

# Process failed jobs
docker-compose exec web python manage.py process_queued_jobs --failed

# Process synchronously (bypass Celery)
docker-compose exec web python manage.py process_queued_jobs --sync

# Auto-fix issues
docker-compose exec web python manage.py check_unprocessed_files --fix
```

### Troubleshooting
```bash
# Check Docker services
docker-compose ps

# Check Celery logs
docker-compose logs celery_worker

# Check all logs for errors
docker-compose logs

# Restart services
docker-compose restart celery_worker redis

# Monitor Celery (Flower)
# Open http://localhost:5555 in browser
```

## File Status Types

### DataFile Status
- **PENDING**: File uploaded but not processed
- **PROCESSING**: File is currently being processed
- **COMPLETED**: File processing finished successfully
- **FAILED**: File processing failed

### FileProcessingJob Status
- **PENDING**: Job created but not started
- **QUEUED**: Job queued for processing
- **PROCESSING**: Job currently running
- **COMPLETED**: Job finished successfully
- **FAILED**: Job failed with error
- **CELERY_ERROR**: Celery connection error
- **SKIPPED**: Skipped due to duplicate content

## Recommendations

### 1. Regular Monitoring
- Run `check_unprocessed_files` command regularly
- Monitor Celery Flower dashboard
- Set up alerts for failed jobs

### 2. Celery Configuration
- Investigate and fix the Celery connection issue
- Consider using synchronous processing as fallback
- Implement proper error handling and retry logic

### 3. Process Improvements
- Add job timeout handling
- Implement automatic retry for failed jobs
- Add monitoring and alerting for stuck jobs

### 4. Documentation
- Document the file processing workflow
- Create troubleshooting guides
- Maintain runbooks for common issues

## Current Status
✅ **RESOLVED**: All files are now properly processed
- File: `generated_transactions_2025.csv` - Status: COMPLETED
- Job: `2b13f52c-90db-4e2e-babe-12958bcf6384` - Status: COMPLETED

## Next Steps
1. Investigate and fix the Celery connection issue for future asynchronous processing
2. Implement the recommended monitoring and alerting
3. Test the file upload and processing workflow with new files
4. Document the complete troubleshooting process 