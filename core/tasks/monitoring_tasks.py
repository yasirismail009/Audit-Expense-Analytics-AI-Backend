"""
Monitoring Tasks Module

Contains all monitoring and system health-related Celery tasks for the analytics system.
"""

from celery import shared_task, current_task
from celery.utils.log import get_task_logger
from django.utils import timezone

# Set up logger
logger = get_task_logger(__name__)
from django.db import transaction
from django.db.models import F, Q, Count, Sum, Avg, Min, Max
import logging
import traceback
import psutil
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List

from ..models import (
    FileProcessingJob, SAPGLPosting, DataFile, CompletenessJob,
    FileProcessingTask, CompletenessTestResult
)

from .utils import (
    send_notification_if_available,
    get_user_from_job,
    log_task_info,
    debug_task_state,
    debug_task_data,
    debug_task_exception,
    get_system_info,
)

logger = logging.getLogger(__name__)


@shared_task(bind=True)
def debug_task(self):
    """
    Debug task to test Celery worker functionality
    """
    try:
        log_task_info("debug_task", "debug", "Starting debug task")
        
        # Get system information
        system_info = get_system_info()
        
        # Test database connection
        try:
            job_count = FileProcessingJob.objects.count()
            data_file_count = DataFile.objects.count()
            transaction_count = SAPGLPosting.objects.count()
            
            db_status = "connected"
            db_info = {
                "jobs": job_count,
                "data_files": data_file_count,
                "transactions": transaction_count
            }
        except Exception as e:
            db_status = "error"
            db_info = {"error": str(e)}
        
        # Test file system access
        try:
            current_dir = os.getcwd()
            files_in_dir = len(os.listdir(current_dir))
            
            fs_status = "accessible"
            fs_info = {
                "current_directory": current_dir,
                "files_count": files_in_dir
            }
        except Exception as e:
            fs_status = "error"
            fs_info = {"error": str(e)}
        
        result = {
            "status": "success",
            "message": "Debug task completed successfully",
            "timestamp": datetime.now().isoformat(),
            "system_info": system_info,
            "database_status": db_status,
            "database_info": db_info,
            "filesystem_status": fs_status,
            "filesystem_info": fs_info,
            "worker_id": self.request.id
        }
        
        log_task_info("debug_task", "debug", "Debug task completed successfully")
        
        return result
        
    except Exception as e:
        logger.error(f"Debug task failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}


@shared_task(bind=True, name='core.tasks.process_queued_jobs')
def process_queued_jobs(self):
    """
    Process queued file processing jobs
    """
    try:
        logger.info("process_queued_jobs task started")
        log_task_info("process_queued_jobs", "monitor", "Starting queued jobs processing")
        
        # Get queued jobs
        queued_jobs = FileProcessingJob.objects.filter(
            status='PENDING'
        ).order_by('created_at')[:10]  # Process up to 10 jobs at a time
        
        if not queued_jobs.exists():
            log_task_info("process_queued_jobs", "monitor", "No queued jobs found")
            return {"status": "success", "message": "No queued jobs found", "processed": 0}
        
        processed_count = 0
        failed_count = 0
        
        for job in queued_jobs:
            try:
                # Update job status
                job.status = 'QUEUED'
                job.save()
                
                # Check if data file exists and is valid
                if not job.data_file:
                    job.status = 'FAILED'
                    job.error_message = "No data file associated with job"
                    job.save()
                    failed_count += 1
                    continue
                
                # Check if file is already processed
                if job.data_file.status == 'COMPLETED':
                    job.status = 'COMPLETED'
                    job.completed_at = timezone.now()
                    job.save()
                    processed_count += 1
                    continue
                
                # Start processing based on job type
                if job.job_type == 'GL_PROCESSING':
                    # Start GL processing
                    from .analysis_tasks import run_restructured_analysis
                    run_restructured_analysis.delay(job.id)
                    processed_count += 1
                    
                elif job.job_type == 'COMPLETENESS':
                    # Start completeness test
                    from .completeness_tasks import run_completeness_job
                    run_completeness_job.delay(job.id)
                    processed_count += 1
                    
                else:
                    # Unknown job type
                    job.status = 'FAILED'
                    job.error_message = f"Unknown job type: {job.job_type}"
                    job.save()
                    failed_count += 1
                
                log_task_info("process_queued_jobs", "monitor", f"Queued job {job.id} for processing")
                
            except Exception as e:
                logger.error(f"Failed to process job {job.id}: {e}")
                job.status = 'FAILED'
                job.error_message = str(e)
                job.save()
                failed_count += 1
        
        result = {
            "status": "success",
            "message": f"Processed {processed_count} jobs, {failed_count} failed",
            "processed": processed_count,
            "failed": failed_count,
            "total_queued": queued_jobs.count()
        }
        
        log_task_info("process_queued_jobs", "monitor", f"Queued jobs processing completed: {result}")
        
        return result
        
    except Exception as e:
        logger.error(f"Process queued jobs failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}


@shared_task(bind=True, name='core.tasks.monitor_processing_jobs')
def monitor_processing_jobs(self):
    """
    Monitor processing jobs and handle timeouts
    """
    try:
        log_task_info("monitor_processing_jobs", "monitor", "Starting processing jobs monitoring")
        
        # Get jobs that have been processing for too long
        timeout_threshold = timezone.now() - timedelta(hours=2)  # 2 hours timeout
        stuck_jobs = FileProcessingJob.objects.filter(
            status='PROCESSING',
            started_at__lt=timeout_threshold
        )
        
        timeout_count = 0
        for job in stuck_jobs:
            try:
                job.status = 'FAILED'
                job.error_message = "Job timed out after 2 hours"
                job.save()
                timeout_count += 1
                
                # Send notification
                user_id = get_user_from_job(job)
                send_notification_if_available(
                    notify_file_processing_status, 
                    user_id, 
                    job.id, 
                    'FAILED'
                )
                
                log_task_info("monitor_processing_jobs", "monitor", f"Timed out job {job.id}")
                
            except Exception as e:
                logger.error(f"Failed to timeout job {job.id}: {e}")
        
        # Get jobs that have been queued for too long
        queue_timeout_threshold = timezone.now() - timedelta(hours=1)  # 1 hour queue timeout
        stuck_queued_jobs = FileProcessingJob.objects.filter(
            status='QUEUED',
            created_at__lt=queue_timeout_threshold
        )
        
        queue_timeout_count = 0
        for job in stuck_queued_jobs:
            try:
                job.status = 'FAILED'
                job.error_message = "Job timed out in queue after 1 hour"
                job.save()
                queue_timeout_count += 1
                
                log_task_info("monitor_processing_jobs", "monitor", f"Queue timed out job {job.id}")
                
            except Exception as e:
                logger.error(f"Failed to timeout queued job {job.id}: {e}")
        
        # Get processing statistics
        processing_stats = {
            'pending': FileProcessingJob.objects.filter(status='PENDING').count(),
            'queued': FileProcessingJob.objects.filter(status='QUEUED').count(),
            'processing': FileProcessingJob.objects.filter(status='PROCESSING').count(),
            'completed': FileProcessingJob.objects.filter(status='COMPLETED').count(),
            'failed': FileProcessingJob.objects.filter(status='FAILED').count(),
        }
        
        result = {
            "status": "success",
            "message": f"Monitoring completed - {timeout_count} timeouts, {queue_timeout_count} queue timeouts",
            "timeouts": timeout_count,
            "queue_timeouts": queue_timeout_count,
            "processing_stats": processing_stats
        }
        
        log_task_info("monitor_processing_jobs", "monitor", f"Processing jobs monitoring completed: {result}")
        
        return result
        
    except Exception as e:
        logger.error(f"Monitor processing jobs failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}


@shared_task(bind=True, name='core.tasks.worker_health_check')
def worker_health_check(self):
    """
    Perform worker health check
    """
    try:
        log_task_info("worker_health_check", "monitor", "Starting worker health check")
        
        # Get system information
        system_info = get_system_info()
        
        # Check database connectivity
        try:
            FileProcessingJob.objects.count()
            db_status = "healthy"
            db_error = None
        except Exception as e:
            db_status = "unhealthy"
            db_error = str(e)
        
        # Check file system access
        try:
            test_file = "/tmp/health_check_test"
            with open(test_file, "w") as f:
                f.write("health check")
            os.remove(test_file)
            fs_status = "healthy"
            fs_error = None
        except Exception as e:
            fs_status = "unhealthy"
            fs_error = str(e)
        
        # Check memory usage
        memory_percent = system_info.get('memory_percent', 0)
        if memory_percent > 90:
            memory_status = "critical"
        elif memory_percent > 80:
            memory_status = "warning"
        else:
            memory_status = "healthy"
        
        # Check CPU usage
        cpu_percent = system_info.get('cpu_percent', 0)
        if cpu_percent > 90:
            cpu_status = "critical"
        elif cpu_percent > 80:
            cpu_status = "warning"
        else:
            cpu_status = "healthy"
        
        # Overall health status
        if db_status == "unhealthy" or fs_status == "unhealthy":
            overall_status = "unhealthy"
        elif memory_status == "critical" or cpu_status == "critical":
            overall_status = "critical"
        elif memory_status == "warning" or cpu_status == "warning":
            overall_status = "warning"
        else:
            overall_status = "healthy"
        
        result = {
            "status": "success",
            "message": f"Worker health check completed - Status: {overall_status}",
            "overall_status": overall_status,
            "database_status": db_status,
            "database_error": db_error,
            "filesystem_status": fs_status,
            "filesystem_error": fs_error,
            "memory_status": memory_status,
            "memory_percent": memory_percent,
            "cpu_status": cpu_status,
            "cpu_percent": cpu_percent,
            "system_info": system_info,
            "worker_id": self.request.id,
            "timestamp": datetime.now().isoformat()
        }
        
        # Send notification if unhealthy
        if overall_status in ["unhealthy", "critical"]:
            send_notification_if_available(
                notify_system_status,
                "system",
                overall_status,
                result
            )
        
        log_task_info("worker_health_check", "monitor", f"Worker health check completed: {overall_status}")
        
        return result
        
    except Exception as e:
        logger.error(f"Worker health check failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}


@shared_task(bind=True, name='core.tasks.monitor_worker_performance')
def monitor_worker_performance(self):
    """
    Monitor worker performance and resource usage
    """
    try:
        log_task_info("monitor_worker_performance", "monitor", "Starting worker performance monitoring")
        
        # Get system information
        system_info = get_system_info()
        
        # Get recent job statistics
        recent_jobs = FileProcessingJob.objects.filter(
            created_at__gte=timezone.now() - timedelta(hours=1)
        )
        
        job_stats = {
            'total_jobs': recent_jobs.count(),
            'completed_jobs': recent_jobs.filter(status='COMPLETED').count(),
            'failed_jobs': recent_jobs.filter(status='FAILED').count(),
            'processing_jobs': recent_jobs.filter(status='PROCESSING').count(),
            'pending_jobs': recent_jobs.filter(status='PENDING').count(),
        }
        
        # Calculate success rate
        total_processed = job_stats['completed_jobs'] + job_stats['failed_jobs']
        success_rate = (job_stats['completed_jobs'] / total_processed * 100) if total_processed > 0 else 0
        
        # Get average processing time
        completed_jobs = recent_jobs.filter(
            status='COMPLETED',
            processing_duration__isnull=False
        )
        
        if completed_jobs.exists():
            avg_processing_time = completed_jobs.aggregate(
                avg_time=Avg('processing_duration')
            )['avg_time'] or 0
        else:
            avg_processing_time = 0
        
        # Performance metrics
        performance_metrics = {
            'success_rate': success_rate,
            'average_processing_time': avg_processing_time,
            'memory_usage_percent': system_info.get('memory_percent', 0),
            'cpu_usage_percent': system_info.get('cpu_percent', 0),
            'available_memory_gb': system_info.get('available_memory_gb', 0),
            'process_count': system_info.get('process_count', 0)
        }
        
        # Determine performance status
        if success_rate < 70:
            performance_status = "poor"
        elif success_rate < 90:
            performance_status = "fair"
        else:
            performance_status = "good"
        
        # Check for performance issues
        issues = []
        if success_rate < 70:
            issues.append("Low success rate")
        if avg_processing_time > 300:  # More than 5 minutes
            issues.append("High average processing time")
        if system_info.get('memory_percent', 0) > 90:
            issues.append("High memory usage")
        if system_info.get('cpu_percent', 0) > 90:
            issues.append("High CPU usage")
        
        result = {
            "status": "success",
            "message": f"Worker performance monitoring completed - Status: {performance_status}",
            "performance_status": performance_status,
            "job_stats": job_stats,
            "performance_metrics": performance_metrics,
            "issues": issues,
            "worker_id": self.request.id,
            "timestamp": datetime.now().isoformat()
        }
        
        # Send notification if performance is poor
        if performance_status == "poor" or issues:
            send_notification_if_available(
                notify_system_status,
                "performance",
                performance_status,
                result
            )
        
        log_task_info("monitor_worker_performance", "monitor", f"Worker performance monitoring completed: {performance_status}")
        
        return result
        
    except Exception as e:
        logger.error(f"Monitor worker performance failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}
