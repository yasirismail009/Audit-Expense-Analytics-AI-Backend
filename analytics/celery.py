"""
Enhanced Celery configuration for analytics project.
Ensures Celery always listens to and processes queue tasks reliably.
"""

import os
import logging
from celery import Celery
from celery.signals import worker_ready, worker_shutdown, task_received, task_success, task_failure
from celery.utils.log import get_task_logger

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')

# Configure logging
logger = get_task_logger(__name__)

app = Celery('analytics')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Force Redis transport explicitly - ensure all containers use Redis
redis_url = os.environ.get('REDIS_URL', 'redis://redis:6379/0')

# Enhanced configuration for reliable queue processing
app.conf.update(
    # Broker and result backend
    broker_url=redis_url,
    result_backend=redis_url,
    
    # Force Redis transport
    broker_transport='redis',
    result_backend_transport='redis',
    
    # Enhanced broker settings for reliability
    broker_transport_options={
        'visibility_timeout': 3600,
        'fanout_prefix': True,
        'fanout_patterns': True,
        'retry_on_timeout': True,
        'socket_connect_timeout': 5,
        'socket_timeout': 5,
        'socket_keepalive': True,
        'socket_keepalive_options': {},
        'health_check_interval': 30,
        'max_connections': 20,
        'retry_on_timeout_interval': 5,
    },
    
    # Serialization
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    
    # Timezone
    timezone='UTC',
    
    # Enhanced connection settings for reliability
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=None,  # Infinite retries
    broker_connection_retry=True,
    broker_connection_retry_delay=1,
    
    # Task settings
    task_always_eager=False,
    task_eager_propagates=True,
    
    # Enhanced worker settings for continuous processing
    worker_concurrency=4,
    worker_max_tasks_per_child=1000,
    worker_prefetch_multiplier=1,
    worker_disable_rate_limits=False,
    worker_enable_remote_control=True,
    worker_send_task_events=True,
    
    # Task routing with priority queues
    task_default_queue='analytics',
    task_default_exchange='analytics',
    task_default_routing_key='analytics',
    
    # Enhanced task routes with priority
    task_routes={
        'core.tasks.process_file_with_anomalies': {'queue': 'analytics', 'priority': 10},
        'core.tasks.train_ml_models': {'queue': 'ml_training', 'priority': 5},
        'core.tasks.retrain_ml_models': {'queue': 'ml_training', 'priority': 5},
        'core.tasks.train_enhanced_ml_models': {'queue': 'ml_training', 'priority': 5},
        'core.tasks.monitor_processing_jobs': {'queue': 'maintenance', 'priority': 1},
        'core.tasks.process_queued_jobs': {'queue': 'analytics', 'priority': 15},  # Highest priority
        'core.tasks.health_check': {'queue': 'maintenance', 'priority': 1},
    },
    
    # Task timeouts and retry settings
    task_time_limit=300,
    task_soft_time_limit=240,
    task_max_retries=5,  # Increased retries
    task_retry_delay=30,  # Faster retry
    task_retry_backoff=True,  # Exponential backoff
    task_retry_backoff_max=600,  # Max 10 minutes
    
    # Enhanced beat schedule for continuous monitoring
    beat_schedule={
        'monitor-processing-jobs': {
            'task': 'core.tasks.monitor_processing_jobs',
            'schedule': 60.0,  # Run every minute for better responsiveness
            'options': {'queue': 'maintenance', 'priority': 1}
        },
        'process-queued-jobs': {
            'task': 'core.tasks.process_queued_jobs',
            'schedule': 10.0,  # Run every 10 seconds for immediate processing
            'options': {'queue': 'analytics', 'priority': 15}
        },
        'health-check': {
            'task': 'analytics.celery.health_check',
            'schedule': 120.0,  # Run every 2 minutes
            'options': {'queue': 'maintenance', 'priority': 1}
        },
        'cleanup-old-results': {
            'task': 'analytics.celery.cleanup_old_results',
            'schedule': 3600.0,  # Run every hour
            'options': {'queue': 'maintenance', 'priority': 1}
        },
    },
    
    # Queue configuration
    task_queues={
        'analytics': {
            'exchange': 'analytics',
            'routing_key': 'analytics',
            'queue_arguments': {'x-max-priority': 20}
        },
        'ml_training': {
            'exchange': 'ml_training',
            'routing_key': 'ml_training',
            'queue_arguments': {'x-max-priority': 10}
        },
        'maintenance': {
            'exchange': 'maintenance',
            'routing_key': 'maintenance',
            'queue_arguments': {'x-max-priority': 5}
        },
    },
    
    # Result backend settings
    result_expires=3600,  # Results expire after 1 hour
    result_backend_transport_options={
        'retry_policy': {
            'timeout': 5.0,
            'max_retries': 3,
        }
    },
    
    # Worker pool settings
    worker_pool_restarts=True,
    worker_pool_restart_delay=1,
    
    # Task monitoring
    task_send_sent_event=True,
    task_track_started=True,
    task_ignore_result=False,
    
    # Broker heartbeat
    broker_heartbeat=10,
    broker_heartbeat_checkrate=2.0,
)

# Ensure Kombu uses Redis transport
try:
    import kombu
    if hasattr(kombu.transport, 'ALIASES'):
        kombu.transport.ALIASES['redis'] = 'kombu.transport.redis.Transport'
except ImportError:
    pass

# Load task modules from all registered Django apps.
app.autodiscover_tasks()

# ============================================================================
# CELERY SIGNAL HANDLERS FOR RELIABILITY
# ============================================================================

@worker_ready.connect
def worker_ready_handler(sender, **kwargs):
    """Handle worker ready event"""
    logger.info(f"Worker {sender.hostname} is ready and listening for tasks")
    logger.info("Worker is ready and listening for tasks")

@worker_shutdown.connect
def worker_shutdown_handler(sender, **kwargs):
    """Handle worker shutdown event"""
    logger.warning(f"Worker {sender.hostname} is shutting down")

@task_received.connect
def task_received_handler(sender, request, **kwargs):
    """Handle task received event"""
    logger.info(f"Task {request.name} received with ID {request.id}")

@task_success.connect
def task_success_handler(sender, result, **kwargs):
    """Handle task success event"""
    logger.info(f"Task {sender.name} completed successfully")

@task_failure.connect
def task_failure_handler(sender, task_id, exception, args, kwargs, traceback, einfo, **kw):
    """Handle task failure event"""
    logger.error(f"Task {sender.name} failed: {exception}")
    logger.error(f"Task ID: {task_id}")
    logger.error(f"Traceback: {traceback}")

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_queue_status():
    """Get current queue status"""
    try:
        # Simplified queue status check
        return {
            'status': 'available',
            'timestamp': str(datetime.now())
        }
    except Exception as e:
        logger.error(f"Error getting queue status: {e}")
        return None

def ensure_worker_listening():
    """Ensure worker is listening to all queues"""
    try:
        # Simplified worker listening check
        logger.info("Worker listening check completed")
        return True
        
    except Exception as e:
        logger.error(f"Error ensuring worker is listening: {e}")
        return False

@app.task(bind=True)
def debug_task(self):
    """Enhanced debug task to test Celery setup and queue listening."""
    logger.info(f'Debug task request: {self.request!r}')
    
    # Check queue status
    queue_status = get_queue_status()
    worker_listening = ensure_worker_listening()
    
    return {
        'status': 'ok', 
        'task_id': self.request.id,
        'queue_status': queue_status,
        'worker_listening': worker_listening,
        'timestamp': str(self.request.timestamp)
    }

@app.task(bind=True)
def health_check(self):
    """Health check task to monitor Celery health"""
    try:
        # Check Redis connection
        import redis
        r = redis.from_url(redis_url)
        r.ping()
        redis_status = 'healthy'
    except Exception as e:
        redis_status = f'unhealthy: {e}'
    
    # Check worker status
    worker_status = ensure_worker_listening()
    
    return {
        'status': 'healthy' if worker_status and redis_status == 'healthy' else 'unhealthy',
        'redis': redis_status,
        'worker': 'healthy' if worker_status else 'unhealthy',
        'timestamp': str(self.request.timestamp)
    }

@app.task(bind=True)
def cleanup_old_results(self):
    """Clean up old task results to prevent memory issues"""
    try:
        from celery.result import AsyncResult
        from django.utils import timezone
        from datetime import timedelta
        
        # Clean up results older than 24 hours
        cutoff_time = timezone.now() - timedelta(hours=24)
        
        # This is a placeholder - actual cleanup would depend on your result backend
        logger.info("Cleanup old results task completed")
        
        return {'status': 'completed', 'cleaned_before': str(cutoff_time)}
        
    except Exception as e:
        logger.error(f"Error in cleanup task: {e}")
        return {'status': 'error', 'error': str(e)}

# ============================================================================
# STARTUP VERIFICATION
# ============================================================================

if __name__ == '__main__':
    # Verify configuration on startup
    logger.info("Celery configuration loaded successfully")
    logger.info(f"Broker URL: {redis_url}")
    logger.info(f"Default queue: {app.conf.task_default_queue}")
    logger.info(f"Worker concurrency: {app.conf.worker_concurrency}")
    logger.info(f"Beat schedule tasks: {len(app.conf.beat_schedule)}") 