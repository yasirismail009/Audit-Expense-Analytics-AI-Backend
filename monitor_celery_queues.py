#!/usr/bin/env python
"""
Celery Queue Monitoring Script

This script continuously monitors Celery queues to ensure they are being
processed and workers are listening for tasks.
"""

import os
import sys
import time
import json
import logging
from datetime import datetime, timedelta
from collections import defaultdict

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Configure Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('celery_monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CeleryQueueMonitor:
    """Monitors Celery queues and worker status"""
    
    def __init__(self):
        self.stats = defaultdict(int)
        self.last_check = datetime.now()
        self.check_interval = 30  # seconds
        self.alert_threshold = 5  # consecutive failures before alert
        
    def check_redis_connection(self):
        """Check Redis connection"""
        try:
            import redis
            from django.conf import settings
            
            redis_url = getattr(settings, 'CELERY_BROKER_URL', 'redis://localhost:6379/0')
            r = redis.from_url(redis_url, socket_timeout=5)
            r.ping()
            return True, "Redis connection successful"
        except Exception as e:
            return False, f"Redis connection failed: {e}"
    
    def check_celery_workers(self):
        """Check if Celery workers are running and responsive"""
        try:
            from celery import current_app
            from celery.task.control import inspect
            
            i = inspect()
            stats = i.stats()
            
            if not stats:
                return False, "No workers responding"
            
            # Check worker details
            worker_count = len(stats)
            active_workers = []
            
            for worker_name, worker_stats in stats.items():
                if worker_stats.get('pool', {}).get('processes'):
                    active_workers.append(worker_name)
            
            if not active_workers:
                return False, "No active worker processes"
            
            return True, f"Found {worker_count} workers, {len(active_workers)} active"
            
        except Exception as e:
            return False, f"Error checking workers: {e}"
    
    def check_queue_status(self):
        """Check queue status and task counts"""
        try:
            from celery import current_app
            from celery.task.control import inspect
            
            i = inspect()
            
            # Get active queues
            active_queues = i.active_queues()
            if not active_queues:
                return False, "No active queues found"
            
            # Get reserved tasks (waiting to be processed)
            reserved_tasks = i.reserved()
            if not reserved_tasks:
                reserved_tasks = {}
            
            # Get active tasks (currently being processed)
            active_tasks = i.active()
            if not active_tasks:
                active_tasks = {}
            
            # Calculate total tasks
            total_reserved = sum(len(tasks) for tasks in reserved_tasks.values())
            total_active = sum(len(tasks) for tasks in active_tasks.values())
            
            queue_info = {
                'active_queues': len(active_queues),
                'total_reserved': total_reserved,
                'total_active': total_active,
                'queues': {}
            }
            
            # Check each queue
            for worker_name, queues in active_queues.items():
                for queue in queues:
                    queue_name = queue.get('name', 'unknown')
                    if queue_name not in queue_info['queues']:
                        queue_info['queues'][queue_name] = {
                            'workers': set(),
                            'reserved': 0,
                            'active': 0
                        }
                    
                    queue_info['queues'][queue_name]['workers'].add(worker_name)
            
            # Add task counts for each queue
            for worker_name, tasks in reserved_tasks.items():
                for task in tasks:
                    queue_name = task.get('delivery_info', {}).get('routing_key', 'default')
                    if queue_name in queue_info['queues']:
                        queue_info['queues'][queue_name]['reserved'] += 1
            
            for worker_name, tasks in active_tasks.items():
                for task in tasks:
                    queue_name = task.get('delivery_info', {}).get('routing_key', 'default')
                    if queue_name in queue_info['queues']:
                        queue_info['queues'][queue_name]['active'] += 1
            
            # Convert sets to lists for JSON serialization
            for queue_name in queue_info['queues']:
                queue_info['queues'][queue_name]['workers'] = list(queue_info['queues'][queue_name]['workers'])
            
            return True, queue_info
            
        except Exception as e:
            return False, f"Error checking queue status: {e}"
    
    def check_task_processing(self):
        """Check if tasks are being processed"""
        try:
            from celery import current_app
            from celery.task.control import inspect
            
            i = inspect()
            
            # Check if there are any active tasks
            active_tasks = i.active()
            if not active_tasks:
                return True, "No active tasks (normal if no work to do)"
            
            # Check task details
            task_types = defaultdict(int)
            total_tasks = 0
            
            for worker_name, tasks in active_tasks.items():
                for task in tasks:
                    task_name = task.get('name', 'unknown')
                    task_types[task_name] += 1
                    total_tasks += 1
            
            return True, {
                'total_active_tasks': total_tasks,
                'task_distribution': dict(task_types)
            }
            
        except Exception as e:
            return False, f"Error checking task processing: {e}"
    
    def check_scheduled_tasks(self):
        """Check scheduled tasks from Celery Beat"""
        try:
            from celery import current_app
            from celery.task.control import inspect
            
            i = inspect()
            
            # Get scheduled tasks
            scheduled = i.scheduled()
            if not scheduled:
                return True, "No scheduled tasks"
            
            total_scheduled = sum(len(tasks) for tasks in scheduled.values())
            
            return True, {
                'total_scheduled': total_scheduled,
                'scheduled_by_worker': {worker: len(tasks) for worker, tasks in scheduled.items()}
            }
            
        except Exception as e:
            return False, f"Error checking scheduled tasks: {e}"
    
    def run_health_check(self):
        """Run comprehensive health check"""
        logger.info("="*60)
        logger.info("CELERY QUEUE HEALTH CHECK")
        logger.info("="*60)
        
        checks = [
            ("Redis Connection", self.check_redis_connection),
            ("Celery Workers", self.check_celery_workers),
            ("Queue Status", self.check_queue_status),
            ("Task Processing", self.check_task_processing),
            ("Scheduled Tasks", self.check_scheduled_tasks),
        ]
        
        results = {}
        overall_status = True
        
        for check_name, check_func in checks:
            try:
                status, message = check_func()
                results[check_name] = {
                    'status': status,
                    'message': message
                }
                
                if status:
                    logger.info(f"✓ {check_name}: {message}")
                else:
                    logger.error(f"✗ {check_name}: {message}")
                    overall_status = False
                    
            except Exception as e:
                logger.error(f"✗ {check_name}: Error during check - {e}")
                results[check_name] = {
                    'status': False,
                    'message': f"Error: {e}"
                }
                overall_status = False
        
        # Update statistics
        if overall_status:
            self.stats['successful_checks'] += 1
            self.stats['consecutive_failures'] = 0
        else:
            self.stats['failed_checks'] += 1
            self.stats['consecutive_failures'] += 1
        
        # Check if we need to alert
        if self.stats['consecutive_failures'] >= self.alert_threshold:
            logger.warning(f"⚠️  ALERT: {self.stats['consecutive_failures']} consecutive failures detected!")
        
        # Log summary
        logger.info("="*60)
        logger.info(f"Overall Status: {'HEALTHY' if overall_status else 'UNHEALTHY'}")
        logger.info(f"Successful Checks: {self.stats['successful_checks']}")
        logger.info(f"Failed Checks: {self.stats['failed_checks']}")
        logger.info(f"Consecutive Failures: {self.stats['consecutive_failures']}")
        logger.info("="*60)
        
        return overall_status, results
    
    def monitor_continuously(self, interval=None):
        """Monitor continuously with specified interval"""
        if interval:
            self.check_interval = interval
        
        logger.info(f"Starting continuous monitoring (check every {self.check_interval} seconds)")
        logger.info("Press Ctrl+C to stop")
        
        try:
            while True:
                self.run_health_check()
                time.sleep(self.check_interval)
                
        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
        except Exception as e:
            logger.error(f"Monitoring error: {e}")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Monitor Celery queues and worker status')
    parser.add_argument('--once', action='store_true', help='Run health check once and exit')
    parser.add_argument('--interval', type=int, default=30, help='Check interval in seconds (default: 30)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    monitor = CeleryQueueMonitor()
    
    if args.once:
        monitor.run_health_check()
    else:
        monitor.monitor_continuously(args.interval)

if __name__ == "__main__":
    main()

