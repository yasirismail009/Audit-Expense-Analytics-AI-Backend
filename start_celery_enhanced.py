#!/usr/bin/env python
"""
Enhanced Celery Startup Script

This script ensures Celery worker is always listening for tasks and automatically
restarts if it stops responding or loses connection to queues.
"""

import os
import sys
import subprocess
import time
import signal
import psutil
import logging
import json
from pathlib import Path
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('celery_worker.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CeleryManager:
    """Manages Celery worker lifecycle and monitoring"""
    
    def __init__(self):
        self.worker_process = None
        self.beat_process = None
        self.redis_process = None
        self.running = True
        self.restart_count = 0
        self.max_restarts = 10
        self.last_health_check = datetime.now()
        self.health_check_interval = 30  # seconds
        
    def check_redis(self):
        """Check if Redis is running and accessible"""
        try:
            import redis
            r = redis.Redis(host='localhost', port=6379, db=0, socket_timeout=5)
            r.ping()
            logger.info("✓ Redis is running and accessible")
            return True
        except Exception as e:
            logger.error(f"✗ Redis is not accessible: {e}")
            return False
    
    def start_redis(self):
        """Start Redis if not running"""
        if not self.check_redis():
            logger.info("Starting Redis...")
            try:
                # Try to start Redis (Windows)
                subprocess.run(['redis-server'], check=False, capture_output=True)
                time.sleep(3)
                if self.check_redis():
                    logger.info("✓ Redis started successfully")
                    return True
            except FileNotFoundError:
                logger.error("Redis not found. Please install Redis or start it manually.")
                return False
        return True
    
    def check_celery_worker(self):
        """Check if Celery worker is already running and responsive"""
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if proc.info['name'] and 'celery' in proc.info['name'].lower():
                    cmdline = ' '.join(proc.info['cmdline'] or [])
                    if 'worker' in cmdline and 'analytics' in cmdline:
                        # Check if worker is responsive
                        if self.is_worker_responsive(proc.info['pid']):
                            logger.info(f"✓ Celery worker already running and responsive (PID: {proc.info['pid']})")
                            return proc.info['pid']
                        else:
                            logger.warning(f"Celery worker found but not responsive (PID: {proc.info['pid']})")
                            # Kill unresponsive worker
                            try:
                                proc.terminate()
                                proc.wait(timeout=5)
                            except:
                                proc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return None
    
    def is_worker_responsive(self, pid):
        """Check if Celery worker is responsive to commands"""
        try:
            result = subprocess.run(
                ['celery', '-A', 'analytics', 'inspect', 'ping'],
                capture_output=True,
                text=True,
                timeout=10
            )
            return result.returncode == 0 and 'pong' in result.stdout
        except Exception:
            return False
    
    def start_celery_worker(self):
        """Start Celery worker with enhanced configuration"""
        logger.info("Starting Celery worker...")
        
        # Set Django settings
        os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
        
        # Enhanced worker command with reliability options
        cmd = [
            sys.executable, '-m', 'celery', '-A', 'analytics', 'worker',
            '--loglevel=info',
            '--concurrency=4',
            '--pool=prefork',
            '--hostname=worker1@%h',
            '--queues=default,analytics,ml_training,maintenance',
            '--without-gossip',
            '--without-mingle',
            '--without-heartbeat',
            '--max-tasks-per-child=1000',
            '--prefetch-multiplier=1',
            '--broker-connection-retry-on-startup',
            '--broker-connection-max-retries=20'
        ]
        
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            # Wait a bit to see if it starts successfully
            time.sleep(5)
            if process.poll() is None:
                logger.info(f"✓ Celery worker started successfully (PID: {process.pid})")
                return process
            else:
                logger.error("Celery worker failed to start")
                return None
                
        except Exception as e:
            logger.error(f"✗ Failed to start Celery worker: {e}")
            return None
    
    def start_celery_beat(self):
        """Start Celery beat scheduler"""
        logger.info("Starting Celery beat...")
        
        os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
        
        cmd = [
            sys.executable, '-m', 'celery', '-A', 'analytics', 'beat',
            '--loglevel=info',
            '--scheduler=django_celery_beat.schedulers:DatabaseScheduler'
        ]
        
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            time.sleep(3)
            if process.poll() is None:
                logger.info(f"✓ Celery beat started successfully (PID: {process.pid})")
                return process
            else:
                logger.error("Celery beat failed to start")
                return None
                
        except Exception as e:
            logger.error(f"✗ Failed to start Celery beat: {e}")
            return None
    
    def check_queue_health(self):
        """Check if queues are healthy and being processed"""
        try:
            result = subprocess.run(
                ['celery', '-A', 'analytics', 'inspect', 'active_queues'],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                logger.info("✓ Queue health check passed")
                return True
            else:
                logger.warning("Queue health check failed")
                return False
                
        except Exception as e:
            logger.error(f"Error checking queue health: {e}")
            return False
    
    def monitor_celery(self):
        """Monitor Celery worker and restart if needed"""
        logger.info("Monitoring Celery worker...")
        
        while self.running:
            try:
                # Check if worker process is still running
                if self.worker_process and self.worker_process.poll() is not None:
                    logger.warning("Celery worker stopped. Restarting...")
                    self.restart_worker()
                    continue
                
                # Check if beat process is still running
                if self.beat_process and self.beat_process.poll() is not None:
                    logger.warning("Celery beat stopped. Restarting...")
                    self.restart_beat()
                    continue
                
                # Periodic health checks
                if (datetime.now() - self.last_health_check).seconds >= self.health_check_interval:
                    if not self.check_queue_health():
                        logger.warning("Queue health check failed. Restarting worker...")
                        self.restart_worker()
                    
                    self.last_health_check = datetime.now()
                
                # Check if worker is responsive
                if self.worker_process and not self.is_worker_responsive(self.worker_process.pid):
                    logger.warning("Celery worker not responding. Restarting...")
                    self.restart_worker()
                
                time.sleep(10)  # Check every 10 seconds
                
            except KeyboardInterrupt:
                logger.info("Received interrupt signal")
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                time.sleep(10)
    
    def restart_worker(self):
        """Restart Celery worker"""
        if self.restart_count >= self.max_restarts:
            logger.error(f"Maximum restart attempts ({self.max_restarts}) reached. Stopping.")
            self.running = False
            return
        
        logger.info(f"Restarting Celery worker (attempt {self.restart_count + 1}/{self.max_restarts})")
        
        # Stop current worker
        if self.worker_process:
            try:
                self.worker_process.terminate()
                self.worker_process.wait(timeout=10)
            except:
                self.worker_process.kill()
        
        # Wait a bit before restarting
        time.sleep(5)
        
        # Start new worker
        self.worker_process = self.start_celery_worker()
        if self.worker_process:
            self.restart_count += 1
            logger.info("Worker restarted successfully")
        else:
            logger.error("Failed to restart worker")
    
    def restart_beat(self):
        """Restart Celery beat"""
        logger.info("Restarting Celery beat...")
        
        # Stop current beat
        if self.beat_process:
            try:
                self.beat_process.terminate()
                self.beat_process.wait(timeout=10)
            except:
                self.beat_process.kill()
        
        # Wait a bit before restarting
        time.sleep(3)
        
        # Start new beat
        self.beat_process = self.start_celery_beat()
        if self.beat_process:
            logger.info("Beat restarted successfully")
        else:
            logger.error("Failed to restart beat")
    
    def cleanup(self):
        """Clean up processes on shutdown"""
        logger.info("Cleaning up processes...")
        
        if self.worker_process:
            try:
                self.worker_process.terminate()
                self.worker_process.wait(timeout=10)
            except:
                self.worker_process.kill()
        
        if self.beat_process:
            try:
                self.beat_process.terminate()
                self.beat_process.wait(timeout=10)
            except:
                self.beat_process.kill()
        
        logger.info("Cleanup completed")
    
    def run(self):
        """Main run method"""
        logger.info("="*60)
        logger.info("ENHANCED CELERY WORKER MANAGER")
        logger.info("="*60)
        
        # Check if already running
        existing_pid = self.check_celery_worker()
        if existing_pid:
            logger.info(f"Celery worker already running with PID: {existing_pid}")
            return
        
        # Start Redis
        if not self.start_redis():
            logger.error("Cannot start Celery without Redis")
            return
        
        # Start Celery worker
        self.worker_process = self.start_celery_worker()
        if not self.worker_process:
            logger.error("Failed to start Celery worker")
            return
        
        # Start Celery beat
        self.beat_process = self.start_celery_beat()
        if not self.beat_process:
            logger.warning("Failed to start Celery beat, continuing with worker only")
        
        # Monitor and restart if needed
        try:
            self.monitor_celery()
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        finally:
            self.cleanup()

def main():
    """Main function"""
    manager = CeleryManager()
    
    # Set up signal handlers
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}")
        manager.running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        manager.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        manager.cleanup()
        sys.exit(1)

if __name__ == "__main__":
    main()

