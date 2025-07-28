#!/usr/bin/env python3
"""
Custom Parallel Processing System
Automatically listens for jobs and processes them in parallel using threading
"""

import os
import time
import threading
from queue import Queue, Empty
from datetime import datetime, timedelta
from django.utils import timezone
from django.db import connection
import logging

logger = logging.getLogger(__name__)

class ParallelProcessor:
    """
    Custom parallel processing system that automatically processes jobs using threading
    """
    
    def __init__(self, num_workers=4, poll_interval=5):
        self.num_workers = num_workers
        self.poll_interval = poll_interval
        self.job_queue = Queue()
        self.result_queue = Queue()
        self.workers = []
        self.scheduler_thread = None
        self.result_thread = None
        self.is_running = False
        self.stats = {
            'jobs_processed': 0,
            'jobs_failed': 0,
            'start_time': None,
            'last_job_time': None
        }
    
    def start(self):
        """Start the parallel processor"""
        if self.is_running:
            logger.warning("Parallel processor is already running")
            return
        
        logger.info(f"Starting Parallel Processor with {self.num_workers} workers")
        self.is_running = True
        self.stats['start_time'] = timezone.now()
        
        # Start worker threads
        for i in range(self.num_workers):
            worker = threading.Thread(
                target=self._worker_thread,
                args=(i,),
                name=f"Worker-{i}",
                daemon=True
            )
            worker.start()
            self.workers.append(worker)
            logger.info(f"Started worker {i}")
        
        # Start scheduler thread
        self.scheduler_thread = threading.Thread(
            target=self._scheduler_loop,
            name="JobScheduler",
            daemon=True
        )
        self.scheduler_thread.start()
        logger.info("Started job scheduler")
        
        # Start result processor thread
        self.result_thread = threading.Thread(
            target=self._result_processor_loop,
            name="ResultProcessor",
            daemon=True
        )
        self.result_thread.start()
        logger.info("Started result processor")
    
    def stop(self):
        """Stop the parallel processor"""
        if not self.is_running:
            return
        
        logger.info("Stopping Parallel Processor...")
        self.is_running = False
        
        # Wait for threads to finish
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            self.scheduler_thread.join(timeout=5)
        
        if self.result_thread and self.result_thread.is_alive():
            self.result_thread.join(timeout=5)
        
        logger.info("Parallel Processor stopped")
    
    def _scheduler_loop(self):
        """Main scheduler loop that polls for new jobs"""
        logger.info("Scheduler started - polling for jobs...")
        
        while self.is_running:
            try:
                # Import here to avoid Django setup issues
                os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
                import django
                django.setup()
                
                from core.models import FileProcessingJob
                
                # Find pending jobs
                pending_jobs = FileProcessingJob.objects.filter(
                    status='PENDING'
                ).order_by('created_at')[:10]  # Process up to 10 jobs at a time
                
                for job in pending_jobs:
                    if not self.is_running:
                        break
                    
                    # Update job status to QUEUED
                    job.status = 'QUEUED'
                    job.save()
                    
                    # Add to processing queue
                    self.job_queue.put({
                        'job_id': str(job.id),
                        'data_file_id': str(job.data_file.id),
                        'requested_anomalies': job.requested_anomalies,
                        'run_anomalies': job.run_anomalies,
                        'timestamp': timezone.now()
                    })
                    
                    logger.info(f"Queued job {job.id} for processing")
                
                # Sleep before next poll
                time.sleep(self.poll_interval)
                
            except Exception as e:
                logger.error(f"Error in scheduler: {e}")
                time.sleep(self.poll_interval)
    
    def _worker_thread(self, worker_id):
        """Worker thread that processes jobs"""
        logger.info(f"Worker {worker_id} started")
        
        # Setup Django for this thread
        os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
        import django
        django.setup()
        
        from core.models import FileProcessingJob, SAPGLPosting
        
        while self.is_running:
            try:
                # Get job from queue
                job_data = self.job_queue.get(timeout=1)
                
                logger.info(f"Worker {worker_id} processing job {job_data['job_id']}")
                
                # Process the job
                result = self._process_job(job_data, worker_id)
                
                # Send result back
                self.result_queue.put(result)
                
                logger.info(f"Worker {worker_id} completed job {job_data['job_id']}")
                
            except Empty:
                # No jobs in queue, continue
                continue
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                # Send error result
                if 'job_data' in locals():
                    self.result_queue.put({
                        'job_id': job_data['job_id'],
                        'status': 'FAILED',
                        'error': str(e),
                        'worker_id': worker_id
                    })
    
    def _process_job(self, job_data, worker_id):
        """Process a single job with full analysis pipeline"""
        
        # Setup Django
        os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
        import django
        django.setup()
        
        from core.models import FileProcessingJob, SAPGLPosting
        
        job_id = job_data['job_id']
        start_time = timezone.now()
        
        try:
            # Get the job
            job = FileProcessingJob.objects.get(id=job_id)
            
            # Update status to PROCESSING
            job.status = 'PROCESSING'
            job.started_at = start_time
            job.save()
            
            # Get transactions
            transactions = SAPGLPosting.objects.filter(data_file=job.data_file)
            
            # Import synchronous analysis functions
            from core.sync_analysis import (
                run_general_analysis_sync, run_duplicate_analysis_sync,
                run_backdated_analysis_sync, run_overall_analysis_sync,
                run_risk_analysis_sync
            )
            
            # Run all analysis tasks synchronously
            analysis_results = {}
            
            # 1. General Analysis
            try:
                general_result = run_general_analysis_sync(job_id)
                analysis_results['general_analysis'] = general_result
                logger.info(f"General Analysis completed: {general_result.get('status', 'UNKNOWN')}")
            except Exception as e:
                analysis_results['general_analysis'] = {'error': str(e)}
                logger.error(f"General Analysis failed: {e}")
            
            # 2. Duplicate Analysis
            try:
                duplicate_result = run_duplicate_analysis_sync(job_id)
                analysis_results['duplicate_analysis'] = duplicate_result
                logger.info(f"Duplicate Analysis completed: {duplicate_result.get('status', 'UNKNOWN')}")
            except Exception as e:
                analysis_results['duplicate_analysis'] = {'error': str(e)}
                logger.error(f"Duplicate Analysis failed: {e}")
            
            # 3. Backdated Analysis
            try:
                backdated_result = run_backdated_analysis_sync(job_id)
                analysis_results['backdated_analysis'] = backdated_result
                logger.info(f"Backdated Analysis completed: {backdated_result.get('status', 'UNKNOWN')}")
            except Exception as e:
                analysis_results['backdated_analysis'] = {'error': str(e)}
                logger.error(f"Backdated Analysis failed: {e}")
            
            # 4. Overall Analysis
            try:
                overall_result = run_overall_analysis_sync(job_id)
                analysis_results['overall_analysis'] = overall_result
                logger.info(f"Overall Analysis completed: {overall_result.get('status', 'UNKNOWN')}")
            except Exception as e:
                analysis_results['overall_analysis'] = {'error': str(e)}
                logger.error(f"Overall Analysis failed: {e}")
            
            # 5. Risk Analysis
            try:
                risk_result = run_risk_analysis_sync(job_id)
                analysis_results['risk_analysis'] = risk_result
                logger.info(f"Risk Analysis completed: {risk_result.get('status', 'UNKNOWN')}")
            except Exception as e:
                analysis_results['risk_analysis'] = {'error': str(e)}
                logger.error(f"Risk Analysis failed: {e}")
            
            # Calculate basic summary
            total_amount = sum(t.amount_local_currency for t in transactions)
            unique_users = len(set(t.user_name for t in transactions))
            unique_accounts = len(set(t.gl_account for t in transactions))
            
            # Update job with results
            job.status = 'COMPLETED'
            job.completed_at = timezone.now()
            job.processing_duration = (job.completed_at - start_time).total_seconds()
            job.analytics_results = {
                'summary': {
                    'total_transactions': len(transactions),
                    'total_amount': float(total_amount),
                    'unique_users': unique_users,
                    'unique_accounts': unique_accounts,
                    'processed_parallel': True,
                    'worker_id': worker_id,
                    'analysis_results': analysis_results
                }
            }
            job.save()
            
            return {
                'job_id': job_id,
                'status': 'COMPLETED',
                'worker_id': worker_id,
                'processing_time': job.processing_duration,
                'analysis_results': analysis_results
            }
            
        except Exception as e:
            # Update job with error
            try:
                job = FileProcessingJob.objects.get(id=job_id)
                job.status = 'FAILED'
                job.error_message = str(e)
                job.completed_at = timezone.now()
                job.save()
            except:
                pass
            
            return {
                'job_id': job_id,
                'status': 'FAILED',
                'error': str(e),
                'worker_id': worker_id
            }
    
    def _result_processor_loop(self):
        """Process results from workers"""
        logger.info("Result processor started")
        
        while self.is_running:
            try:
                # Get result from queue
                result = self.result_queue.get(timeout=1)
                
                # Update stats
                if result['status'] == 'COMPLETED':
                    self.stats['jobs_processed'] += 1
                else:
                    self.stats['jobs_failed'] += 1
                
                self.stats['last_job_time'] = timezone.now()
                
                logger.info(f"Job {result['job_id']} {result['status']} by worker {result.get('worker_id', 'unknown')}")
                
            except Empty:
                # No results in queue, continue
                continue
            except Exception as e:
                logger.error(f"Error in result processor: {e}")
    
    def get_stats(self):
        """Get processing statistics"""
        uptime = None
        if self.stats['start_time']:
            uptime = timezone.now() - self.stats['start_time']
        
        return {
            'is_running': self.is_running,
            'num_workers': self.num_workers,
            'jobs_processed': self.stats['jobs_processed'],
            'jobs_failed': self.stats['jobs_failed'],
            'uptime': uptime,
            'last_job_time': self.stats['last_job_time']
        }

# Global processor instance
_processor = None

def get_processor():
    """Get the global processor instance"""
    global _processor
    if _processor is None:
        _processor = ParallelProcessor()
    return _processor

def start_processor(num_workers=4):
    """Start the parallel processor"""
    processor = get_processor()
    processor.num_workers = num_workers
    processor.start()
    return processor

def stop_processor():
    """Stop the parallel processor"""
    global _processor
    if _processor:
        _processor.stop()
        _processor = None

def get_processor_stats():
    """Get processor statistics"""
    if _processor:
        return _processor.get_stats()
    return {'is_running': False} 