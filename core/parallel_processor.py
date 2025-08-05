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
        """Process a single job with rule-based analysis pipeline"""
        
        # Setup Django
        os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
        import django
        django.setup()
        
        from core.models import FileProcessingJob, SAPGLPosting
        from core.tasks import (
            run_general_analysis, run_duplicate_analysis, run_backdated_analysis,
            run_user_analysis, run_unusual_days_analysis, run_closing_entries_analysis,
            run_holiday_analysis, run_overall_analysis, run_risk_analysis,
            train_rule_based_models,
            train_duplicate_analysis_model, train_backdated_analysis_model,
            train_user_analysis_model, train_unusual_days_analysis_model,
            train_closing_entries_analysis_model, train_holiday_analysis_model,
            train_overall_risk_analysis_model
        )
        
        job_id = job_data['job_id']
        start_time = timezone.now()
        
        try:
            # Get the job
            job = FileProcessingJob.objects.get(id=job_id)
            
            # Update status to PROCESSING
            job.status = 'PROCESSING'
            job.started_at = start_time
            job.save()
            
            # Get transactions for this file
            transactions = list(SAPGLPosting.objects.filter(data_file=job.data_file))
            
            logger.info(f"Worker {worker_id}: Processing {len(transactions)} transactions")
            
            # Run rule-based analysis pipeline
            analysis_results = {}
            
            # PHASE 1: Individual Analyses (can run in parallel)
            try:
                logger.info(f"Worker {worker_id}: Starting Phase 1 - Individual Analyses")
                
                # 1. General Analysis
                try:
                    logger.info(f"Worker {worker_id}: Starting General Analysis")
                    general_result = run_general_analysis.apply_async(args=[job_id])
                    analysis_results['general_analysis'] = {
                        'status': 'COMPLETED',
                        'task_id': str(general_result.id),
                        'processing_duration': 0
                    }
                    logger.info(f"Worker {worker_id}: General Analysis completed")
            except Exception as e:
                    logger.error(f"Worker {worker_id}: General Analysis failed: {e}")
                analysis_results['general_analysis'] = {'error': str(e)}
            
            # 2. Duplicate Analysis
            try:
                logger.info(f"Worker {worker_id}: Starting Duplicate Analysis")
                    duplicate_result = run_duplicate_analysis.apply_async(args=[job_id])
                    analysis_results['duplicate_analysis'] = {
                        'status': 'COMPLETED',
                        'task_id': str(duplicate_result.id),
                        'processing_duration': 0
                    }
                    logger.info(f"Worker {worker_id}: Duplicate Analysis completed")
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Duplicate Analysis failed: {e}")
                    analysis_results['duplicate_analysis'] = {'error': str(e)}
            
            # 3. Backdated Analysis
            try:
                logger.info(f"Worker {worker_id}: Starting Backdated Analysis")
                    backdated_result = run_backdated_analysis.apply_async(args=[job_id])
                analysis_results['backdated_analysis'] = {
                    'status': 'COMPLETED',
                        'task_id': str(backdated_result.id),
                        'processing_duration': 0
                    }
                    logger.info(f"Worker {worker_id}: Backdated Analysis completed")
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Backdated Analysis failed: {e}")
                    analysis_results['backdated_analysis'] = {'error': str(e)}
            
            # 4. User Analysis
            try:
                logger.info(f"Worker {worker_id}: Starting User Analysis")
                    user_result = run_user_analysis.apply_async(args=[job_id])
                analysis_results['user_analysis'] = {
                    'status': 'COMPLETED',
                        'task_id': str(user_result.id),
                        'processing_duration': 0
                    }
                    logger.info(f"Worker {worker_id}: User Analysis completed")
                except Exception as e:
                    logger.error(f"Worker {worker_id}: User Analysis failed: {e}")
                    analysis_results['user_analysis'] = {'error': str(e)}
            
            # 5. Unusual Days Analysis
            try:
                logger.info(f"Worker {worker_id}: Starting Unusual Days Analysis")
                    unusual_days_result = run_unusual_days_analysis.apply_async(args=[job_id])
                analysis_results['unusual_days_analysis'] = {
                    'status': 'COMPLETED',
                        'task_id': str(unusual_days_result.id),
                        'processing_duration': 0
                    }
                    logger.info(f"Worker {worker_id}: Unusual Days Analysis completed")
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Unusual Days Analysis failed: {e}")
                    analysis_results['unusual_days_analysis'] = {'error': str(e)}
                
                # 6. Closing Entries Analysis
                try:
                    logger.info(f"Worker {worker_id}: Starting Closing Entries Analysis")
                    closing_entries_result = run_closing_entries_analysis.apply_async(args=[job_id])
                    analysis_results['closing_entries_analysis'] = {
                        'status': 'COMPLETED',
                        'task_id': str(closing_entries_result.id),
                        'processing_duration': 0
                    }
                    logger.info(f"Worker {worker_id}: Closing Entries Analysis completed")
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Closing Entries Analysis failed: {e}")
                    analysis_results['closing_entries_analysis'] = {'error': str(e)}
                
                # 7. Holiday Analysis
            try:
                logger.info(f"Worker {worker_id}: Starting Holiday Analysis")
                    holiday_result = run_holiday_analysis.apply_async(args=[job_id])
                analysis_results['holiday_analysis'] = {
                    'status': 'COMPLETED',
                        'task_id': str(holiday_result.id),
                        'processing_duration': 0
                    }
                    logger.info(f"Worker {worker_id}: Holiday Analysis completed")
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Holiday Analysis failed: {e}")
                    analysis_results['holiday_analysis'] = {'error': str(e)}
                
                logger.info(f"Worker {worker_id}: Phase 1 - Individual Analyses completed")
                
            except Exception as e:
                logger.error(f"Worker {worker_id}: Phase 1 failed: {e}")
                analysis_results['phase_1_error'] = str(e)
        
            # PHASE 2: Dependent Analyses (Overall and Risk - depend on individual analyses)
            try:
                logger.info(f"Worker {worker_id}: Starting Phase 2 - Dependent Analyses")
                
                # 8. Overall Analysis (depends on individual analyses)
                try:
                    logger.info(f"Worker {worker_id}: Starting Overall Analysis")
                    overall_result = run_overall_analysis.apply_async(args=[job_id])
                analysis_results['overall_analysis'] = {
                    'status': 'COMPLETED',
                        'task_id': str(overall_result.id),
                        'processing_duration': 0
                    }
                    logger.info(f"Worker {worker_id}: Overall Analysis completed")
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Overall Analysis failed: {e}")
                    analysis_results['overall_analysis'] = {'error': str(e)}
                
                # 9. Risk Analysis (depends on all individual analyses)
                try:
                    logger.info(f"Worker {worker_id}: Starting Risk Analysis")
                    risk_result = run_risk_analysis.apply_async(args=[job_id])
                    analysis_results['risk_analysis'] = {
                        'status': 'COMPLETED',
                        'task_id': str(risk_result.id),
                        'processing_duration': 0
                    }
                    logger.info(f"Worker {worker_id}: Risk Analysis completed")
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Risk Analysis failed: {e}")
                    analysis_results['risk_analysis'] = {'error': str(e)}
                
                logger.info(f"Worker {worker_id}: Phase 2 - Dependent Analyses completed")
                
            except Exception as e:
                logger.error(f"Worker {worker_id}: Phase 2 failed: {e}")
                analysis_results['phase_2_error'] = str(e)
        
            # PHASE 3: Model Training (can run in parallel)
            try:
                logger.info(f"Worker {worker_id}: Starting Phase 3 - Model Training")
                
                # 10. Rule-Based Model Training
                try:
                    logger.info(f"Worker {worker_id}: Starting Rule-Based Model Training")
                    rule_training_result = train_rule_based_models.apply_async(args=[job_id])
                    analysis_results['rule_training'] = {
                    'status': 'COMPLETED',
                        'task_id': str(rule_training_result.id),
                        'processing_duration': 0
                    }
                    logger.info(f"Worker {worker_id}: Rule-Based Model Training completed")
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Rule-Based Model Training failed: {e}")
                    analysis_results['rule_training'] = {'error': str(e)}
                
                # 11. Individual Model Training Tasks
                try:
                    logger.info(f"Worker {worker_id}: Starting Individual Model Training")
                    
                    # Train duplicate analysis model
                    duplicate_model_result = train_duplicate_analysis_model.apply_async(args=[job_id])
                    analysis_results['duplicate_model_training'] = {
                        'status': 'COMPLETED',
                        'task_id': str(duplicate_model_result.id),
                        'processing_duration': 0
                    }
                    
                    # Train backdated analysis model
                    backdated_model_result = train_backdated_analysis_model.apply_async(args=[job_id])
                    analysis_results['backdated_model_training'] = {
                        'status': 'COMPLETED',
                        'task_id': str(backdated_model_result.id),
                        'processing_duration': 0
                    }
                    
                    # Train user analysis model
                    user_model_result = train_user_analysis_model.apply_async(args=[job_id])
                    analysis_results['user_model_training'] = {
                        'status': 'COMPLETED',
                        'task_id': str(user_model_result.id),
                        'processing_duration': 0
                    }
                    
                    # Train unusual days analysis model
                    unusual_days_model_result = train_unusual_days_analysis_model.apply_async(args=[job_id])
                    analysis_results['unusual_days_model_training'] = {
                        'status': 'COMPLETED',
                        'task_id': str(unusual_days_model_result.id),
                        'processing_duration': 0
                    }
                    
                    # Train closing entries analysis model
                    closing_entries_model_result = train_closing_entries_analysis_model.apply_async(args=[job_id])
                    analysis_results['closing_entries_model_training'] = {
                        'status': 'COMPLETED',
                        'task_id': str(closing_entries_model_result.id),
                        'processing_duration': 0
                    }
                    
                    # Train holiday analysis model
                    holiday_model_result = train_holiday_analysis_model.apply_async(args=[job_id])
                    analysis_results['holiday_model_training'] = {
                        'status': 'COMPLETED',
                        'task_id': str(holiday_model_result.id),
                        'processing_duration': 0
                    }
                    
                    # Train overall risk analysis model
                    overall_risk_model_result = train_overall_risk_analysis_model.apply_async(args=[job_id])
                    analysis_results['overall_risk_model_training'] = {
                        'status': 'COMPLETED',
                        'task_id': str(overall_risk_model_result.id),
                        'processing_duration': 0
                    }
                    
                    logger.info(f"Worker {worker_id}: Individual Model Training completed")
                    
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Individual Model Training failed: {e}")
                    analysis_results['individual_model_training_error'] = str(e)
                
                logger.info(f"Worker {worker_id}: Phase 3 - Model Training completed")
                
            except Exception as e:
                logger.error(f"Worker {worker_id}: Phase 3 failed: {e}")
                analysis_results['phase_3_error'] = str(e)
            
            # Calculate total processing duration
            total_duration = (timezone.now() - start_time).total_seconds()
            
            # Update job status
            job.status = 'COMPLETED'
            job.completed_at = timezone.now()
            job.processing_duration = total_duration
            job.analytics_results = {
                'task_ids': {
                    'general_analysis_task_id': analysis_results.get('general_analysis', {}).get('task_id'),
                    'duplicate_analysis_task_id': analysis_results.get('duplicate_analysis', {}).get('task_id'),
                    'backdated_analysis_task_id': analysis_results.get('backdated_analysis', {}).get('task_id'),
                    'user_analysis_task_id': analysis_results.get('user_analysis', {}).get('task_id'),
                    'unusual_days_analysis_task_id': analysis_results.get('unusual_days_analysis', {}).get('task_id'),
                    'closing_entries_analysis_task_id': analysis_results.get('closing_entries_analysis', {}).get('task_id'),
                    'holiday_analysis_task_id': analysis_results.get('holiday_analysis', {}).get('task_id'),
                    'overall_analysis_task_id': analysis_results.get('overall_analysis', {}).get('task_id'),
                    'risk_analysis_task_id': analysis_results.get('risk_analysis', {}).get('task_id'),
                    'rule_training_task_id': analysis_results.get('rule_training', {}).get('task_id'),
                    'duplicate_model_training_task_id': analysis_results.get('duplicate_model_training', {}).get('task_id'),
                    'backdated_model_training_task_id': analysis_results.get('backdated_model_training', {}).get('task_id'),
                    'user_model_training_task_id': analysis_results.get('user_model_training', {}).get('task_id'),
                    'unusual_days_model_training_task_id': analysis_results.get('unusual_days_model_training', {}).get('task_id'),
                    'closing_entries_model_training_task_id': analysis_results.get('closing_entries_model_training', {}).get('task_id'),
                    'holiday_model_training_task_id': analysis_results.get('holiday_model_training', {}).get('task_id'),
                    'overall_risk_model_training_task_id': analysis_results.get('overall_risk_model_training', {}).get('task_id'),
                }
            }
            job.save()
            
            logger.info(f"Worker {worker_id}: Job {job_id} completed in {total_duration:.2f} seconds")
            
            return {
                'job_id': job_id,
                'status': 'COMPLETED',
                'processing_duration': total_duration,
                'analysis_results': analysis_results,
                'worker_id': worker_id
            }
            
        except Exception as e:
            logger.error(f"Worker {worker_id}: Job {job_id} failed: {e}")
            
            # Update job status to failed
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