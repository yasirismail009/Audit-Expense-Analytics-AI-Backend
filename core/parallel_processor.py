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
        """Process a single job with full analysis pipeline using MLAnalysisOrchestrator"""
        
        # Setup Django
        os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
        import django
        django.setup()
        
        from core.models import FileProcessingJob, SAPGLPosting
        from core.ml_analysis_orchestrator import MLAnalysisOrchestrator
        
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
            transactions = list(SAPGLPosting.objects.filter(data_file=job.data_file))
            
            # Initialize the ML Analysis Orchestrator
            orchestrator = MLAnalysisOrchestrator()
            
            # Run analysis pipeline with result sharing
            analysis_results = {}
            
            # 1. General Analysis
            try:
                logger.info(f"Worker {worker_id}: Starting General Analysis for {len(transactions)} transactions")
                general_result = orchestrator.run_general_analysis(transactions)
                analysis_results['general_analysis'] = {
                    'status': 'COMPLETED',
                    'result': general_result,
                    'processing_duration': general_result.get('processing_duration', 0)
                }
                
                # Save general analysis results to database
                try:
                    from .models import GeneralAnalysisResult
                    general_analysis_result = GeneralAnalysisResult.objects.create(
                        data_file=job.data_file,
                        processing_job=job,
                        analysis_type='general_analysis',
                        analysis_version='1.0.0',
                        trial_balance_summary=general_result.get('trial_balance_summary', {}),
                        gl_account_summaries=general_result.get('gl_account_summaries', []),
                        user_summaries=general_result.get('user_summaries', []),
                        statistical_calculations=general_result.get('statistical_calculations', {}),
                        chart_data=general_result.get('chart_data', {}),
                        export_data=general_result.get('export_data', []),
                        processing_duration=general_result.get('processing_duration', 0),
                        status='COMPLETED'
                    )
                    logger.info(f"Worker {worker_id}: General analysis saved to database with ID: {general_analysis_result.id}")
                except Exception as db_error:
                    logger.error(f"Worker {worker_id}: Failed to save general analysis to database: {db_error}")
                
                logger.info(f"Worker {worker_id}: General Analysis completed successfully")
            except Exception as e:
                analysis_results['general_analysis'] = {'error': str(e)}
                logger.error(f"Worker {worker_id}: General Analysis failed: {e}")
            
            # 2. Duplicate Analysis
            try:
                logger.info(f"Worker {worker_id}: Starting Duplicate Analysis")
                
                # Add timeout for duplicate analysis to prevent hanging
                import threading
                import queue
                
                result_queue = queue.Queue()
                exception_queue = queue.Queue()
                
                def run_duplicate_analysis():
                    try:
                        result = orchestrator.run_duplicate_analysis(transactions)
                        result_queue.put(result)
                    except Exception as e:
                        exception_queue.put(e)
                
                # Start duplicate analysis in a separate thread
                analysis_thread = threading.Thread(target=run_duplicate_analysis)
                analysis_thread.daemon = True
                analysis_thread.start()
                
                # Wait for result with timeout (5 minutes)
                try:
                    duplicate_result = result_queue.get(timeout=300)
                    analysis_results['duplicate_analysis'] = {
                        'status': 'COMPLETED',
                        'result': duplicate_result,
                        'processing_duration': duplicate_result.get('processing_duration', 0)
                    }
                except queue.Empty:
                    logger.error(f"Worker {worker_id}: Duplicate Analysis timed out after 300 seconds")
                    analysis_results['duplicate_analysis'] = {
                        'status': 'TIMEOUT',
                        'error': 'Duplicate analysis timed out after 300 seconds'
                    }
                    raise TimeoutError("Duplicate analysis timed out after 300 seconds")
                except Exception as e:
                    # Check if there was an exception in the analysis thread
                    try:
                        analysis_exception = exception_queue.get_nowait()
                        raise analysis_exception
                    except queue.Empty:
                        raise e
                
                # Save duplicate analysis results to database
                try:
                    from .models import DuplicateAnalysisResult
                    duplicate_analysis_result = DuplicateAnalysisResult.objects.create(
                        data_file=job.data_file,
                        processing_job=job,
                        analysis_type='comprehensive_duplicate',
                        analysis_version='1.0.0',
                        analysis_info=duplicate_result.get('analysis_info', {
                            'total_transactions': len(transactions),
                            'duplicates_found': duplicate_result.get('duplicates_found', 0),
                            'duplicate_percentage': duplicate_result.get('compliance_assessment', {}).get('duplicate_percentage', 0)
                        }),
                        duplicate_list=duplicate_result.get('duplicate_pairs', []),
                        breakdowns=duplicate_result.get('breakdowns', {
                            'duplicate_by_document': [],
                            'duplicate_by_account': [],
                            'duplicate_by_user': [],
                            'audit_recommendations': duplicate_result.get('audit_recommendations', {}),
                            'compliance_assessment': duplicate_result.get('compliance_assessment', {}),
                            'financial_statement_impact': duplicate_result.get('financial_statement_impact', {}),
                            'risk_distribution': duplicate_result.get('breakdowns', {}).get('risk_distribution', {}),
                            'compliance_issues': duplicate_result.get('breakdowns', {}).get('compliance_issues', []),
                            'high_priority_recommendations': duplicate_result.get('breakdowns', {}).get('high_priority_recommendations', [])
                        }),
                        chart_data=duplicate_result.get('chart_data', {}),
                        export_data=duplicate_result.get('export_data', []),
                        processing_duration=duplicate_result.get('processing_duration', 0),
                        status='COMPLETED'
                    )
                    logger.info(f"Worker {worker_id}: Duplicate analysis saved to database with ID: {duplicate_analysis_result.id}")
                except Exception as db_error:
                    logger.error(f"Worker {worker_id}: Failed to save duplicate analysis to database: {db_error}")
                
                logger.info(f"Worker {worker_id}: Duplicate Analysis completed successfully")
            except Exception as e:
                analysis_results['duplicate_analysis'] = {'error': str(e)}
                logger.error(f"Worker {worker_id}: Duplicate Analysis failed: {e}")
            
            # 3. Backdated Analysis
            try:
                logger.info(f"Worker {worker_id}: Starting Backdated Analysis")
                backdated_result = orchestrator.run_backdated_analysis(transactions)
                analysis_results['backdated_analysis'] = {
                    'status': 'COMPLETED',
                    'result': backdated_result,
                    'processing_duration': backdated_result.get('processing_duration', 0)
                }
                
                # Save backdated analysis results to database
                try:
                    from .models import BackdatedAnalysisResult
                    backdated_analysis_result = BackdatedAnalysisResult.objects.create(
                        data_file=job.data_file,
                        processing_job=job,
                        analysis_type='enhanced_backdated',
                        analysis_version='1.0.0',
                        analysis_info={
                            'total_transactions': len(transactions),
                            'backdated_entries_found': backdated_result.get('backdated_entries_found', 0),
                            'backdated_percentage': backdated_result.get('compliance_assessment', {}).get('backdated_percentage', 0)
                        },
                        backdated_entries=backdated_result.get('backdated_entries', []),
                        backdated_by_document=backdated_result.get('backdated_by_document', []),
                        backdated_by_account=backdated_result.get('backdated_by_account', []),
                        backdated_by_user=backdated_result.get('backdated_by_user', []),
                        audit_recommendations=backdated_result.get('audit_recommendations', {}),
                        compliance_assessment=backdated_result.get('compliance_assessment', {}),
                        financial_statement_impact=backdated_result.get('financial_statement_impact', {}),
                        chart_data=backdated_result.get('chart_data', {}),
                        export_data=backdated_result.get('export_data', []),
                        processing_duration=backdated_result.get('processing_duration', 0),
                        status='COMPLETED'
                    )
                    logger.info(f"Worker {worker_id}: Backdated analysis saved to database with ID: {backdated_analysis_result.id}")
                except Exception as db_error:
                    logger.error(f"Worker {worker_id}: Failed to save backdated analysis to database: {db_error}")
                
                logger.info(f"Worker {worker_id}: Backdated Analysis completed successfully")
            except Exception as e:
                analysis_results['backdated_analysis'] = {'error': str(e)}
                logger.error(f"Worker {worker_id}: Backdated Analysis failed: {e}")
            
            # 4. User Analysis
            try:
                logger.info(f"Worker {worker_id}: Starting User Analysis")
                user_result = orchestrator.run_user_analysis(transactions)
                analysis_results['user_analysis'] = {
                    'status': 'COMPLETED',
                    'result': user_result,
                    'processing_duration': user_result.get('processing_duration', 0)
                }
                
                # Save user analysis results to database
                try:
                    from .models import UserAnalysisResult
                    user_analysis_result = UserAnalysisResult.objects.create(
                        data_file=job.data_file,
                        processing_job=job,
                        analysis_type='user_analysis',
                        analysis_version='1.0.0',
                        analysis_info={
                            'total_transactions': len(transactions),
                            'total_users': user_result.get('total_users', 0),
                            'user_anomalies_found': user_result.get('user_anomalies_found', 0)
                        },
                        user_transaction_summary=user_result.get('user_transaction_summary', []),
                        user_debit_analysis=user_result.get('user_debit_analysis', []),
                        user_account_distribution=user_result.get('user_account_distribution', []),
                        user_fs_line_distribution=user_result.get('user_fs_line_distribution', []),
                        user_anomalies=user_result.get('user_anomalies', []),
                        user_risk_assessment=user_result.get('user_risk_assessment', {}),
                        user_patterns=user_result.get('user_patterns', {}),
                        chart_data=user_result.get('chart_data', {}),
                        export_data=user_result.get('export_data', []),
                        processing_duration=user_result.get('processing_duration', 0),
                        status='COMPLETED'
                    )
                    logger.info(f"Worker {worker_id}: User analysis saved to database with ID: {user_analysis_result.id}")
                except Exception as db_error:
                    logger.error(f"Worker {worker_id}: Failed to save user analysis to database: {db_error}")
                
                logger.info(f"Worker {worker_id}: User Analysis completed successfully")
            except Exception as e:
                analysis_results['user_analysis'] = {'error': str(e)}
                logger.error(f"Worker {worker_id}: User Analysis failed: {e}")
            
            # 5. Unusual Days Analysis
            try:
                logger.info(f"Worker {worker_id}: Starting Unusual Days Analysis")
                unusual_days_result = orchestrator.run_unusual_days_analysis(transactions)
                analysis_results['unusual_days_analysis'] = {
                    'status': 'COMPLETED',
                    'result': unusual_days_result,
                    'processing_duration': unusual_days_result.get('processing_duration', 0)
                }
                
                # Save unusual days analysis results to database
                try:
                    from .models import UnusualDaysAnalysisResult
                    unusual_days_analysis_result = UnusualDaysAnalysisResult.objects.create(
                        data_file=job.data_file,
                        processing_job=job,
                        analysis_type='unusual_days_analysis',
                        analysis_version='1.0.0',
                        analysis_info={
                            'total_transactions': len(transactions),
                            'weekend_transactions': unusual_days_result.get('weekend_transactions', 0),
                            'unusual_days_found': unusual_days_result.get('unusual_days_found', 0)
                        },
                        weekend_postings=unusual_days_result.get('weekend_postings', []),
                        day_of_week_activity=unusual_days_result.get('day_of_week_activity', {}),
                        user_day_patterns=unusual_days_result.get('user_day_patterns', []),
                        fs_line_day_patterns=unusual_days_result.get('fs_line_day_patterns', []),
                        unusual_days=unusual_days_result.get('unusual_days', []),
                        risk_assessment=unusual_days_result.get('risk_assessment', {}),
                        chart_data=unusual_days_result.get('chart_data', {}),
                        export_data=unusual_days_result.get('export_data', []),
                        processing_duration=unusual_days_result.get('processing_duration', 0),
                        status='COMPLETED'
                    )
                    logger.info(f"Worker {worker_id}: Unusual days analysis saved to database with ID: {unusual_days_analysis_result.id}")
                except Exception as db_error:
                    logger.error(f"Worker {worker_id}: Failed to save unusual days analysis to database: {db_error}")
                
                logger.info(f"Worker {worker_id}: Unusual Days Analysis completed successfully")
            except Exception as e:
                analysis_results['unusual_days_analysis'] = {'error': str(e)}
                logger.error(f"Worker {worker_id}: Unusual Days Analysis failed: {e}")
            
            # 6. Holiday Analysis
            try:
                logger.info(f"Worker {worker_id}: Starting Holiday Analysis")
                holiday_result = orchestrator.run_holiday_analysis(transactions)
                analysis_results['holiday_analysis'] = {
                    'status': 'COMPLETED',
                    'result': holiday_result,
                    'processing_duration': holiday_result.get('processing_duration', 0)
                }
                
                # Save holiday analysis results to database
                try:
                    from .models import HolidayAnalysisResult
                    holiday_analysis_result = HolidayAnalysisResult.objects.create(
                        data_file=job.data_file,
                        processing_job=job,
                        analysis_type='holiday_analysis',
                        analysis_version='1.0.0',
                        analysis_info={
                            'total_transactions': len(transactions),
                            'holiday_postings_count': holiday_result.get('holiday_postings_count', 0),
                            'holiday_percentage': holiday_result.get('holiday_percentage', 0),
                            'unique_holidays': holiday_result.get('unique_holidays', 0),
                            'country_code': holiday_result.get('country_code', 'saudiarabian'),
                            'fiscal_year': holiday_result.get('fiscal_year', '')
                        },
                        holiday_postings=holiday_result.get('holiday_postings', []),
                        holiday_by_fs_line=holiday_result.get('holiday_by_fs_line', []),
                        holiday_by_account=holiday_result.get('holiday_by_account', []),
                        holiday_by_user=holiday_result.get('holiday_by_user', []),
                        gl_activity_by_holiday=holiday_result.get('gl_activity_by_holiday', {}),
                        audit_recommendations=holiday_result.get('risk_assessment', {}),
                        compliance_assessment={
                            'total_holiday_postings': len(holiday_result.get('holiday_postings', [])),
                            'holiday_percentage': holiday_result.get('analysis_info', {}).get('holiday_percentage', 0),
                            'unique_holidays': holiday_result.get('analysis_info', {}).get('unique_holidays', 0)
                        },
                        financial_statement_impact={
                            'holiday_amount': sum(p.get('amount', 0) for p in holiday_result.get('holiday_postings', [])),
                            'high_value_holiday_amount': sum(p.get('amount', 0) for p in holiday_result.get('holiday_postings', []) if p.get('amount', 0) > 1000000)
                        },
                        chart_data=holiday_result.get('chart_data', {}),
                        export_data=holiday_result.get('export_data', []),
                        processing_duration=holiday_result.get('processing_duration', 0),
                        status='COMPLETED'
                    )
                    logger.info(f"Worker {worker_id}: Holiday analysis saved to database with ID: {holiday_analysis_result.id}")
                except Exception as db_error:
                    logger.error(f"Worker {worker_id}: Failed to save holiday analysis to database: {db_error}")
                
                logger.info(f"Worker {worker_id}: Holiday Analysis completed successfully")
            except Exception as e:
                analysis_results['holiday_analysis'] = {'error': str(e)}
                logger.error(f"Worker {worker_id}: Holiday Analysis failed: {e}")
            
            # 7. Closing Entries Analysis
            try:
                logger.info(f"Worker {worker_id}: Starting Closing Entries Analysis")
                closing_entries_result = orchestrator.run_closing_entries_analysis(transactions)
                analysis_results['closing_entries_analysis'] = {
                    'status': 'COMPLETED',
                    'result': closing_entries_result,
                    'processing_duration': closing_entries_result.get('processing_duration', 0)
                }
                
                # Save closing entries analysis results to database
                try:
                    from .models import ClosingEntriesAnalysisResult
                    closing_entries_analysis_result = ClosingEntriesAnalysisResult.objects.create(
                        data_file=job.data_file,
                        processing_job=job,
                        analysis_type='closing_entries_analysis',
                        analysis_version='1.0.0',
                        analysis_info={
                            'total_transactions': len(transactions),
                            'closing_entries_count': closing_entries_result.get('closing_entries_count', 0),
                            'post_close_entries_count': closing_entries_result.get('post_close_entries_count', 0)
                        },
                        closing_entries=closing_entries_result.get('closing_entries', []),
                        post_close_analysis=closing_entries_result.get('post_close_analysis', {}),
                        fs_line_closing=closing_entries_result.get('fs_line_closing', {}),
                        user_closing=closing_entries_result.get('user_closing', {}),
                        month_end_patterns=closing_entries_result.get('month_end_patterns', {}),
                        closing_window_analysis=closing_entries_result.get('closing_window_analysis', {}),
                        risk_assessment=closing_entries_result.get('risk_assessment', {}),
                        chart_data=closing_entries_result.get('chart_data', {}),
                        export_data=closing_entries_result.get('export_data', []),
                        processing_duration=closing_entries_result.get('processing_duration', 0),
                        status='COMPLETED'
                    )
                    logger.info(f"Worker {worker_id}: Closing entries analysis saved to database with ID: {closing_entries_analysis_result.id}")
                except Exception as db_error:
                    logger.error(f"Worker {worker_id}: Failed to save closing entries analysis to database: {db_error}")
                
                logger.info(f"Worker {worker_id}: Closing Entries Analysis completed successfully")
            except Exception as e:
                analysis_results['closing_entries_analysis'] = {'error': str(e)}
                logger.error(f"Worker {worker_id}: Closing Entries Analysis failed: {e}")
            
            # 7. Overall Analysis (uses all previous results)
            try:
                logger.info(f"Worker {worker_id}: Starting Overall Analysis (using previous results)")
                
                # Ensure we have all the required analysis results for overall analysis
                required_results = {}
                
                # Get general analysis results
                if 'general_analysis' in analysis_results and 'result' in analysis_results['general_analysis']:
                    required_results['general_results'] = analysis_results['general_analysis']['result'].get('ml_results', [])
                else:
                    logger.warning(f"Worker {worker_id}: Missing general analysis results for overall analysis")
                    required_results['general_results'] = []
                
                # Get duplicate analysis results
                if 'duplicate_analysis' in analysis_results and 'result' in analysis_results['duplicate_analysis']:
                    required_results['duplicate_results'] = analysis_results['duplicate_analysis']['result'].get('ml_results', [])
                else:
                    logger.warning(f"Worker {worker_id}: Missing duplicate analysis results for overall analysis")
                    required_results['duplicate_results'] = []
                
                # Get backdated analysis results
                if 'backdated_analysis' in analysis_results and 'result' in analysis_results['backdated_analysis']:
                    required_results['backdated_results'] = analysis_results['backdated_analysis']['result'].get('ml_results', [])
                else:
                    logger.warning(f"Worker {worker_id}: Missing backdated analysis results for overall analysis")
                    required_results['backdated_results'] = []
                
                # Get user analysis results
                if 'user_analysis' in analysis_results and 'result' in analysis_results['user_analysis']:
                    required_results['user_results'] = analysis_results['user_analysis']['result'].get('ml_results', [])
                else:
                    logger.warning(f"Worker {worker_id}: Missing user analysis results for overall analysis")
                    required_results['user_results'] = []
                
                # Get unusual days analysis results
                if 'unusual_days_analysis' in analysis_results and 'result' in analysis_results['unusual_days_analysis']:
                    required_results['unusual_days_results'] = analysis_results['unusual_days_analysis']['result'].get('unusual_transactions', [])
                else:
                    logger.warning(f"Worker {worker_id}: Missing unusual days analysis results for overall analysis")
                    required_results['unusual_days_results'] = []
                
                # Get holiday analysis results
                if 'holiday_analysis' in analysis_results and 'result' in analysis_results['holiday_analysis']:
                    required_results['holiday_results'] = analysis_results['holiday_analysis']['result'].get('holiday_postings', [])
                else:
                    logger.warning(f"Worker {worker_id}: Missing holiday analysis results for overall analysis")
                    required_results['holiday_results'] = []
                
                # Get closing entries analysis results
                if 'closing_entries_analysis' in analysis_results and 'result' in analysis_results['closing_entries_analysis']:
                    required_results['closing_entries_results'] = analysis_results['closing_entries_analysis']['result'].get('closing_entries', [])
                else:
                    logger.warning(f"Worker {worker_id}: Missing closing entries analysis results for overall analysis")
                    required_results['closing_entries_results'] = []
                
                # Run overall analysis with all available results
                overall_result = orchestrator.run_overall_analysis(transactions)
                analysis_results['overall_analysis'] = {
                    'status': 'COMPLETED',
                    'result': overall_result,
                    'processing_duration': overall_result.get('processing_duration', 0)
                }
                
                # Save overall analysis results to database
                try:
                    from .models import OverallAnalysisResult
                    overall_analysis_result = OverallAnalysisResult.objects.create(
                        data_file=job.data_file,
                        processing_job=job,
                        analysis_type='overall_analysis',
                        analysis_version='1.0.0',
                        transaction_summary=overall_result.get('transaction_summary', {}),
                        flagged_transactions=overall_result.get('flagged_transactions', []),
                        flag_summary=overall_result.get('flag_summary', {}),
                        expense_analysis=overall_result.get('expense_analysis', {}),
                        risk_assessment=overall_result.get('risk_assessment', {}),
                        chart_data=overall_result.get('chart_data', {}),
                        export_data=overall_result.get('export_data', []),
                        processing_duration=overall_result.get('processing_duration', 0),
                        status='COMPLETED'
                    )
                    logger.info(f"Worker {worker_id}: Overall analysis saved to database with ID: {overall_analysis_result.id}")
                except Exception as db_error:
                    logger.error(f"Worker {worker_id}: Failed to save overall analysis to database: {db_error}")
                
                logger.info(f"Worker {worker_id}: Overall Analysis completed successfully")
            except Exception as e:
                analysis_results['overall_analysis'] = {'error': str(e)}
                logger.error(f"Worker {worker_id}: Overall Analysis failed: {e}")
            
            # 6. Risk Analysis (uses all previous results)
            try:
                logger.info(f"Worker {worker_id}: Starting Risk Analysis (using all previous results)")
                # Extract overall results from the overall analysis
                overall_results = analysis_results.get('overall_analysis', {}).get('result', {}).get('ml_results', [])
                risk_result = orchestrator.run_risk_analysis(transactions, overall_results=overall_results)
                analysis_results['risk_analysis'] = {
                    'status': 'COMPLETED',
                    'result': risk_result,
                    'processing_duration': risk_result.get('processing_duration', 0)
                }
                
                # Save risk analysis results to RiskScoringDocument table
                try:
                    from .models import RiskScoringDocument
                    from .models import GeneralAnalysisResult, DuplicateAnalysisResult, BackdatedAnalysisResult, OverallAnalysisResult, ClosingEntriesAnalysisResult, HolidayAnalysisResult
                    
                    # Get results from previous analyses for risk factors
                    general_analysis = GeneralAnalysisResult.objects.filter(data_file=job.data_file).first()
                    duplicate_analysis = DuplicateAnalysisResult.objects.filter(data_file=job.data_file).first()
                    backdated_analysis = BackdatedAnalysisResult.objects.filter(data_file=job.data_file).first()
                    user_analysis = UserAnalysisResult.objects.filter(data_file=job.data_file).first()
                    unusual_days_analysis = UnusualDaysAnalysisResult.objects.filter(data_file=job.data_file).first()
                    holiday_analysis = HolidayAnalysisResult.objects.filter(data_file=job.data_file).first()
                    closing_entries_analysis = ClosingEntriesAnalysisResult.objects.filter(data_file=job.data_file).first()
                    overall_analysis = OverallAnalysisResult.objects.filter(data_file=job.data_file).first()
                    
                    # Calculate risk statistics from ML results
                    risk_classification = risk_result.get('risk_classification', {})
                    final_risk_scores = risk_result.get('final_risk_scores', {})
                    high_risk_transactions = risk_result.get('high_risk_transactions', [])
                    
                    # Create risk scoring document
                    risk_scoring_document = RiskScoringDocument.objects.create(
                        data_file=job.data_file,
                        processing_job=job,
                        document_type='comprehensive_risk_scoring',
                        document_version='1.0.0',
                        methodology_overview={
                            'description': 'Comprehensive risk scoring using ML models and analysis results',
                            'models_used': ['risk_model', 'duplicate_model', 'backdated_model'],
                            'analysis_sources': ['general_analysis', 'duplicate_analysis', 'backdated_analysis', 'overall_analysis']
                        },
                        risk_factors={
                            'duplicate_risk': len(duplicate_analysis.duplicate_list) if duplicate_analysis else 0,
                            'backdated_risk': len(backdated_analysis.backdated_entries) if backdated_analysis else 0,
                            'user_risk': len(user_analysis.user_anomalies) if user_analysis else 0,
                            'unusual_days_risk': len(unusual_days_analysis.weekend_postings) if unusual_days_analysis else 0,
                            'holiday_risk': len(holiday_analysis.holiday_postings) if holiday_analysis else 0,
                            'closing_entries_risk': len(closing_entries_analysis.closing_entries) if closing_entries_analysis else 0,
                            'high_value_risk': len([t for t in transactions if float(t.amount_local_currency) > 1000000]),
                            'unusual_pattern_risk': len(overall_results) if overall_results else 0
                        },
                        scoring_criteria={
                            'critical_risk_threshold': 80,
                            'high_risk_threshold': 60,
                            'medium_risk_threshold': 30,
                            'low_risk_threshold': 0
                        },
                        risk_calculations=risk_result.get('ml_results', []),
                        risk_distributions=risk_classification,
                        recommendations={
                            'high_priority': high_risk_transactions[:10],
                            'medium_priority': [r for r in risk_result.get('ml_results', []) if r.get('risk_class') == 1][:10],
                            'low_priority': [r for r in risk_result.get('ml_results', []) if r.get('risk_class') == 0][:10]
                        },
                        audit_implications={
                            'focus_areas': ['high_risk_transactions', 'duplicate_entries', 'backdated_entries'],
                            'sampling_strategy': 'risk_based',
                            'follow_up_actions': ['detailed_review', 'documentation_verification', 'management_inquiry']
                        },
                        total_transactions=len(transactions),
                        high_risk_transactions=risk_classification.get('high_risk', 0) + risk_classification.get('critical_risk', 0),
                        medium_risk_transactions=risk_classification.get('medium_risk', 0),
                        low_risk_transactions=risk_classification.get('low_risk', 0),
                        overall_risk_score=final_risk_scores.get('average', 0.0),
                        processing_duration=risk_result.get('processing_duration', 0),
                        status='COMPLETED'
                    )
                    
                    logger.info(f"Worker {worker_id}: Risk analysis saved to database with ID: {risk_scoring_document.id}")
                    
                except Exception as db_error:
                    logger.error(f"Worker {worker_id}: Failed to save risk analysis to database: {db_error}")
                
                logger.info(f"Worker {worker_id}: Risk Analysis completed successfully")
            except Exception as e:
                analysis_results['risk_analysis'] = {'error': str(e)}
                logger.error(f"Worker {worker_id}: Risk Analysis failed: {e}")
            
            # Calculate basic summary
            total_amount = sum(float(t.amount_local_currency) for t in transactions)
            unique_users = len(set(t.user_name for t in transactions))
            unique_accounts = len(set(t.gl_account for t in transactions))
            
            # Calculate total processing duration
            total_processing_duration = sum(
                analysis_results.get(analysis_type, {}).get('processing_duration', 0)
                for analysis_type in ['general_analysis', 'duplicate_analysis', 'backdated_analysis', 
                                    'user_analysis', 'unusual_days_analysis', 'holiday_analysis', 
                                    'closing_entries_analysis', 'overall_analysis', 'risk_analysis']
            )
            
            # Update job with results
            job.status = 'COMPLETED'
            job.completed_at = timezone.now()
            job.processing_duration = (job.completed_at - start_time).total_seconds()
            job.analytics_results = {
                'summary': {
                    'total_transactions': len(transactions),
                    'total_amount': total_amount,
                    'unique_users': unique_users,
                    'unique_accounts': unique_accounts,
                    'processed_parallel': True,
                    'worker_id': worker_id,
                    'total_processing_duration': total_processing_duration,
                    'analysis_results': analysis_results
                }
            }
            job.save()
            
            return {
                'job_id': job_id,
                'status': 'COMPLETED',
                'worker_id': worker_id,
                'processing_time': job.processing_duration,
                'total_processing_duration': total_processing_duration,
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