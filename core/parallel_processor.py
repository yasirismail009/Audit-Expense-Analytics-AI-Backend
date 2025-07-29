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
                duplicate_result = orchestrator.run_duplicate_analysis(transactions)
                analysis_results['duplicate_analysis'] = {
                    'status': 'COMPLETED',
                    'result': duplicate_result,
                    'processing_duration': duplicate_result.get('processing_duration', 0)
                }
                
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
            
            # 5. Overall Analysis (uses all previous results)
            try:
                logger.info(f"Worker {worker_id}: Starting Overall Analysis (using previous results)")
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
                    from .models import GeneralAnalysisResult, DuplicateAnalysisResult, BackdatedAnalysisResult, OverallAnalysisResult
                    
                    # Get results from previous analyses for risk factors
                    general_analysis = GeneralAnalysisResult.objects.filter(data_file=job.data_file).first()
                    duplicate_analysis = DuplicateAnalysisResult.objects.filter(data_file=job.data_file).first()
                    backdated_analysis = BackdatedAnalysisResult.objects.filter(data_file=job.data_file).first()
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
                                    'user_analysis', 'overall_analysis', 'risk_analysis']
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