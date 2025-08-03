"""
Celery tasks for streamlined analysis processing
"""

from celery import shared_task, current_task
from django.utils import timezone
from django.db import transaction
import logging
import traceback
import psutil
import os
from datetime import datetime
from decimal import Decimal, InvalidOperation

from .models import FileProcessingJob, SAPGLPosting, DataFile, MLModelTraining, AnalysisSession, BackdatedAnalysisResult
from .analytics import SAPGLAnalyzer
from .analytics_db_saver import AnalyticsDBSaver, save_analytics_to_db
from .specialized_ml_models import SpecializedAnomalyDetector
from .general_analysis import GeneralAnalyzer
from .overall_analysis import OverallAnalyzer

logger = logging.getLogger(__name__)

def log_task_info(task_name, job_id, message, level="info"):
    """Log task information with consistent formatting"""
    log_message = f"[TASK:{task_name}] [JOB:{job_id}] {message}"
    if level == "info":
        logger.info(log_message)
    elif level == "error":
        logger.error(log_message)
    elif level == "warning":
        logger.warning(log_message)
    elif level == "debug":
        logger.debug(log_message)
    print(f"🔍 {log_message}")

def debug_task_state(task_name, job_id, state, details=None):
    """Debug task state transitions"""
    log_message = f"[TASK:{task_name}] [JOB:{job_id}] STATE: {state}"
    if details:
        log_message += f" | DETAILS: {details}"
    logger.info(log_message)
    print(f"🔄 {log_message}")

def debug_task_data(task_name, job_id, data_type, data, max_items=5):
    """Debug task data with size limits"""
    if isinstance(data, (list, tuple)):
        data_info = f"List/Tuple with {len(data)} items"
        if len(data) > 0:
            sample_items = data[:max_items]
            data_info += f" | Sample: {sample_items}"
            if len(data) > max_items:
                data_info += f" ... (showing first {max_items})"
    elif isinstance(data, dict):
        data_info = f"Dict with {len(data)} keys: {list(data.keys())}"
    else:
        data_info = f"Type: {type(data).__name__}, Value: {str(data)[:100]}"
    
    log_message = f"[TASK:{task_name}] [JOB:{job_id}] DATA[{data_type}]: {data_info}"
    logger.debug(log_message)
    print(f"📊 {log_message}")

def debug_task_exception(task_name, job_id, exception, context=""):
    """Debug task exceptions with full context"""
    import traceback
    log_message = f"[TASK:{task_name}] [JOB:{job_id}] EXCEPTION: {type(exception).__name__}: {str(exception)}"
    if context:
        log_message += f" | CONTEXT: {context}"
    
    logger.error(log_message)
    logger.error(f"[TASK:{task_name}] [JOB:{job_id}] TRACEBACK: {traceback.format_exc()}")
    print(f"❌ {log_message}")
    print(f"📋 TRACEBACK: {traceback.format_exc()}")

def get_system_info():
    """Get system information for debugging"""
    try:
        return {
            'pid': os.getpid(),
            'memory_usage_mb': psutil.Process().memory_info().rss / 1024 / 1024,
            'cpu_percent': psutil.Process().cpu_percent(),
            'worker_hostname': current_task.request.hostname if current_task else 'unknown',
            'worker_pid': current_task.request.pid if current_task else 'unknown',
            'task_id': current_task.request.id if current_task else 'unknown',
        }
    except Exception as e:
        return {'error': str(e)}

# ============================================================================
# STREAMLINED ANALYSIS TASKS
# ============================================================================

@shared_task(bind=True, max_retries=3, default_retry_delay=60, time_limit=600, soft_time_limit=480)
def run_restructured_analysis(self, job_id):
    """
    Main orchestrator task that runs all analysis types separately and saves to their specific database tables.
    
    Flow:
    1. Upload file → Data saved
    2. Task added to queue
    3. Queue processes each analysis separately:
       - General Analysis → Save to GeneralAnalysisResult
       - Overall Analysis → Save to OverallAnalysisResult 
       - Duplicate Analysis → Save to DuplicateAnalysisResult
       - Backdated Analysis → Save to BackdatedAnalysisResult
    4. Risk Analysis → Uses all above analyses and calculates risk → Save to RiskScoringDocument
    
    Args:
        job_id (str): UUID of the FileProcessingJob to process
    """
    task_name = "run_restructured_analysis"
    start_time = timezone.now()
    
    debug_task_state(task_name, job_id, "STARTED", f"Task ID: {self.request.id}")
    
    try:
        # Get the processing job
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        debug_task_state(task_name, job_id, "JOB_RETRIEVED", f"Processing file: {data_file.file_name}")
        
        # Update job status
        job.status = 'PROCESSING'
        job.started_at = timezone.now()
        job.save()
        
        # Get transactions for this file
        transactions = SAPGLPosting.objects.filter(
            document_number__in=data_file.get_transaction_document_numbers()
        )
        
        debug_task_data(task_name, job_id, "TRANSACTIONS", f"Found {len(transactions)} transactions")
        
        # Run each analysis type separately and save to their specific database tables
        analysis_results = {}
        
        # 1. RUN GENERAL ANALYSIS → Save to GeneralAnalysisResult
        debug_task_state(task_name, job_id, "GENERAL_ANALYSIS", "Starting General Analysis...")
        general_result = run_general_analysis.delay(job_id)
        analysis_results['general_analysis'] = general_result
        
        # 2. RUN DUPLICATE ANALYSIS → Save to DuplicateAnalysisResult
        debug_task_state(task_name, job_id, "DUPLICATE_ANALYSIS", "Starting Duplicate Analysis...")
        duplicate_result = run_duplicate_analysis.delay(job_id)
        analysis_results['duplicate_analysis'] = duplicate_result
        
        # 3. RUN BACKDATED ANALYSIS → Save to BackdatedAnalysisResult
        debug_task_state(task_name, job_id, "BACKDATED_ANALYSIS", "Starting Backdated Analysis...")
        backdated_result = run_backdated_analysis.delay(job_id)
        analysis_results['backdated_analysis'] = backdated_result
        
        # 4. RUN USER ANALYSIS → Save to UserAnalysisResult
        debug_task_state(task_name, job_id, "USER_ANALYSIS", "Starting User Analysis...")
        user_result = run_user_analysis.delay(job_id)
        analysis_results['user_analysis'] = user_result
        
        # 5. RUN UNUSUAL DAYS ANALYSIS → Save to UnusualDaysAnalysisResult
        debug_task_state(task_name, job_id, "UNUSUAL_DAYS_ANALYSIS", "Starting Unusual Days Analysis...")
        unusual_days_result = run_unusual_days_analysis.delay(job_id)
        analysis_results['unusual_days_analysis'] = unusual_days_result
        
        # 6. RUN CLOSING ENTRIES ANALYSIS → Save to ClosingEntriesAnalysisResult
        debug_task_state(task_name, job_id, "CLOSING_ENTRIES_ANALYSIS", "Starting Closing Entries Analysis...")
        closing_entries_result = run_closing_entries_analysis.delay(job_id)
        analysis_results['closing_entries_analysis'] = closing_entries_result
        
        # 7. RUN OVERALL ANALYSIS → Save to OverallAnalysisResult
        debug_task_state(task_name, job_id, "OVERALL_ANALYSIS", "Starting Overall Analysis...")
        overall_result = run_overall_analysis.delay(job_id)
        analysis_results['overall_analysis'] = overall_result
        
        # 5. RUN RISK ANALYSIS → Uses all above analyses → Save to RiskScoringDocument
        debug_task_state(task_name, job_id, "RISK_ANALYSIS", "Starting Risk Analysis...")
        risk_result = run_risk_analysis.delay(job_id)
        analysis_results['risk_analysis'] = risk_result
        
        # 6. RUN ML MODEL TRAINING → Train models with all data → Save to MLModelTraining
        debug_task_state(task_name, job_id, "ML_TRAINING", "Starting ML Model Training...")
        ml_training_result = train_ml_models.delay(job_id)
        analysis_results['ml_training'] = ml_training_result
        
        # Calculate total processing duration
        total_duration = (timezone.now() - start_time).total_seconds()
        
        # Update job with task IDs (not results - results are in separate tables)
        job.status = 'COMPLETED'
        job.completed_at = timezone.now()
        job.processing_duration = total_duration
        job.analytics_results = {
            'task_ids': {
                'general_analysis_task_id': str(general_result.id),
                'duplicate_analysis_task_id': str(duplicate_result.id),
                'backdated_analysis_task_id': str(backdated_result.id),
                'user_analysis_task_id': str(user_result.id),
                'unusual_days_analysis_task_id': str(unusual_days_result.id),
                'closing_entries_analysis_task_id': str(closing_entries_result.id),
                'overall_analysis_task_id': str(overall_result.id),
                'risk_analysis_task_id': str(risk_result.id),
                'ml_training_task_id': str(ml_training_result.id),
            },
            'analysis_tables': {
                'general_analysis': 'GeneralAnalysisResult',
                'duplicate_analysis': 'DuplicateAnalysisResult',
                'backdated_analysis': 'BackdatedAnalysisResult',
                'user_analysis': 'UserAnalysisResult',
                'unusual_days_analysis': 'UnusualDaysAnalysisResult',
                'closing_entries_analysis': 'ClosingEntriesAnalysisResult',
                'overall_analysis': 'OverallAnalysisResult',
                'risk_analysis': 'RiskScoringDocument',
                'ml_training': 'MLModelTraining',
            }
        }
        job.save()
        
        debug_task_state(task_name, job_id, "COMPLETED", 
                        f"Restructured analysis orchestration completed in {total_duration:.2f} seconds")
        
        return {
            'job_id': str(job.id),
            'status': 'COMPLETED',
            'processing_duration': total_duration,
            'task_ids': analysis_results,
            'message': 'All analysis tasks have been queued. Check individual analysis tables for results.'
        }
        
    except FileProcessingJob.DoesNotExist:
        error_msg = f"FileProcessingJob with ID {job_id} not found"
        debug_task_exception(task_name, job_id, Exception(error_msg), "JOB_NOT_FOUND")
        return {'error': error_msg}
        
    except Exception as e:
        error_msg = f"Error in restructured analysis orchestration: {str(e)}"
        debug_task_exception(task_name, job_id, e, "ANALYSIS_ERROR")
        
        # Update job status to failed
        try:
            job = FileProcessingJob.objects.get(id=job_id)
            job.status = 'FAILED'
            job.error_message = error_msg
            job.save()
        except:
            pass
        
        return {'error': error_msg}

@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def run_general_analysis(self, job_id):
    """
    Run General Analysis and save results to GeneralAnalysisResult table
    
    Args:
        job_id (str): UUID of the FileProcessingJob to process
    """
    task_name = "run_general_analysis"
    start_time = timezone.now()
    
    debug_task_state(task_name, job_id, "STARTED", f"Task ID: {self.request.id}")
    
    try:
        # Get the processing job and data file
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get transactions for this file
        transactions = SAPGLPosting.objects.filter(
            document_number__in=data_file.get_transaction_document_numbers()
        )
        
        debug_task_data(task_name, job_id, "TRANSACTIONS", f"Found {len(transactions)} transactions")
        
        # Run General Analysis
        from .general_analysis import GeneralAnalyzer
        general_analyzer = GeneralAnalyzer()
        general_results = general_analyzer.run_general_analysis(transactions, data_file, job)
        
        # Save to GeneralAnalysisResult table
        from .models import GeneralAnalysisResult
        
        general_analysis_result = GeneralAnalysisResult.objects.create(
            data_file=data_file,
            processing_job=job,
            analysis_type='general_analysis',
            analysis_version='1.0.0',
            trial_balance_summary=general_results.get('trial_balance_summary', {}),
            gl_account_summaries=general_results.get('gl_account_summaries', []),
            user_summaries=general_results.get('user_summaries', []),
            statistical_calculations=general_results.get('statistical_calculations', {}),
            chart_data=general_results.get('chart_data', {}),
            export_data=general_results.get('export_data', []),
            processing_duration=general_results.get('processing_duration', 0),
            status='COMPLETED'
        )
        
        processing_duration = (timezone.now() - start_time).total_seconds()
        
        debug_task_state(task_name, job_id, "COMPLETED", 
                        f"General Analysis completed and saved to database in {processing_duration:.2f} seconds")
        
        return {
            'analysis_id': str(general_analysis_result.id),
            'status': 'COMPLETED',
            'processing_duration': processing_duration,
            'table': 'GeneralAnalysisResult'
        }
        
    except Exception as e:
        error_msg = f"Error in General Analysis: {str(e)}"
        debug_task_exception(task_name, job_id, e, "GENERAL_ANALYSIS_ERROR")
        
        # Save failed result to database
        try:
            from .models import GeneralAnalysisResult
            GeneralAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=job,
                analysis_type='general_analysis',
                analysis_version='1.0.0',
                status='FAILED',
                error_message=error_msg
            )
        except:
            pass
        
        return {'error': error_msg}

@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def run_duplicate_analysis(self, job_id):
    """
    Run Duplicate Analysis and save results to DuplicateAnalysisResult table
    
    Args:
        job_id (str): UUID of the FileProcessingJob to process
    """
    task_name = "run_duplicate_analysis"
    start_time = timezone.now()
    
    debug_task_state(task_name, job_id, "STARTED", f"Task ID: {self.request.id}")
    
    try:
        # Get the processing job and data file
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get transactions for this file
        transactions = SAPGLPosting.objects.filter(
            document_number__in=data_file.get_transaction_document_numbers()
        )
        
        debug_task_data(task_name, job_id, "TRANSACTIONS", f"Found {len(transactions)} transactions")
        
        # Run Duplicate Analysis with ML model
        from .specialized_analysis_models import AnalysisModelManager
        model_manager = AnalysisModelManager()
        duplicate_model = model_manager.get_model('duplicate')
        duplicate_results = duplicate_model.predict(transactions)
        
        # Convert results to the format expected by DuplicateAnalysisResult
        duplicate_entries = [r for r in duplicate_results if r['is_duplicate']]
        
        # Save to DuplicateAnalysisResult table
        from .models import DuplicateAnalysisResult
        
        duplicate_analysis_result = DuplicateAnalysisResult.objects.create(
            data_file=data_file,
            processing_job=job,
            analysis_type='enhanced_duplicate',
            analysis_version='1.0.0',
            analysis_info={
                'total_transactions': len(transactions),
                'duplicates_found': len(duplicate_entries),
                'duplicate_percentage': (len(duplicate_entries) / len(transactions)) * 100 if transactions else 0
            },
            duplicate_list=duplicate_entries,
            chart_data={},  # Will be populated by the analyzer
            breakdowns={},  # Will be populated by the analyzer
            slicer_filters={},  # Will be populated by the analyzer
            summary_table=[],  # Will be populated by the analyzer
            export_data=[],  # Will be populated by the analyzer
            detailed_insights={},  # Will be populated by the analyzer
            processing_duration=(timezone.now() - start_time).total_seconds(),
            status='COMPLETED'
        )
        
        processing_duration = (timezone.now() - start_time).total_seconds()
        
        debug_task_state(task_name, job_id, "COMPLETED", 
                        f"Duplicate Analysis completed and saved to database in {processing_duration:.2f} seconds")
        
        return {
            'analysis_id': str(duplicate_analysis_result.id),
            'status': 'COMPLETED',
            'processing_duration': processing_duration,
            'duplicates_found': len(duplicate_entries),
            'table': 'DuplicateAnalysisResult'
        }
        
    except Exception as e:
        error_msg = f"Error in Duplicate Analysis: {str(e)}"
        debug_task_exception(task_name, job_id, e, "DUPLICATE_ANALYSIS_ERROR")
        
        # Save failed result to database
        try:
            from .models import DuplicateAnalysisResult
            DuplicateAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=job,
                analysis_type='enhanced_duplicate',
                analysis_version='1.0.0',
                status='FAILED',
                error_message=error_msg
            )
        except:
            pass
        
        return {'error': error_msg}

@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def run_backdated_analysis(self, job_id):
    """
    Run Backdated Analysis and save results to BackdatedAnalysisResult table
    
    Args:
        job_id (str): UUID of the FileProcessingJob to process
    """
    task_name = "run_backdated_analysis"
    start_time = timezone.now()
    
    debug_task_state(task_name, job_id, "STARTED", f"Task ID: {self.request.id}")
    
    try:
        # Get the processing job and data file
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get transactions for this file
        transactions = SAPGLPosting.objects.filter(
            document_number__in=data_file.get_transaction_document_numbers()
        )
        
        debug_task_data(task_name, job_id, "TRANSACTIONS", f"Found {len(transactions)} transactions")
        
        # Run Backdated Analysis with ML model
        from .specialized_analysis_models import AnalysisModelManager
        model_manager = AnalysisModelManager()
        backdated_model = model_manager.get_model('backdated')
        backdated_results = backdated_model.predict(transactions)
        
        # Convert results to the format expected by BackdatedAnalysisResult
        backdated_entries = [r for r in backdated_results if r['is_backdated']]
        
        # Save to BackdatedAnalysisResult table
        from .models import BackdatedAnalysisResult
        
        backdated_analysis_result = BackdatedAnalysisResult.objects.create(
            data_file=data_file,
            processing_job=job,
            analysis_type='enhanced_backdated',
            analysis_version='1.0.0',
            analysis_info={
                'total_transactions': len(transactions),
                'backdated_entries_found': len(backdated_entries),
                'backdated_percentage': (len(backdated_entries) / len(transactions)) * 100 if transactions else 0
            },
            backdated_entries=backdated_entries,
            backdated_by_document=[],  # Will be populated by the analyzer
            backdated_by_account=[],  # Will be populated by the analyzer
            backdated_by_user=[],  # Will be populated by the analyzer
            audit_recommendations={},  # Will be populated by the analyzer
            compliance_assessment={},  # Will be populated by the analyzer
            financial_statement_impact={},  # Will be populated by the analyzer
            chart_data={},  # Will be populated by the analyzer
            export_data=[],  # Will be populated by the analyzer
            processing_duration=(timezone.now() - start_time).total_seconds(),
            status='COMPLETED'
        )
        
        processing_duration = (timezone.now() - start_time).total_seconds()
        
        debug_task_state(task_name, job_id, "COMPLETED", 
                        f"Backdated Analysis completed and saved to database in {processing_duration:.2f} seconds")
        
        return {
            'analysis_id': str(backdated_analysis_result.id),
            'status': 'COMPLETED',
            'processing_duration': processing_duration,
            'backdated_entries_found': len(backdated_entries),
            'table': 'BackdatedAnalysisResult'
        }
        
    except Exception as e:
        error_msg = f"Error in Backdated Analysis: {str(e)}"
        debug_task_exception(task_name, job_id, e, "BACKDATED_ANALYSIS_ERROR")
        
        # Save failed result to database
        try:
            from .models import BackdatedAnalysisResult
            BackdatedAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=job,
                analysis_type='enhanced_backdated',
                analysis_version='1.0.0',
                status='FAILED',
                error_message=error_msg
            )
        except:
            pass
        
        return {'error': error_msg}

@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def run_user_analysis(self, job_id):
    """
    Run User Analysis and save results to UserAnalysisResult table
    
    Args:
        job_id (str): UUID of the FileProcessingJob to process
    """
    task_name = "run_user_analysis"
    start_time = timezone.now()
    
    debug_task_state(task_name, job_id, "STARTED", f"Task ID: {self.request.id}")
    
    try:
        # Get the processing job and data file
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Run User Analysis
        from .user_analysis import UserAnalyzer
        user_analyzer = UserAnalyzer()
        user_results = user_analyzer.run_user_analysis(data_file, job)
        
        processing_duration = (timezone.now() - start_time).total_seconds()
        
        debug_task_state(task_name, job_id, "COMPLETED", 
                        f"User Analysis completed and saved to database in {processing_duration:.2f} seconds")
        
        return {
            'analysis_id': str(user_results['analysis_id']),
            'status': 'COMPLETED',
            'processing_duration': processing_duration,
            'table': 'UserAnalysisResult'
        }
        
    except Exception as e:
        error_msg = f"Error in User Analysis: {str(e)}"
        debug_task_exception(task_name, job_id, e, "USER_ANALYSIS_ERROR")
        
        # Save failed result to database
        try:
            from .models import UserAnalysisResult
            UserAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=job,
                analysis_type='user_analysis',
                analysis_version='1.0.0',
                status='FAILED',
                error_message=error_msg
            )
        except:
            pass
        
        return {'error': error_msg}

@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def run_unusual_days_analysis(self, job_id):
    """
    Run Unusual Days Analysis and save results to UnusualDaysAnalysisResult table
    
    Args:
        job_id (str): UUID of the FileProcessingJob to process
    """
    task_name = "run_unusual_days_analysis"
    start_time = timezone.now()
    
    debug_task_state(task_name, job_id, "STARTED", f"Task ID: {self.request.id}")
    
    try:
        # Get the processing job and data file
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Run Unusual Days Analysis
        from .unusual_days_analysis import UnusualDaysAnalyzer
        unusual_days_analyzer = UnusualDaysAnalyzer()
        unusual_days_results = unusual_days_analyzer.run_unusual_days_analysis(data_file, job)
        
        processing_duration = (timezone.now() - start_time).total_seconds()
        
        debug_task_state(task_name, job_id, "COMPLETED", 
                        f"Unusual Days Analysis completed and saved to database in {processing_duration:.2f} seconds")
        
        return {
            'analysis_id': str(unusual_days_results['analysis_id']),
            'status': 'COMPLETED',
            'processing_duration': processing_duration,
            'table': 'UnusualDaysAnalysisResult'
        }
        
    except Exception as e:
        error_msg = f"Error in Unusual Days Analysis: {str(e)}"
        debug_task_exception(task_name, job_id, e, "UNUSUAL_DAYS_ANALYSIS_ERROR")
        
        # Save failed result to database
        try:
            from .models import UnusualDaysAnalysisResult
            UnusualDaysAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=job,
                analysis_type='unusual_days_analysis',
                analysis_version='1.0.0',
                status='FAILED',
                error_message=error_msg
            )
        except:
            pass
        
        return {'error': error_msg}

@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def run_closing_entries_analysis(self, job_id):
    """
    Run Closing Entries Analysis and save results to ClosingEntriesAnalysisResult table
    
    Args:
        job_id (str): UUID of the FileProcessingJob to process
    """
    task_name = "run_closing_entries_analysis"
    start_time = timezone.now()
    
    debug_task_state(task_name, job_id, "STARTED", f"Task ID: {self.request.id}")
    
    try:
        # Get the processing job and data file
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Run Closing Entries Analysis
        from .closing_entries_analysis import ClosingEntriesAnalyzer
        closing_entries_analyzer = ClosingEntriesAnalyzer()
        closing_entries_results = closing_entries_analyzer.run_closing_entries_analysis(data_file, job)
        
        processing_duration = (timezone.now() - start_time).total_seconds()
        
        debug_task_state(task_name, job_id, "COMPLETED", 
                        f"Closing Entries Analysis completed and saved to database in {processing_duration:.2f} seconds")
        
        return {
            'analysis_id': str(closing_entries_results['analysis_id']),
            'status': 'COMPLETED',
            'processing_duration': processing_duration,
            'table': 'ClosingEntriesAnalysisResult'
        }
        
    except Exception as e:
        error_msg = f"Error in Closing Entries Analysis: {str(e)}"
        debug_task_exception(task_name, job_id, e, "CLOSING_ENTRIES_ANALYSIS_ERROR")
        
        # Save failed result to database
        try:
            from .models import ClosingEntriesAnalysisResult
            ClosingEntriesAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=job,
                analysis_type='closing_entries_analysis',
                analysis_version='1.0.0',
                status='FAILED',
                error_message=error_msg
            )
        except:
            pass
        
        return {'error': error_msg}

@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def run_overall_analysis(self, job_id):
    """
    Run Overall Analysis and save results to OverallAnalysisResult table
    
    Args:
        job_id (str): UUID of the FileProcessingJob to process
    """
    task_name = "run_overall_analysis"
    start_time = timezone.now()
    
    debug_task_state(task_name, job_id, "STARTED", f"Task ID: {self.request.id}")
    
    try:
        # Get the processing job and data file
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Run Overall Analysis
        from .overall_analysis import OverallAnalyzer
        overall_analyzer = OverallAnalyzer()
        overall_results = overall_analyzer.run_overall_analysis(data_file, job)
        
        # Save to OverallAnalysisResult table
        from .models import OverallAnalysisResult
        
        overall_analysis_result = OverallAnalysisResult.objects.create(
            data_file=data_file,
            processing_job=job,
            analysis_type='overall_analysis',
            analysis_version='1.0.0',
            transaction_summary=overall_results.get('transaction_summary', {}),
            flagged_transactions=overall_results.get('flagged_transactions', []),
            flag_summary=overall_results.get('flag_summary', {}),
            expense_analysis=overall_results.get('expense_analysis', {}),
            risk_assessment=overall_results.get('risk_assessment', {}),
            chart_data=overall_results.get('chart_data', {}),
            export_data=overall_results.get('export_data', []),
            processing_duration=overall_results.get('processing_duration', 0),
            status='COMPLETED'
        )
        
        processing_duration = (timezone.now() - start_time).total_seconds()
        
        debug_task_state(task_name, job_id, "COMPLETED", 
                        f"Overall Analysis completed and saved to database in {processing_duration:.2f} seconds")
        
        return {
            'analysis_id': str(overall_analysis_result.id),
            'status': 'COMPLETED',
            'processing_duration': processing_duration,
            'table': 'OverallAnalysisResult'
        }
        
    except Exception as e:
        error_msg = f"Error in Overall Analysis: {str(e)}"
        debug_task_exception(task_name, job_id, e, "OVERALL_ANALYSIS_ERROR")
        
        # Save failed result to database
        try:
            from .models import OverallAnalysisResult
            OverallAnalysisResult.objects.create(
                data_file=data_file,
                processing_job=job,
                analysis_type='overall_analysis',
                analysis_version='1.0.0',
                status='FAILED',
                error_message=error_msg
            )
        except:
            pass
        
        return {'error': error_msg}

@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def run_risk_analysis(self, job_id):
    """
    Run Risk Analysis using all previous analyses and save results to RiskScoringDocument table
    
    Args:
        job_id (str): UUID of the FileProcessingJob to process
    """
    task_name = "run_risk_analysis"
    start_time = timezone.now()
    
    debug_task_state(task_name, job_id, "STARTED", f"Task ID: {self.request.id}")
    
    try:
        # Get the processing job and data file
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get transactions for this file
        transactions = SAPGLPosting.objects.filter(
            document_number__in=data_file.get_transaction_document_numbers()
        )
        
        debug_task_data(task_name, job_id, "TRANSACTIONS", f"Found {len(transactions)} transactions")
        
        # Get results from previous analyses
        from .models import GeneralAnalysisResult, DuplicateAnalysisResult, BackdatedAnalysisResult, UserAnalysisResult, UnusualDaysAnalysisResult, ClosingEntriesAnalysisResult, OverallAnalysisResult
        
        general_analysis = GeneralAnalysisResult.objects.filter(data_file=data_file).first()
        duplicate_analysis = DuplicateAnalysisResult.objects.filter(data_file=data_file).first()
        backdated_analysis = BackdatedAnalysisResult.objects.filter(data_file=data_file).first()
        user_analysis = UserAnalysisResult.objects.filter(data_file=data_file).first()
        unusual_days_analysis = UnusualDaysAnalysisResult.objects.filter(data_file=data_file).first()
        closing_entries_analysis = ClosingEntriesAnalysisResult.objects.filter(data_file=data_file).first()
        overall_analysis = OverallAnalysisResult.objects.filter(data_file=data_file).first()
        
        # Run Risk Analysis with ML model
        from .specialized_analysis_models import AnalysisModelManager
        model_manager = AnalysisModelManager()
        risk_model = model_manager.get_model('risk')
        
        # Use flagged transactions from overall analysis if available
        flagged_transactions = []
        if overall_analysis and overall_analysis.flagged_transactions:
            flagged_transactions = overall_analysis.flagged_transactions
        
        risk_results = risk_model.predict(transactions, flagged_transactions)
        
        # Calculate risk statistics
        high_risk_count = len([r for r in risk_results if r.get('risk_level') == 'HIGH'])
        medium_risk_count = len([r for r in risk_results if r.get('risk_level') == 'MEDIUM'])
        low_risk_count = len([r for r in risk_results if r.get('risk_level') == 'LOW'])
        
        # Calculate overall risk score
        if risk_results:
            overall_risk_score = sum(r.get('risk_score', 0) for r in risk_results) / len(risk_results)
        else:
            overall_risk_score = 0.0
        
        # Save to RiskScoringDocument table
        from .models import RiskScoringDocument
        
        risk_scoring_document = RiskScoringDocument.objects.create(
            data_file=data_file,
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
                'closing_entries_risk': len(closing_entries_analysis.closing_entries) if closing_entries_analysis else 0,
                'high_value_risk': len([t for t in transactions if float(t.amount_local_currency) > 1000000]),
                'unusual_pattern_risk': len(flagged_transactions)
            },
            scoring_criteria={
                'high_risk_threshold': 70,
                'medium_risk_threshold': 40,
                'low_risk_threshold': 0
            },
            risk_calculations=risk_results,
            risk_distributions={
                'high_risk_count': high_risk_count,
                'medium_risk_count': medium_risk_count,
                'low_risk_count': low_risk_count,
                'total_transactions': len(transactions)
            },
            recommendations={
                'high_priority': [r for r in risk_results if r.get('risk_level') == 'HIGH'][:10],
                'medium_priority': [r for r in risk_results if r.get('risk_level') == 'MEDIUM'][:10],
                'low_priority': [r for r in risk_results if r.get('risk_level') == 'LOW'][:10]
            },
            audit_implications={
                'focus_areas': ['high_risk_transactions', 'duplicate_entries', 'backdated_entries'],
                'sampling_strategy': 'risk_based',
                'follow_up_actions': ['detailed_review', 'documentation_verification', 'management_inquiry']
            },
            total_transactions=len(transactions),
            high_risk_transactions=high_risk_count,
            medium_risk_transactions=medium_risk_count,
            low_risk_transactions=low_risk_count,
            overall_risk_score=overall_risk_score,
            processing_duration=(timezone.now() - start_time).total_seconds(),
            status='COMPLETED'
        )
        
        processing_duration = (timezone.now() - start_time).total_seconds()
        
        debug_task_state(task_name, job_id, "COMPLETED", 
                        f"Risk Analysis completed and saved to database in {processing_duration:.2f} seconds")
        
        return {
            'document_id': str(risk_scoring_document.id),
            'status': 'COMPLETED',
            'processing_duration': processing_duration,
            'overall_risk_score': overall_risk_score,
            'high_risk_count': high_risk_count,
            'medium_risk_count': medium_risk_count,
            'low_risk_count': low_risk_count,
            'table': 'RiskScoringDocument'
        }
        
    except Exception as e:
        error_msg = f"Error in Risk Analysis: {str(e)}"
        debug_task_exception(task_name, job_id, e, "RISK_ANALYSIS_ERROR")
        
        # Save failed result to database
        try:
            from .models import RiskScoringDocument
            RiskScoringDocument.objects.create(
                data_file=data_file,
                processing_job=job,
                document_type='comprehensive_risk_scoring',
                document_version='1.0.0',
                status='FAILED',
                error_message=error_msg
            )
        except:
            pass
        
        return {'error': error_msg}

# ============================================================================
# ML MODEL TRAINING TASKS
# ============================================================================

@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def train_ml_models(self, job_id):
    """
    Train ML models and save results to MLModelTraining table
    
    Args:
        job_id (str): UUID of the FileProcessingJob to process
    """
    task_name = "train_ml_models"
    start_time = timezone.now()
    
    debug_task_state(task_name, job_id, "STARTED", f"Task ID: {self.request.id}")
    
    try:
        # Get the processing job and data file
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get all transactions for training
        transactions = SAPGLPosting.objects.all()
        
        debug_task_data(task_name, job_id, "TRANSACTIONS", f"Found {len(transactions)} transactions for training")
        
        if len(transactions) < 10:
            error_msg = "Insufficient data for training. Need at least 10 transactions."
            debug_task_state(task_name, job_id, "FAILED", error_msg)
            
            # Save failed result to database
            MLModelTraining.objects.create(
                session_name=f"Training Session {job_id}",
                description="ML model training session",
                model_type='all',
                training_data_size=len(transactions),
                feature_count=15,  # Set default feature count
                status='FAILED',
                error_message=error_msg
            )
            return {'error': error_msg}
        
        # Create training session
        training_session = MLModelTraining.objects.create(
            session_name=f"Training Session {job_id}",
            description="ML model training session for comprehensive analysis",
            model_type='all',
            training_data_size=len(transactions),
            training_data_date_range={
                'min_date': min(t.posting_date for t in transactions).isoformat(),
                'max_date': max(t.posting_date for t in transactions).isoformat()
            },
            status='TRAINING',
            started_at=timezone.now()
        )
        
        # Train specialized models
        from .specialized_analysis_models import AnalysisModelManager
        model_manager = AnalysisModelManager()
        
        # Train all models
        training_results = model_manager.train_all_models(transactions)
        
        # Calculate performance metrics
        performance_metrics = {
            'models_trained': len(training_results),
            'training_success': all(training_results.values()),
            'feature_count': 15,  # Standard feature count for our models
            'training_duration': (timezone.now() - start_time).total_seconds()
        }
        
        # Update training session
        training_session.status = 'COMPLETED'
        training_session.completed_at = timezone.now()
        training_session.training_duration = performance_metrics['training_duration']
        training_session.performance_metrics = performance_metrics
        training_session.feature_count = performance_metrics['feature_count']
        training_session.save()
        
        processing_duration = (timezone.now() - start_time).total_seconds()
        
        debug_task_state(task_name, job_id, "COMPLETED", 
                        f"ML Model Training completed in {processing_duration:.2f} seconds")
        
        return {
            'training_id': str(training_session.id),
            'status': 'COMPLETED',
            'processing_duration': processing_duration,
            'models_trained': performance_metrics['models_trained'],
            'table': 'MLModelTraining'
        }
        
    except Exception as e:
        error_msg = f"Error in ML Model Training: {str(e)}"
        debug_task_exception(task_name, job_id, e, "ML_TRAINING_ERROR")
        
        # Save failed result to database
        try:
            MLModelTraining.objects.create(
                session_name=f"Training Session {job_id}",
                description="ML model training session",
                model_type='all',
                training_data_size=len(transactions) if 'transactions' in locals() else 0,
                feature_count=15,  # Set default feature count
                status='FAILED',
                error_message=error_msg
            )
        except:
            pass
        
        return {'error': error_msg}

@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def retrain_ml_models(self, job_id):
    """
    Retrain ML models with new data and save results to MLModelTraining table
    
    Args:
        job_id (str): UUID of the FileProcessingJob to process
    """
    task_name = "retrain_ml_models"
    start_time = timezone.now()
    
    debug_task_state(task_name, job_id, "STARTED", f"Task ID: {self.request.id}")
    
    try:
        # Get the processing job and data file
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get all transactions for retraining
        transactions = SAPGLPosting.objects.all()
        
        debug_task_data(task_name, job_id, "TRANSACTIONS", f"Found {len(transactions)} transactions for retraining")
        
        if len(transactions) < 10:
            error_msg = "Insufficient data for retraining. Need at least 10 transactions."
            debug_task_state(task_name, job_id, "FAILED", error_msg)
            
            # Save failed result to database
            MLModelTraining.objects.create(
                session_name=f"Retraining Session {job_id}",
                description="ML model retraining session",
                model_type='all',
                training_data_size=len(transactions),
                feature_count=15,  # Set default feature count
                status='FAILED',
                error_message=error_msg
            )
            return {'error': error_msg}
        
        # Create retraining session
        training_session = MLModelTraining.objects.create(
            session_name=f"Retraining Session {job_id}",
            description="ML model retraining session with new data",
            model_type='all',
            training_data_size=len(transactions),
            training_data_date_range={
                'min_date': min(t.posting_date for t in transactions).isoformat(),
                'max_date': max(t.posting_date for t in transactions).isoformat()
            },
            status='TRAINING',
            started_at=timezone.now()
        )
        
        # Retrain specialized models
        from .specialized_analysis_models import AnalysisModelManager
        model_manager = AnalysisModelManager()
        
        # Retrain all models
        training_results = model_manager.train_all_models(transactions)
        
        # Calculate performance metrics
        performance_metrics = {
            'models_retrained': len(training_results),
            'retraining_success': all(training_results.values()),
            'feature_count': 15,  # Standard feature count for our models
            'training_duration': (timezone.now() - start_time).total_seconds()
        }
        
        # Update training session
        training_session.status = 'COMPLETED'
        training_session.completed_at = timezone.now()
        training_session.training_duration = performance_metrics['training_duration']
        training_session.performance_metrics = performance_metrics
        training_session.feature_count = performance_metrics['feature_count']
        training_session.save()
        
        processing_duration = (timezone.now() - start_time).total_seconds()
        
        debug_task_state(task_name, job_id, "COMPLETED", 
                        f"ML Model Retraining completed in {processing_duration:.2f} seconds")
        
        return {
            'training_id': str(training_session.id),
            'status': 'COMPLETED',
            'processing_duration': processing_duration,
            'models_retrained': performance_metrics['models_retrained'],
            'table': 'MLModelTraining'
        }
        
    except Exception as e:
        error_msg = f"Error in ML Model Retraining: {str(e)}"
        debug_task_exception(task_name, job_id, e, "ML_RETRAINING_ERROR")
        
        # Save failed result to database
        try:
            MLModelTraining.objects.create(
                session_name=f"Retraining Session {job_id}",
                description="ML model retraining session",
                model_type='all',
                training_data_size=len(transactions) if 'transactions' in locals() else 0,
                feature_count=15,  # Set default feature count
                status='FAILED',
                error_message=error_msg
            )
        except:
            pass
        
        return {'error': error_msg}

# ============================================================================
# UTILITY TASKS
# ============================================================================

@shared_task(bind=True)
def debug_task(self):
    """Debug task for testing Celery functionality"""
    task_name = "debug_task"
    task_id = self.request.id
    
    debug_task_state(task_name, task_id, "STARTED", f"Task ID: {task_id}")
    
    try:
        # Get system information
        system_info = get_system_info()
        debug_task_data(task_name, task_id, "SYSTEM_INFO", system_info)
        
        # Simulate some work
        import time
        time.sleep(2)
        
        debug_task_state(task_name, task_id, "COMPLETED", "Debug task completed successfully")
        
        return {
            'task_id': task_id,
            'status': 'COMPLETED',
            'system_info': system_info,
            'message': 'Debug task completed successfully'
        }
        
    except Exception as e:
        debug_task_exception(task_name, task_id, e, "Debug task error")
        return {'error': str(e)}

@shared_task(bind=True)
def worker_health_check(self):
    """Health check task for Celery workers"""
    task_name = "worker_health_check"
    task_id = self.request.id
    
    debug_task_state(task_name, task_id, "STARTED", f"Task ID: {task_id}")
    
    try:
        # Get system information
        system_info = get_system_info()
        
        # Check database connectivity
        from django.db import connection
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            db_status = "OK"
        
        # Check Celery broker connectivity
        from celery import current_app
        broker_status = "OK" if current_app.control.inspect().active() else "ERROR"
        
        health_status = {
            'task_id': task_id,
            'status': 'HEALTHY',
            'system_info': system_info,
            'database': db_status,
            'broker': broker_status,
            'timestamp': timezone.now().isoformat()
        }
        
        debug_task_state(task_name, task_id, "COMPLETED", "Health check completed")
        
        return health_status
        
    except Exception as e:
        debug_task_exception(task_name, task_id, e, "Health check error")
        return {
            'task_id': task_id,
            'status': 'UNHEALTHY',
            'error': str(e),
            'timestamp': timezone.now().isoformat()
        }

@shared_task(bind=True)
def monitor_worker_performance(self):
    """Monitor worker performance and resource usage"""
    task_name = "monitor_worker_performance"
    task_id = self.request.id
    
    debug_task_state(task_name, task_id, "STARTED", f"Task ID: {task_id}")
    
    try:
        # Get detailed system information
        system_info = get_system_info()
        
        # Get Celery worker statistics
        from celery import current_app
        inspect = current_app.control.inspect()
        
        worker_stats = {
            'active_tasks': inspect.active(),
            'registered_tasks': inspect.registered(),
            'worker_stats': inspect.stats(),
            'system_info': system_info,
            'timestamp': timezone.now().isoformat()
        }
        
        debug_task_state(task_name, task_id, "COMPLETED", "Performance monitoring completed")
        
        return worker_stats
        
    except Exception as e:
        debug_task_exception(task_name, task_id, e, "Performance monitoring error")
        return {'error': str(e)} 