"""
Celery tasks for background processing of file uploads with targeted anomaly detection
"""

from celery import shared_task, current_task
from django.utils import timezone
from django.db import transaction
import csv
import io
import logging
import traceback
import psutil
import os
from datetime import datetime
from decimal import Decimal, InvalidOperation

from .models import FileProcessingJob, SAPGLPosting, DataFile, MLModelTraining, AnalysisSession
from .analytics import SAPGLAnalyzer
from .analytics_db_saver import AnalyticsDBSaver, save_analytics_to_db

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

@shared_task(bind=True, max_retries=3, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def process_file_with_anomalies(self, job_id):
    """
    Background task to process file upload with conditional anomaly detection
    
    Args:
        job_id (str): UUID of the FileProcessingJob to process
    """
    task_name = "process_file_with_anomalies"
    start_time = timezone.now()
    
    debug_task_state(task_name, job_id, "STARTED", f"Task ID: {self.request.id}")
    log_task_info(task_name, job_id, f"===== TASK STARTED =====")
    log_task_info(task_name, job_id, f"Task ID: {self.request.id}")
    log_task_info(task_name, job_id, f"Worker: {self.request.hostname}")
    log_task_info(task_name, job_id, f"PID: {self.request.pid}")
    log_task_info(task_name, job_id, f"System info: {get_system_info()}")
    
    try:
        # Get the processing job
        debug_task_state(task_name, job_id, "FETCHING_JOB", "Getting FileProcessingJob from database...")
        log_task_info(task_name, job_id, "Getting FileProcessingJob from database...")
        
        try:
            job = FileProcessingJob.objects.get(id=job_id)
            debug_task_data(task_name, job_id, "JOB_INFO", {
                'id': str(job.id),
                'status': job.status,
                'run_anomalies': job.run_anomalies,
                'requested_anomalies': job.requested_anomalies,
                'file_name': job.data_file.file_name if job.data_file else 'None'
            })
            log_task_info(task_name, job_id, f"Job retrieved successfully")
            log_task_info(task_name, job_id, f"Job status: {job.status}")
            log_task_info(task_name, job_id, f"Run anomalies: {job.run_anomalies}")
            log_task_info(task_name, job_id, f"Requested anomalies: {job.requested_anomalies}")
        except FileProcessingJob.DoesNotExist:
            debug_task_exception(task_name, job_id, Exception(f"Job {job_id} not found"), "Database query")
            raise Exception(f"FileProcessingJob with ID {job_id} not found")
        except Exception as e:
            debug_task_exception(task_name, job_id, e, "Database query")
            raise
        
        # Update job status
        debug_task_state(task_name, job_id, "UPDATING_STATUS", "Updating job status to PROCESSING...")
        log_task_info(task_name, job_id, "Updating job status to PROCESSING...")
        try:
            job.status = 'PROCESSING'
            job.started_at = timezone.now()
            job.save()
            log_task_info(task_name, job_id, "Job status updated successfully")
        except Exception as e:
            debug_task_exception(task_name, job_id, e, "Status update")
            raise
        
        log_task_info(task_name, job_id, f"Start time: {start_time}")
        
        # Process the file content
        debug_task_state(task_name, job_id, "PROCESSING_FILE", "Processing file content...")
        log_task_info(task_name, job_id, "Processing file content...")
        try:
            result = _process_file_content(job)
            debug_task_data(task_name, job_id, "PROCESSING_RESULT", result)
            log_task_info(task_name, job_id, f"File content processing result: {result['success']}")
        except Exception as e:
            debug_task_exception(task_name, job_id, e, "File content processing")
            raise
        
        if result['success']:
            # Initialize database saver for tracking and saving results
            print(f"🔍 DEBUG: ===== Initializing AnalyticsDBSaver =====")
            print(f"🔍 DEBUG: Job ID: {job.id}")
            print(f"🔍 DEBUG: Data file: {job.data_file.file_name}")
            db_saver = AnalyticsDBSaver(job)
            print(f"🔍 DEBUG: AnalyticsDBSaver initialized successfully")
            
            # Run analytics and anomaly detection
            debug_task_state(task_name, job_id, "RUNNING_ANALYTICS", "Running analytics and anomaly detection...")
            log_task_info(task_name, job_id, "Running analytics and anomaly detection...")
            
            # Run default analytics and save to database
            debug_task_state(task_name, job_id, "DEFAULT_ANALYTICS", "Running default analytics...")
            log_task_info(task_name, job_id, "Running default analytics...")
            try:
                analytics_results = _run_default_analytics(result['transactions'], job.data_file)
                debug_task_data(task_name, job_id, "ANALYTICS_RESULTS", analytics_results)
                log_task_info(task_name, job_id, "Default analytics completed successfully")
                
                # Save to database instead of keeping in memory
                print(f"🔍 DEBUG: ===== Saving default analytics to database =====")
                print(f"🔍 DEBUG: Analytics results keys: {list(analytics_results.keys())}")
                db_saver.save_default_analytics(analytics_results)
                print(f"🔍 DEBUG: Default analytics saved to database successfully")
                log_task_info(task_name, job_id, "Default analytics saved to database")
                
            except Exception as analytics_error:
                debug_task_exception(task_name, job_id, analytics_error, "Default analytics")
                log_task_info(task_name, job_id, f"Error in default analytics: {analytics_error}", "error")
                log_task_info(task_name, job_id, f"Analytics error type: {type(analytics_error).__name__}", "error")
                log_task_info(task_name, job_id, f"Analytics error traceback: {traceback.format_exc()}", "error")
                analytics_results = {'error': str(analytics_error)}
            
            # Run comprehensive expense analytics and save to database
            log_task_info(task_name, job_id, "Running comprehensive expense analytics...")
            try:
                expense_analytics = _run_comprehensive_expense_analytics(result['transactions'], job.data_file)
                log_task_info(task_name, job_id, "Comprehensive expense analytics completed successfully")
                
                # Save to database instead of keeping in memory
                print(f"🔍 DEBUG: ===== Saving comprehensive analytics to database =====")
                print(f"🔍 DEBUG: Expense analytics keys: {list(expense_analytics.keys())}")
                db_saver.save_comprehensive_analytics(expense_analytics)
                print(f"🔍 DEBUG: Comprehensive analytics saved to database successfully")
                log_task_info(task_name, job_id, "Comprehensive analytics saved to database")
                
            except Exception as expense_error:
                log_task_info(task_name, job_id, f"Error in comprehensive expense analytics: {expense_error}", "error")
                log_task_info(task_name, job_id, f"Expense error type: {type(expense_error).__name__}", "error")
                log_task_info(task_name, job_id, f"Expense error traceback: {traceback.format_exc()}", "error")
                expense_analytics = {'error': str(expense_error)}
            
            # Run duplicate analysis and save to database
            log_task_info(task_name, job_id, "Running duplicate analysis...")
            try:
                duplicate_results = _run_duplicate_analysis(result['transactions'], job.data_file)
                log_task_info(task_name, job_id, "Duplicate analysis completed successfully")
                
                # Save to database instead of keeping in memory
                print(f"🔍 DEBUG: ===== Saving duplicate analysis to database =====")
                print(f"🔍 DEBUG: Duplicate results keys: {list(duplicate_results.keys())}")
                db_saver.save_duplicate_analysis(duplicate_results)
                print(f"🔍 DEBUG: Duplicate analysis saved to database successfully")
                log_task_info(task_name, job_id, "Duplicate analysis saved to database")
                
            except Exception as duplicate_error:
                log_task_info(task_name, job_id, f"Error in duplicate analysis: {duplicate_error}", "error")
                log_task_info(task_name, job_id, f"Duplicate error type: {type(duplicate_error).__name__}", "error")
                log_task_info(task_name, job_id, f"Duplicate error traceback: {traceback.format_exc()}", "error")
                duplicate_results = {'error': str(duplicate_error)}
            
            # Run requested anomaly tests and save to database
            log_task_info(task_name, job_id, "Running requested anomaly tests...")
            anomaly_results = {}
            if job.run_anomalies and job.requested_anomalies:
                try:
                    anomaly_results = _run_requested_anomalies(
                        result['transactions'], 
                        job.requested_anomalies
                    )
                    log_task_info(task_name, job_id, "Requested anomalies completed successfully")
                    
                    # Save to database instead of keeping in memory
                    print(f"🔍 DEBUG: ===== Saving anomaly detection results to database =====")
                    print(f"🔍 DEBUG: Anomaly results keys: {list(anomaly_results.keys())}")
                    db_saver.save_anomaly_detection_results(anomaly_results)
                    print(f"🔍 DEBUG: Anomaly detection results saved to database successfully")
                    log_task_info(task_name, job_id, "Anomaly detection results saved to database")
                    
                except Exception as anomaly_error:
                    log_task_info(task_name, job_id, f"Error in requested anomalies: {anomaly_error}", "error")
                    log_task_info(task_name, job_id, f"Anomaly error type: {type(anomaly_error).__name__}", "error")
                    log_task_info(task_name, job_id, f"Anomaly error traceback: {traceback.format_exc()}", "error")
                    anomaly_results = {'error': str(anomaly_error)}
            else:
                log_task_info(task_name, job_id, "No anomalies requested, skipping")
            
            # Auto-train ML models and save to database
            log_task_info(task_name, job_id, "Running ML model training...")
            try:
                ml_training_result = _auto_train_ml_models(result['transactions'], job.data_file)
                log_task_info(task_name, job_id, "ML training completed successfully")
                
                # Save to database instead of keeping in memory
                print(f"🔍 DEBUG: ===== Saving ML training results to database =====")
                print(f"🔍 DEBUG: ML training result keys: {list(ml_training_result.keys())}")
                db_saver.save_ml_processing_result(ml_training_result, 'all')
                print(f"🔍 DEBUG: ML training results saved to database successfully")
                log_task_info(task_name, job_id, "ML training results saved to database")
                
            except Exception as ml_error:
                log_task_info(task_name, job_id, f"Error in ML training: {ml_error}", "error")
                log_task_info(task_name, job_id, f"ML error type: {type(ml_error).__name__}", "error")
                log_task_info(task_name, job_id, f"ML error traceback: {traceback.format_exc()}", "error")
                ml_training_result = {'error': str(ml_error)}
            
            # Calculate processing duration
            end_time = timezone.now()
            processing_duration = (end_time - start_time).total_seconds()
            log_task_info(task_name, job_id, f"Processing duration: {processing_duration:.2f} seconds")
            
            # Finalize processing and update job status
            log_task_info(task_name, job_id, "Finalizing processing...")
            try:
                # Finalize the processing with success
                print(f"🔍 DEBUG: ===== Finalizing processing =====")
                db_saver.finalize_processing(success=True)
                print(f"🔍 DEBUG: Processing finalized successfully")
                
                # Update job status to completed
                job.status = 'COMPLETED'
                job.completed_at = end_time
                job.processing_duration = processing_duration
                job.save()
                
                log_task_info(task_name, job_id, "Job completed and all results saved to database")
            except Exception as save_error:
                log_task_info(task_name, job_id, f"Error finalizing processing: {save_error}", "error")
                log_task_info(task_name, job_id, f"Save error type: {type(save_error).__name__}", "error")
                log_task_info(task_name, job_id, f"Save error traceback: {traceback.format_exc()}", "error")
                raise
            
            log_task_info(task_name, job_id, "Background processing completed successfully")
            
        else:
            log_task_info(task_name, job_id, f"File processing failed: {result['error']}", "error")
            # Handle processing failure
            try:
                db_saver = AnalyticsDBSaver(job)
                db_saver.finalize_processing(success=False, error_message=result['error'])
            except:
                pass  # Don't let tracker errors prevent job failure
            
            job.status = 'FAILED'
            job.error_message = result['error']
            job.completed_at = timezone.now()
            job.save()
            
            log_task_info(task_name, job_id, f"Background processing failed: {result['error']}", "error")
            
    except FileProcessingJob.DoesNotExist:
        log_task_info(task_name, job_id, f"FileProcessingJob {job_id} not found", "error")
        logger.error(f"FileProcessingJob {job_id} not found")
    except Exception as e:
        log_task_info(task_name, job_id, f"Unexpected error in background processing!", "error")
        log_task_info(task_name, job_id, f"Error type: {type(e).__name__}", "error")
        log_task_info(task_name, job_id, f"Error message: {str(e)}", "error")
        log_task_info(task_name, job_id, f"Error traceback: {traceback.format_exc()}", "error")
        logger.error(f"Unexpected error in background processing for job {job_id}: {e}")
        
        # Update job status to failed
        try:
            job = FileProcessingJob.objects.get(id=job_id)
            
            # Try to finalize processing with error
            try:
                db_saver = AnalyticsDBSaver(job)
                db_saver.finalize_processing(success=False, error_message=str(e))
            except:
                pass  # Don't let tracker errors prevent job failure
            
            job.status = 'FAILED'
            job.error_message = f"Unexpected error: {str(e)}\nTraceback: {traceback.format_exc()}"
            job.completed_at = timezone.now()
            job.save()
            log_task_info(task_name, job_id, "Job status updated to FAILED")
        except Exception as update_error:
            log_task_info(task_name, job_id, f"Error updating job status: {update_error}", "error")
            pass
    
    finally:
        # Log final system info
        final_system_info = get_system_info()
        log_task_info(task_name, job_id, f"Final system info: {final_system_info}")
        log_task_info(task_name, job_id, f"===== TASK COMPLETED =====")

def _process_file_content(job):
    """Get transactions from SAPGLPosting model instead of creating sample data"""
    task_name = "_process_file_content"
    job_id = str(job.id)
    
    debug_task_state(task_name, job_id, "STARTED", "Getting transactions from database...")
    print(f"🔍 DEBUG: ===== _process_file_content STARTED =====")
    print(f"🔍 DEBUG: Job ID: {job.id}")
    print(f"🔍 DEBUG: Data file: {job.data_file.file_name}")
    
    try:
        # Get transactions from SAPGLPosting model instead of creating sample data
        debug_task_state(task_name, job_id, "GETTING_TRANSACTIONS", "Getting transactions from database...")
        print(f"🔍 DEBUG: Getting transactions from database...")
        
        # Get transactions from the database related to this specific job
        # For testing and when no specific filtering is needed, get all transactions
        # In production, you might want to filter by specific criteria
        try:
            # Try to filter by transactions created after the file was uploaded
            transactions = list(SAPGLPosting.objects.filter(
                created_at__gte=job.data_file.uploaded_at
            ).order_by('created_at'))
            
            # If no transactions found with the filter, get all transactions
            if not transactions:
                print(f"🔍 DEBUG: No transactions found with upload date filter, getting all transactions")
                transactions = list(SAPGLPosting.objects.all().order_by('created_at'))
        except Exception as e:
            print(f"🔍 DEBUG: Error filtering transactions, getting all transactions: {e}")
            transactions = list(SAPGLPosting.objects.all().order_by('created_at'))
        debug_task_data(task_name, job_id, "DATABASE_TRANSACTIONS", transactions)
        print(f"🔍 DEBUG: Retrieved {len(transactions)} transactions from database")
        
        # Log some sample transactions for debugging
        if transactions:
            sample_transactions = transactions[:5]
            print(f"🔍 DEBUG: Sample transactions:")
            for i, t in enumerate(sample_transactions):
                print(f"  {i+1}. Doc: {t.document_number}, Amount: {t.amount_local_currency}, User: {t.user_name}")
        
        # Update DataFile record with actual counts
        debug_task_state(task_name, job_id, "UPDATING_DATA_FILE", "Updating DataFile record...")
        print(f"🔍 DEBUG: Updating DataFile record...")
        try:
            data_file = job.data_file
            data_file.total_records = len(transactions)
            data_file.processed_records = len(transactions)
            data_file.failed_records = 0
            data_file.status = 'COMPLETED'
            data_file.processed_at = timezone.now()
            data_file.save()
            print(f"🔍 DEBUG: DataFile record updated successfully")
        except Exception as data_file_error:
            debug_task_exception(task_name, job_id, data_file_error, "Updating DataFile")
            print(f"🔍 DEBUG: Error updating DataFile: {data_file_error}")
            print(f"🔍 DEBUG: DataFile error type: {type(data_file_error).__name__}")
            raise
        
        result = {
            'success': True,
            'transactions': transactions
        }
        debug_task_data(task_name, job_id, "FINAL_RESULT", result)
        debug_task_state(task_name, job_id, "COMPLETED", f"Returning success result with {len(transactions)} transactions from database")
        print(f"🔍 DEBUG: Returning success result with {len(transactions)} transactions from database")
        return result
        
    except Exception as e:
        debug_task_exception(task_name, job_id, e, "Main file processing")
        print(f"🔍 DEBUG: Error in _process_file_content!")
        print(f"🔍 DEBUG: Error type: {type(e).__name__}")
        print(f"🔍 DEBUG: Error message: {str(e)}")
        logger.error(f"Error processing file content: {e}")
        return {
            'success': False,
            'error': str(e)
        }

# Removed _create_sample_transactions function - no longer needed
# The system now uses actual data from SAPGLPosting model

def _run_default_analytics(transactions, data_file):
    """Run default analytics (TB, TE, GL summaries)"""
    task_name = "_run_default_analytics"
    job_id = str(data_file.id) if data_file else "unknown"
    
    debug_task_state(task_name, job_id, "STARTED", "Running default analytics...")
    
    try:
        debug_task_state(task_name, job_id, "CALCULATING_STATISTICS", "Calculating basic statistics...")
        
        # Calculate basic statistics
        total_transactions = len(transactions)
        total_amount = sum(t.amount_local_currency for t in transactions)
        
        # Trial Balance calculation
        total_debits = sum(t.amount_local_currency for t in transactions if t.transaction_type == 'DEBIT')
        total_credits = sum(t.amount_local_currency for t in transactions if t.transaction_type == 'CREDIT')
        trial_balance = total_debits - total_credits
        
        # GL Account summaries
        gl_accounts = {}
        for transaction in transactions:
            account_id = transaction.gl_account
            if account_id not in gl_accounts:
                gl_accounts[account_id] = {
                    'account_id': account_id,
                    'total_debits': Decimal('0.00'),
                    'total_credits': Decimal('0.00'),
                    'transaction_count': 0
                }
            
            gl_accounts[account_id]['transaction_count'] += 1
            if transaction.transaction_type == 'DEBIT':
                gl_accounts[account_id]['total_debits'] += transaction.amount_local_currency
            else:
                gl_accounts[account_id]['total_credits'] += transaction.amount_local_currency
        
        # Calculate trial balance for each account
        for account_data in gl_accounts.values():
            account_data['trial_balance'] = float(account_data['total_debits'] - account_data['total_credits'])
            account_data['total_debits'] = float(account_data['total_debits'])
            account_data['total_credits'] = float(account_data['total_credits'])
        
        return {
            'total_transactions': total_transactions,
            'total_amount': float(total_amount),
            'total_debits': float(total_debits),
            'total_credits': float(total_credits),
            'trial_balance': float(trial_balance),
            'gl_account_summaries': list(gl_accounts.values()),
            'unique_accounts': len(gl_accounts),
            'unique_users': len(set(t.user_name for t in transactions)),
            'date_range': {
                'min_date': min(t.posting_date for t in transactions if t.posting_date).isoformat() if transactions and any(t.posting_date for t in transactions) else None,
                'max_date': max(t.posting_date for t in transactions if t.posting_date).isoformat() if transactions and any(t.posting_date for t in transactions) else None
            } if transactions else {}
        }
        
    except Exception as e:
        logger.error(f"Error running default analytics: {e}")
        return {'error': str(e)}

def _run_requested_anomalies(transactions, requested_anomalies):
    """Run requested anomaly tests"""
    task_name = "_run_requested_anomalies"
    job_id = "unknown"  # We don't have job context here
    
    debug_task_state(task_name, job_id, "STARTED", "Running requested anomalies...")
    print(f"🔍 DEBUG: ===== _run_requested_anomalies STARTED =====")
    print(f"🔍 DEBUG: Transactions count: {len(transactions)}")
    print(f"🔍 DEBUG: Requested anomalies: {requested_anomalies}")
    
    try:
        debug_task_state(task_name, job_id, "CREATING_ANALYZER", "Creating SAPGLAnalyzer...")
        print(f"🔍 DEBUG: Creating SAPGLAnalyzer...")
        analyzer = SAPGLAnalyzer()
        print(f"🔍 DEBUG: SAPGLAnalyzer created successfully")
        results = {}
        
        # Map anomaly types to analyzer methods
        print(f"🔍 DEBUG: Setting up anomaly methods mapping...")
        anomaly_methods = {
            'duplicate': analyzer.detect_duplicate_entries,
            'backdated': analyzer.detect_backdated_entries,
            'closing': analyzer.detect_closing_entries,
            'unusual_days': analyzer.detect_unusual_days,
            'holiday': analyzer.detect_holiday_entries,
            'user_anomalies': analyzer.detect_user_anomalies,
        }
        print(f"🔍 DEBUG: Anomaly methods mapping created")
        
        for anomaly_type in requested_anomalies:
            print(f"🔍 DEBUG: Processing anomaly type: {anomaly_type}")
            if anomaly_type in anomaly_methods:
                print(f"🔍 DEBUG: Anomaly type {anomaly_type} found in methods")
                try:
                    method = anomaly_methods[anomaly_type]
                    print(f"🔍 DEBUG: Calling method for {anomaly_type}...")
                    anomaly_results = method(transactions)
                    # Handle different return types from anomaly methods
                    if isinstance(anomaly_results, dict):
                        # If it returns a dict with 'duplicates' key
                        if 'duplicates' in anomaly_results:
                            anomalies_found = len(anomaly_results['duplicates'])
                            details = anomaly_results['duplicates'][:10] if anomaly_results['duplicates'] else []
                        else:
                            anomalies_found = len(anomaly_results)
                            details = list(anomaly_results.values())[:10] if anomaly_results else []
                    elif isinstance(anomaly_results, list):
                        anomalies_found = len(anomaly_results)
                        details = anomaly_results[:10]
                    else:
                        anomalies_found = 0
                        details = []
                    
                    print(f"🔍 DEBUG: Method {anomaly_type} completed, found {anomalies_found} anomalies")
                    results[anomaly_type] = {
                        'anomalies_found': anomalies_found,
                        'details': details
                    }
                    print(f"🔍 DEBUG: Results for {anomaly_type} stored successfully")
                except Exception as e:
                    print(f"🔍 DEBUG: Error running {anomaly_type} anomaly detection!")
                    print(f"🔍 DEBUG: Error type: {type(e).__name__}")
                    print(f"🔍 DEBUG: Error message: {str(e)}")
                    logger.error(f"Error running {anomaly_type} anomaly detection: {e}")
                    results[anomaly_type] = {
                        'anomalies_found': 0,
                        'error': str(e)
                    }
            else:
                print(f"🔍 DEBUG: Anomaly type {anomaly_type} not found in methods")
        
        print(f"🔍 DEBUG: All anomaly types processed")
        print(f"🔍 DEBUG: Final results: {list(results.keys())}")
        return results
        
    except Exception as e:
        print(f"🔍 DEBUG: General error in _run_requested_anomalies!")
        print(f"🔍 DEBUG: Error type: {type(e).__name__}")
        print(f"🔍 DEBUG: Error message: {str(e)}")
        logger.error(f"Error running requested anomalies: {e}")
        return {'error': str(e)}

def _auto_train_ml_models(transactions, data_file):
    """Automatically train ML models if sufficient data is available"""
    task_name = "_auto_train_ml_models"
    job_id = str(data_file.id) if data_file else "unknown"
    
    debug_task_state(task_name, job_id, "STARTED", "Auto-training ML models with stored data...")
    
    try:
        debug_task_state(task_name, job_id, "CHECKING_DATA_SUFFICIENCY", f"Checking if {len(transactions)} stored transactions are sufficient...")
        
        if len(transactions) < 10:
            debug_task_state(task_name, job_id, "SKIPPED", f"Insufficient data for training. Found {len(transactions)} transactions, need at least 10.")
            return {
                'status': 'SKIPPED',
                'reason': f'Insufficient data for training. Found {len(transactions)} transactions, need at least 10.',
                'transactions_count': len(transactions),
                'data_source': 'database'
            }
        
        # Check if we already have trained models
        from .ml_models import MLAnomalyDetector
        ml_detector = MLAnomalyDetector()
        
        # Try to load existing models
        try:
            ml_detector.load_models_from_memory()
            if ml_detector.is_trained:
                return {
                    'status': 'SKIPPED',
                    'reason': 'ML models already trained and loaded',
                    'transactions_count': len(transactions),
                    'models_loaded': True,
                    'data_source': 'database'
                }
        except:
            pass
        
        # Create training session
        training_session = MLModelTraining.objects.create(
            session_name=f'Auto-Training-{data_file.file_name}-{timezone.now().strftime("%Y%m%d_%H%M%S")}',
            description=f'Automatic ML model training triggered by file upload: {data_file.file_name}',
            model_type='all',
            training_data_size=len(transactions),
            feature_count=10,  # Default feature count for ML models
            training_parameters={
                'auto_training': True,
                'source_file': str(data_file.id),
                'transactions_count': len(transactions)
            },
            status='PENDING'
        )
        
        # ML training will be picked up by queue processor
        # No direct Celery call needed
        
        return {
            'status': 'STARTED',
            'training_session_id': str(training_session.id),
            'session_name': training_session.session_name,
            'transactions_count': len(transactions),
            'message': 'ML model training started in background'
        }
        
    except Exception as e:
        logger.error(f"Error in auto ML training: {e}")
        return {
            'status': 'FAILED',
            'error': str(e),
            'transactions_count': len(transactions)
        }

def _run_duplicate_analysis(transactions, data_file):
    """Run enhanced duplicate analysis with ML model enhancement"""
    task_name = "_run_duplicate_analysis"
    job_id = str(data_file.id) if data_file else "unknown"
    
    debug_task_state(task_name, job_id, "STARTED", "Running enhanced duplicate analysis...")
    
    try:
        if not transactions:
            debug_task_state(task_name, job_id, "SKIPPED", "No transactions to analyze")
            return {'error': 'No transactions to analyze'}
        
        # Step 1: Always run enhanced duplicate analysis first (primary method)
        debug_task_state(task_name, job_id, "RUNNING_ENHANCED_ANALYSIS", "Running enhanced duplicate analysis...")
        print(f"🔍 DEBUG: Running enhanced duplicate analysis as primary method")
        
        try:
            from .enhanced_duplicate_analysis import EnhancedDuplicateAnalyzer
            enhanced_analyzer = EnhancedDuplicateAnalyzer()
            enhanced_result = enhanced_analyzer.analyze_duplicates(transactions)
            
            debug_task_state(task_name, job_id, "ENHANCED_COMPLETED", f"Enhanced duplicate analysis completed with {len(enhanced_result.get('duplicate_list', []))} duplicates")
            print(f"🔍 DEBUG: Enhanced duplicate analysis completed with {len(enhanced_result.get('duplicate_list', []))} duplicates")
            
            # Log detailed analysis info
            analysis_info = enhanced_result.get('analysis_info', {})
            print(f"🔍 DEBUG: Analysis Info: {analysis_info}")
            
            # Log breakdowns
            breakdowns = enhanced_result.get('breakdowns', {})
            if breakdowns:
                print(f"🔍 DEBUG: Duplicate Types Found: {list(breakdowns.get('type_breakdown', {}).keys())}")
                print(f"🔍 DEBUG: Risk Breakdown: {breakdowns.get('risk_breakdown', {})}")
            
            # Step 2: Enhance with ML model if available (optional enhancement)
            ml_enhancement = {}
            try:
                from .ml_models import MLAnomalyDetector
                ml_detector = MLAnomalyDetector()
                
                if ml_detector.duplicate_model:
                    debug_task_state(task_name, job_id, "ENHANCING_WITH_ML", "Enhancing results with ML model...")
                    print(f"🔍 DEBUG: Enhancing enhanced analysis with ML model")
                    
                    try:
                        # Run ML analysis to enhance the results (without saving to old system)
                        file_id = str(data_file.id)
                        
                        # Get ML analysis without saving to old system
                        if hasattr(ml_detector.duplicate_model, 'get_comprehensive_duplicate_analysis'):
                            ml_result = ml_detector.duplicate_model.get_comprehensive_duplicate_analysis(transactions)
                        else:
                            # Fallback: create basic ML enhancement info
                            ml_result = {
                                'duplicate_list': [],
                                'model_accuracy': 'N/A'
                            }
                        
                        if ml_result and ml_result.get('duplicate_list'):
                            # Merge ML insights with enhanced analysis
                            ml_enhancement = {
                                'ml_model_available': True,
                                'ml_duplicates_found': len(ml_result.get('duplicate_list', [])),
                                'ml_model_trained': ml_detector.duplicate_model.is_trained(),
                                'ml_accuracy': ml_result.get('model_accuracy', 'N/A'),
                                'ml_enhanced_duplicates': ml_result.get('duplicate_list', [])
                            }
                            
                            debug_task_state(task_name, job_id, "ML_ENHANCEMENT_COMPLETED", f"ML enhancement completed with {len(ml_result.get('duplicate_list', []))} ML duplicates")
                            print(f"🔍 DEBUG: ML enhancement completed with {len(ml_result.get('duplicate_list', []))} ML duplicates")
                        else:
                            ml_enhancement = {
                                'ml_model_available': True,
                                'ml_duplicates_found': 0,
                                'ml_model_trained': ml_detector.duplicate_model.is_trained(),
                                'ml_accuracy': 'N/A',
                                'ml_enhanced_duplicates': []
                            }
                            print(f"🔍 DEBUG: ML model available but no additional duplicates found")
                            
                    except Exception as ml_error:
                        ml_enhancement = {
                            'ml_model_available': True,
                            'ml_error': str(ml_error),
                            'ml_duplicates_found': 0
                        }
                        print(f"🔍 DEBUG: ML enhancement failed: {ml_error}")
                else:
                    ml_enhancement = {
                        'ml_model_available': False,
                        'ml_duplicates_found': 0
                    }
                    print(f"🔍 DEBUG: ML model not available for enhancement")
                    
            except Exception as ml_init_error:
                ml_enhancement = {
                    'ml_model_available': False,
                    'ml_error': str(ml_init_error),
                    'ml_duplicates_found': 0
                }
                print(f"🔍 DEBUG: ML model initialization failed: {ml_init_error}")
            
            # Step 3: Combine enhanced analysis with ML enhancement
            final_result = {
                **enhanced_result,
                'ml_enhancement': ml_enhancement,
                'analysis_method': 'enhanced_with_ml_enhancement',
                'enhanced_analysis_used': True,
                'ml_enhancement_used': ml_enhancement.get('ml_model_available', False)
            }
            
            debug_task_state(task_name, job_id, "COMPLETED", f"Enhanced duplicate analysis with ML enhancement completed")
            print(f"🔍 DEBUG: Final result combines enhanced analysis with ML enhancement")
            return final_result
            
        except Exception as enhanced_error:
            debug_task_state(task_name, job_id, "ENHANCED_FAILED", f"Enhanced analyzer failed: {str(enhanced_error)}")
            print(f"🔍 DEBUG: Enhanced analyzer failed: {enhanced_error}")
            
            # Fallback to basic duplicate detection
            debug_task_state(task_name, job_id, "USING_BASIC_DETECTION", "Using basic duplicate detection as fallback")
            print(f"🔍 DEBUG: Using basic duplicate detection as fallback")
            
            try:
                # Basic duplicate detection
                duplicates = []
                seen_combinations = set()
                
                for t in transactions:
                    # Create combination key
                    key = f"{t.gl_account}_{t.amount_local_currency}_{t.user_name}_{t.posting_date}"
                    
                    if key in seen_combinations:
                        duplicates.append({
                            'id': str(t.id),
                            'gl_account': t.gl_account,
                            'amount': float(t.amount_local_currency),
                            'user_name': t.user_name,
                            'posting_date': t.posting_date.isoformat() if t.posting_date else None,
                            'duplicate_type': 'Basic Duplicate',
                            'risk_score': 50,
                            'document_number': t.document_number,
                            'text': t.text or ''
                        })
                    else:
                        seen_combinations.add(key)
                
                basic_result = {
                    'analysis_info': {
                        'total_transactions': len(transactions),
                        'total_duplicate_groups': len(duplicates),
                        'total_duplicate_transactions': len(duplicates),
                        'total_amount_involved': sum(d['amount'] for d in duplicates),
                        'analysis_date': timezone.now().isoformat()
                    },
                    'duplicate_list': duplicates,
                    'chart_data': {},
                    'breakdowns': {},
                    'slicer_filters': {},
                    'summary_table': duplicates,
                    'export_data': duplicates,
                    'detailed_insights': {},
                    'ml_enhancement': {
                        'ml_model_available': False,
                        'ml_duplicates_found': 0
                    },
                    'analysis_method': 'basic_fallback',
                    'enhanced_analysis_used': False,
                    'ml_enhancement_used': False
                }
                
                debug_task_state(task_name, job_id, "COMPLETED", f"Basic duplicate detection completed with {len(duplicates)} duplicates")
                print(f"🔍 DEBUG: Basic duplicate detection completed with {len(duplicates)} duplicates")
                return basic_result
                
            except Exception as basic_error:
                debug_task_state(task_name, job_id, "FAILED", f"Basic detection failed: {str(basic_error)}")
                print(f"🔍 DEBUG: Basic detection failed: {basic_error}")
                return {'error': f'All duplicate analysis methods failed: {str(basic_error)}'}
        
    except Exception as e:
        debug_task_exception(task_name, job_id, e, "Duplicate analysis")
        logger.error(f"Error in duplicate analysis: {e}")
        return {'error': str(e)}

def _run_comprehensive_expense_analytics(transactions, data_file):
    """Run comprehensive expense analytics on the file data"""
    try:
        if not transactions:
            return {'error': 'No transactions to analyze'}
        
        # Initialize analyzer
        analyzer = SAPGLAnalyzer()
        
        # Create analysis session for comprehensive analysis
        analysis_session = AnalysisSession.objects.create(
            session_name=f"Auto-Analysis-{data_file.file_name}",
            description=f"Automatic analysis for {data_file.file_name}",
            date_from=min(t.posting_date for t in transactions if t.posting_date) if any(t.posting_date for t in transactions) else None,
            date_to=max(t.posting_date for t in transactions if t.posting_date) if any(t.posting_date for t in transactions) else None,
            status='PENDING'
        )
        
        # Run comprehensive analysis using the session
        analysis_result = analyzer.analyze_transactions(analysis_session)
        
        # Generate detailed expense breakdown
        expense_breakdown = _generate_expense_breakdown(transactions)
        
        # Generate user expense patterns
        user_patterns = _generate_user_expense_patterns(transactions)
        
        # Generate account expense patterns
        account_patterns = _generate_account_expense_patterns(transactions)
        
        # Generate temporal patterns
        temporal_patterns = _generate_temporal_patterns(transactions)
        
        # Generate risk assessment
        risk_assessment = _generate_risk_assessment(transactions)
        
        return {
            'summary': {
                'total_transactions': len(transactions),
                'total_amount': float(sum(t.amount_local_currency for t in transactions)),
                'unique_users': len(set(t.user_name for t in transactions)),
                'unique_accounts': len(set(t.gl_account for t in transactions)),
                'date_range': {
                    'start': min(t.posting_date for t in transactions if t.posting_date).isoformat() if any(t.posting_date for t in transactions) else None,
                    'end': max(t.posting_date for t in transactions if t.posting_date).isoformat() if any(t.posting_date for t in transactions) else None
                }
            },
            'expense_breakdown': expense_breakdown,
            'user_patterns': user_patterns,
            'account_patterns': account_patterns,
            'temporal_patterns': temporal_patterns,
            'risk_assessment': risk_assessment,
            'analysis_details': analysis_result
        }
        
    except Exception as e:
        logger.error(f"Error in comprehensive expense analytics: {e}")
        return {'error': str(e)}

def _generate_expense_breakdown(transactions):
    """Generate detailed expense breakdown by category"""
    try:
        # Group by GL account
        account_totals = {}
        for transaction in transactions:
            account = transaction.gl_account
            if account not in account_totals:
                account_totals[account] = {
                    'count': 0,
                    'total_amount': 0,
                    'avg_amount': 0,
                    'min_amount': float('inf'),
                    'max_amount': 0
                }
            
            amount = float(transaction.amount_local_currency)
            account_totals[account]['count'] += 1
            account_totals[account]['total_amount'] += amount
            account_totals[account]['min_amount'] = min(account_totals[account]['min_amount'], amount)
            account_totals[account]['max_amount'] = max(account_totals[account]['max_amount'], amount)
        
        # Calculate averages
        for account in account_totals:
            account_totals[account]['avg_amount'] = account_totals[account]['total_amount'] / account_totals[account]['count']
        
        # Sort by total amount
        sorted_accounts = sorted(account_totals.items(), key=lambda x: x[1]['total_amount'], reverse=True)
        
        return {
            'by_account': dict(sorted_accounts),
            'top_accounts': sorted_accounts[:10],
            'total_accounts': len(account_totals)
        }
        
    except Exception as e:
        logger.error(f"Error generating expense breakdown: {e}")
        return {'error': str(e)}

def _generate_user_expense_patterns(transactions):
    """Generate user expense patterns and analysis"""
    try:
        # Group by user
        user_totals = {}
        
        for transaction in transactions:
            user = transaction.user_name
            if user not in user_totals:
                user_totals[user] = {
                    'count': 0,
                    'total_amount': 0,
                    'avg_amount': 0,
                    'min_amount': float('inf'),
                    'max_amount': 0,
                    'accounts_used': set(),
                    'date_range': {'min': None, 'max': None}
                }
            
            amount = float(transaction.amount_local_currency)
            user_totals[user]['count'] += 1
            user_totals[user]['total_amount'] += amount
            user_totals[user]['min_amount'] = min(user_totals[user]['min_amount'], amount)
            user_totals[user]['max_amount'] = max(user_totals[user]['max_amount'], amount)
            user_totals[user]['accounts_used'].add(transaction.gl_account)
            
            if transaction.posting_date:
                if user_totals[user]['date_range']['min'] is None or transaction.posting_date < user_totals[user]['date_range']['min']:
                    user_totals[user]['date_range']['min'] = transaction.posting_date
                if user_totals[user]['date_range']['max'] is None or transaction.posting_date > user_totals[user]['date_range']['max']:
                    user_totals[user]['date_range']['max'] = transaction.posting_date
        
        # Calculate averages and convert sets to lists
        for user in user_totals:
            user_totals[user]['avg_amount'] = user_totals[user]['total_amount'] / user_totals[user]['count']
            user_totals[user]['accounts_used'] = list(user_totals[user]['accounts_used'])
            user_totals[user]['accounts_count'] = len(user_totals[user]['accounts_used'])
            
            # Convert dates to ISO format
            if user_totals[user]['date_range']['min']:
                user_totals[user]['date_range']['min'] = user_totals[user]['date_range']['min'].isoformat()
            if user_totals[user]['date_range']['max']:
                user_totals[user]['date_range']['max'] = user_totals[user]['date_range']['max'].isoformat()
        
        # Sort by total amount
        sorted_users = sorted(user_totals.items(), key=lambda x: x[1]['total_amount'], reverse=True)
        
        return {
            'by_user': dict(sorted_users),
            'top_users': sorted_users[:10],
            'total_users': len(user_totals),
            'user_activity': {
                'most_active': sorted_users[0] if sorted_users else None,
                'highest_spender': sorted_users[0] if sorted_users else None,
                'most_accounts': max(user_totals.items(), key=lambda x: x[1]['accounts_count']) if user_totals else None
            }
        }
        
    except Exception as e:
        logger.error(f"Error generating user patterns: {e}")
        return {'error': str(e)}

def _generate_account_expense_patterns(transactions):
    """Generate account expense patterns and analysis"""
    try:
        # Group by account and analyze patterns
        account_analysis = {}
        
        for transaction in transactions:
            account = transaction.gl_account
            if account not in account_analysis:
                account_analysis[account] = {
                    'transactions': [],
                    'users': set(),
                    'amounts': [],
                    'dates': []
                }
            
            account_analysis[account]['transactions'].append(transaction)
            account_analysis[account]['users'].add(transaction.user_name)
            account_analysis[account]['amounts'].append(float(transaction.amount_local_currency))
            if transaction.posting_date:
                account_analysis[account]['dates'].append(transaction.posting_date)
        
        # Analyze patterns for each account
        for account, data in account_analysis.items():
            amounts = data['amounts']
            dates = data['dates']
            
            data['summary'] = {
                'total_transactions': len(data['transactions']),
                'total_amount': sum(amounts),
                'avg_amount': sum(amounts) / len(amounts) if amounts else 0,
                'min_amount': min(amounts) if amounts else 0,
                'max_amount': max(amounts) if amounts else 0,
                'unique_users': len(data['users']),
                'users_list': list(data['users']),
                'date_range': {
                    'min': min(dates).isoformat() if dates else None,
                    'max': max(dates).isoformat() if dates else None
                }
            }
            
            # Remove raw data to keep response clean
            del data['transactions']
            del data['amounts']
            del data['dates']
            data['users'] = list(data['users'])
        
        return {
            'by_account': account_analysis,
            'total_accounts': len(account_analysis),
            'most_used_accounts': sorted(account_analysis.items(), key=lambda x: x[1]['summary']['total_transactions'], reverse=True)[:10]
        }
        
    except Exception as e:
        logger.error(f"Error generating account patterns: {e}")
        return {'error': str(e)}

def _generate_temporal_patterns(transactions):
    """Generate temporal patterns in expense data"""
    try:
        # Group by month
        monthly_data = {}
        # Group by day of week
        daily_data = {}
        
        for transaction in transactions:
            if not transaction.posting_date:
                continue
            
            # Monthly patterns
            month_key = transaction.posting_date.strftime('%Y-%m')
            if month_key not in monthly_data:
                monthly_data[month_key] = {'count': 0, 'amount': 0}
            monthly_data[month_key]['count'] += 1
            monthly_data[month_key]['amount'] += float(transaction.amount_local_currency)
            
            # Daily patterns
            day_key = transaction.posting_date.strftime('%A')
            if day_key not in daily_data:
                daily_data[day_key] = {'count': 0, 'amount': 0}
            daily_data[day_key]['count'] += 1
            daily_data[day_key]['amount'] += float(transaction.amount_local_currency)
        
        # Calculate averages
        for month in monthly_data:
            monthly_data[month]['avg_amount'] = monthly_data[month]['amount'] / monthly_data[month]['count']
        
        for day in daily_data:
            daily_data[day]['avg_amount'] = daily_data[day]['amount'] / daily_data[day]['count']
        
        return {
            'monthly_patterns': monthly_data,
            'daily_patterns': daily_data,
            'peak_months': sorted(monthly_data.items(), key=lambda x: x[1]['amount'], reverse=True)[:3],
            'peak_days': sorted(daily_data.items(), key=lambda x: x[1]['amount'], reverse=True)[:3]
        }
        
    except Exception as e:
        logger.error(f"Error generating temporal patterns: {e}")
        return {'error': str(e)}

def _generate_risk_assessment(transactions):
    """Generate risk assessment for expense data"""
    try:
        risk_factors = {
            'high_value_transactions': 0,
            'unusual_patterns': 0,
            'weekend_transactions': 0,
            'holiday_transactions': 0,
            'late_hour_transactions': 0,
            'duplicate_amounts': 0,
            'round_amounts': 0
        }
        
        amounts = []
        dates = []
        
        for transaction in transactions:
            amount = float(transaction.amount_local_currency)
            amounts.append(amount)
            
            if transaction.posting_date:
                dates.append(transaction.posting_date)
            
            # Check for high value transactions (above 95th percentile)
            if amount > 1000000:  # 1M SAR threshold
                risk_factors['high_value_transactions'] += 1
            
            # Check for round amounts
            if amount % 1000 == 0:
                risk_factors['round_amounts'] += 1
        
        # Calculate statistics
        if amounts:
            mean_amount = sum(amounts) / len(amounts)
            sorted_amounts = sorted(amounts)
            median_amount = sorted_amounts[len(sorted_amounts) // 2]
            p95_amount = sorted_amounts[int(len(sorted_amounts) * 0.95)]
            
            # Check for unusual patterns
            for amount in amounts:
                if amount > p95_amount * 2:
                    risk_factors['unusual_patterns'] += 1
        else:
            mean_amount = median_amount = p95_amount = 0
        
        # Calculate risk score
        total_transactions = len(transactions)
        risk_score = 0
        
        if total_transactions > 0:
            risk_score += (risk_factors['high_value_transactions'] / total_transactions) * 30
            risk_score += (risk_factors['unusual_patterns'] / total_transactions) * 25
            risk_score += (risk_factors['round_amounts'] / total_transactions) * 15
        
        # Determine risk level
        if risk_score > 50:
            risk_level = 'HIGH'
        elif risk_score > 25:
            risk_level = 'MEDIUM'
        else:
            risk_level = 'LOW'
        
        return {
            'risk_factors': risk_factors,
            'risk_score': round(risk_score, 2),
            'risk_level': risk_level,
            'statistics': {
                'mean_amount': mean_amount,
                'median_amount': median_amount,
                'p95_amount': p95_amount,
                'total_transactions': total_transactions
            },
            'recommendations': _generate_risk_recommendations(risk_factors, risk_score)
        }
        
    except Exception as e:
        logger.error(f"Error generating risk assessment: {e}")
        return {'error': str(e)}

def _generate_risk_recommendations(risk_factors, risk_score):
    """Generate risk-based recommendations"""
    recommendations = []
    
    if risk_factors['high_value_transactions'] > 0:
        recommendations.append({
            'type': 'HIGH_VALUE',
            'message': f"Found {risk_factors['high_value_transactions']} high-value transactions (>1M SAR). Review these for approval compliance.",
            'priority': 'HIGH'
        })
    
    if risk_factors['unusual_patterns'] > 0:
        recommendations.append({
            'type': 'UNUSUAL_PATTERNS',
            'message': f"Found {risk_factors['unusual_patterns']} transactions with unusual patterns. Investigate for potential anomalies.",
            'priority': 'MEDIUM'
        })
    
    if risk_factors['round_amounts'] > len(risk_factors) * 0.1:
        recommendations.append({
            'type': 'ROUND_AMOUNTS',
            'message': f"High percentage of round amounts ({risk_factors['round_amounts']} transactions). Verify supporting documentation.",
            'priority': 'LOW'
        })
    
    if risk_score > 50:
        recommendations.append({
            'type': 'OVERALL_RISK',
            'message': f"Overall risk score is {risk_score} (HIGH). Consider detailed audit review.",
            'priority': 'HIGH'
        })
    
    return recommendations

# Removed cleanup_failed_jobs task - no longer needed

@shared_task
def monitor_processing_jobs():
    """Monitor processing jobs and log statistics"""
    try:
        total_jobs = FileProcessingJob.objects.count()
        pending_jobs = FileProcessingJob.objects.filter(status='PENDING').count()
        queued_jobs = FileProcessingJob.objects.filter(status='QUEUED').count()
        processing_jobs = FileProcessingJob.objects.filter(status='PROCESSING').count()
        completed_jobs = FileProcessingJob.objects.filter(status='COMPLETED').count()
        failed_jobs = FileProcessingJob.objects.filter(status='FAILED').count()
        
        logger.info(f"Processing Jobs Status - Total: {total_jobs}, "
                   f"Pending: {pending_jobs}, Queued: {queued_jobs}, Processing: {processing_jobs}, "
                   f"Completed: {completed_jobs}, Failed: {failed_jobs}")
        
    except Exception as e:
        logger.error(f"Error monitoring processing jobs: {e}")

@shared_task
def process_queued_jobs():
    """Process jobs that are queued and waiting for worker"""
    try:
        from django.utils import timezone
        from .analytics_db_saver import AnalyticsDBSaver
        
        # Get queued jobs
        queued_jobs = FileProcessingJob.objects.filter(
            status='QUEUED'
        ).order_by('created_at')  # Process oldest first
        
        logger.info(f"Found {queued_jobs.count()} queued jobs to process")
        
        for job in queued_jobs:
            try:
                logger.info(f"Processing queued job {job.id}")
                
                # Update status to processing
                job.status = 'PROCESSING'
                job.started_at = timezone.now()
                job.save()
                
                # Initialize database saver
                db_saver = AnalyticsDBSaver(job)
                
                # Step 1: Process and save data to database first
                result = _process_file_content(job)
                
                if result['success']:
                    # Step 2: Use transactions from the processing result
                    stored_transactions = result.get('transactions', [])
                    
                    logger.info(f"Using {len(stored_transactions)} transactions from processing result")
                    
                    # Step 3: Train ML models with stored data
                    ml_training_result = _auto_train_ml_models(stored_transactions, job.data_file)
                    
                    # Step 4: Run analytics with stored data and save to database
                    analytics_results = _run_default_analytics(stored_transactions, job.data_file)
                    db_saver.save_default_analytics(analytics_results)
                    
                    # Step 5: Run comprehensive expense analytics and save to database
                    expense_analytics = _run_comprehensive_expense_analytics(stored_transactions, job.data_file)
                    db_saver.save_comprehensive_analytics(expense_analytics)
                    
                    # Step 6: Run anomalies if requested with stored data (excluding duplicates)
                    anomaly_results = {}
                    if job.run_anomalies and job.requested_anomalies:
                        # Filter out 'duplicates' from requested anomalies to avoid double processing
                        other_anomalies = [a for a in job.requested_anomalies if a != 'duplicates']
                        if other_anomalies:
                            anomaly_results = _run_requested_anomalies(stored_transactions, other_anomalies)
                            db_saver.save_anomaly_detection_results(anomaly_results)
                    
                    # Step 7: Run duplicate analysis (always run this - enhanced version) and save to database
                    duplicate_results = _run_duplicate_analysis(stored_transactions, job.data_file)
                    db_saver.save_duplicate_analysis(duplicate_results)
                    
                    # Step 8: Save ML processing results to database
                    # Convert ML training result to expected format
                    ml_processing_result = _convert_ml_training_to_processing_result(ml_training_result, duplicate_results)
                    db_saver.save_ml_processing_result(ml_processing_result, 'all')
                    
                    # Update job with results
                    # Convert date objects to strings for JSON serialization
                    def convert_dates_for_json(obj):
                        if isinstance(obj, dict):
                            return {k: convert_dates_for_json(v) for k, v in obj.items()}
                        elif isinstance(obj, list):
                            return [convert_dates_for_json(item) for item in obj]
                        elif hasattr(obj, 'date'):  # datetime.date objects
                            return obj.isoformat()
                        elif hasattr(obj, 'isoformat'):  # datetime objects
                            return obj.isoformat()
                        else:
                            return obj
                    
                    # Convert results for JSON serialization
                    serializable_analytics = convert_dates_for_json(analytics_results)
                    serializable_anomalies = convert_dates_for_json(anomaly_results)
                    serializable_ml_training = convert_dates_for_json(ml_training_result)
                    serializable_duplicates = convert_dates_for_json(duplicate_results)
                    
                    job.analytics_results = serializable_analytics
                    job.anomaly_results = serializable_anomalies
                    job.ml_training_results = serializable_ml_training
                    
                    # Add duplicate results to anomaly_results for compatibility
                    if 'duplicate_analysis' not in job.anomaly_results:
                        job.anomaly_results['duplicate_analysis'] = serializable_duplicates
                    
                    # If duplicates were requested in anomalies, add them to anomaly_results
                    if job.run_anomalies and job.requested_anomalies and 'duplicates' in job.requested_anomalies:
                        job.anomaly_results['duplicates'] = {
                            'anomalies_found': len(serializable_duplicates.get('duplicate_list', [])),
                            'details': serializable_duplicates.get('duplicate_list', [])[:10],  # First 10 duplicates
                            'analysis_method': serializable_duplicates.get('analysis_method', 'enhanced'),
                            'enhanced_analysis_used': serializable_duplicates.get('enhanced_analysis_used', True)
                        }
                    
                    # Finalize processing
                    db_saver.finalize_processing(True)
                    
                    job.status = 'COMPLETED'
                    job.completed_at = timezone.now()
                    job.processing_duration = (timezone.now() - job.started_at).total_seconds()
                    job.save()
                    
                    logger.info(f"Queued job {job.id} completed successfully")
                else:
                    # Handle processing failure
                    db_saver.finalize_processing(False, result.get('error', 'Unknown error'))
                    job.status = 'FAILED'
                    job.error_message = result.get('error', 'Unknown error')
                    job.completed_at = timezone.now()
                    job.save()
                    
                    logger.error(f"Queued job {job.id} failed: {result.get('error')}")
                    
            except Exception as e:
                logger.error(f"Error processing queued job {job.id}: {e}")
                try:
                    db_saver.finalize_processing(False, str(e))
                except:
                    pass
                job.status = 'FAILED'
                job.error_message = str(e)
                job.completed_at = timezone.now()
                job.save()
        
    except Exception as e:
        logger.error(f"Error in process_queued_jobs: {e}")

def _convert_ml_training_to_processing_result(ml_training_result, duplicate_results):
    """Convert ML training result to the format expected by save_ml_processing_result"""
    try:
        # Extract duplicate information
        duplicate_list = duplicate_results.get('duplicate_list', [])
        duplicates_found = len(duplicate_list)
        
        # Calculate risk score based on duplicates
        risk_score = 0.0
        if duplicates_found > 0:
            # Simple risk scoring based on number of duplicates
            if duplicates_found <= 5:
                risk_score = 0.3
            elif duplicates_found <= 10:
                risk_score = 0.6
            else:
                risk_score = 0.9
        
        # Calculate confidence score based on ML training status
        confidence_score = 0.0
        if ml_training_result.get('status') == 'COMPLETED':
            confidence_score = 0.9
        elif ml_training_result.get('status') == 'STARTED':
            confidence_score = 0.5
        elif ml_training_result.get('status') == 'SKIPPED':
            confidence_score = 0.7  # Models already trained
        
        # Count anomalies from duplicate results
        anomalies_detected = duplicates_found  # For now, duplicates are considered anomalies
        
        return {
            'anomalies_detected': anomalies_detected,
            'duplicates_found': duplicates_found,
            'risk_score': risk_score,
            'confidence_score': confidence_score,
            'data_size': ml_training_result.get('transactions_count', 0),
            'model_type': 'all',
            'detailed_results': {
                'ml_training_status': ml_training_result.get('status'),
                'ml_training_message': ml_training_result.get('message', ''),
                'ml_training_reason': ml_training_result.get('reason', ''),
                'duplicate_analysis': duplicate_results,
                'training_session_id': ml_training_result.get('training_session_id'),
                'session_name': ml_training_result.get('session_name')
            },
            'model_metrics': {
                'training_status': ml_training_result.get('status'),
                'transactions_processed': ml_training_result.get('transactions_count', 0),
                'duplicates_found': duplicates_found,
                'risk_level': 'HIGH' if risk_score > 0.7 else 'MEDIUM' if risk_score > 0.3 else 'LOW'
            },
            'feature_importance': {
                'duplicate_detection': 0.8,
                'anomaly_detection': 0.6,
                'risk_assessment': 0.7
            }
        }
    except Exception as e:
        logger.error(f"Error converting ML training result: {e}")
        return {
            'anomalies_detected': 0,
            'duplicates_found': 0,
            'risk_score': 0.0,
            'confidence_score': 0.0,
            'data_size': 0,
            'model_type': 'all',
            'detailed_results': {'error': str(e)},
            'model_metrics': {},
            'feature_importance': {}
        }

@shared_task(bind=True, max_retries=2, default_retry_delay=120, time_limit=600, soft_time_limit=480)
def train_ml_models(self, training_session_id):
    """
    Background task to train ML models on historical data
    """
    try:
        training_session = MLModelTraining.objects.get(id=training_session_id)
        training_session.status = 'TRAINING'
        training_session.started_at = timezone.now()
        training_session.save()
        
        start_time = timezone.now()
        
        # Get training data based on session parameters
        transactions = SAPGLPosting.objects.all()
        
        # Apply date filters if specified
        if training_session.training_parameters.get('date_from'):
            transactions = transactions.filter(
                posting_date__gte=training_session.training_parameters['date_from']
            )
        
        if training_session.training_parameters.get('date_to'):
            transactions = transactions.filter(
                posting_date__lte=training_session.training_parameters['date_to']
            )
        
        # Convert to list for processing
        transactions_list = list(transactions)
        
        if len(transactions_list) < 10:
            training_session.status = 'FAILED'
            training_session.error_message = f"Insufficient data for training. Found {len(transactions_list)} transactions, need at least 10."
            training_session.completed_at = timezone.now()
            training_session.save()
            return
        
        # Initialize ML detector
        from .ml_models import MLAnomalyDetector
        ml_detector = MLAnomalyDetector()
        
        # Run comprehensive duplicate analysis during training and save in model
        if hasattr(ml_detector, 'duplicate_model') and ml_detector.duplicate_model:
            try:
                print("Running comprehensive duplicate analysis during training...")
                # Save analysis for each file to avoid re-computation
                file_ids = list(set(str(t.id)[:8] for t in transactions_list))  # Get unique file IDs
                for file_id in file_ids[:5]:  # Limit to first 5 files to avoid excessive processing
                    file_transactions = [t for t in transactions_list if str(t.id).startswith(file_id)]
                    if file_transactions:
                        duplicate_analysis = ml_detector.duplicate_model.run_comprehensive_analysis(file_transactions, file_id)
                        if duplicate_analysis:
                            log_task_info("train_ml_models", training_session_id, f"Duplicate analysis completed for file {file_id}. Found {len(duplicate_analysis.get('duplicate_list', []))} duplicates.")
                        else:
                            log_task_info("train_ml_models", training_session_id, f"Duplicate analysis failed for file {file_id}.")
                
                # Also run general analysis for the entire dataset
                general_analysis = ml_detector.duplicate_model.run_comprehensive_duplicate_analysis(transactions_list)
                if general_analysis:
                    log_task_info("train_ml_models", training_session_id, f"General duplicate analysis completed with {len(general_analysis.get('duplicate_list', []))} entries")
                else:
                    log_task_info("train_ml_models", training_session_id, "General duplicate analysis failed or returned empty results.")
            except Exception as e:
                log_task_info("train_ml_models", training_session_id, f"Error during duplicate analysis: {e}")
        
        # Train models
        performance_metrics = ml_detector.train_models(transactions_list)
        
        # Update training session
        end_time = timezone.now()
        training_duration = (end_time - start_time).total_seconds()
        
        training_session.training_data_size = len(transactions_list)
        training_session.feature_count = len(ml_detector.feature_columns)
        training_session.performance_metrics = performance_metrics
        training_session.training_duration = training_duration
        training_session.status = 'COMPLETED'
        training_session.completed_at = end_time
        training_session.model_file_path = ml_detector.models_dir
        training_session.save()
        
        logger.info(f"ML model training completed for session {training_session_id}")
        
    except MLModelTraining.DoesNotExist:
        logger.error(f"MLModelTraining {training_session_id} not found")
    except Exception as e:
        logger.error(f"Unexpected error in ML model training for session {training_session_id}: {e}")
        try:
            training_session = MLModelTraining.objects.get(id=training_session_id)
            training_session.status = 'FAILED'
            training_session.error_message = str(e)
            training_session.completed_at = timezone.now()
            training_session.save()
        except:
            pass

@shared_task(bind=True, max_retries=2, default_retry_delay=120, time_limit=600, soft_time_limit=480)
def retrain_ml_models(self, training_session_id):
    """
    Background task to retrain ML models with new data
    """
    try:
        training_session = MLModelTraining.objects.get(id=training_session_id)
        training_session.status = 'TRAINING'
        training_session.started_at = timezone.now()
        training_session.save()
        
        start_time = timezone.now()
        
        # Get all transactions for retraining
        transactions = SAPGLPosting.objects.all()
        transactions_list = list(transactions)
        
        if len(transactions_list) < 10:
            training_session.status = 'FAILED'
            training_session.error_message = f"Insufficient data for retraining. Found {len(transactions_list)} transactions, need at least 10."
            training_session.completed_at = timezone.now()
            training_session.save()
            return
        
        # Initialize ML detector and load existing models
        from .ml_models import MLAnomalyDetector
        ml_detector = MLAnomalyDetector()
        ml_detector.load_models_from_memory()
        
        # Retrain models
        performance_metrics = ml_detector.retrain_models(transactions_list)
        
        # Update training session
        end_time = timezone.now()
        training_duration = (end_time - start_time).total_seconds()
        
        training_session.training_data_size = len(transactions_list)
        training_session.feature_count = len(ml_detector.feature_columns)
        training_session.performance_metrics = performance_metrics
        training_session.training_duration = training_duration
        training_session.status = 'COMPLETED'
        training_session.completed_at = end_time
        training_session.model_file_path = ml_detector.models_dir
        training_session.save()
        
        logger.info(f"ML model retraining completed for session {training_session_id}")
        
    except MLModelTraining.DoesNotExist:
        logger.error(f"MLModelTraining {training_session_id} not found")
    except Exception as e:
        logger.error(f"Unexpected error in ML model retraining for session {training_session_id}: {e}")
        try:
            training_session = MLModelTraining.objects.get(id=training_session_id)
            training_session.status = 'FAILED'
            training_session.error_message = str(e)
            training_session.completed_at = timezone.now()
            training_session.save()
        except:
            pass

@shared_task(bind=True, max_retries=2, default_retry_delay=120, time_limit=600, soft_time_limit=480)
def train_enhanced_ml_models(self, training_session_id):
    """
    Enhanced ML model training task using the new 6 duplicate type definitions
    """
    print(f"🔍 DEBUG: ===== train_enhanced_ml_models STARTED =====")
    print(f"🔍 DEBUG: Training session ID: {training_session_id}")
    
    try:
        from .models import MLModelTraining, SAPGLPosting
        from .analytics import SAPGLAnalyzer
        from .ml_models import MLAnomalyDetector
        from django.utils import timezone
        
        # Get training session
        training_session = MLModelTraining.objects.get(id=training_session_id)
        training_session.status = 'RUNNING'
        training_session.started_at = timezone.now()
        training_session.save()
        
        print(f"🔍 DEBUG: Training session: {training_session.session_name}")
        
        # Get transactions for training
        transactions = list(SAPGLPosting.objects.all())
        print(f"🔍 DEBUG: Total transactions for training: {len(transactions)}")
        
        if len(transactions) < 10:
            training_session.status = 'FAILED'
            training_session.error_message = f'Insufficient data for training. Found {len(transactions)} transactions, need at least 10.'
            training_session.save()
            return {
                'status': 'FAILED',
                'error': 'Insufficient training data'
            }
        
        # Use enhanced duplicate detection for training
        analyzer = SAPGLAnalyzer()
        duplicate_results = analyzer.detect_duplicate_entries(transactions)
        enhanced_duplicates = duplicate_results.get('duplicates', [])
        
        print(f"🔍 DEBUG: Enhanced duplicates found: {len(enhanced_duplicates)}")
        
        # Create enhanced training data with 6 duplicate types
        training_data = {
            'transactions': len(transactions),
            'duplicate_groups': len(enhanced_duplicates),
            'duplicate_types_found': list(set(dup['type'] for dup in enhanced_duplicates)),
            'enhanced_features': True,
            'training_session_id': str(training_session.id),
            'duplicate_breakdown': {
                'Type 1 Duplicate': len([d for d in enhanced_duplicates if d['type'] == 'Type 1 Duplicate']),
                'Type 2 Duplicate': len([d for d in enhanced_duplicates if d['type'] == 'Type 2 Duplicate']),
                'Type 3 Duplicate': len([d for d in enhanced_duplicates if d['type'] == 'Type 3 Duplicate']),
                'Type 4 Duplicate': len([d for d in enhanced_duplicates if d['type'] == 'Type 4 Duplicate']),
                'Type 5 Duplicate': len([d for d in enhanced_duplicates if d['type'] == 'Type 5 Duplicate']),
                'Type 6 Duplicate': len([d for d in enhanced_duplicates if d['type'] == 'Type 6 Duplicate'])
            }
        }
        
        # Initialize ML detector with enhanced features
        ml_detector = MLAnomalyDetector()
        
        # Train models with enhanced duplicate data
        training_result = ml_detector.train_enhanced_models(
            transactions=transactions,
            enhanced_duplicates=enhanced_duplicates,
            training_session=training_session
        )
        
        # Update training session with enhanced results
        training_session.training_data_size = len(transactions)
        training_session.performance_metrics = {
            'duplicate_groups_found': len(enhanced_duplicates),
            'duplicate_types_detected': len(set(dup['type'] for dup in enhanced_duplicates)),
            'enhanced_analysis': True,
            'training_accuracy': training_result.get('accuracy', 0),
            'model_performance': training_result.get('performance', {}),
            'duplicate_breakdown': training_data['duplicate_breakdown']
        }
        training_session.status = 'COMPLETED'
        training_session.completed_at = timezone.now()
        training_session.save()
        
        print(f"🔍 DEBUG: Enhanced ML training completed successfully")
        
        return {
            'status': 'COMPLETED',
            'training_session_id': str(training_session.id),
            'transactions_processed': len(transactions),
            'duplicate_groups_found': len(enhanced_duplicates),
            'enhanced_features': True,
            'training_data': training_data,
            'model_performance': training_result
        }
        
    except Exception as e:
        print(f"🔍 DEBUG: Error in enhanced ML training: {e}")
        logger.error(f"Enhanced ML training failed: {e}")
        
        try:
            training_session = MLModelTraining.objects.get(id=training_session_id)
            training_session.status = 'FAILED'
            training_session.error_message = str(e)
            training_session.save()
        except:
            pass
        
        return {
            'status': 'FAILED',
            'error': str(e)
        }

# Removed cleanup_old_training_sessions task - no longer needed

@shared_task
def monitor_ml_model_performance():
    """
    Periodic task to monitor ML model performance
    """
    try:
        from .ml_models import MLAnomalyDetector
        
        ml_detector = MLAnomalyDetector()
        if ml_detector.load_models():
            model_info = ml_detector.get_model_info()
            logger.info(f"ML models status: {model_info}")
        else:
            logger.warning("No trained ML models found")
            
    except Exception as e:
        logger.error(f"Error in monitor_ml_model_performance: {e}") 

@shared_task(bind=True)
def debug_task(self):
    """Debug task to test Celery worker connectivity and health"""
    task_name = "debug_task"
    job_id = "debug"
    
    debug_task_state(task_name, job_id, "STARTED", f"Task ID: {self.request.id}")
    log_task_info(task_name, job_id, f"===== DEBUG TASK STARTED =====")
    log_task_info(task_name, job_id, f"Task ID: {self.request.id}")
    log_task_info(task_name, job_id, f"Worker: {self.request.hostname}")
    log_task_info(task_name, job_id, f"PID: {self.request.pid}")
    log_task_info(task_name, job_id, f"System info: {get_system_info()}")
    
    # Test database connectivity
    try:
        from django.db import connection
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
        log_task_info(task_name, job_id, f"Database connectivity: OK (result: {result})")
    except Exception as e:
        log_task_info(task_name, job_id, f"Database connectivity: FAILED - {e}", "error")
    
    # Test file system access
    try:
        import tempfile
        with tempfile.NamedTemporaryFile(delete=True) as f:
            f.write(b"test")
        log_task_info(task_name, job_id, "File system access: OK")
    except Exception as e:
        log_task_info(task_name, job_id, f"File system access: FAILED - {e}", "error")
    
    # Test memory usage
    try:
        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        log_task_info(task_name, job_id, f"Memory usage: {memory_info.rss / 1024 / 1024:.2f} MB")
    except Exception as e:
        log_task_info(task_name, job_id, f"Memory check: FAILED - {e}", "error")
    
    # Test CPU usage
    try:
        cpu_percent = psutil.Process().cpu_percent()
        log_task_info(task_name, job_id, f"CPU usage: {cpu_percent}%")
    except Exception as e:
        log_task_info(task_name, job_id, f"CPU check: FAILED - {e}", "error")
    
    log_task_info(task_name, job_id, f"===== DEBUG TASK COMPLETED =====")
    
    return {
        'task_id': self.request.id,
        'worker': self.request.hostname,
        'pid': self.request.pid,
        'status': 'success',
        'timestamp': timezone.now().isoformat()
    }

@shared_task(bind=True)
def worker_health_check(self):
    """Comprehensive worker health check task"""
    task_name = "worker_health_check"
    job_id = "health_check"
    
    log_task_info(task_name, job_id, f"===== WORKER HEALTH CHECK STARTED =====")
    
    health_status = {
        'task_id': self.request.id,
        'worker': self.request.hostname,
        'pid': self.request.pid,
        'timestamp': timezone.now().isoformat(),
        'checks': {}
    }
    
    # Check 1: Basic system info
    try:
        system_info = get_system_info()
        health_status['checks']['system_info'] = {
            'status': 'OK',
            'data': system_info
        }
        log_task_info(task_name, job_id, f"System info check: OK")
    except Exception as e:
        health_status['checks']['system_info'] = {
            'status': 'FAILED',
            'error': str(e)
        }
        log_task_info(task_name, job_id, f"System info check: FAILED - {e}", "error")
    
    # Check 2: Database connectivity
    try:
        from django.db import connection
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
        health_status['checks']['database'] = {
            'status': 'OK',
            'data': {'test_query_result': result}
        }
        log_task_info(task_name, job_id, "Database connectivity: OK")
    except Exception as e:
        health_status['checks']['database'] = {
            'status': 'FAILED',
            'error': str(e)
        }
        log_task_info(task_name, job_id, f"Database connectivity: FAILED - {e}", "error")
    
    # Check 3: File system access
    try:
        import tempfile
        with tempfile.NamedTemporaryFile(delete=True) as f:
            f.write(b"health_check_test")
        health_status['checks']['file_system'] = {
            'status': 'OK',
            'data': {'temp_file_test': 'passed'}
        }
        log_task_info(task_name, job_id, "File system access: OK")
    except Exception as e:
        health_status['checks']['file_system'] = {
            'status': 'FAILED',
            'error': str(e)
        }
        log_task_info(task_name, job_id, f"File system access: FAILED - {e}", "error")
    
    # Check 4: Memory usage
    try:
        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        health_status['checks']['memory'] = {
            'status': 'OK',
            'data': {
                'rss_mb': memory_info.rss / 1024 / 1024,
                'vms_mb': memory_info.vms / 1024 / 1024
            }
        }
        log_task_info(task_name, job_id, f"Memory usage: {memory_info.rss / 1024 / 1024:.2f} MB")
    except Exception as e:
        health_status['checks']['memory'] = {
            'status': 'FAILED',
            'error': str(e)
        }
        log_task_info(task_name, job_id, f"Memory check: FAILED - {e}", "error")
    
    # Check 5: CPU usage
    try:
        cpu_percent = psutil.Process().cpu_percent()
        health_status['checks']['cpu'] = {
            'status': 'OK',
            'data': {'cpu_percent': cpu_percent}
        }
        log_task_info(task_name, job_id, f"CPU usage: {cpu_percent}%")
    except Exception as e:
        health_status['checks']['cpu'] = {
            'status': 'FAILED',
            'error': str(e)
        }
        log_task_info(task_name, job_id, f"CPU check: FAILED - {e}", "error")
    
    # Check 6: Model imports
    try:
        from .models import FileProcessingJob, DataFile, SAPGLPosting
        health_status['checks']['model_imports'] = {
            'status': 'OK',
            'data': {'models_loaded': ['FileProcessingJob', 'DataFile', 'SAPGLPosting']}
        }
        log_task_info(task_name, job_id, "Model imports: OK")
    except Exception as e:
        health_status['checks']['model_imports'] = {
            'status': 'FAILED',
            'error': str(e)
        }
        log_task_info(task_name, job_id, f"Model imports: FAILED - {e}", "error")
    
    # Calculate overall health status
    failed_checks = [check for check in health_status['checks'].values() if check['status'] == 'FAILED']
    if failed_checks:
        health_status['overall_status'] = 'UNHEALTHY'
        health_status['failed_checks_count'] = len(failed_checks)
        log_task_info(task_name, job_id, f"Health check: UNHEALTHY ({len(failed_checks)} failed checks)", "warning")
    else:
        health_status['overall_status'] = 'HEALTHY'
        health_status['failed_checks_count'] = 0
        log_task_info(task_name, job_id, "Health check: HEALTHY")
    
    log_task_info(task_name, job_id, f"===== WORKER HEALTH CHECK COMPLETED =====")
    
    return health_status

@shared_task(bind=True)
def monitor_worker_performance(self):
    """Monitor worker performance metrics"""
    task_name = "monitor_worker_performance"
    job_id = "performance_monitor"
    
    log_task_info(task_name, job_id, f"===== PERFORMANCE MONITORING STARTED =====")
    
    performance_data = {
        'task_id': self.request.id,
        'worker': self.request.hostname,
        'pid': self.request.pid,
        'timestamp': timezone.now().isoformat(),
        'metrics': {}
    }
    
    try:
        import psutil
        
        # Memory metrics
        process = psutil.Process()
        memory_info = process.memory_info()
        performance_data['metrics']['memory'] = {
            'rss_mb': memory_info.rss / 1024 / 1024,
            'vms_mb': memory_info.vms / 1024 / 1024,
            'percent': process.memory_percent()
        }
        
        # CPU metrics
        performance_data['metrics']['cpu'] = {
            'percent': process.cpu_percent(),
            'num_threads': process.num_threads(),
            'num_fds': process.num_fds() if hasattr(process, 'num_fds') else None
        }
        
        # System metrics
        system_memory = psutil.virtual_memory()
        performance_data['metrics']['system'] = {
            'total_memory_mb': system_memory.total / 1024 / 1024,
            'available_memory_mb': system_memory.available / 1024 / 1024,
            'memory_percent': system_memory.percent,
            'cpu_count': psutil.cpu_count()
        }
        
        log_task_info(task_name, job_id, f"Performance metrics collected successfully")
        
    except Exception as e:
        log_task_info(task_name, job_id, f"Performance monitoring failed: {e}", "error")
        performance_data['error'] = str(e)
    
    log_task_info(task_name, job_id, f"===== PERFORMANCE MONITORING COMPLETED =====")
    
    return performance_data 