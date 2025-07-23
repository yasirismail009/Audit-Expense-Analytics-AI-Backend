"""
Database saver utilities for analytics and ML processing results
This module handles saving all processing results to the database for better tracking and frontend access
"""

import logging
from django.utils import timezone
from django.db import transaction
from decimal import Decimal
import psutil
import os
from typing import Dict, List, Any, Optional
import json
from datetime import datetime, date

from .models import (
    MLModelProcessingResult, 
    AnalyticsProcessingResult, 
    ProcessingJobTracker,
    FileProcessingJob,
    DataFile
)

logger = logging.getLogger(__name__)

def serialize_for_json(obj):
    """Convert objects to JSON-serializable format"""
    if obj is None:
        return None
    elif isinstance(obj, (str, int, float, bool)):
        return obj
    elif isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    elif hasattr(obj, 'isoformat'):  # Handle pandas Timestamp and other datetime-like objects
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: serialize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [serialize_for_json(item) for item in obj]
    elif hasattr(obj, '__dict__'):  # Handle custom objects
        return str(obj)
    else:
        return str(obj)

class AnalyticsDBSaver:
    """Utility class to save analytics results to database"""
    
    def __init__(self, processing_job: FileProcessingJob):
        self.processing_job = processing_job
        self.data_file = processing_job.data_file
        self.job_tracker = None
        self._initialize_tracker()
    
    def _initialize_tracker(self):
        """Initialize or get existing job tracker"""
        try:
            self.job_tracker, created = ProcessingJobTracker.objects.get_or_create(
                processing_job=self.processing_job,
                defaults={
                    'data_file': self.data_file,
                    'total_steps': 5,  # File processing, Analytics, ML, Anomaly, Final
                    'completed_steps': 0,
                    'overall_progress': 0.0,
                    'started_at': timezone.now()
                }
            )
            if created:
                logger.info(f"Created new job tracker for job {self.processing_job.id}")
        except Exception as e:
            logger.error(f"Error initializing job tracker: {e}")
    
    def update_progress(self, step_name: str, progress_percentage: float, status: str = 'PROCESSING'):
        """Update processing progress"""
        if self.job_tracker:
            try:
                self.job_tracker.update_progress(step_name, progress_percentage, status)
                logger.info(f"Updated progress for {step_name}: {progress_percentage}% - {status}")
            except Exception as e:
                logger.error(f"Error updating progress: {e}")
    
    def save_default_analytics(self, analytics_results: Dict[str, Any]) -> AnalyticsProcessingResult:
        """Save default analytics results to database"""
        try:
            print(f"🔍 DEBUG: ===== save_default_analytics STARTED =====")
            print(f"🔍 DEBUG: Data file: {self.data_file.file_name}")
            print(f"🔍 DEBUG: Processing job: {self.processing_job.id}")
            print(f"🔍 DEBUG: Analytics results keys: {list(analytics_results.keys())}")
            
            self.update_progress("Default Analytics", 20, "PROCESSING")
            
            # Extract key metrics
            total_transactions = analytics_results.get('total_transactions', 0)
            total_amount = Decimal(str(analytics_results.get('total_amount', 0)))
            unique_users = analytics_results.get('unique_users', 0)
            unique_accounts = analytics_results.get('unique_accounts', 0)
            
            print(f"🔍 DEBUG: Extracted metrics - Transactions: {total_transactions}, Amount: {total_amount}, Users: {unique_users}, Accounts: {unique_accounts}")
            
            # Create analytics result record
            analytics_result = AnalyticsProcessingResult.objects.create(
                data_file=self.data_file,
                processing_job=self.processing_job,
                analytics_type='default_analytics',
                processing_status='COMPLETED',
                total_transactions=total_transactions,
                total_amount=total_amount,
                unique_users=unique_users,
                unique_accounts=unique_accounts,
                trial_balance_data=serialize_for_json({
                    'total_debits': analytics_results.get('total_debits', 0),
                    'total_credits': analytics_results.get('total_credits', 0),
                    'trial_balance': analytics_results.get('trial_balance', 0),
                    'gl_account_summaries': analytics_results.get('gl_account_summaries', [])
                }),
                chart_data=serialize_for_json(analytics_results.get('chart_data', {})),
                export_data=serialize_for_json(analytics_results.get('export_data', [])),
                processed_at=timezone.now()
            )
            
            print(f"🔍 DEBUG: Created AnalyticsProcessingResult with ID: {analytics_result.id}")
            
            self.update_progress("Default Analytics", 100, "COMPLETED")
            logger.info(f"Saved default analytics for file {self.data_file.file_name}")
            
            return analytics_result
            
        except Exception as e:
            print(f"🔍 DEBUG: ERROR in save_default_analytics: {e}")
            logger.error(f"Error saving default analytics: {e}")
            self.update_progress("Default Analytics", 0, "FAILED")
            raise
    
    def save_comprehensive_analytics(self, analytics_results: Dict[str, Any]) -> AnalyticsProcessingResult:
        """Save comprehensive expense analytics results to database"""
        try:
            print(f"🔍 DEBUG: ===== save_comprehensive_analytics STARTED =====")
            print(f"🔍 DEBUG: Data file: {self.data_file.file_name}")
            print(f"🔍 DEBUG: Processing job: {self.processing_job.id}")
            print(f"🔍 DEBUG: Analytics results keys: {list(analytics_results.keys())}")
            
            self.update_progress("Comprehensive Analytics", 40, "PROCESSING")
            
            # Extract summary data
            summary = analytics_results.get('summary', {})
            total_transactions = summary.get('total_transactions', 0)
            total_amount = Decimal(str(summary.get('total_amount', 0)))
            unique_users = summary.get('unique_users', 0)
            unique_accounts = summary.get('unique_accounts', 0)
            
            print(f"🔍 DEBUG: Extracted metrics - Transactions: {total_transactions}, Amount: {total_amount}, Users: {unique_users}, Accounts: {unique_accounts}")
            
            # Extract detailed results
            expense_breakdown = analytics_results.get('expense_breakdown', {})
            user_patterns = analytics_results.get('user_patterns', {})
            account_patterns = analytics_results.get('account_patterns', {})
            temporal_patterns = analytics_results.get('temporal_patterns', {})
            risk_assessment = analytics_results.get('risk_assessment', {})
            
            # Create analytics result record
            analytics_result = AnalyticsProcessingResult.objects.create(
                data_file=self.data_file,
                processing_job=self.processing_job,
                analytics_type='comprehensive_expense',
                processing_status='COMPLETED',
                total_transactions=total_transactions,
                total_amount=total_amount,
                unique_users=unique_users,
                unique_accounts=unique_accounts,
                expense_breakdown=serialize_for_json(expense_breakdown),
                user_patterns=serialize_for_json(user_patterns),
                account_patterns=serialize_for_json(account_patterns),
                temporal_patterns=serialize_for_json(temporal_patterns),
                risk_assessment=serialize_for_json(risk_assessment),
                chart_data=serialize_for_json(analytics_results.get('chart_data', {})),
                export_data=serialize_for_json(analytics_results.get('export_data', [])),
                processed_at=timezone.now()
            )
            
            print(f"🔍 DEBUG: Created AnalyticsProcessingResult with ID: {analytics_result.id}")
            
            self.update_progress("Comprehensive Analytics", 100, "COMPLETED")
            logger.info(f"Saved comprehensive analytics for file {self.data_file.file_name}")
            
            return analytics_result
            
        except Exception as e:
            print(f"🔍 DEBUG: ERROR in save_comprehensive_analytics: {e}")
            logger.error(f"Error saving comprehensive analytics: {e}")
            self.update_progress("Comprehensive Analytics", 0, "FAILED")
            raise
    
    def save_duplicate_analysis(self, duplicate_results: Dict[str, Any]) -> AnalyticsProcessingResult:
        """Save duplicate analysis results to database"""
        try:
            print(f"🔍 DEBUG: ===== save_duplicate_analysis STARTED =====")
            print(f"🔍 DEBUG: Data file: {self.data_file.file_name}")
            print(f"🔍 DEBUG: Processing job: {self.processing_job.id}")
            print(f"🔍 DEBUG: Duplicate results keys: {list(duplicate_results.keys())}")
            
            self.update_progress("Duplicate Analysis", 60, "PROCESSING")
            
            # Extract analysis info
            analysis_info = duplicate_results.get('analysis_info', {})
            total_transactions = analysis_info.get('total_transactions', 0)
            total_amount = Decimal(str(analysis_info.get('total_amount_involved', 0)))
            duplicates_found = analysis_info.get('total_duplicate_transactions', 0)
            
            print(f"🔍 DEBUG: Extracted metrics - Transactions: {total_transactions}, Amount: {total_amount}, Duplicates: {duplicates_found}")
            
            # Serialize all data to ensure JSON compatibility
            serialized_duplicate_list = serialize_for_json(duplicate_results.get('duplicate_list', []))
            serialized_analysis_info = serialize_for_json(analysis_info)
            serialized_breakdowns = serialize_for_json(duplicate_results.get('breakdowns', {}))
            serialized_slicer_filters = serialize_for_json(duplicate_results.get('slicer_filters', {}))
            serialized_summary_table = serialize_for_json(duplicate_results.get('summary_table', []))
            serialized_detailed_insights = serialize_for_json(duplicate_results.get('detailed_insights', {}))
            serialized_ml_enhancement = serialize_for_json(duplicate_results.get('ml_enhancement', {}))
            serialized_chart_data = serialize_for_json(duplicate_results.get('chart_data', {}))
            serialized_export_data = serialize_for_json(duplicate_results.get('export_data', []))
            
            # Create analytics result record
            analytics_result = AnalyticsProcessingResult.objects.create(
                data_file=self.data_file,
                processing_job=self.processing_job,
                analytics_type='duplicate_analysis',
                processing_status='COMPLETED',
                total_transactions=total_transactions,
                total_amount=total_amount,
                duplicates_found=duplicates_found,
                trial_balance_data={
                    'duplicate_list': serialized_duplicate_list,
                    'analysis_info': serialized_analysis_info,
                    'breakdowns': serialized_breakdowns,
                    'slicer_filters': serialized_slicer_filters,
                    'summary_table': serialized_summary_table,
                    'detailed_insights': serialized_detailed_insights,
                    'ml_enhancement': serialized_ml_enhancement
                },
                chart_data=serialized_chart_data,
                export_data=serialized_export_data,
                processed_at=timezone.now()
            )
            
            print(f"🔍 DEBUG: Created AnalyticsProcessingResult with ID: {analytics_result.id}")
            
            self.update_progress("Duplicate Analysis", 100, "COMPLETED")
            logger.info(f"Saved duplicate analysis for file {self.data_file.file_name}")
            
            return analytics_result
            
        except Exception as e:
            print(f"🔍 DEBUG: ERROR in save_duplicate_analysis: {e}")
            logger.error(f"Error saving duplicate analysis: {e}")
            self.update_progress("Duplicate Analysis", 0, "FAILED")
            raise
    
    def save_ml_processing_result(self, ml_results: Dict[str, Any], model_type: str = 'all') -> MLModelProcessingResult:
        """Save ML model processing results to database"""
        try:
            print(f"🔍 DEBUG: ===== save_ml_processing_result STARTED =====")
            print(f"🔍 DEBUG: Data file: {self.data_file.file_name}")
            print(f"🔍 DEBUG: Processing job: {self.processing_job.id}")
            print(f"🔍 DEBUG: Model type: {model_type}")
            print(f"🔍 DEBUG: ML results keys: {list(ml_results.keys())}")
            
            self.update_progress("ML Processing", 80, "PROCESSING")
            
            # Extract ML metrics
            anomalies_detected = ml_results.get('anomalies_detected', 0)
            duplicates_found = ml_results.get('duplicates_found', 0)
            risk_score = ml_results.get('risk_score', 0.0)
            confidence_score = ml_results.get('confidence_score', 0.0)
            data_size = ml_results.get('data_size', 0)
            
            print(f"🔍 DEBUG: Extracted ML metrics - Anomalies: {anomalies_detected}, Duplicates: {duplicates_found}, Risk Score: {risk_score}, Confidence: {confidence_score}, Data Size: {data_size}")
            
            # Create ML processing result record
            ml_result = MLModelProcessingResult.objects.create(
                data_file=self.data_file,
                processing_job=self.processing_job,
                model_type=model_type,
                processing_status='COMPLETED',
                anomalies_detected=anomalies_detected,
                duplicates_found=duplicates_found,
                risk_score=risk_score,
                confidence_score=confidence_score,
                data_size=data_size,
                detailed_results=serialize_for_json(ml_results.get('detailed_results', {})),
                model_metrics=serialize_for_json(ml_results.get('model_metrics', {})),
                feature_importance=serialize_for_json(ml_results.get('feature_importance', {})),
                processed_at=timezone.now()
            )
            
            print(f"🔍 DEBUG: Created MLModelProcessingResult with ID: {ml_result.id}")
            
            self.update_progress("ML Processing", 100, "COMPLETED")
            logger.info(f"Saved ML processing results for file {self.data_file.file_name}")
            
            return ml_result
            
        except Exception as e:
            print(f"🔍 DEBUG: ERROR in save_ml_processing_result: {e}")
            logger.error(f"Error saving ML processing results: {e}")
            self.update_progress("ML Processing", 0, "FAILED")
            raise
    
    def save_anomaly_detection_results(self, anomaly_results: Dict[str, Any]) -> AnalyticsProcessingResult:
        """Save anomaly detection results to database"""
        try:
            print(f"🔍 DEBUG: ===== save_anomaly_detection_results STARTED =====")
            print(f"🔍 DEBUG: Data file: {self.data_file.file_name}")
            print(f"🔍 DEBUG: Processing job: {self.processing_job.id}")
            print(f"🔍 DEBUG: Anomaly results keys: {list(anomaly_results.keys())}")
            
            self.update_progress("Anomaly Detection", 90, "PROCESSING")
            
            # Calculate total anomalies
            total_anomalies = 0
            for anomaly_type, result in anomaly_results.items():
                if isinstance(result, dict) and 'anomalies_found' in result:
                    total_anomalies += result['anomalies_found']
            
            print(f"🔍 DEBUG: Total anomalies calculated: {total_anomalies}")
            
            # Create analytics result record
            analytics_result = AnalyticsProcessingResult.objects.create(
                data_file=self.data_file,
                processing_job=self.processing_job,
                analytics_type='anomaly_detection',
                processing_status='COMPLETED',
                anomalies_found=total_anomalies,
                trial_balance_data=serialize_for_json({
                    'anomaly_results': anomaly_results,
                    'anomaly_types': list(anomaly_results.keys()),
                    'total_anomalies': total_anomalies
                }),
                processed_at=timezone.now()
            )
            
            print(f"🔍 DEBUG: Created AnalyticsProcessingResult with ID: {analytics_result.id}")
            
            self.update_progress("Anomaly Detection", 100, "COMPLETED")
            logger.info(f"Saved anomaly detection results for file {self.data_file.file_name}")
            
            return analytics_result
            
        except Exception as e:
            print(f"🔍 DEBUG: ERROR in save_anomaly_detection_results: {e}")
            logger.error(f"Error saving anomaly detection results: {e}")
            self.update_progress("Anomaly Detection", 0, "FAILED")
            raise
    
    def finalize_processing(self, success: bool = True, error_message: str = None):
        """Finalize the processing job"""
        try:
            print(f"🔍 DEBUG: ===== finalize_processing STARTED =====")
            print(f"🔍 DEBUG: Success: {success}")
            print(f"🔍 DEBUG: Error message: {error_message}")
            
            if self.job_tracker:
                if success:
                    self.job_tracker.overall_progress = 100.0
                    self.job_tracker.completed_steps = self.job_tracker.total_steps
                    self.job_tracker.current_step = "Completed"
                    self.job_tracker.completed_at = timezone.now()
                    
                    # Update performance metrics
                    process = psutil.Process()
                    self.job_tracker.memory_usage_mb = process.memory_info().rss / 1024 / 1024
                    self.job_tracker.cpu_usage_percent = process.cpu_percent()
                    
                    print(f"🔍 DEBUG: Processing finalized successfully")
                    logger.info(f"Processing completed successfully for job {self.processing_job.id}")
                else:
                    self.job_tracker.current_step = "Failed"
                    if error_message:
                        error_log_entry = {
                            'step': 'finalization',
                            'error': error_message,
                            'timestamp': timezone.now().isoformat()
                        }
                        self.job_tracker.error_log.append(error_log_entry)
                    
                    print(f"🔍 DEBUG: Processing failed")
                    logger.error(f"Processing failed for job {self.processing_job.id}: {error_message}")
                
                self.job_tracker.save()
            else:
                print(f"🔍 DEBUG: No job tracker found")
                
        except Exception as e:
            print(f"🔍 DEBUG: ERROR in finalize_processing: {e}")
            logger.error(f"Error finalizing processing: {e}")
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """Get a summary of all processing results"""
        try:
            summary = {
                'job_id': str(self.processing_job.id),
                'file_name': self.data_file.file_name,
                'file_id': str(self.data_file.id),
                'processing_status': self.processing_job.status,
                'progress': self.job_tracker.get_progress_summary() if self.job_tracker else None,
                'analytics_results': [],
                'ml_results': [],
                'created_at': self.processing_job.created_at.isoformat(),
                'completed_at': self.processing_job.completed_at.isoformat() if self.processing_job.completed_at else None,
            }
            
            # Get analytics results
            analytics_results = AnalyticsProcessingResult.objects.filter(
                data_file=self.data_file,
                processing_job=self.processing_job
            ).order_by('created_at')
            
            for result in analytics_results:
                summary['analytics_results'].append(result.get_summary())
            
            # Get ML results
            ml_results = MLModelProcessingResult.objects.filter(
                data_file=self.data_file,
                processing_job=self.processing_job
            ).order_by('created_at')
            
            for result in ml_results:
                summary['ml_results'].append(result.get_summary())
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting processing summary: {e}")
            return {'error': str(e)}

def save_analytics_to_db(processing_job: FileProcessingJob, analytics_type: str, results: Dict[str, Any]) -> bool:
    """Convenience function to save analytics results to database"""
    try:
        saver = AnalyticsDBSaver(processing_job)
        
        if analytics_type == 'default_analytics':
            saver.save_default_analytics(results)
        elif analytics_type == 'comprehensive_analytics':
            saver.save_comprehensive_analytics(results)
        elif analytics_type == 'duplicate_analysis':
            saver.save_duplicate_analysis(results)
        elif analytics_type == 'anomaly_detection':
            saver.save_anomaly_detection_results(results)
        elif analytics_type == 'ml_processing':
            model_type = results.get('model_type', 'all')
            saver.save_ml_processing_result(results, model_type)
        else:
            logger.warning(f"Unknown analytics type: {analytics_type}")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"Error saving {analytics_type} to database: {e}")
        return False

def get_file_processing_summary(data_file: DataFile) -> Dict[str, Any]:
    """Get comprehensive processing summary for a file"""
    try:
        # Get the latest processing job
        latest_job = FileProcessingJob.objects.filter(
            data_file=data_file
        ).order_by('-created_at').first()
        
        if not latest_job:
            return {'error': 'No processing job found for this file'}
        
        saver = AnalyticsDBSaver(latest_job)
        return saver.get_processing_summary()
        
    except Exception as e:
        logger.error(f"Error getting file processing summary: {e}")
        return {'error': str(e)} 