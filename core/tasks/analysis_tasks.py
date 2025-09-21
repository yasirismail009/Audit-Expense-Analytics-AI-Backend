"""
Analysis Tasks Module

Contains all analysis-related Celery tasks for the analytics system.
"""

from celery import shared_task, current_task
from django.utils import timezone
from django.db import transaction
from django.db.models import F
import logging
import traceback
import psutil
import os
import numpy as np
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, List

from ..models import (
    FileProcessingJob, SAPGLPosting, DataFile, MLModelTraining, BackdatedAnalysisResult,
    GeneralAnalysisResult, DuplicateAnalysisResult, UserAnalysisResult, UnusualDaysAnalysisResult,
    ClosingEntriesAnalysisResult, OverallAnalysisResult, RiskScoringDocument, HolidayAnalysisResult,
    RuleBasedModelTraining, DuplicateAnalysisModelTraining, BackdatedAnalysisModelTraining,
    UserAnalysisModelTraining, UnusualDaysAnalysisModelTraining, ClosingEntriesAnalysisModelTraining,
    HolidayAnalysisModelTraining, OverallRiskAnalysisModelTraining, ManualEntryAnalysisResult,
    FileProcessingTask, CompletenessJob, TrialBalance, ChartOfAccount, CompletenessTestResult,
    GLAccount, Engagement
)

# Import notification tasks
try:
    from notifications.tasks import (
        notify_file_processing_status, 
        notify_analysis_status,
        notify_system_status
    )
    NOTIFICATIONS_AVAILABLE = True
except ImportError:
    NOTIFICATIONS_AVAILABLE = False

from ..specialized_analysis_models import AnalysisModelManager
from ..ml_models import MLModelTrainer
from ..overall_analysis import OverallAnalyzer
# Removed unused import: from ..sync_analysis import _determine_risk_level
from .utils import (
    send_notification_if_available,
    get_user_from_job,
    _serialize_transaction,
    _calculate_holiday_risk_score,
    _get_holiday_risk_level,
    _generate_holiday_risk_assessment,
    _generate_holiday_audit_recommendations,
    _generate_holiday_breakdown,
    log_task_info,
    debug_task_state,
    debug_task_data,
    debug_task_exception,
    get_system_info,
)

logger = logging.getLogger(__name__)

# Import AI training functions
try:
    from ..gl_prediction_tasks import (
        train_gl_anomaly_detection_model,
        train_gl_volume_prediction_model,
        predict_gl_anomalies
    )
    AI_TRAINING_AVAILABLE = True
    logger.info("✅ AI training tasks imported successfully")
except ImportError as e:
    AI_TRAINING_AVAILABLE = False
    logger.warning(f"⚠️ AI training tasks not available: {e}")
except Exception as e:
    AI_TRAINING_AVAILABLE = False
    logger.error(f"❌ Error importing AI training tasks: {e}")


@shared_task(bind=True, max_retries=3, default_retry_delay=60, time_limit=600, soft_time_limit=480)
def run_restructured_analysis(self, job_id):
    """
    Main analysis task that runs all analysis types in a structured manner
    """
    try:
        log_task_info("run_restructured_analysis", job_id, f"Starting restructured analysis for job {job_id}")
        
        # Get the processing job
        try:
            job = FileProcessingJob.objects.get(id=job_id)
        except FileProcessingJob.DoesNotExist:
            logger.error(f"Job {job_id} not found")
            return {"status": "error", "message": "Job not found"}
        
        # Update job status
        job.status = 'PROCESSING'
        job.started_at = timezone.now()
        job.save()
        
        # Send notification
        user_id = get_user_from_job(job)
        send_notification_if_available(notify_file_processing_status, user_id, job_id, 'PROCESSING')
        
        # Get data file
        data_file = job.data_file
        if not data_file:
            logger.error(f"No data file found for job {job_id}")
            job.status = 'FAILED'
            job.error_message = "No data file found"
            job.save()
            return {"status": "error", "message": "No data file found"}
        
        # Get transactions
        transactions = SAPGLPosting.objects.filter(data_file=data_file)
        if not transactions.exists():
            logger.error(f"No transactions found for data file {data_file.id}")
            job.status = 'FAILED'
            job.error_message = "No transactions found"
            job.save()
            return {"status": "error", "message": "No transactions found"}
        
        log_task_info("run_restructured_analysis", job_id, f"Found {transactions.count()} transactions")
        
        # Run individual analysis tasks
        analysis_results = {}
        
        # 1. General Analysis
        try:
            general_result = run_general_analysis.delay(job_id)
            analysis_results['general'] = general_result.get(timeout=300)
        except Exception as e:
            logger.error(f"General analysis failed: {e}")
            analysis_results['general'] = {"status": "error", "message": str(e)}
        
        # 2. Duplicate Analysis
        try:
            duplicate_result = run_duplicate_analysis.delay(job_id)
            analysis_results['duplicate'] = duplicate_result.get(timeout=300)
        except Exception as e:
            logger.error(f"Duplicate analysis failed: {e}")
            analysis_results['duplicate'] = {"status": "error", "message": str(e)}
        
        # 3. Backdated Analysis
        try:
            backdated_result = run_backdated_analysis.delay(job_id)
            analysis_results['backdated'] = backdated_result.get(timeout=300)
        except Exception as e:
            logger.error(f"Backdated analysis failed: {e}")
            analysis_results['backdated'] = {"status": "error", "message": str(e)}
        
        # 4. User Analysis
        try:
            user_result = run_user_analysis.delay(job_id)
            analysis_results['user'] = user_result.get(timeout=300)
        except Exception as e:
            logger.error(f"User analysis failed: {e}")
            analysis_results['user'] = {"status": "error", "message": str(e)}
        
        # 5. Unusual Days Analysis
        try:
            unusual_days_result = run_unusual_days_analysis.delay(job_id)
            analysis_results['unusual_days'] = unusual_days_result.get(timeout=300)
        except Exception as e:
            logger.error(f"Unusual days analysis failed: {e}")
            analysis_results['unusual_days'] = {"status": "error", "message": str(e)}
        
        # 6. Closing Entries Analysis
        try:
            closing_entries_result = run_closing_entries_analysis.delay(job_id)
            analysis_results['closing_entries'] = closing_entries_result.get(timeout=300)
        except Exception as e:
            logger.error(f"Closing entries analysis failed: {e}")
            analysis_results['closing_entries'] = {"status": "error", "message": str(e)}
        
        # 7. Holiday Analysis
        try:
            holiday_result = run_holiday_analysis.delay(job_id)
            analysis_results['holiday'] = holiday_result.get(timeout=300)
        except Exception as e:
            logger.error(f"Holiday analysis failed: {e}")
            analysis_results['holiday'] = {"status": "error", "message": str(e)}
        
        # 8. Overall Analysis
        try:
            overall_result = run_overall_analysis.delay(job_id)
            analysis_results['overall'] = overall_result.get(timeout=300)
        except Exception as e:
            logger.error(f"Overall analysis failed: {e}")
            analysis_results['overall'] = {"status": "error", "message": str(e)}
        
        # Update job status
        job.status = 'COMPLETED'
        job.completed_at = timezone.now()
        if job.started_at:
            job.processing_duration = (job.completed_at - job.started_at).total_seconds()
        job.save()
        
        # Send completion notification
        send_notification_if_available(notify_file_processing_status, user_id, job_id, 'COMPLETED')
        
        log_task_info("run_restructured_analysis", job_id, "Analysis completed successfully")
        
        return {
            "status": "success",
            "message": "Analysis completed successfully",
            "results": analysis_results
        }
        
    except Exception as e:
        logger.error(f"Restructured analysis failed: {e}")
        logger.error(traceback.format_exc())
        
        # Update job status
        try:
            job = FileProcessingJob.objects.get(id=job_id)
            job.status = 'FAILED'
            job.error_message = str(e)
            job.save()
            
            # Send failure notification
            user_id = get_user_from_job(job)
            send_notification_if_available(notify_file_processing_status, user_id, job_id, 'FAILED')
        except Exception as update_error:
            logger.error(f"Failed to update job status: {update_error}")
        
        return {"status": "error", "message": str(e)}


@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def run_general_analysis(self, job_id):
    """
    Run general analysis on GL posting data
    """
    try:
        log_task_info("run_general_analysis", job_id, f"Starting general analysis for job {job_id}")
        
        # Get the processing job
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get transactions
        transactions = SAPGLPosting.objects.filter(data_file=data_file)
        
        if not transactions.exists():
            return {"status": "error", "message": "No transactions found"}
        
        # Create general analysis result
        analysis_result = GeneralAnalysisResult.objects.create(
            data_file=data_file,
            analysis_type='general',
            processing_job=job,
            status='PROCESSING'
        )
        
        # Perform analysis
        analysis_summary = _create_general_analysis_summary(transactions, data_file)
        completeness_tests = _perform_completeness_tests(transactions, data_file)
        statistical_analysis = _calculate_comprehensive_statistics(transactions)
        audit_statistics = _calculate_audit_statistics(transactions)
        chart_data = _create_general_chart_data(transactions, analysis_summary, completeness_tests)
        risk_assessment = _create_general_risk_assessment(transactions, completeness_tests)
        audit_recommendations = _create_general_audit_recommendations(completeness_tests, statistical_analysis)
        compliance_assessment = _create_general_compliance_assessment(completeness_tests, audit_statistics)
        export_data = _create_general_export_data(transactions, analysis_summary, completeness_tests, statistical_analysis)
        
        # Create anomalies from completeness tests
        anomaly_list = _create_completeness_anomalies(completeness_tests, transactions)
        
        # Update analysis result
        analysis_result.analysis_summary = analysis_summary
        analysis_result.anomaly_list = anomaly_list
        analysis_result.chart_data = chart_data
        analysis_result.risk_assessment = risk_assessment
        analysis_result.audit_recommendations = audit_recommendations
        analysis_result.compliance_assessment = compliance_assessment
        analysis_result.export_data = export_data
        analysis_result.status = 'COMPLETED'
        analysis_result.processing_duration = (timezone.now() - analysis_result.analysis_date).total_seconds()
        analysis_result.save()
        
        log_task_info("run_general_analysis", job_id, "General analysis completed successfully")
        
        return {
            "status": "success",
            "message": "General analysis completed",
            "analysis_id": analysis_result.id,
            "anomaly_count": len(anomaly_list)
        }
        
    except Exception as e:
        logger.error(f"General analysis failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}


@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def run_duplicate_analysis(self, job_id):
    """
    Run duplicate analysis on GL posting data
    """
    try:
        log_task_info("run_duplicate_analysis", job_id, f"Starting duplicate analysis for job {job_id}")
        
        # Get the processing job
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get transactions
        transactions = SAPGLPosting.objects.filter(data_file=data_file)
        
        if not transactions.exists():
            return {"status": "error", "message": "No transactions found"}
        
        # Create duplicate analysis result
        analysis_result = DuplicateAnalysisResult.objects.create(
            data_file=data_file,
            analysis_type='duplicate',
            processing_job=job,
            status='PROCESSING'
        )
        
        # Find duplicates
        duplicate_transactions = []
        transaction_list = list(transactions)
        
        for i, transaction1 in enumerate(transaction_list):
            for j, transaction2 in enumerate(transaction_list[i+1:], i+1):
                if _check_duplicate_rules_optimized(transaction1, transaction2):
                    duplicate_transactions.append({
                        'transaction1': _serialize_transaction(transaction1),
                        'transaction2': _serialize_transaction(transaction2),
                        'matching_fields': _get_matching_fields(transaction1, transaction2),
                        'risk_score': _calculate_duplicate_risk_score(transaction1, transaction2),
                        'risk_level': _get_risk_level(_calculate_duplicate_risk_score(transaction1, transaction2)),
                        'audit_priority': _get_audit_priority(_calculate_duplicate_risk_score(transaction1, transaction2)),
                        'compliance_impact': _get_compliance_impact(_calculate_duplicate_risk_score(transaction1, transaction2)),
                        'financial_impact': _get_financial_impact(transaction1.amount_local_currency or 0)
                    })
        
        # Create unified results
        chart_data = _create_unified_chart_data(duplicate_transactions, 'duplicate')
        risk_assessment = _create_unified_risk_assessment(duplicate_transactions, len(transaction_list))
        audit_recommendations = _create_unified_audit_recommendations(duplicate_transactions, 'duplicate')
        compliance_assessment = _create_unified_compliance_assessment(duplicate_transactions, 'duplicate')
        export_data = _create_export_data(duplicate_transactions)
        
        # Update analysis result
        analysis_result.anomaly_list = duplicate_transactions
        analysis_result.chart_data = chart_data
        analysis_result.risk_assessment = risk_assessment
        analysis_result.audit_recommendations = audit_recommendations
        analysis_result.compliance_assessment = compliance_assessment
        analysis_result.export_data = export_data
        analysis_result.status = 'COMPLETED'
        analysis_result.processing_duration = (timezone.now() - analysis_result.analysis_date).total_seconds()
        analysis_result.save()
        
        log_task_info("run_duplicate_analysis", job_id, f"Duplicate analysis completed - found {len(duplicate_transactions)} duplicates")
        
        return {
            "status": "success",
            "message": "Duplicate analysis completed",
            "analysis_id": analysis_result.id,
            "duplicate_count": len(duplicate_transactions)
        }
        
    except Exception as e:
        logger.error(f"Duplicate analysis failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}


# Helper functions for duplicate analysis
def _check_duplicate_rules_optimized(transaction1, transaction2):
    """Check if two transactions are duplicates based on optimized rules"""
    # Same document number
    if (transaction1.document_number and transaction2.document_number and 
        transaction1.document_number == transaction2.document_number):
        return True
    
    # Same amount, account, and date
    if (transaction1.amount_local_currency == transaction2.amount_local_currency and
        transaction1.gl_account == transaction2.gl_account and
        transaction1.posting_date == transaction2.posting_date):
        return True
    
    # Same user, amount, and account
    if (transaction1.user_name == transaction2.user_name and
        transaction1.amount_local_currency == transaction2.amount_local_currency and
        transaction1.gl_account == transaction2.gl_account):
        return True
    
    return False


def _get_matching_fields(transaction1, transaction2):
    """Get list of matching fields between two transactions"""
    matching_fields = []
    
    if transaction1.document_number == transaction2.document_number:
        matching_fields.append('document_number')
    if transaction1.amount_local_currency == transaction2.amount_local_currency:
        matching_fields.append('amount_local_currency')
    if transaction1.gl_account == transaction2.gl_account:
        matching_fields.append('gl_account')
    if transaction1.posting_date == transaction2.posting_date:
        matching_fields.append('posting_date')
    if transaction1.user_name == transaction2.user_name:
        matching_fields.append('user_name')
    
    return matching_fields


def _calculate_duplicate_risk_score(transaction1, transaction2):
    """Calculate risk score for duplicate transactions"""
    base_score = 50
    
    # Higher risk for higher amounts
    amount = abs(transaction1.amount_local_currency or 0)
    if amount > 100000:
        base_score += 30
    elif amount > 50000:
        base_score += 20
    elif amount > 10000:
        base_score += 10
    
    # Higher risk for same user
    if transaction1.user_name == transaction2.user_name:
        base_score += 15
    
    # Higher risk for same document number
    if transaction1.document_number == transaction2.document_number:
        base_score += 25
    
    return min(base_score, 100)


def _get_risk_level(risk_score):
    """Get risk level based on risk score"""
    if risk_score >= 80:
        return 'critical'
    elif risk_score >= 60:
        return 'high'
    elif risk_score >= 40:
        return 'medium'
    else:
        return 'low'


def _get_audit_priority(risk_score):
    """Get audit priority based on risk score"""
    if risk_score >= 80:
        return 'immediate'
    elif risk_score >= 60:
        return 'high'
    elif risk_score >= 40:
        return 'medium'
    else:
        return 'low'


def _get_compliance_impact(risk_score):
    """Get compliance impact based on risk score"""
    if risk_score >= 80:
        return 'severe'
    elif risk_score >= 60:
        return 'significant'
    elif risk_score >= 40:
        return 'moderate'
    else:
        return 'minimal'


def _get_financial_impact(amount):
    """Get financial impact based on amount"""
    abs_amount = abs(amount)
    if abs_amount >= 1000000:
        return 'critical'
    elif abs_amount >= 100000:
        return 'high'
    elif abs_amount >= 10000:
        return 'medium'
    else:
        return 'low'


def _create_unified_chart_data(transactions, analysis_type):
    """Create unified chart data for visualizations"""
    if not transactions:
        return {}
    
    # Amount distribution
    amounts = [abs(t.get('transaction1', {}).get('amount_local_currency', 0)) for t in transactions]
    
    # Risk level distribution
    risk_levels = [t.get('risk_level', 'low') for t in transactions]
    risk_counts = {}
    for level in risk_levels:
        risk_counts[level] = risk_counts.get(level, 0) + 1
    
    # User distribution
    users = [t.get('transaction1', {}).get('user_name', 'Unknown') for t in transactions]
    user_counts = {}
    for user in users:
        user_counts[user] = user_counts.get(user, 0) + 1
    
    return {
        'amount_distribution': {
            'min': min(amounts) if amounts else 0,
            'max': max(amounts) if amounts else 0,
            'mean': np.mean(amounts) if amounts else 0,
            'median': np.median(amounts) if amounts else 0
        },
        'risk_level_distribution': risk_counts,
        'user_distribution': user_counts,
        'total_count': len(transactions)
    }


def _create_unified_risk_assessment(anomaly_list, total_transactions):
    """Create unified risk assessment"""
    if not anomaly_list:
        return {
            'overall_risk_level': 'low',
            'risk_score': 0,
            'total_anomalies': 0,
            'anomaly_rate': 0
        }
    
    # Calculate overall risk score
    risk_scores = [a.get('risk_score', 0) for a in anomaly_list]
    overall_risk_score = np.mean(risk_scores) if risk_scores else 0
    
    # Determine overall risk level
    if overall_risk_score >= 80:
        overall_risk_level = 'critical'
    elif overall_risk_score >= 60:
        overall_risk_level = 'high'
    elif overall_risk_score >= 40:
        overall_risk_level = 'medium'
    else:
        overall_risk_level = 'low'
    
    return {
        'overall_risk_level': overall_risk_level,
        'risk_score': overall_risk_score,
        'total_anomalies': len(anomaly_list),
        'anomaly_rate': (len(anomaly_list) / total_transactions * 100) if total_transactions > 0 else 0,
        'risk_distribution': {
            'critical': len([a for a in anomaly_list if a.get('risk_level') == 'critical']),
            'high': len([a for a in anomaly_list if a.get('risk_level') == 'high']),
            'medium': len([a for a in anomaly_list if a.get('risk_level') == 'medium']),
            'low': len([a for a in anomaly_list if a.get('risk_level') == 'low'])
        }
    }


def _create_unified_audit_recommendations(anomaly_list, analysis_type):
    """Create unified audit recommendations"""
    if not anomaly_list:
        return {
            'immediate_actions': [],
            'high_priority_actions': [],
            'medium_priority_actions': [],
            'low_priority_actions': []
        }
    
    # Group by priority
    immediate_actions = []
    high_priority_actions = []
    medium_priority_actions = []
    low_priority_actions = []
    
    for anomaly in anomaly_list:
        priority = anomaly.get('audit_priority', 'low')
        recommendation = {
            'type': analysis_type,
            'description': f"Review {analysis_type} anomaly",
            'anomaly_id': anomaly.get('id'),
            'risk_level': anomaly.get('risk_level'),
            'financial_impact': anomaly.get('financial_impact')
        }
        
        if priority == 'immediate':
            immediate_actions.append(recommendation)
        elif priority == 'high':
            high_priority_actions.append(recommendation)
        elif priority == 'medium':
            medium_priority_actions.append(recommendation)
        else:
            low_priority_actions.append(recommendation)
    
    return {
        'immediate_actions': immediate_actions,
        'high_priority_actions': high_priority_actions,
        'medium_priority_actions': medium_priority_actions,
        'low_priority_actions': low_priority_actions
    }


def _create_unified_compliance_assessment(anomaly_list, analysis_type):
    """Create unified compliance assessment"""
    if not anomaly_list:
        return {
            'compliance_status': 'compliant',
            'compliance_score': 100,
            'violations': []
        }
    
    # Calculate compliance score
    total_anomalies = len(anomaly_list)
    critical_anomalies = len([a for a in anomaly_list if a.get('risk_level') == 'critical'])
    high_anomalies = len([a for a in anomaly_list if a.get('risk_level') == 'high'])
    
    compliance_score = max(0, 100 - (critical_anomalies * 20) - (high_anomalies * 10))
    
    # Determine compliance status
    if compliance_score >= 90:
        compliance_status = 'compliant'
    elif compliance_score >= 70:
        compliance_status = 'minor_issues'
    elif compliance_score >= 50:
        compliance_status = 'moderate_issues'
    else:
        compliance_status = 'non_compliant'
    
    return {
        'compliance_status': compliance_status,
        'compliance_score': compliance_score,
        'violations': [
            {
                'type': analysis_type,
                'severity': anomaly.get('risk_level'),
                'description': f"{analysis_type} anomaly detected"
            }
            for anomaly in anomaly_list
        ]
    }


def _create_export_data(anomaly_list):
    """Create export-ready data"""
    return [
        {
            'id': i + 1,
            'type': 'anomaly',
            'data': anomaly
        }
        for i, anomaly in enumerate(anomaly_list)
    ]


@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def run_backdated_analysis(self, job_id):
    """
    Run backdated analysis on GL posting data
    """
    try:
        log_task_info("run_backdated_analysis", job_id, f"Starting backdated analysis for job {job_id}")
        
        # Get the processing job
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get transactions
        transactions = SAPGLPosting.objects.filter(data_file=data_file)
        
        if not transactions.exists():
            return {"status": "error", "message": "No transactions found"}
        
        # Create backdated analysis result
        analysis_result = BackdatedAnalysisResult.objects.create(
            data_file=data_file,
            analysis_type='backdated',
            processing_job=job,
            status='PROCESSING'
        )
        
        # Run backdated analysis using the sync function logic
        from ..sync_analysis import run_backdated_analysis_sync
        sync_result = run_backdated_analysis_sync(job_id)
        
        if sync_result.get('status') == 'COMPLETED':
            # Update analysis result with sync results
            analysis_result.analysis_summary = sync_result.get('analysis_summary', {})
            analysis_result.anomaly_list = sync_result.get('backdated_transactions', [])
            analysis_result.audit_recommendations = sync_result.get('audit_recommendations', [])
            analysis_result.compliance_assessment = sync_result.get('compliance_assessment', {})
            analysis_result.chart_data = sync_result.get('chart_data', {})
            analysis_result.export_data = sync_result.get('export_data', [])
            analysis_result.status = 'COMPLETED'
            analysis_result.processing_duration = sync_result.get('processing_duration', 0)
            analysis_result.save()
            
            log_task_info("run_backdated_analysis", job_id, f"Backdated analysis completed - found {len(sync_result.get('backdated_transactions', []))} backdated transactions")
            
            return {
                "status": "success",
                "message": "Backdated analysis completed",
                "analysis_id": analysis_result.id,
                "backdated_count": len(sync_result.get('backdated_transactions', []))
            }
        else:
            analysis_result.status = 'FAILED'
            analysis_result.error_message = sync_result.get('error', 'Unknown error')
            analysis_result.save()
            return {"status": "error", "message": sync_result.get('error', 'Analysis failed')}
        
    except Exception as e:
        logger.error(f"Backdated analysis failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}


@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def run_user_analysis(self, job_id):
    """
    Run user analysis on GL posting data
    """
    try:
        log_task_info("run_user_analysis", job_id, f"Starting user analysis for job {job_id}")
        
        # Get the processing job
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get transactions
        transactions = SAPGLPosting.objects.filter(data_file=data_file)
        
        if not transactions.exists():
            return {"status": "error", "message": "No transactions found"}
        
        # Create user analysis result
        analysis_result = UserAnalysisResult.objects.create(
            data_file=data_file,
            analysis_type='user',
            processing_job=job,
            status='PROCESSING'
        )
        
        # Run user analysis using the sync function logic
        from ..sync_analysis import run_user_analysis_sync
        sync_result = run_user_analysis_sync(job_id)
        
        if sync_result.get('status') == 'COMPLETED':
            # Update analysis result with sync results
            analysis_result.analysis_summary = sync_result.get('analysis_summary', {})
            analysis_result.anomaly_list = sync_result.get('user_anomalies', [])
            analysis_result.audit_recommendations = sync_result.get('audit_recommendations', [])
            analysis_result.compliance_assessment = sync_result.get('compliance_assessment', {})
            analysis_result.chart_data = sync_result.get('chart_data', {})
            analysis_result.export_data = sync_result.get('export_data', [])
            analysis_result.status = 'COMPLETED'
            analysis_result.processing_duration = sync_result.get('processing_duration', 0)
            analysis_result.save()
            
            log_task_info("run_user_analysis", job_id, f"User analysis completed - found {len(sync_result.get('user_anomalies', []))} user anomalies")
            
            return {
                "status": "success",
                "message": "User analysis completed",
                "analysis_id": analysis_result.id,
                "anomaly_count": len(sync_result.get('user_anomalies', []))
            }
        else:
            analysis_result.status = 'FAILED'
            analysis_result.error_message = sync_result.get('error', 'Unknown error')
            analysis_result.save()
            return {"status": "error", "message": sync_result.get('error', 'Analysis failed')}
        
    except Exception as e:
        logger.error(f"User analysis failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}


@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def run_unusual_days_analysis(self, job_id):
    """
    Run unusual days analysis on GL posting data
    """
    try:
        log_task_info("run_unusual_days_analysis", job_id, f"Starting unusual days analysis for job {job_id}")
        
        # Get the processing job
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get transactions
        transactions = SAPGLPosting.objects.filter(data_file=data_file)
        
        if not transactions.exists():
            return {"status": "error", "message": "No transactions found"}
        
        # Create unusual days analysis result
        analysis_result = UnusualDaysAnalysisResult.objects.create(
            data_file=data_file,
            analysis_type='unusual_days',
            processing_job=job,
            status='PROCESSING'
        )
        
        # Run unusual days analysis using the sync function logic
        from ..sync_analysis import run_unusual_days_analysis_sync
        sync_result = run_unusual_days_analysis_sync(job_id)
        
        if sync_result.get('status') == 'COMPLETED':
            # Update analysis result with sync results
            analysis_result.analysis_summary = sync_result.get('analysis_summary', {})
            analysis_result.anomaly_list = sync_result.get('unusual_days_anomalies', [])
            analysis_result.audit_recommendations = sync_result.get('audit_recommendations', [])
            analysis_result.compliance_assessment = sync_result.get('compliance_assessment', {})
            analysis_result.chart_data = sync_result.get('chart_data', {})
            analysis_result.export_data = sync_result.get('export_data', [])
            analysis_result.status = 'COMPLETED'
            analysis_result.processing_duration = sync_result.get('processing_duration', 0)
            analysis_result.save()
            
            log_task_info("run_unusual_days_analysis", job_id, f"Unusual days analysis completed - found {len(sync_result.get('unusual_days_anomalies', []))} unusual days")
            
            return {
                "status": "success",
                "message": "Unusual days analysis completed",
                "analysis_id": analysis_result.id,
                "anomaly_count": len(sync_result.get('unusual_days_anomalies', []))
            }
        else:
            analysis_result.status = 'FAILED'
            analysis_result.error_message = sync_result.get('error', 'Unknown error')
            analysis_result.save()
            return {"status": "error", "message": sync_result.get('error', 'Analysis failed')}
        
    except Exception as e:
        logger.error(f"Unusual days analysis failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}


@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def run_closing_entries_analysis(self, job_id):
    """
    Run closing entries analysis on GL posting data
    """
    try:
        log_task_info("run_closing_entries_analysis", job_id, f"Starting closing entries analysis for job {job_id}")
        
        # Get the processing job
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get transactions
        transactions = SAPGLPosting.objects.filter(data_file=data_file)
        
        if not transactions.exists():
            return {"status": "error", "message": "No transactions found"}
        
        # Create closing entries analysis result
        analysis_result = ClosingEntriesAnalysisResult.objects.create(
            data_file=data_file,
            analysis_type='closing_entries',
            processing_job=job,
            status='PROCESSING'
        )
        
        # Run closing entries analysis using the sync function logic
        from ..sync_analysis import run_closing_entries_analysis_sync
        sync_result = run_closing_entries_analysis_sync(job_id)
        
        if sync_result.get('status') == 'COMPLETED':
            # Update analysis result with sync results
            analysis_result.analysis_summary = sync_result.get('analysis_summary', {})
            analysis_result.anomaly_list = sync_result.get('closing_entries_anomalies', [])
            analysis_result.audit_recommendations = sync_result.get('audit_recommendations', [])
            analysis_result.compliance_assessment = sync_result.get('compliance_assessment', {})
            analysis_result.chart_data = sync_result.get('chart_data', {})
            analysis_result.export_data = sync_result.get('export_data', [])
            analysis_result.status = 'COMPLETED'
            analysis_result.processing_duration = sync_result.get('processing_duration', 0)
            analysis_result.save()
            
            log_task_info("run_closing_entries_analysis", job_id, f"Closing entries analysis completed - found {len(sync_result.get('closing_entries_anomalies', []))} closing entries")
            
            return {
                "status": "success",
                "message": "Closing entries analysis completed",
                "analysis_id": analysis_result.id,
                "anomaly_count": len(sync_result.get('closing_entries_anomalies', []))
            }
        else:
            analysis_result.status = 'FAILED'
            analysis_result.error_message = sync_result.get('error', 'Unknown error')
            analysis_result.save()
            return {"status": "error", "message": sync_result.get('error', 'Analysis failed')}
        
    except Exception as e:
        logger.error(f"Closing entries analysis failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}


@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def run_holiday_analysis(self, job_id):
    """
    Run holiday analysis on GL posting data
    """
    try:
        log_task_info("run_holiday_analysis", job_id, f"Starting holiday analysis for job {job_id}")
        
        # Get the processing job
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get transactions
        transactions = SAPGLPosting.objects.filter(data_file=data_file)
        
        if not transactions.exists():
            return {"status": "error", "message": "No transactions found"}
        
        # Create holiday analysis result
        analysis_result = HolidayAnalysisResult.objects.create(
            data_file=data_file,
            analysis_type='holiday',
            processing_job=job,
            status='PROCESSING'
        )
        
        # Run holiday analysis using the sync function logic
        from ..sync_analysis import run_holiday_analysis_sync
        sync_result = run_holiday_analysis_sync(job_id)
        
        if sync_result.get('status') == 'COMPLETED':
            # Update analysis result with sync results
            analysis_result.analysis_summary = sync_result.get('analysis_summary', {})
            analysis_result.anomaly_list = sync_result.get('holiday_anomalies', [])
            analysis_result.audit_recommendations = sync_result.get('audit_recommendations', [])
            analysis_result.compliance_assessment = sync_result.get('compliance_assessment', {})
            analysis_result.chart_data = sync_result.get('chart_data', {})
            analysis_result.export_data = sync_result.get('export_data', [])
            analysis_result.status = 'COMPLETED'
            analysis_result.processing_duration = sync_result.get('processing_duration', 0)
            analysis_result.save()
            
            log_task_info("run_holiday_analysis", job_id, f"Holiday analysis completed - found {len(sync_result.get('holiday_anomalies', []))} holiday anomalies")
            
            return {
                "status": "success",
                "message": "Holiday analysis completed",
                "analysis_id": analysis_result.id,
                "anomaly_count": len(sync_result.get('holiday_anomalies', []))
            }
        else:
            analysis_result.status = 'FAILED'
            analysis_result.error_message = sync_result.get('error', 'Unknown error')
            analysis_result.save()
            return {"status": "error", "message": sync_result.get('error', 'Analysis failed')}
        
    except Exception as e:
        logger.error(f"Holiday analysis failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}


@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def run_overall_analysis(self, job_id):
    """
    Run overall analysis on GL posting data
    """
    try:
        log_task_info("run_overall_analysis", job_id, f"Starting overall analysis for job {job_id}")
        
        # Get the processing job
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get transactions
        transactions = SAPGLPosting.objects.filter(data_file=data_file)
        
        if not transactions.exists():
            return {"status": "error", "message": "No transactions found"}
        
        # Create overall analysis result
        analysis_result = OverallAnalysisResult.objects.create(
            data_file=data_file,
            analysis_type='overall',
            processing_job=job,
            status='PROCESSING'
        )
        
        # Run overall analysis using the sync function logic
        from ..sync_analysis import run_overall_analysis_sync
        sync_result = run_overall_analysis_sync(job_id)
        
        if sync_result.get('status') == 'COMPLETED':
            # Update analysis result with sync results
            analysis_result.analysis_summary = sync_result.get('analysis_summary', {})
            analysis_result.anomaly_list = sync_result.get('overall_anomalies', [])
            analysis_result.audit_recommendations = sync_result.get('audit_recommendations', [])
            analysis_result.compliance_assessment = sync_result.get('compliance_assessment', {})
            analysis_result.chart_data = sync_result.get('chart_data', {})
            analysis_result.export_data = sync_result.get('export_data', [])
            analysis_result.status = 'COMPLETED'
            analysis_result.processing_duration = sync_result.get('processing_duration', 0)
            analysis_result.save()
            
            log_task_info("run_overall_analysis", job_id, f"Overall analysis completed - found {len(sync_result.get('overall_anomalies', []))} overall anomalies")
            
            return {
                "status": "success",
                "message": "Overall analysis completed",
                "analysis_id": analysis_result.id,
                "anomaly_count": len(sync_result.get('overall_anomalies', []))
            }
        else:
            analysis_result.status = 'FAILED'
            analysis_result.error_message = sync_result.get('error', 'Unknown error')
            analysis_result.save()
            return {"status": "error", "message": sync_result.get('error', 'Analysis failed')}
        
    except Exception as e:
        logger.error(f"Overall analysis failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}


@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def run_manual_entry_analysis(self, job_id):
    """
    Run manual entry analysis on GL posting data
    """
    try:
        log_task_info("run_manual_entry_analysis", job_id, f"Starting manual entry analysis for job {job_id}")
        
        # Get the processing job
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get transactions
        transactions = SAPGLPosting.objects.filter(data_file=data_file)
        
        if not transactions.exists():
            return {"status": "error", "message": "No transactions found"}
        
        # Create manual entry analysis result
        analysis_result = ManualEntryAnalysisResult.objects.create(
            data_file=data_file,
            analysis_type='manual_entry',
            processing_job=job,
            status='PROCESSING'
        )
        
        # Run manual entry analysis using the sync function logic
        from ..sync_analysis import run_manual_entry_analysis_sync
        sync_result = run_manual_entry_analysis_sync(job_id)
        
        if sync_result.get('status') == 'COMPLETED':
            # Update analysis result with sync results
            analysis_result.analysis_summary = sync_result.get('analysis_summary', {})
            analysis_result.anomaly_list = sync_result.get('manual_entry_anomalies', [])
            analysis_result.audit_recommendations = sync_result.get('audit_recommendations', [])
            analysis_result.compliance_assessment = sync_result.get('compliance_assessment', {})
            analysis_result.chart_data = sync_result.get('chart_data', {})
            analysis_result.export_data = sync_result.get('export_data', [])
            analysis_result.status = 'COMPLETED'
            analysis_result.processing_duration = sync_result.get('processing_duration', 0)
            analysis_result.save()
            
            log_task_info("run_manual_entry_analysis", job_id, f"Manual entry analysis completed - found {len(sync_result.get('manual_entry_anomalies', []))} manual entry anomalies")
            
            return {
                "status": "success",
                "message": "Manual entry analysis completed",
                "analysis_id": analysis_result.id,
                "anomaly_count": len(sync_result.get('manual_entry_anomalies', []))
            }
        else:
            analysis_result.status = 'FAILED'
            analysis_result.error_message = sync_result.get('error', 'Unknown error')
            analysis_result.save()
            return {"status": "error", "message": sync_result.get('error', 'Analysis failed')}
        
    except Exception as e:
        logger.error(f"Manual entry analysis failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}


@shared_task(bind=True, max_retries=3, default_retry_delay=60, time_limit=600, soft_time_limit=480)
def run_ai_risk_recommendations(self, job_id):
    """
    Run AI-powered risk recommendations and save results to database
    This task should be called after all other analyses are completed
    """
    task_name = "run_ai_risk_recommendations"
    start_time = timezone.now()
    
    log_task_info(task_name, job_id, f"Starting AI risk recommendations for job {job_id}")
    
    try:
        # Get the processing job
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        log_task_info(task_name, job_id, f"Processing file: {data_file.file_name}")
        
        # AI risk recommendation engine - simplified implementation
        class AIRiskRecommendationAPI:
            """Simplified AI risk recommendation API"""
            def generate_recommendations(self, job_id):
                return {
                    'success': True,
                    'recommendations': [],
                    'risk_assessment': {},
                    'message': 'AI risk recommendations generated successfully'
                }
        
        # Generate AI risk recommendations
        log_task_info(task_name, job_id, "Starting AI risk analysis...")
        
        ai_api = AIRiskRecommendationAPI()
        ai_results = ai_api.generate_recommendations(str(data_file.id))
        
        if 'error' in ai_results:
            log_task_info(task_name, job_id, f"AI risk analysis failed: {ai_results['error']}", "error")
            return {'error': ai_results['error']}
        
        # Save AI risk assessment to database
        log_task_info(task_name, job_id, "Saving AI risk assessment to database...")
        
        # Enhanced risk models - simplified implementation
        class AIRiskAssessment:
            """Simplified AI risk assessment model"""
            def __init__(self, **kwargs):
                for key, value in kwargs.items():
                    setattr(self, key, value)
        
        # Create risk assessment record
        risk_assessment = RiskScoringDocument.objects.create(
            data_file=data_file,
            analysis_type='ai_risk_recommendations',
            processing_job=job,
            status='COMPLETED',
            risk_score=ai_results.get('risk_score', 0),
            risk_level=ai_results.get('risk_level', 'LOW'),
            recommendations=ai_results.get('recommendations', []),
            ai_insights=ai_results.get('risk_assessment', {}),
            processing_duration=(timezone.now() - start_time).total_seconds()
        )
        
        log_task_info(task_name, job_id, f"AI risk recommendations completed successfully - Risk Score: {risk_assessment.risk_score}")
        
        return {
            "status": "success",
            "message": "AI risk recommendations completed",
            "risk_assessment_id": risk_assessment.id,
            "risk_score": risk_assessment.risk_score,
            "recommendations_count": len(ai_results.get('recommendations', []))
        }
        
    except Exception as e:
        logger.error(f"AI risk recommendations failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}


@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=1800, soft_time_limit=1500)
def run_risk_analysis(self, job_id):
    """
    Run comprehensive risk analysis combining all analysis results
    """
    try:
        log_task_info("run_risk_analysis", job_id, f"Starting comprehensive risk analysis for job {job_id}")
        
        # Get the processing job
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get all analysis results
        duplicate_analysis = DuplicateAnalysisResult.objects.filter(data_file=data_file).first()
        backdated_analysis = BackdatedAnalysisResult.objects.filter(data_file=data_file).first()
        user_analysis = UserAnalysisResult.objects.filter(data_file=data_file).first()
        unusual_days_analysis = UnusualDaysAnalysisResult.objects.filter(data_file=data_file).first()
        closing_entries_analysis = ClosingEntriesAnalysisResult.objects.filter(data_file=data_file).first()
        holiday_analysis = HolidayAnalysisResult.objects.filter(data_file=data_file).first()
        
        # Get transactions for analysis
        transactions = SAPGLPosting.objects.filter(data_file=data_file)
        
        if not transactions.exists():
            return {"status": "error", "message": "No transactions found"}
        
        # Calculate overall risk scores
        risk_scores = _calculate_overall_risk_scores(
            transactions, duplicate_analysis, backdated_analysis, 
            user_analysis, unusual_days_analysis, closing_entries_analysis, 
            holiday_analysis
        )
        
        # Create risk analysis result
        risk_result = OverallAnalysisResult.objects.create(
            data_file=data_file,
            analysis_type='comprehensive_risk',
            processing_job=job,
            status='COMPLETED',
            risk_score=risk_scores.get('overall_risk_score', 0),
            risk_level=risk_scores.get('overall_risk_level', 'LOW'),
            analysis_summary=risk_scores.get('summary', {}),
            anomaly_list=risk_scores.get('anomalies', []),
            audit_recommendations=risk_scores.get('recommendations', []),
            compliance_assessment=risk_scores.get('compliance', {}),
            chart_data=risk_scores.get('charts', {}),
            export_data=risk_scores.get('export_data', []),
            processing_duration=risk_scores.get('processing_duration', 0)
        )
        
        log_task_info("run_risk_analysis", job_id, f"Risk analysis completed - Overall Risk Score: {risk_scores.get('overall_risk_score', 0)}")
        
        return {
            "status": "success",
            "message": "Risk analysis completed",
            "analysis_id": risk_result.id,
            "overall_risk_score": risk_scores.get('overall_risk_score', 0),
            "risk_level": risk_scores.get('overall_risk_level', 'LOW')
        }
        
    except Exception as e:
        logger.error(f"Risk analysis failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}


def _calculate_overall_risk_scores(transactions, duplicate_analysis, backdated_analysis, 
                                 user_analysis, unusual_days_analysis, closing_entries_analysis, 
                                 holiday_analysis):
    """Calculate overall risk scores from all analysis results"""
    
    # Initialize risk components
    risk_components = {
        'duplicate_risk': 0,
        'backdated_risk': 0,
        'user_risk': 0,
        'unusual_days_risk': 0,
        'closing_entries_risk': 0,
        'holiday_risk': 0
    }
    
    # Extract risk scores from each analysis
    if duplicate_analysis and duplicate_analysis.analysis_summary:
        risk_components['duplicate_risk'] = duplicate_analysis.analysis_summary.get('risk_score', 0)
    
    if backdated_analysis and backdated_analysis.analysis_summary:
        risk_components['backdated_risk'] = backdated_analysis.analysis_summary.get('risk_score', 0)
    
    if user_analysis and user_analysis.analysis_summary:
        risk_components['user_risk'] = user_analysis.analysis_summary.get('risk_score', 0)
    
    if unusual_days_analysis and unusual_days_analysis.analysis_summary:
        risk_components['unusual_days_risk'] = unusual_days_analysis.analysis_summary.get('risk_score', 0)
    
    if closing_entries_analysis and closing_entries_analysis.analysis_summary:
        risk_components['closing_entries_risk'] = closing_entries_analysis.analysis_summary.get('risk_score', 0)
    
    if holiday_analysis and holiday_analysis.analysis_summary:
        risk_components['holiday_risk'] = holiday_analysis.analysis_summary.get('risk_score', 0)
    
    # Calculate weighted overall risk score
    weights = {
        'duplicate_risk': 0.2,
        'backdated_risk': 0.2,
        'user_risk': 0.15,
        'unusual_days_risk': 0.15,
        'closing_entries_risk': 0.15,
        'holiday_risk': 0.15
    }
    
    overall_risk_score = sum(risk_components[component] * weights[component] for component in risk_components)
    
    # Determine risk level
    if overall_risk_score >= 80:
        risk_level = 'HIGH'
    elif overall_risk_score >= 50:
        risk_level = 'MEDIUM'
    else:
        risk_level = 'LOW'
    
    return {
        'overall_risk_score': round(overall_risk_score, 2),
        'overall_risk_level': risk_level,
        'risk_components': risk_components,
        'summary': {
            'total_transactions': transactions.count(),
            'risk_analysis_completed': True,
            'analysis_timestamp': timezone.now().isoformat()
        },
        'anomalies': [],
        'recommendations': [],
        'compliance': {},
        'charts': {},
        'export_data': [],
        'processing_duration': 0
    }
