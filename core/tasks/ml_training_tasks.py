"""
ML Training Tasks Module

Contains all machine learning training-related Celery tasks for the analytics system.
"""

from celery import shared_task, current_task
from django.utils import timezone
from django.db import transaction
from django.db.models import F, Q, Count, Sum, Avg, Min, Max
import logging
import traceback
import numpy as np
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, Any, List

from ..models import (
    FileProcessingJob, SAPGLPosting, DataFile, MLModelTraining,
    DuplicateAnalysisModelTraining, BackdatedAnalysisModelTraining,
    UserAnalysisModelTraining, UnusualDaysAnalysisModelTraining,
    ClosingEntriesAnalysisModelTraining, HolidayAnalysisModelTraining,
    OverallRiskAnalysisModelTraining, RuleBasedModelTraining
)

from ..specialized_analysis_models import AnalysisModelManager
from ..ml_models import MLModelTrainer

from .utils import (
    send_notification_if_available,
    get_user_from_job,
    log_task_info,
    debug_task_state,
    debug_task_data,
    debug_task_exception,
)

logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=600, soft_time_limit=480)
def train_rule_based_models(self, job_id):
    """
    Train rule-based models for analysis
    """
    try:
        log_task_info("train_rule_based_models", job_id, f"Starting rule-based model training for job {job_id}")
        
        # Get the processing job
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get transactions
        transactions = list(SAPGLPosting.objects.filter(data_file=data_file))
        
        if not transactions:
            return {"status": "error", "message": "No transactions found"}
        
        # Create training record
        training_record = RuleBasedModelTraining.objects.create(
            model_name="RuleBasedAnalysis",
            model_type="rule_based",
            training_data_size=len(transactions),
            status='TRAINING',
            training_started_at=timezone.now()
        )
        
        try:
            # Train individual rule sets
            duplicate_rules = _train_duplicate_rules(transactions)
            backdated_rules = _train_backdated_rules(transactions)
            user_rules = _train_user_rules(transactions)
            unusual_days_rules = _train_unusual_days_rules(transactions)
            closing_entries_rules = _train_closing_entries_rules(transactions)
            holiday_rules = _train_holiday_rules(transactions)
            risk_scoring_rules = _train_risk_scoring_rules(transactions)
            
            # Combine all rules
            combined_rules = {
                'duplicate_rules': duplicate_rules,
                'backdated_rules': backdated_rules,
                'user_rules': user_rules,
                'unusual_days_rules': unusual_days_rules,
                'closing_entries_rules': closing_entries_rules,
                'holiday_rules': holiday_rules,
                'risk_scoring_rules': risk_scoring_rules
            }
            
            # Update training record
            training_record.model_parameters = combined_rules
            training_record.status = 'COMPLETED'
            training_record.training_completed_at = timezone.now()
            training_record.training_duration = (training_record.training_completed_at - training_record.training_started_at).total_seconds()
            training_record.save()
            
            log_task_info("train_rule_based_models", job_id, "Rule-based model training completed successfully")
            
            return {
                "status": "success",
                "message": "Rule-based model training completed",
                "training_id": training_record.id,
                "rules_count": sum(len(rules) for rules in combined_rules.values())
            }
            
        except Exception as e:
            training_record.status = 'FAILED'
            training_record.error_message = str(e)
            training_record.save()
            raise e
        
    except Exception as e:
        logger.error(f"Rule-based model training failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}


@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def retrain_rule_based_models(self, job_id):
    """
    Retrain rule-based models with new data
    """
    try:
        log_task_info("retrain_rule_based_models", job_id, f"Starting rule-based model retraining for job {job_id}")
        
        # Get the processing job
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get transactions
        transactions = list(SAPGLPosting.objects.filter(data_file=data_file))
        
        if not transactions:
            return {"status": "error", "message": "No transactions found"}
        
        # Get existing training record
        existing_training = RuleBasedModelTraining.objects.filter(
            model_name="RuleBasedAnalysis"
        ).order_by('-created_at').first()
        
        if not existing_training:
            # If no existing training, create new one
            return train_rule_based_models.delay(job_id)
        
        # Create new training record
        training_record = RuleBasedModelTraining.objects.create(
            model_name="RuleBasedAnalysis",
            model_type="rule_based",
            model_version=f"{existing_training.model_version.split('.')[0]}.{int(existing_training.model_version.split('.')[1]) + 1}.0",
            training_data_size=len(transactions),
            status='TRAINING',
            training_started_at=timezone.now()
        )
        
        try:
            # Retrain with combined data (existing + new)
            all_transactions = list(SAPGLPosting.objects.filter(
                data_file__engagement=data_file.engagement
            ))
            
            # Train individual rule sets
            duplicate_rules = _train_duplicate_rules(all_transactions)
            backdated_rules = _train_backdated_rules(all_transactions)
            user_rules = _train_user_rules(all_transactions)
            unusual_days_rules = _train_unusual_days_rules(all_transactions)
            closing_entries_rules = _train_closing_entries_rules(all_transactions)
            holiday_rules = _train_holiday_rules(all_transactions)
            risk_scoring_rules = _train_risk_scoring_rules(all_transactions)
            
            # Combine all rules
            combined_rules = {
                'duplicate_rules': duplicate_rules,
                'backdated_rules': backdated_rules,
                'user_rules': user_rules,
                'unusual_days_rules': unusual_days_rules,
                'closing_entries_rules': closing_entries_rules,
                'holiday_rules': holiday_rules,
                'risk_scoring_rules': risk_scoring_rules
            }
            
            # Update training record
            training_record.model_parameters = combined_rules
            training_record.status = 'COMPLETED'
            training_record.training_completed_at = timezone.now()
            training_record.training_duration = (training_record.training_completed_at - training_record.training_started_at).total_seconds()
            training_record.save()
            
            # Calculate improvement
            improvement = _calculate_retraining_improvement({
                'old_rules_count': sum(len(rules) for rules in existing_training.model_parameters.values()) if existing_training.model_parameters else 0,
                'new_rules_count': sum(len(rules) for rules in combined_rules.values()),
                'old_data_size': existing_training.training_data_size,
                'new_data_size': len(all_transactions)
            })
            
            log_task_info("retrain_rule_based_models", job_id, "Rule-based model retraining completed successfully")
            
            return {
                "status": "success",
                "message": "Rule-based model retraining completed",
                "training_id": training_record.id,
                "improvement": improvement
            }
            
        except Exception as e:
            training_record.status = 'FAILED'
            training_record.error_message = str(e)
            training_record.save()
            raise e
        
    except Exception as e:
        logger.error(f"Rule-based model retraining failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}


@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def train_duplicate_analysis_model(self, job_id):
    """
    Train ML model for duplicate analysis
    """
    try:
        log_task_info("train_duplicate_analysis_model", job_id, f"Starting duplicate analysis model training for job {job_id}")
        
        # Get the processing job
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get transactions
        transactions = list(SAPGLPosting.objects.filter(data_file=data_file))
        
        if not transactions:
            return {"status": "error", "message": "No transactions found"}
        
        # Create training record
        training_record = DuplicateAnalysisModelTraining.objects.create(
            model_name="DuplicateAnalysisML",
            model_type="classification",
            training_data_size=len(transactions),
            status='TRAINING',
            training_started_at=timezone.now()
        )
        
        try:
            # Train the model
            model_trainer = MLModelTrainer()
            training_results = model_trainer.train_duplicate_model(transactions)
            
            # Update training record
            training_record.training_accuracy = training_results.get('training_accuracy', 0)
            training_record.validation_accuracy = training_results.get('validation_accuracy', 0)
            training_record.test_accuracy = training_results.get('test_accuracy', 0)
            training_record.model_parameters = training_results.get('model_parameters', {})
            training_record.feature_importance = training_results.get('feature_importance', [])
            training_record.status = 'COMPLETED'
            training_record.training_completed_at = timezone.now()
            training_record.training_duration = (training_record.training_completed_at - training_record.training_started_at).total_seconds()
            training_record.save()
            
            log_task_info("train_duplicate_analysis_model", job_id, "Duplicate analysis model training completed successfully")
            
            return {
                "status": "success",
                "message": "Duplicate analysis model training completed",
                "training_id": training_record.id,
                "accuracy": training_results.get('validation_accuracy', 0)
            }
            
        except Exception as e:
            training_record.status = 'FAILED'
            training_record.error_message = str(e)
            training_record.save()
            raise e
        
    except Exception as e:
        logger.error(f"Duplicate analysis model training failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}


@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def train_backdated_analysis_model(self, job_id):
    """
    Train ML model for backdated analysis
    """
    try:
        log_task_info("train_backdated_analysis_model", job_id, f"Starting backdated analysis model training for job {job_id}")
        
        # Get the processing job
        job = FileProcessingJob.objects.get(id=job_id)
        data_file = job.data_file
        
        # Get transactions
        transactions = list(SAPGLPosting.objects.filter(data_file=data_file))
        
        if not transactions:
            return {"status": "error", "message": "No transactions found"}
        
        # Create training record
        training_record = BackdatedAnalysisModelTraining.objects.create(
            model_name="BackdatedAnalysisML",
            model_type="classification",
            training_data_size=len(transactions),
            status='TRAINING',
            training_started_at=timezone.now()
        )
        
        try:
            # Train the model
            model_trainer = MLModelTrainer()
            training_results = model_trainer.train_backdated_model(transactions)
            
            # Update training record
            training_record.training_accuracy = training_results.get('training_accuracy', 0)
            training_record.validation_accuracy = training_results.get('validation_accuracy', 0)
            training_record.test_accuracy = training_results.get('test_accuracy', 0)
            training_record.model_parameters = training_results.get('model_parameters', {})
            training_record.feature_importance = training_results.get('feature_importance', [])
            training_record.status = 'COMPLETED'
            training_record.training_completed_at = timezone.now()
            training_record.training_duration = (training_record.training_completed_at - training_record.training_started_at).total_seconds()
            training_record.save()
            
            log_task_info("train_backdated_analysis_model", job_id, "Backdated analysis model training completed successfully")
            
            return {
                "status": "success",
                "message": "Backdated analysis model training completed",
                "training_id": training_record.id,
                "accuracy": training_results.get('validation_accuracy', 0)
            }
            
        except Exception as e:
            training_record.status = 'FAILED'
            training_record.error_message = str(e)
            training_record.save()
            raise e
        
    except Exception as e:
        logger.error(f"Backdated analysis model training failed: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}


# Continue with other ML training tasks...
# (The rest of the ML training tasks would be added here following the same pattern)


def _train_duplicate_rules(transactions):
    """Train duplicate detection rules"""
    rules = []
    
    # Rule 1: Same document number
    doc_numbers = [t.document_number for t in transactions if t.document_number]
    duplicate_docs = [doc for doc in set(doc_numbers) if doc_numbers.count(doc) > 1]
    
    if duplicate_docs:
        rules.append({
            'type': 'same_document_number',
            'description': 'Transactions with same document number',
            'threshold': 1,
            'examples': duplicate_docs[:5]
        })
    
    # Rule 2: Same amount, account, and date
    amount_account_date = {}
    for t in transactions:
        key = (t.amount_local_currency, t.gl_account, t.posting_date)
        if key not in amount_account_date:
            amount_account_date[key] = []
        amount_account_date[key].append(t.id)
    
    duplicate_combinations = {k: v for k, v in amount_account_date.items() if len(v) > 1}
    
    if duplicate_combinations:
        rules.append({
            'type': 'same_amount_account_date',
            'description': 'Transactions with same amount, account, and date',
            'threshold': 1,
            'count': len(duplicate_combinations)
        })
    
    return rules


def _train_backdated_rules(transactions):
    """Train backdated detection rules"""
    rules = []
    
    # Rule 1: Document date after posting date
    backdated_count = 0
    for t in transactions:
        if t.document_date and t.posting_date and t.document_date > t.posting_date:
            backdated_count += 1
    
    if backdated_count > 0:
        rules.append({
            'type': 'document_after_posting',
            'description': 'Transactions with document date after posting date',
            'threshold': 1,
            'count': backdated_count
        })
    
    # Rule 2: Large time differences
    large_differences = 0
    for t in transactions:
        if t.document_date and t.posting_date:
            diff = (t.posting_date - t.document_date).days
            if diff > 30:  # More than 30 days
                large_differences += 1
    
    if large_differences > 0:
        rules.append({
            'type': 'large_time_difference',
            'description': 'Transactions with large time differences (>30 days)',
            'threshold': 30,
            'count': large_differences
        })
    
    return rules


def _train_user_rules(transactions):
    """Train user analysis rules"""
    rules = []
    
    # Rule 1: Users with high transaction volumes
    user_counts = {}
    for t in transactions:
        user = t.user_name
        user_counts[user] = user_counts.get(user, 0) + 1
    
    high_volume_users = {user: count for user, count in user_counts.items() if count > 100}
    
    if high_volume_users:
        rules.append({
            'type': 'high_volume_users',
            'description': 'Users with high transaction volumes',
            'threshold': 100,
            'users': high_volume_users
        })
    
    # Rule 2: Users with high-value transactions
    user_amounts = {}
    for t in transactions:
        user = t.user_name
        amount = abs(t.amount_local_currency or 0)
        if user not in user_amounts:
            user_amounts[user] = []
        user_amounts[user].append(amount)
    
    high_value_users = {}
    for user, amounts in user_amounts.items():
        max_amount = max(amounts)
        if max_amount > 100000:  # More than 100,000
            high_value_users[user] = max_amount
    
    if high_value_users:
        rules.append({
            'type': 'high_value_users',
            'description': 'Users with high-value transactions',
            'threshold': 100000,
            'users': high_value_users
        })
    
    return rules


def _train_unusual_days_rules(transactions):
    """Train unusual days detection rules"""
    rules = []
    
    # Rule 1: Weekend transactions
    weekend_count = 0
    for t in transactions:
        if t.posting_date and t.posting_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
            weekend_count += 1
    
    if weekend_count > 0:
        rules.append({
            'type': 'weekend_transactions',
            'description': 'Transactions posted on weekends',
            'threshold': 0,
            'count': weekend_count
        })
    
    # Rule 2: Holiday transactions (simplified)
    holiday_count = 0
    for t in transactions:
        if t.posting_date:
            # Simple holiday detection (New Year, Christmas, etc.)
            if (t.posting_date.month == 1 and t.posting_date.day == 1) or \
               (t.posting_date.month == 12 and t.posting_date.day == 25):
                holiday_count += 1
    
    if holiday_count > 0:
        rules.append({
            'type': 'holiday_transactions',
            'description': 'Transactions posted on holidays',
            'threshold': 0,
            'count': holiday_count
        })
    
    return rules


def _train_closing_entries_rules(transactions):
    """Train closing entries detection rules"""
    rules = []
    
    # Rule 1: Month-end transactions
    month_end_count = 0
    for t in transactions:
        if t.posting_date:
            # Check if it's the last day of the month
            next_day = t.posting_date + timedelta(days=1)
            if next_day.month != t.posting_date.month:
                month_end_count += 1
    
    if month_end_count > 0:
        rules.append({
            'type': 'month_end_transactions',
            'description': 'Transactions posted on month-end',
            'threshold': 0,
            'count': month_end_count
        })
    
    # Rule 2: Year-end transactions
    year_end_count = 0
    for t in transactions:
        if t.posting_date and t.posting_date.month == 12 and t.posting_date.day == 31:
            year_end_count += 1
    
    if year_end_count > 0:
        rules.append({
            'type': 'year_end_transactions',
            'description': 'Transactions posted on year-end',
            'threshold': 0,
            'count': year_end_count
        })
    
    return rules


def _train_holiday_rules(transactions):
    """Train holiday detection rules"""
    rules = []
    
    # Rule 1: Known holidays
    holiday_count = 0
    for t in transactions:
        if t.posting_date:
            # Check for common holidays
            if (t.posting_date.month == 1 and t.posting_date.day == 1) or \
               (t.posting_date.month == 12 and t.posting_date.day == 25) or \
               (t.posting_date.month == 7 and t.posting_date.day == 4):
                holiday_count += 1
    
    if holiday_count > 0:
        rules.append({
            'type': 'holiday_transactions',
            'description': 'Transactions posted on known holidays',
            'threshold': 0,
            'count': holiday_count
        })
    
    return rules


def _train_risk_scoring_rules(transactions):
    """Train risk scoring rules"""
    rules = []
    
    # Rule 1: High-value transactions
    high_value_count = 0
    for t in transactions:
        if abs(t.amount_local_currency or 0) > 100000:
            high_value_count += 1
    
    if high_value_count > 0:
        rules.append({
            'type': 'high_value_transactions',
            'description': 'High-value transactions (>100,000)',
            'threshold': 100000,
            'count': high_value_count
        })
    
    # Rule 2: Round number transactions
    round_number_count = 0
    for t in transactions:
        amount = abs(t.amount_local_currency or 0)
        if amount > 0 and amount % 1000 == 0:  # Round thousands
            round_number_count += 1
    
    if round_number_count > 0:
        rules.append({
            'type': 'round_number_transactions',
            'description': 'Round number transactions',
            'threshold': 0,
            'count': round_number_count
        })
    
    return rules


def _calculate_retraining_improvement(training_results):
    """Calculate improvement from retraining"""
    old_rules_count = training_results.get('old_rules_count', 0)
    new_rules_count = training_results.get('new_rules_count', 0)
    old_data_size = training_results.get('old_data_size', 0)
    new_data_size = training_results.get('new_data_size', 0)
    
    rules_improvement = ((new_rules_count - old_rules_count) / old_rules_count * 100) if old_rules_count > 0 else 0
    data_improvement = ((new_data_size - old_data_size) / old_data_size * 100) if old_data_size > 0 else 0
    
    return {
        'rules_improvement_percent': rules_improvement,
        'data_improvement_percent': data_improvement,
        'old_rules_count': old_rules_count,
        'new_rules_count': new_rules_count,
        'old_data_size': old_data_size,
        'new_data_size': new_data_size
    }
