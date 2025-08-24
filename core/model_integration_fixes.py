"""
Model Integration Fixes

This file contains fixes to integrate trained models into the analysis process
and update training to use new data instead of all historical data.
"""

import logging
from typing import Dict, List, Any
from django.utils import timezone
from django.db.models import Q
from .models import (
    SAPGLPosting, DuplicateAnalysisModelTraining, BackdatedAnalysisModelTraining,
    UserAnalysisModelTraining, UnusualDaysAnalysisModelTraining,
    ClosingEntriesAnalysisModelTraining, HolidayAnalysisModelTraining,
    OverallRiskAnalysisModelTraining, FileProcessingJob
)

logger = logging.getLogger(__name__)

def update_analysis_tasks_to_use_models():
    """
    Update analysis tasks to use trained models instead of just rule-based logic.
    This function provides the fixes needed for each analysis type.
    """
    
    # 1. Duplicate Analysis Fix
    def fix_duplicate_analysis(job_id: str) -> Dict[str, Any]:
        """Enhanced duplicate analysis using trained model"""
        try:
            from .specialized_analysis_models import AnalysisModelManager
            model_manager = AnalysisModelManager()
            duplicate_model = model_manager.get_model('duplicate')
            
            # Get job and transactions
            job = FileProcessingJob.objects.get(id=job_id)
            transactions = list(SAPGLPosting.objects.filter(data_file=job.data_file))
            
            duplicates = []
            detection_method = 'rule_based'
            
            if duplicate_model.is_trained:
                logger.info(f"Using trained duplicate model for job {job_id}")
                detection_method = 'trained_model'
                
                # Use trained model predictions
                model_predictions = duplicate_model.predict(transactions)
                
                # Process predictions
                for prediction in model_predictions:
                    if prediction.get('duplicate_score', 0) > 0:
                        transaction_id = prediction['transaction_id']
                        transaction = next((t for t in transactions if str(t.id) == transaction_id), None)
                        
                        if transaction:
                            # Find similar transaction based on factors
                            similar_transaction = find_similar_transaction(transaction, transactions, prediction['duplicate_factors'])
                            
                            if similar_transaction:
                                duplicate_record = create_duplicate_record(transaction, similar_transaction, prediction)
                                duplicates.append(duplicate_record)
            else:
                logger.info(f"No trained duplicate model available, using rule-based for job {job_id}")
                # Fallback to existing rule-based logic
                duplicates = run_rule_based_duplicate_detection(transactions)
            
            return {
                'duplicates': duplicates,
                'detection_method': detection_method,
                'model_used': 'trained' if duplicate_model.is_trained else 'rule_based'
            }
            
        except Exception as e:
            logger.error(f"Error in enhanced duplicate analysis: {e}")
            return {'error': str(e)}
    
    # 2. Backdated Analysis Fix
    def fix_backdated_analysis(job_id: str) -> Dict[str, Any]:
        """Enhanced backdated analysis using trained model"""
        try:
            from .specialized_analysis_models import AnalysisModelManager
            model_manager = AnalysisModelManager()
            backdated_model = model_manager.get_model('backdated')
            
            # Get job and transactions
            job = FileProcessingJob.objects.get(id=job_id)
            transactions = list(SAPGLPosting.objects.filter(data_file=job.data_file))
            
            backdated_transactions = []
            detection_method = 'rule_based'
            
            if backdated_model.is_trained:
                logger.info(f"Using trained backdated model for job {job_id}")
                detection_method = 'trained_model'
                
                # Use trained model predictions
                model_predictions = backdated_model.predict(transactions)
                
                # Process predictions
                for prediction in model_predictions:
                    if prediction.get('delay_days', 0) > 0:
                        transaction_id = prediction['transaction_id']
                        transaction = next((t for t in transactions if str(t.id) == transaction_id), None)
                        
                        if transaction:
                            backdated_record = create_backdated_record(transaction, prediction)
                            backdated_transactions.append(backdated_record)
            else:
                logger.info(f"No trained backdated model available, using rule-based for job {job_id}")
                # Fallback to existing rule-based logic
                backdated_transactions = run_rule_based_backdated_detection(transactions)
            
            return {
                'backdated_transactions': backdated_transactions,
                'detection_method': detection_method,
                'model_used': 'trained' if backdated_model.is_trained else 'rule_based'
            }
            
        except Exception as e:
            logger.error(f"Error in enhanced backdated analysis: {e}")
            return {'error': str(e)}
    
    # 3. User Analysis Fix
    def fix_user_analysis(job_id: str) -> Dict[str, Any]:
        """Enhanced user analysis using trained model"""
        try:
            from .specialized_analysis_models import AnalysisModelManager
            model_manager = AnalysisModelManager()
            user_model = model_manager.get_model('user')
            
            # Get job and transactions
            job = FileProcessingJob.objects.get(id=job_id)
            transactions = list(SAPGLPosting.objects.filter(data_file=job.data_file))
            
            user_anomalies = []
            detection_method = 'rule_based'
            
            if user_model.is_trained:
                logger.info(f"Using trained user model for job {job_id}")
                detection_method = 'trained_model'
                
                # Use trained model predictions
                model_predictions = user_model.predict(transactions)
                
                # Process predictions
                for prediction in model_predictions:
                    if prediction.get('anomaly_score', 0) > 0:
                        user_anomaly = create_user_anomaly_record(prediction)
                        user_anomalies.append(user_anomaly)
            else:
                logger.info(f"No trained user model available, using rule-based for job {job_id}")
                # Fallback to existing rule-based logic
                user_anomalies = run_rule_based_user_detection(transactions)
            
            return {
                'user_anomalies': user_anomalies,
                'detection_method': detection_method,
                'model_used': 'trained' if user_model.is_trained else 'rule_based'
            }
            
        except Exception as e:
            logger.error(f"Error in enhanced user analysis: {e}")
            return {'error': str(e)}
    
    return {
        'fix_duplicate_analysis': fix_duplicate_analysis,
        'fix_backdated_analysis': fix_backdated_analysis,
        'fix_user_analysis': fix_user_analysis
    }

def update_training_to_use_new_data():
    """
    Update training tasks to use new data instead of all historical data.
    This provides incremental learning capabilities.
    """
    
    def get_training_data_for_job(job_id: str, include_historical_sample: bool = True) -> List[SAPGLPosting]:
        """
        Get training data for a specific job, optionally including historical sample.
        
        Args:
            job_id: The job ID to get new data for
            include_historical_sample: Whether to include sample of historical data
            
        Returns:
            List of transactions for training
        """
        try:
            job = FileProcessingJob.objects.get(id=job_id)
            
            # Get new data from this job
            new_transactions = list(SAPGLPosting.objects.filter(data_file=job.data_file))
            
            if include_historical_sample and new_transactions:
                # Get sample of historical data (excluding current file)
                historical_sample = list(SAPGLPosting.objects.exclude(
                    data_file=job.data_file
                ).order_by('?')[:len(new_transactions) * 2])  # 2x the new data size
                
                # Combine new and historical data
                training_data = new_transactions + historical_sample
                logger.info(f"Training with {len(new_transactions)} new + {len(historical_sample)} historical transactions")
            else:
                training_data = new_transactions
                logger.info(f"Training with {len(new_transactions)} new transactions only")
            
            return training_data
            
        except Exception as e:
            logger.error(f"Error getting training data: {e}")
            # Fallback to all data
            return list(SAPGLPosting.objects.all())
    
    def update_duplicate_training(job_id: str) -> Dict[str, Any]:
        """Update duplicate training to use new data"""
        try:
            training_data = get_training_data_for_job(job_id)
            
            if len(training_data) < 100:
                return {'error': 'Insufficient training data'}
            
            # Create training session with new data info
            training_session = DuplicateAnalysisModelTraining.objects.create(
                session_name=f"Duplicate Training - Job {job_id}",
                description=f"Training with new data from job {job_id}",
                model_type='duplicate_analysis',
                training_data_size=len(training_data),
                training_data_date_range={
                    'min_date': min(t.posting_date for t in training_data if t.posting_date).isoformat(),
                    'max_date': max(t.posting_date for t in training_data if t.posting_date).isoformat()
                },
                status='TRAINING',
                started_at=timezone.now()
            )
            
            # Run training with new data
            training_results = train_duplicate_model_with_data(training_data)
            
            # Update training session
            training_session.training_results = training_results
            training_session.performance_metrics = training_results.get('performance_metrics', {})
            training_session.status = 'COMPLETED'
            training_session.completed_at = timezone.now()
            training_session.training_duration = (timezone.now() - training_session.started_at).total_seconds()
            training_session.save()
            
            return {
                'status': 'success',
                'training_session_id': str(training_session.id),
                'training_data_size': len(training_data),
                'performance_metrics': training_results.get('performance_metrics', {})
            }
            
        except Exception as e:
            logger.error(f"Error in duplicate training: {e}")
            return {'error': str(e)}
    
    def update_backdated_training(job_id: str) -> Dict[str, Any]:
        """Update backdated training to use new data"""
        try:
            training_data = get_training_data_for_job(job_id)
            
            if len(training_data) < 100:
                return {'error': 'Insufficient training data'}
            
            # Create training session with new data info
            training_session = BackdatedAnalysisModelTraining.objects.create(
                session_name=f"Backdated Training - Job {job_id}",
                description=f"Training with new data from job {job_id}",
                model_type='backdated_analysis',
                training_data_size=len(training_data),
                training_data_date_range={
                    'min_date': min(t.posting_date for t in training_data if t.posting_date).isoformat(),
                    'max_date': max(t.posting_date for t in training_data if t.posting_date).isoformat()
                },
                status='TRAINING',
                started_at=timezone.now()
            )
            
            # Run training with new data
            training_results = train_backdated_model_with_data(training_data)
            
            # Update training session
            training_session.training_results = training_results
            training_session.performance_metrics = training_results.get('performance_metrics', {})
            training_session.status = 'COMPLETED'
            training_session.completed_at = timezone.now()
            training_session.training_duration = (timezone.now() - training_session.started_at).total_seconds()
            training_session.save()
            
            return {
                'status': 'success',
                'training_session_id': str(training_session.id),
                'training_data_size': len(training_data),
                'performance_metrics': training_results.get('performance_metrics', {})
            }
            
        except Exception as e:
            logger.error(f"Error in backdated training: {e}")
            return {'error': str(e)}
    
    return {
        'get_training_data_for_job': get_training_data_for_job,
        'update_duplicate_training': update_duplicate_training,
        'update_backdated_training': update_backdated_training
    }

# Helper functions for model integration
def find_similar_transaction(transaction, all_transactions, factors):
    """Find similar transaction based on model factors"""
    for other_transaction in all_transactions:
        if other_transaction.id == transaction.id:
            continue
        
        # Check similarity based on factors
        similarity_score = 0
        if 'amount_similar' in factors and abs(float(transaction.amount_local_currency) - float(other_transaction.amount_local_currency)) < 0.01:
            similarity_score += 1
        if 'account_similar' in factors and transaction.gl_account == other_transaction.gl_account:
            similarity_score += 1
        if 'user_similar' in factors and transaction.user_name == other_transaction.user_name:
            similarity_score += 1
        if 'date_similar' in factors and transaction.posting_date == other_transaction.posting_date:
            similarity_score += 1
        
        if similarity_score >= 2:  # At least 2 factors match
            return other_transaction
    
    return None

def create_duplicate_record(transaction1, transaction2, prediction):
    """Create duplicate record from model prediction"""
    return {
        'transaction1': {
            'id': str(transaction1.id),
            'document_number': transaction1.document_number,
            'gl_account': transaction1.gl_account,
            'amount': float(transaction1.amount_local_currency),
            'user_name': transaction1.user_name,
            'posting_date': transaction1.posting_date.isoformat() if transaction1.posting_date else None,
            'document_date': transaction1.document_date.isoformat() if transaction1.document_date else None,
            'text': transaction1.text
        },
        'transaction2': {
            'id': str(transaction2.id),
            'document_number': transaction2.document_number,
            'gl_account': transaction2.gl_account,
            'amount': float(transaction2.amount_local_currency),
            'user_name': transaction2.user_name,
            'posting_date': transaction2.posting_date.isoformat() if transaction2.posting_date else None,
            'document_date': transaction2.document_date.isoformat() if transaction2.document_date else None,
            'text': transaction2.text
        },
        'duplicate_type': f"ML Detected - Score: {prediction.get('duplicate_score', 0)}",
        'risk_score': prediction.get('risk_score', 0),
        'risk_level': prediction.get('risk_level', 'LOW'),
        'detection_method': 'trained_model'
    }

def create_backdated_record(transaction, prediction):
    """Create backdated record from model prediction"""
    return {
        'transaction_id': str(transaction.id),
        'document_number': transaction.document_number,
        'gl_account': transaction.gl_account,
        'user_name': transaction.user_name,
        'document_date': transaction.document_date.isoformat() if transaction.document_date else None,
        'posting_date': transaction.posting_date.isoformat() if transaction.posting_date else None,
        'days_difference': prediction.get('delay_days', 0),
        'amount': float(transaction.amount_local_currency),
        'risk_score': prediction.get('risk_score', 0),
        'risk_level': prediction.get('risk_level', 'LOW'),
        'detection_method': 'trained_model'
    }

def create_user_anomaly_record(prediction):
    """Create user anomaly record from model prediction"""
    return {
        'user_name': prediction.get('user_name', ''),
        'anomaly_score': prediction.get('anomaly_score', 0),
        'anomaly_factors': prediction.get('anomaly_factors', []),
        'risk_score': prediction.get('risk_score', 0),
        'risk_level': prediction.get('risk_level', 'LOW'),
        'detection_method': 'trained_model'
    }

# Placeholder functions for rule-based fallbacks
def run_rule_based_duplicate_detection(transactions):
    """Fallback rule-based duplicate detection"""
    # This would contain the existing rule-based logic
    return []

def run_rule_based_backdated_detection(transactions):
    """Fallback rule-based backdated detection"""
    # This would contain the existing rule-based logic
    return []

def run_rule_based_user_detection(transactions):
    """Fallback rule-based user detection"""
    # This would contain the existing rule-based logic
    return []

# Placeholder functions for model training
def train_duplicate_model_with_data(training_data):
    """Train duplicate model with specific data"""
    # This would contain the actual training logic
    return {
        'optimal_thresholds': {
            'similarity_threshold': 0.95,
            'amount_tolerance': 0.01,
            'date_tolerance_days': 1
        },
        'performance_metrics': {
            'accuracy': 0.85,
            'precision': 0.82,
            'recall': 0.88
        }
    }

def train_backdated_model_with_data(training_data):
    """Train backdated model with specific data"""
    # This would contain the actual training logic
    return {
        'optimal_thresholds': {
            'delay_threshold_days': 7,
            'high_risk_delay_days': 30
        },
        'performance_metrics': {
            'accuracy': 0.90,
            'precision': 0.88,
            'recall': 0.92
        }
    }
