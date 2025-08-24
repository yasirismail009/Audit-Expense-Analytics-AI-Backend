"""
Specialized Analysis Models Manager

Provides model management and prediction capabilities for the analytics system.
Handles loading trained models and applying them to new data.
"""

import logging
from typing import Dict, List, Any, Optional
from django.core.cache import cache
from django.utils import timezone
from .models import (
    DuplicateAnalysisModelTraining, BackdatedAnalysisModelTraining,
    UserAnalysisModelTraining, UnusualDaysAnalysisModelTraining,
    ClosingEntriesAnalysisModelTraining, HolidayAnalysisModelTraining,
    OverallRiskAnalysisModelTraining, RuleBasedModelTraining
)

logger = logging.getLogger(__name__)

class AnalysisModelManager:
    """
    Manages specialized analysis models and provides prediction capabilities.
    """
    
    def __init__(self):
        self.cache_timeout = 3600  # 1 hour
        self._models = {}
    
    def get_model(self, model_type: str) -> 'BaseAnalysisModel':
        """
        Get a trained model for the specified analysis type.
        
        Args:
            model_type: Type of analysis model ('duplicate', 'backdated', 'user', etc.)
            
        Returns:
            Trained model instance
        """
        if model_type not in self._models:
            self._models[model_type] = self._load_model(model_type)
        
        return self._models[model_type]
    
    def _load_model(self, model_type: str) -> 'BaseAnalysisModel':
        """Load the latest trained model for the specified type"""
        try:
            if model_type == 'duplicate':
                return DuplicateAnalysisModel()
            elif model_type == 'backdated':
                return BackdatedAnalysisModel()
            elif model_type == 'user':
                return UserAnalysisModel()
            elif model_type == 'unusual_days':
                return UnusualDaysAnalysisModel()
            elif model_type == 'closing_entries':
                return ClosingEntriesAnalysisModel()
            elif model_type == 'holiday':
                return HolidayAnalysisModel()
            elif model_type == 'risk':
                return RiskAnalysisModel()
            else:
                logger.warning(f"Unknown model type: {model_type}")
                return FallbackAnalysisModel()
        except Exception as e:
            logger.error(f"Error loading model {model_type}: {e}")
            return FallbackAnalysisModel()
    
    def refresh_models(self) -> None:
        """Clear model cache to force reload"""
        self._models.clear()
        logger.info("Model cache cleared")

class BaseAnalysisModel:
    """Base class for all analysis models"""
    
    def __init__(self):
        self.model_type = "base"
        self.is_trained = False
        self.training_session = None
        self.thresholds = {}
    
    def predict(self, transactions: List[Any], context: Dict = None) -> List[Dict]:
        """
        Predict anomalies for transactions.
        
        Args:
            transactions: List of transactions to analyze
            context: Additional context data
            
        Returns:
            List of prediction results
        """
        raise NotImplementedError
    
    def _load_latest_training(self) -> Optional[Any]:
        """Load the latest training session for this model type"""
        raise NotImplementedError
    
    def _apply_thresholds(self, scores: List[float]) -> List[Dict]:
        """Apply trained thresholds to scores"""
        if not self.thresholds:
            return [{'risk_score': score, 'risk_level': 'LOW'} for score in scores]
        
        results = []
        for score in scores:
            if score >= self.thresholds.get('high_threshold', 80):
                risk_level = 'HIGH'
            elif score >= self.thresholds.get('medium_threshold', 50):
                risk_level = 'MEDIUM'
            else:
                risk_level = 'LOW'
            
            results.append({
                'risk_score': score,
                'risk_level': risk_level
            })
        
        return results

class DuplicateAnalysisModel(BaseAnalysisModel):
    """Duplicate detection model"""
    
    def __init__(self):
        super().__init__()
        self.model_type = "duplicate"
        self._load_latest_training()
    
    def _load_latest_training(self):
        """Load latest duplicate analysis training session"""
        try:
            self.training_session = DuplicateAnalysisModelTraining.objects.filter(
                status='COMPLETED'
            ).order_by('-completed_at').first()
            
            if self.training_session:
                self.is_trained = True
                training_results = self.training_session.training_results
                self.thresholds = training_results.get('optimal_thresholds', {})
                logger.info(f"Loaded duplicate model with thresholds: {self.thresholds}")
            else:
                logger.warning("No trained duplicate model found")
        except Exception as e:
            logger.error(f"Error loading duplicate model: {e}")
    
    def predict(self, transactions: List[Any], context: Dict = None) -> List[Dict]:
        """Predict duplicate transactions"""
        if not self.is_trained or not transactions:
            return []
        
        try:
            predictions = []
            
            # Apply trained thresholds for duplicate detection
            similarity_threshold = self.thresholds.get('similarity_threshold', 0.95)
            amount_tolerance = self.thresholds.get('amount_tolerance', 0.01)
            date_tolerance_days = self.thresholds.get('date_tolerance_days', 1)
            
            # Group transactions by potential duplicates
            for i, transaction in enumerate(transactions):
                duplicate_score = 0
                duplicate_factors = []
                
                # Check against other transactions
                for j, other_transaction in enumerate(transactions):
                    if i == j:
                        continue
                    
                    # Calculate similarity based on trained thresholds
                    if self._is_similar_transaction(transaction, other_transaction, 
                                                   similarity_threshold, amount_tolerance, 
                                                   date_tolerance_days):
                        duplicate_score += 25
                        duplicate_factors.append(f'similar_to_{j}')
                
                if duplicate_score > 0:
                    predictions.append({
                        'transaction_id': str(transaction.id),
                        'duplicate_score': duplicate_score,
                        'duplicate_factors': duplicate_factors,
                        'risk_score': min(duplicate_score, 100),
                        'risk_level': 'HIGH' if duplicate_score > 50 else 'MEDIUM'
                    })
            
            return predictions
            
        except Exception as e:
            logger.error(f"Error predicting duplicates: {e}")
            return []
    
    def _is_similar_transaction(self, t1, t2, similarity_threshold, amount_tolerance, date_tolerance_days):
        """Check if two transactions are similar based on trained thresholds"""
        # Amount similarity
        amount_diff = abs(float(t1.amount_local_currency) - float(t2.amount_local_currency))
        amount_similar = amount_diff <= (float(t1.amount_local_currency) * amount_tolerance)
        
        # Account similarity
        account_similar = t1.gl_account == t2.gl_account
        
        # Date similarity
        if t1.posting_date and t2.posting_date:
            date_diff = abs((t1.posting_date - t2.posting_date).days)
            date_similar = date_diff <= date_tolerance_days
        else:
            date_similar = False
        
        # User similarity
        user_similar = t1.user_name == t2.user_name
        
        # Calculate overall similarity
        similarity_score = 0
        if amount_similar: similarity_score += 0.4
        if account_similar: similarity_score += 0.3
        if date_similar: similarity_score += 0.2
        if user_similar: similarity_score += 0.1
        
        return similarity_score >= similarity_threshold

class BackdatedAnalysisModel(BaseAnalysisModel):
    """Backdated detection model"""
    
    def __init__(self):
        super().__init__()
        self.model_type = "backdated"
        self._load_latest_training()
    
    def _load_latest_training(self):
        """Load latest backdated analysis training session"""
        try:
            self.training_session = BackdatedAnalysisModelTraining.objects.filter(
                status='COMPLETED'
            ).order_by('-completed_at').first()
            
            if self.training_session:
                self.is_trained = True
                training_results = self.training_session.training_results
                self.thresholds = training_results.get('optimal_thresholds', {})
                logger.info(f"Loaded backdated model with thresholds: {self.thresholds}")
            else:
                logger.warning("No trained backdated model found")
        except Exception as e:
            logger.error(f"Error loading backdated model: {e}")
    
    def predict(self, transactions: List[Any], context: Dict = None) -> List[Dict]:
        """Predict backdated transactions"""
        if not self.is_trained or not transactions:
            return []
        
        try:
            predictions = []
            
            # Apply trained thresholds
            delay_threshold_days = self.thresholds.get('delay_threshold_days', 7)
            high_risk_delay_days = self.thresholds.get('high_risk_delay_days', 30)
            
            for transaction in transactions:
                if not transaction.document_date or not transaction.posting_date:
                    continue
                
                delay_days = (transaction.posting_date - transaction.document_date).days
                
                if delay_days > delay_threshold_days:
                    # Calculate risk score based on delay
                    if delay_days > high_risk_delay_days:
                        risk_score = 100
                        risk_level = 'HIGH'
                    else:
                        risk_score = min(50 + (delay_days - delay_threshold_days) * 2, 100)
                        risk_level = 'MEDIUM'
                    
                    predictions.append({
                        'transaction_id': str(transaction.id),
                        'delay_days': delay_days,
                        'risk_score': risk_score,
                        'risk_level': risk_level,
                        'backdated_factor': 'document_posting_delay'
                    })
            
            return predictions
            
        except Exception as e:
            logger.error(f"Error predicting backdated transactions: {e}")
            return []

class UserAnalysisModel(BaseAnalysisModel):
    """User anomaly detection model"""
    
    def __init__(self):
        super().__init__()
        self.model_type = "user"
        self._load_latest_training()
    
    def _load_latest_training(self):
        """Load latest user analysis training session"""
        try:
            self.training_session = UserAnalysisModelTraining.objects.filter(
                status='COMPLETED'
            ).order_by('-completed_at').first()
            
            if self.training_session:
                self.is_trained = True
                training_results = self.training_session.training_results
                self.thresholds = training_results.get('optimal_thresholds', {})
                logger.info(f"Loaded user model with thresholds: {self.thresholds}")
            else:
                logger.warning("No trained user model found")
        except Exception as e:
            logger.error(f"Error loading user model: {e}")
    
    def predict(self, transactions: List[Any], context: Dict = None) -> List[Dict]:
        """Predict user anomalies"""
        if not self.is_trained or not transactions:
            return []
        
        try:
            predictions = []
            
            # Apply trained thresholds
            volume_threshold = self.thresholds.get('volume_threshold', 100)
            amount_threshold = self.thresholds.get('amount_threshold', 1000000)
            weekend_threshold = self.thresholds.get('weekend_threshold', 5)
            account_threshold = self.thresholds.get('account_threshold', 20)
            
            # Group by user
            user_transactions = {}
            for transaction in transactions:
                user = transaction.user_name
                if user not in user_transactions:
                    user_transactions[user] = []
                user_transactions[user].append(transaction)
            
            # Analyze each user
            for user, user_txns in user_transactions.items():
                anomaly_score = 0
                anomaly_factors = []
                
                # Volume anomaly
                if len(user_txns) > volume_threshold:
                    anomaly_score += 25
                    anomaly_factors.append('high_volume')
                
                # Amount anomaly
                total_amount = sum(abs(float(txn.amount_local_currency)) for txn in user_txns)
                if total_amount > amount_threshold:
                    anomaly_score += 20
                    anomaly_factors.append('high_amount')
                
                # Weekend posting anomaly
                weekend_count = len([txn for txn in user_txns 
                                   if txn.posting_date and txn.posting_date.weekday() in [4, 5]])
                if weekend_count > weekend_threshold:
                    anomaly_score += 15
                    anomaly_factors.append('weekend_posting')
                
                # Account diversity anomaly
                unique_accounts = len(set(txn.gl_account for txn in user_txns))
                if unique_accounts > account_threshold:
                    anomaly_score += 15
                    anomaly_factors.append('unusual_accounts')
                
                if anomaly_score > 0:
                    predictions.append({
                        'user_name': user,
                        'anomaly_score': anomaly_score,
                        'anomaly_factors': anomaly_factors,
                        'risk_score': min(anomaly_score, 100),
                        'risk_level': 'HIGH' if anomaly_score > 50 else 'MEDIUM'
                    })
            
            return predictions
            
        except Exception as e:
            logger.error(f"Error predicting user anomalies: {e}")
            return []

class UnusualDaysAnalysisModel(BaseAnalysisModel):
    """Unusual days detection model"""
    
    def __init__(self):
        super().__init__()
        self.model_type = "unusual_days"
        self._load_latest_training()
    
    def _load_latest_training(self):
        """Load latest unusual days analysis training session"""
        try:
            self.training_session = UnusualDaysAnalysisModelTraining.objects.filter(
                status='COMPLETED'
            ).order_by('-completed_at').first()
            
            if self.training_session:
                self.is_trained = True
                training_results = self.training_session.training_results
                self.thresholds = training_results.get('optimal_thresholds', {})
                logger.info(f"Loaded unusual days model with thresholds: {self.thresholds}")
            else:
                logger.warning("No trained unusual days model found")
        except Exception as e:
            logger.error(f"Error loading unusual days model: {e}")
    
    def predict(self, transactions: List[Any], context: Dict = None) -> List[Dict]:
        """Predict unusual days transactions"""
        if not self.is_trained or not transactions:
            return []
        
        try:
            predictions = []
            
            # Apply trained thresholds
            weekend_activity_threshold = self.thresholds.get('weekend_activity_threshold', 0.05)
            
            for transaction in transactions:
                if not transaction.posting_date:
                    continue
                
                # Check if weekend posting
                if transaction.posting_date.weekday() in [4, 5]:  # Friday/Saturday
                    predictions.append({
                        'transaction_id': str(transaction.id),
                        'unusual_day_type': 'weekend',
                        'risk_score': 40,
                        'risk_level': 'MEDIUM',
                        'unusual_factor': 'weekend_posting'
                    })
            
            return predictions
            
        except Exception as e:
            logger.error(f"Error predicting unusual days: {e}")
            return []

class ClosingEntriesAnalysisModel(BaseAnalysisModel):
    """Closing entries detection model"""
    
    def __init__(self):
        super().__init__()
        self.model_type = "closing_entries"
        self._load_latest_training()
    
    def _load_latest_training(self):
        """Load latest closing entries analysis training session"""
        try:
            self.training_session = ClosingEntriesAnalysisModelTraining.objects.filter(
                status='COMPLETED'
            ).order_by('-completed_at').first()
            
            if self.training_session:
                self.is_trained = True
                training_results = self.training_session.training_results
                self.thresholds = training_results.get('optimal_thresholds', {})
                logger.info(f"Loaded closing entries model with thresholds: {self.thresholds}")
            else:
                logger.warning("No trained closing entries model found")
        except Exception as e:
            logger.error(f"Error loading closing entries model: {e}")
    
    def predict(self, transactions: List[Any], context: Dict = None) -> List[Dict]:
        """Predict closing entries"""
        if not self.is_trained or not transactions:
            return []
        
        try:
            predictions = []
            
            # Apply trained thresholds
            closing_days_before = self.thresholds.get('closing_days_before', 3)
            closing_days_after = self.thresholds.get('closing_days_after', 2)
            
            for transaction in transactions:
                if not transaction.posting_date:
                    continue
                
                # Check if near month end
                day_of_month = transaction.posting_date.day
                days_in_month = (transaction.posting_date.replace(day=28) + timezone.timedelta(days=4)).replace(day=1) - timezone.timedelta(days=1)
                days_in_month = days_in_month.day
                
                # Check if closing entry
                if day_of_month >= (days_in_month - closing_days_before) or day_of_month <= closing_days_after:
                    predictions.append({
                        'transaction_id': str(transaction.id),
                        'closing_entry_type': 'month_end',
                        'risk_score': 30,
                        'risk_level': 'MEDIUM',
                        'closing_factor': 'month_end_proximity'
                    })
            
            return predictions
            
        except Exception as e:
            logger.error(f"Error predicting closing entries: {e}")
            return []

class HolidayAnalysisModel(BaseAnalysisModel):
    """Holiday detection model"""
    
    def __init__(self):
        super().__init__()
        self.model_type = "holiday"
        self._load_latest_training()
    
    def _load_latest_training(self):
        """Load latest holiday analysis training session"""
        try:
            self.training_session = HolidayAnalysisModelTraining.objects.filter(
                status='COMPLETED'
            ).order_by('-completed_at').first()
            
            if self.training_session:
                self.is_trained = True
                training_results = self.training_session.training_results
                self.thresholds = training_results.get('optimal_thresholds', {})
                logger.info(f"Loaded holiday model with thresholds: {self.thresholds}")
            else:
                logger.warning("No trained holiday model found")
        except Exception as e:
            logger.error(f"Error loading holiday model: {e}")
    
    def predict(self, transactions: List[Any], context: Dict = None) -> List[Dict]:
        """Predict holiday transactions"""
        if not self.is_trained or not transactions:
            return []
        
        try:
            predictions = []
            
            # Import holiday utilities
            from .holiday_utils import is_holiday
            
            for transaction in transactions:
                if not transaction.posting_date:
                    continue
                
                # Check if holiday posting
                if is_holiday(transaction.posting_date):
                    predictions.append({
                        'transaction_id': str(transaction.id),
                        'holiday_type': 'public_holiday',
                        'risk_score': 60,
                        'risk_level': 'HIGH',
                        'holiday_factor': 'holiday_posting'
                    })
            
            return predictions
            
        except Exception as e:
            logger.error(f"Error predicting holiday transactions: {e}")
            return []

class RiskAnalysisModel(BaseAnalysisModel):
    """Overall risk analysis model"""
    
    def __init__(self):
        super().__init__()
        self.model_type = "risk"
        self._load_latest_training()
    
    def _load_latest_training(self):
        """Load latest risk analysis training session"""
        try:
            self.training_session = OverallRiskAnalysisModelTraining.objects.filter(
                status='COMPLETED'
            ).order_by('-completed_at').first()
            
            if self.training_session:
                self.is_trained = True
                training_results = self.training_session.training_results
                self.thresholds = training_results.get('optimal_thresholds', {})
                logger.info(f"Loaded risk model with thresholds: {self.thresholds}")
            else:
                logger.warning("No trained risk model found")
        except Exception as e:
            logger.error(f"Error loading risk model: {e}")
    
    def predict(self, transactions: List[Any], context: Dict = None) -> List[Dict]:
        """Predict overall risk for transactions"""
        if not transactions:
            return []
        
        try:
            predictions = []
            
            # Apply trained risk weights
            risk_weights = self.thresholds.get('risk_weights', {
                'duplicate': 0.3,
                'backdated': 0.25,
                'user': 0.2,
                'holiday': 0.15,
                'unusual_days': 0.1
            })
            
            for transaction in transactions:
                risk_score = 0
                risk_factors = []
                
                # Calculate risk based on transaction characteristics
                # This is a simplified version - in practice, you'd use the results
                # from other analysis models
                
                # High value transaction
                if abs(float(transaction.amount_local_currency)) > 1000000:
                    risk_score += 20
                    risk_factors.append('high_value')
                
                # Manual entry
                if transaction.document_type and 'MANUAL' in transaction.document_type.upper():
                    risk_score += 15
                    risk_factors.append('manual_entry')
                
                # Weekend posting
                if transaction.posting_date and transaction.posting_date.weekday() in [4, 5]:
                    risk_score += 10
                    risk_factors.append('weekend_posting')
                
                # Large amount
                if abs(float(transaction.amount_local_currency)) > 100000:
                    risk_score += 10
                    risk_factors.append('large_amount')
                
                if risk_score > 0:
                    predictions.append({
                        'transaction_id': str(transaction.id),
                        'risk_score': min(risk_score, 100),
                        'risk_level': 'HIGH' if risk_score > 50 else 'MEDIUM' if risk_score > 25 else 'LOW',
                        'risk_factors': risk_factors
                    })
            
            return predictions
            
        except Exception as e:
            logger.error(f"Error predicting risk: {e}")
            return []

class FallbackAnalysisModel(BaseAnalysisModel):
    """Fallback model when trained models are not available"""
    
    def __init__(self):
        super().__init__()
        self.model_type = "fallback"
        self.is_trained = False
    
    def predict(self, transactions: List[Any], context: Dict = None) -> List[Dict]:
        """Fallback prediction using basic rules"""
        logger.warning("Using fallback model - no trained model available")
        return []
