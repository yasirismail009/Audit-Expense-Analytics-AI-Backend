"""
Specialized ML Models for Analysis Types

This module provides dedicated ML models for each analysis type:
1. General Analysis Model - Statistical and pattern analysis
2. Duplicate Analysis Model - Duplicate detection and classification
3. Backdated Analysis Model - Backdated entry detection
4. Overall Analysis Model - Combined risk assessment
5. Risk Analysis Model - Risk scoring and classification
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest, RandomForestClassifier, RandomForestRegressor
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
import joblib
import os
import logging
from typing import List, Dict, Any, Tuple
from django.conf import settings
from .models import SAPGLPosting

# Import the orchestrator for comprehensive analysis
try:
    from .ml_analysis_orchestrator import MLAnalysisOrchestrator
except ImportError:
    MLAnalysisOrchestrator = None

logger = logging.getLogger(__name__)

class BaseAnalysisModel:
    """Base class for all analysis models"""
    
    def __init__(self, model_name: str):
        self.model_name = model_name
        self.model = None
        self.scaler = StandardScaler()
        self.label_encoders = {}
        self.is_trained = False
        self.model_path = os.path.join(settings.BASE_DIR, 'trained_models', f'{model_name}.joblib')
        self.scaler_path = os.path.join(settings.BASE_DIR, 'trained_models', f'{model_name}_scaler.joblib')
        
    def save_model(self):
        """Save the trained model and scaler"""
        os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
        joblib.dump(self.model, self.model_path)
        joblib.dump(self.scaler, self.scaler_path)
        logger.info(f"Model {self.model_name} saved successfully")
    
    def load_model(self):
        """Load the trained model and scaler"""
        try:
            if os.path.exists(self.model_path) and os.path.exists(self.scaler_path):
                self.model = joblib.load(self.model_path)
                self.scaler = joblib.load(self.scaler_path)
                self.is_trained = True
                logger.info(f"Model {self.model_name} loaded successfully")
                return True
        except Exception as e:
            logger.error(f"Error loading model {self.model_name}: {e}")
        return False

class GeneralAnalysisModel(BaseAnalysisModel):
    """ML Model for General Analysis - Statistical patterns and accounting anomalies"""
    
    def __init__(self):
        super().__init__('general_analysis')
        self.model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')
    
    def extract_features(self, transactions: List[SAPGLPosting]) -> pd.DataFrame:
        """Extract features for general analysis"""
        if not transactions:
            return pd.DataFrame()
        
        features = []
        for t in transactions:
            feature_dict = {
                'is_debit': 1 if t.transaction_type == 'DEBIT' else 0,
                'is_credit': 1 if t.transaction_type == 'CREDIT' else 0,
                'day_of_week': t.posting_date.weekday() if t.posting_date else 0,
                'day_of_month': t.posting_date.day if t.posting_date else 0,
                'month': t.posting_date.month if t.posting_date else 0,
                'quarter': (t.posting_date.month - 1) // 3 + 1 if t.posting_date else 0,
                'is_month_end': 1 if t.posting_date and t.posting_date.day >= 25 else 0,
                'is_quarter_end': 1 if t.posting_date and t.posting_date.month in [3, 6, 9, 12] and t.posting_date.day >= 25 else 0,
                'is_year_end': 1 if t.posting_date and t.posting_date.month == 12 and t.posting_date.day >= 25 else 0,
                'is_high_value': 1 if t.is_high_value else 0,
                'is_cleared': 1 if t.is_cleared else 0,
                'account_type_code': self._get_account_type_code(t.gl_account),
                'fiscal_year': t.fiscal_year,
                'posting_period': t.posting_period
            }
            features.append(feature_dict)
        
        return pd.DataFrame(features)
    
    def _get_account_type_code(self, account_id: str) -> int:
        """Get account type code for ML features"""
        if not account_id:
            return 0
        try:
            account_num = int(account_id)
            if 1000 <= account_num <= 1999:
                return 1  # Asset
            elif 2000 <= account_num <= 2999:
                return 2  # Liability
            elif 3000 <= account_num <= 3999:
                return 3  # Equity
            elif 4000 <= account_num <= 4999:
                return 4  # Revenue
            elif 5000 <= account_num <= 5999:
                return 5  # Expense
            else:
                return 0  # Other
        except:
            return 0
    
    def train(self, transactions: List[SAPGLPosting], labels: List[int]):
        """Train the general analysis model"""
        try:
            X = self.extract_features(transactions)
            if X.empty:
                logger.error("No features extracted for training")
                return False
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(X, labels, test_size=0.2, random_state=42)
            
            # Scale features
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_test_scaled = self.scaler.transform(X_test)
            
            # Train model
            self.model.fit(X_train_scaled, y_train)
            
            # Evaluate
            y_pred = self.model.predict(X_test_scaled)
            accuracy = accuracy_score(y_test, y_pred)
            
            logger.info(f"General Analysis Model trained with accuracy: {accuracy:.4f}")
            
            self.is_trained = True
            self.save_model()
            return True
            
        except Exception as e:
            logger.error(f"Error training General Analysis Model: {e}")
            return False
    
    def predict(self, transactions: List[SAPGLPosting]) -> List[Dict[str, Any]]:
        """Predict general analysis results"""
        if not self.is_trained:
            self.load_model()
        
        if not self.is_trained:
            return []
        
        try:
            X = self.extract_features(transactions)
            if X.empty:
                return []
            
            X_scaled = self.scaler.transform(X)
            predictions = self.model.predict(X_scaled)
            probabilities = self.model.predict_proba(X_scaled)
            
            results = []
            for i, t in enumerate(transactions):
                results.append({
                    'transaction_id': str(t.id),
                    'prediction': int(predictions[i]),
                    'confidence': float(max(probabilities[i])),
                    'risk_score': float(probabilities[i][1] * 100) if len(probabilities[i]) > 1 else 0.0
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Error predicting with General Analysis Model: {e}")
            return []

class DuplicateAnalysisModel(BaseAnalysisModel):
    """ML Model for Duplicate Analysis - Enhanced duplicate detection"""
    
    def __init__(self):
        super().__init__('duplicate_analysis')
        self.model = IsolationForest(contamination=0.05, random_state=42)
    
    def extract_features(self, transactions: List[SAPGLPosting]) -> pd.DataFrame:
        """Extract features for duplicate detection"""
        if not transactions:
            return pd.DataFrame()
        
        features = []
        for t in transactions:
            feature_dict = {
                'account_numeric': self._account_to_numeric(t.gl_account),
                'user_numeric': self._encode_user(t.user_name),
                'document_type_numeric': self._encode_document_type(t.document_type),
                'posting_date_numeric': self._date_to_numeric(t.posting_date),
                'document_date_numeric': self._date_to_numeric(t.document_date),
                'days_between_dates': self._days_between_dates(t.document_date, t.posting_date),
                'is_high_value': 1 if t.is_high_value else 0,
                'fiscal_year': t.fiscal_year,
                'posting_period': t.posting_period
            }
            features.append(feature_dict)
        
        return pd.DataFrame(features)
    
    def _account_to_numeric(self, account_id: str) -> int:
        """Convert account ID to numeric for ML"""
        try:
            return int(account_id) if account_id else 0
        except:
            return 0
    
    def _encode_user(self, user_name: str) -> int:
        """Encode user name to numeric"""
        if user_name not in self.label_encoders:
            self.label_encoders['user'] = LabelEncoder()
            # This would need to be fitted with all users
        return hash(user_name) % 10000 if user_name else 0
    
    def _encode_document_type(self, doc_type: str) -> int:
        """Encode document type to numeric"""
        if not doc_type:
            return 0
        return hash(doc_type) % 100 if doc_type else 0
    
    def _date_to_numeric(self, date) -> int:
        """Convert date to numeric for ML"""
        if not date:
            return 0
        return date.toordinal()
    
    def _days_between_dates(self, doc_date, posting_date) -> int:
        """Calculate days between document and posting date"""
        if not doc_date or not posting_date:
            return 0
        return (posting_date - doc_date).days
    
    def train(self, transactions: List[SAPGLPosting]):
        """Train the duplicate analysis model"""
        try:
            X = self.extract_features(transactions)
            if X.empty:
                logger.error("No features extracted for duplicate training")
                return False
            
            # Train isolation forest
            self.model.fit(X)
            
            logger.info("Duplicate Analysis Model trained successfully")
            
            self.is_trained = True
            self.save_model()
            return True
            
        except Exception as e:
            logger.error(f"Error training Duplicate Analysis Model: {e}")
            return False
    
    def predict(self, transactions: List[SAPGLPosting]) -> List[Dict[str, Any]]:
        """Predict duplicate analysis results"""
        if not self.is_trained:
            self.load_model()
        
        if not self.is_trained:
            return []
        
        try:
            X = self.extract_features(transactions)
            if X.empty:
                return []
            
            # Predict anomalies (duplicates are anomalies)
            predictions = self.model.predict(X)
            scores = self.model.decision_function(X)
            
            results = []
            for i, t in enumerate(transactions):
                is_duplicate = int(predictions[i] == -1)  # -1 indicates anomaly/duplicate
                results.append({
                    'transaction_id': str(t.id),
                    'is_duplicate': is_duplicate,
                    'duplicate_score': float(scores[i]),
                    'risk_score': float(abs(scores[i]) * 100)
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Error predicting with Duplicate Analysis Model: {e}")
            return []

class BackdatedAnalysisModel(BaseAnalysisModel):
    """ML Model for Backdated Analysis - Backdated entry detection"""
    
    def __init__(self):
        super().__init__('backdated_analysis')
        self.model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')
    
    def extract_features(self, transactions: List[SAPGLPosting]) -> pd.DataFrame:
        """Extract features for backdated detection"""
        if not transactions:
            return pd.DataFrame()
        
        features = []
        for t in transactions:
            days_diff = self._days_between_dates(t.document_date, t.posting_date)
            
            feature_dict = {
                'days_difference': days_diff,
                'days_difference_abs': abs(days_diff),
                'is_backdated': 1 if days_diff > 0 else 0,
                'is_significantly_backdated': 1 if days_diff > 7 else 0,
                'is_month_end_backdated': 1 if days_diff > 0 and t.posting_date and t.posting_date.day >= 25 else 0,
                'is_quarter_end_backdated': 1 if days_diff > 0 and t.posting_date and t.posting_date.month in [3, 6, 9, 12] and t.posting_date.day >= 25 else 0,
                'is_year_end_backdated': 1 if days_diff > 0 and t.posting_date and t.posting_date.month == 12 and t.posting_date.day >= 25 else 0,
                'user_numeric': self._encode_user(t.user_name),
                'account_numeric': self._account_to_numeric(t.gl_account),
                'is_high_value': 1 if t.is_high_value else 0,
                'fiscal_year': t.fiscal_year,
                'posting_period': t.posting_period
            }
            features.append(feature_dict)
        
        return pd.DataFrame(features)
    
    def _days_between_dates(self, doc_date, posting_date) -> int:
        """Calculate days between document and posting date"""
        if not doc_date or not posting_date:
            return 0
        return (posting_date - doc_date).days
    
    def _encode_user(self, user_name: str) -> int:
        """Encode user name to numeric"""
        return hash(user_name) % 10000 if user_name else 0
    
    def _account_to_numeric(self, account_id: str) -> int:
        """Convert account ID to numeric for ML"""
        try:
            return int(account_id) if account_id else 0
        except:
            return 0
    
    def train(self, transactions: List[SAPGLPosting], labels: List[int]):
        """Train the backdated analysis model"""
        try:
            X = self.extract_features(transactions)
            if X.empty:
                logger.error("No features extracted for backdated training")
                return False
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(X, labels, test_size=0.2, random_state=42)
            
            # Scale features
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_test_scaled = self.scaler.transform(X_test)
            
            # Train model
            self.model.fit(X_train_scaled, y_train)
            
            # Evaluate
            y_pred = self.model.predict(X_test_scaled)
            accuracy = accuracy_score(y_test, y_pred)
            
            logger.info(f"Backdated Analysis Model trained with accuracy: {accuracy:.4f}")
            
            self.is_trained = True
            self.save_model()
            return True
            
        except Exception as e:
            logger.error(f"Error training Backdated Analysis Model: {e}")
            return False
    
    def predict(self, transactions: List[SAPGLPosting]) -> List[Dict[str, Any]]:
        """Predict backdated analysis results"""
        if not self.is_trained:
            self.load_model()
        
        if not self.is_trained:
            return []
        
        try:
            X = self.extract_features(transactions)
            if X.empty:
                return []
            
            X_scaled = self.scaler.transform(X)
            predictions = self.model.predict(X_scaled)
            probabilities = self.model.predict_proba(X_scaled)
            
            results = []
            for i, t in enumerate(transactions):
                days_diff = self._days_between_dates(t.document_date, t.posting_date)
                results.append({
                    'transaction_id': str(t.id),
                    'is_backdated': int(predictions[i]),
                    'confidence': float(max(probabilities[i])),
                    'days_difference': days_diff,
                    'risk_score': float(probabilities[i][1] * 100) if len(probabilities[i]) > 1 else 0.0
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Error predicting with Backdated Analysis Model: {e}")
            return []

class UserAnalysisModel(BaseAnalysisModel):
    """ML Model for User Analysis - Detects user activity anomalies"""
    
    def __init__(self):
        super().__init__('user_analysis')
        self.model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')
    
    def extract_features(self, transactions: List[SAPGLPosting]) -> pd.DataFrame:
        """Extract features for user analysis"""
        if not transactions:
            return pd.DataFrame()
        
        features = []
        for t in transactions:
            feature_dict = {
                'is_debit': 1 if t.transaction_type == 'DEBIT' else 0,
                'is_credit': 1 if t.transaction_type == 'CREDIT' else 0,
                'day_of_week': t.posting_date.weekday() if t.posting_date else 0,
                'day_of_month': t.posting_date.day if t.posting_date else 0,
                'month': t.posting_date.month if t.posting_date else 0,
                'is_month_end': 1 if t.posting_date and t.posting_date.day >= 25 else 0,
                'is_quarter_end': 1 if t.posting_date and t.posting_date.month in [3, 6, 9, 12] and t.posting_date.day >= 25 else 0,
                'is_year_end': 1 if t.posting_date and t.posting_date.month == 12 and t.posting_date.day >= 25 else 0,
                'is_high_value': 1 if t.is_high_value else 0,
                'is_cleared': 1 if t.is_cleared else 0,
                'account_type_code': self._get_account_type_code(t.gl_account),
                'user_encoded': self._encode_user(t.user_name),
                'fiscal_year': t.fiscal_year,
                'posting_period': t.posting_period
            }
            features.append(feature_dict)
        
        return pd.DataFrame(features)
    
    def _get_account_type_code(self, account_id: str) -> int:
        """Get account type code for ML features"""
        if not account_id:
            return 0
        try:
            # Simple account type mapping based on account ID patterns
            if account_id.startswith('1'):  # Assets
                return 1
            elif account_id.startswith('2'):  # Liabilities
                return 2
            elif account_id.startswith('3'):  # Equity
                return 3
            elif account_id.startswith('4'):  # Revenue
                return 4
            elif account_id.startswith('5'):  # Expenses
                return 5
            else:
                return 0
        except:
            return 0
    
    def _encode_user(self, user_name: str) -> int:
        """Encode user name to numeric for ML"""
        if not user_name:
            return 0
        try:
            # Simple hash-based encoding
            return hash(user_name) % 1000
        except:
            return 0
    
    def train(self, transactions: List[SAPGLPosting], labels: List[int]):
        """Train the user analysis model"""
        try:
            # Extract features
            features = self.extract_features(transactions)
            
            if features.empty or len(features) < 10:
                logger.warning("Insufficient data for user analysis model training")
                return False
            
            # Prepare labels (use provided labels or create simple heuristic)
            if not labels:
                # Create simple heuristic labels based on transaction patterns
                labels = []
                for t in transactions:
                    # Simple heuristic: flag unusual patterns
                    is_anomaly = 0
                    if t.is_high_value and t.transaction_type == 'DEBIT':
                        is_anomaly = 1
                    elif t.posting_date and t.posting_date.weekday() >= 5:  # Weekend
                        is_anomaly = 1
                    labels.append(is_anomaly)
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                features, labels, test_size=0.2, random_state=42, stratify=labels
            )
            
            # Scale features
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_test_scaled = self.scaler.transform(X_test)
            
            # Train model
            self.model.fit(X_train_scaled, y_train)
            
            # Evaluate
            y_pred = self.model.predict(X_test_scaled)
            accuracy = accuracy_score(y_test, y_pred)
            
            logger.info(f"User analysis model trained with accuracy: {accuracy:.3f}")
            
            # Save model
            self.save_model()
            self.is_trained = True
            
            return True
            
        except Exception as e:
            logger.error(f"Error training user analysis model: {e}")
            return False
    
    def predict(self, transactions: List[SAPGLPosting]) -> List[Dict[str, Any]]:
        """Predict user anomalies"""
        try:
            if not self.is_trained:
                self.load_model()
            
            if not self.is_trained:
                logger.warning("User analysis model not trained")
                return []
            
            # Extract features
            features = self.extract_features(transactions)
            
            if features.empty:
                return []
            
            # Scale features
            features_scaled = self.scaler.transform(features)
            
            # Predict
            predictions = self.model.predict(features_scaled)
            probabilities = self.model.predict_proba(features_scaled)
            
            # Create results
            results = []
            for i, t in enumerate(transactions):
                prediction = predictions[i]
                confidence = max(probabilities[i])
                
                results.append({
                    'transaction_id': str(t.id),
                    'prediction': int(prediction),
                    'confidence': float(confidence),
                    'risk_score': float(confidence * 100) if prediction == 1 else 0.0,
                    'anomaly_type': 'user_anomaly' if prediction == 1 else 'normal',
                    'user_name': t.user_name,
                    'posting_date': t.posting_date.isoformat() if t.posting_date else None
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Error in user analysis prediction: {e}")
            return []

class OverallAnalysisModel(BaseAnalysisModel):
    """ML Model for Overall Analysis - Combined risk assessment"""
    
    def __init__(self):
        super().__init__('overall_analysis')
        self.model = RandomForestRegressor(n_estimators=100, random_state=42)
    
    def extract_features(self, transactions: List[SAPGLPosting], 
                        general_results: List[Dict], 
                        duplicate_results: List[Dict], 
                        backdated_results: List[Dict],
                        user_results: List[Dict],
                        unusual_days_results: List[Dict] = None,
                        holiday_results: List[Dict] = None,
                        closing_entries_results: List[Dict] = None) -> pd.DataFrame:
        """Extract features for overall analysis"""
        if not transactions:
            return pd.DataFrame()
        
        # Create lookup dictionaries for results
        general_lookup = {r['transaction_id']: r for r in general_results}
        duplicate_lookup = {r['transaction_id']: r for r in duplicate_results}
        backdated_lookup = {r['transaction_id']: r for r in backdated_results}
        user_lookup = {r['transaction_id']: r for r in user_results}
        unusual_days_lookup = {r['transaction_id']: r for r in (unusual_days_results or [])}
        holiday_lookup = {r['transaction_id']: r for r in (holiday_results or [])}
        closing_entries_lookup = {r['transaction_id']: r for r in (closing_entries_results or [])}
        
        features = []
        for t in transactions:
            t_id = str(t.id)
            
            # Get results from other analyses with safe defaults
            general_result = general_lookup.get(t_id, {})
            duplicate_result = duplicate_lookup.get(t_id, {})
            backdated_result = backdated_lookup.get(t_id, {})
            user_result = user_lookup.get(t_id, {})
            unusual_days_result = unusual_days_lookup.get(t_id, {})
            holiday_result = holiday_lookup.get(t_id, {})
            closing_entries_result = closing_entries_lookup.get(t_id, {})
            
            feature_dict = {
                'is_high_value': 1 if t.is_high_value else 0,
                'is_cleared': 1 if t.is_cleared else 0,
                'account_type_code': self._get_account_type_code(t.gl_account),
                'user_numeric': self._encode_user(t.user_name),
                'fiscal_year': t.fiscal_year,
                'posting_period': t.posting_period,
                
                # General analysis features
                'general_risk_score': general_result.get('risk_score', 0.0),
                'general_confidence': general_result.get('confidence', 0.0),
                
                # Duplicate analysis features
                'is_duplicate': duplicate_result.get('is_duplicate', 0),
                'duplicate_score': duplicate_result.get('duplicate_score', 0.0),
                'duplicate_risk_score': duplicate_result.get('risk_score', 0.0),
                
                # Backdated analysis features
                'is_backdated': backdated_result.get('is_backdated', 0),
                'days_difference': backdated_result.get('days_difference', 0),
                'backdated_risk_score': backdated_result.get('risk_score', 0.0),
                'backdated_confidence': backdated_result.get('confidence', 0.0),
                
                # User analysis features (with safe defaults)
                'is_user_anomaly': user_result.get('prediction', 0) if user_result else 0,
                'user_anomaly_confidence': user_result.get('confidence', 0.0) if user_result else 0.0,
                'user_risk_score': user_result.get('risk_score', 0.0) if user_result else 0.0,
                
                # Unusual days analysis features
                'is_unusual_day': unusual_days_result.get('is_unusual_day', 0) if unusual_days_result else 0,
                'unusual_day_risk_score': unusual_days_result.get('risk_score', 0.0) if unusual_days_result else 0.0,
                'weekend_posting': unusual_days_result.get('weekend_posting', 0) if unusual_days_result else 0,
                
                # Holiday analysis features
                'is_holiday': holiday_result.get('is_holiday', 0) if holiday_result else 0,
                'holiday_risk_score': holiday_result.get('risk_score', 0.0) if holiday_result else 0.0,
                'holiday_type_numeric': self._encode_holiday_type(holiday_result.get('holiday_type', '')) if holiday_result else 0,
                
                # Closing entries analysis features
                'is_closing_entry': closing_entries_result.get('is_closing_entry', 0) if closing_entries_result else 0,
                'closing_entry_risk_score': closing_entries_result.get('risk_score', 0.0) if closing_entries_result else 0.0,
                'is_post_close': closing_entries_result.get('is_post_close', 0) if closing_entries_result else 0,
                'days_from_month_end': closing_entries_result.get('days_from_month_end', 0) if closing_entries_result else 0,
                
                # Combined risk indicators
                'total_flags': (duplicate_result.get('is_duplicate', 0) + 
                               backdated_result.get('is_backdated', 0) +
                               (user_result.get('prediction', 0) if user_result else 0) +
                               (unusual_days_result.get('is_unusual_day', 0) if unusual_days_result else 0) +
                               (holiday_result.get('is_holiday', 0) if holiday_result else 0) +
                               (closing_entries_result.get('is_closing_entry', 0) if closing_entries_result else 0)),
                'max_risk_score': max([
                    general_result.get('risk_score', 0.0),
                    duplicate_result.get('risk_score', 0.0),
                    backdated_result.get('risk_score', 0.0),
                    user_result.get('risk_score', 0.0) if user_result else 0.0,
                    unusual_days_result.get('risk_score', 0.0) if unusual_days_result else 0.0,
                    holiday_result.get('risk_score', 0.0) if holiday_result else 0.0,
                    closing_entries_result.get('risk_score', 0.0) if closing_entries_result else 0.0
                ])
            }
            features.append(feature_dict)
        
        return pd.DataFrame(features)
    
    def _get_account_type_code(self, account_id: str) -> int:
        """Get account type code for ML features"""
        if not account_id:
            return 0
        try:
            account_num = int(account_id)
            if 1000 <= account_num <= 1999:
                return 1  # Asset
            elif 2000 <= account_num <= 2999:
                return 2  # Liability
            elif 3000 <= account_num <= 3999:
                return 3  # Equity
            elif 4000 <= account_num <= 4999:
                return 4  # Revenue
            elif 5000 <= account_num <= 5999:
                return 5  # Expense
            else:
                return 0  # Other
        except:
            return 0
    
    def _encode_user(self, user_name: str) -> int:
        """Encode user name to numeric"""
        return hash(user_name) % 10000 if user_name else 0
    
    def _encode_holiday_type(self, holiday_type: str) -> int:
        """Encode holiday type to numeric"""
        if not holiday_type:
            return 0
        holiday_types = {
            'Public holiday': 1,
            'Observance': 2,
            'Bank holiday': 3,
            'National holiday': 4
        }
        return holiday_types.get(holiday_type, 0)
    
    def train(self, transactions: List[SAPGLPosting], 
              general_results: List[Dict], 
              duplicate_results: List[Dict], 
              backdated_results: List[Dict],
              user_results: List[Dict],
              unusual_days_results: List[Dict] = None,
              holiday_results: List[Dict] = None,
              closing_entries_results: List[Dict] = None,
              overall_risk_scores: List[float] = None):
        """Train the overall analysis model"""
        try:
            X = self.extract_features(transactions, general_results, duplicate_results, backdated_results, user_results, unusual_days_results, holiday_results, closing_entries_results)
            if X.empty:
                logger.error("No features extracted for overall training")
                return False
            
            # Generate risk scores if not provided
            if overall_risk_scores is None:
                logger.info("Generating synthetic risk scores for overall model training")
                overall_risk_scores = []
                for i, t in enumerate(transactions):
                    # Calculate a comprehensive risk score based on all analysis results
                    risk_score = 30.0  # Base risk score
                    
                    # Add risk from general analysis
                    general_result = next((r for r in general_results if r['transaction_id'] == str(t.id)), {})
                    if general_result:
                        risk_score += general_result.get('risk_score', 0) * 0.3
                    
                    # Add risk from duplicate analysis
                    duplicate_result = next((r for r in duplicate_results if r['transaction_id'] == str(t.id)), {})
                    if duplicate_result:
                        risk_score += duplicate_result.get('risk_score', 0) * 0.2
                    
                    # Add risk from backdated analysis
                    backdated_result = next((r for r in backdated_results if r['transaction_id'] == str(t.id)), {})
                    if backdated_result:
                        risk_score += backdated_result.get('risk_score', 0) * 0.2
                    
                    # Add risk from user analysis
                    user_result = next((r for r in user_results if r['transaction_id'] == str(t.id)), {})
                    if user_result:
                        risk_score += user_result.get('risk_score', 0) * 0.15
                    
                    # Add risk from unusual days analysis
                    if unusual_days_results:
                        unusual_result = next((r for r in unusual_days_results if r['transaction_id'] == str(t.id)), {})
                        if unusual_result:
                            risk_score += unusual_result.get('risk_score', 0) * 0.1
                    
                    # Add risk from closing entries analysis
                    if closing_entries_results:
                        closing_result = next((r for r in closing_entries_results if r['transaction_id'] == str(t.id)), {})
                        if closing_result:
                            risk_score += closing_result.get('risk_score', 0) * 0.05
                    
                    # Add transaction-specific risk factors
                    if t.is_high_value:
                        risk_score += 20.0
                    
                    if t.transaction_type == 'DEBIT':
                        risk_score += 10.0
                    
                    if t.posting_date and t.posting_date.day >= 25:
                        risk_score += 15.0
                    
                    # Ensure risk score is within bounds
                    risk_score = max(0.0, min(100.0, risk_score))
                    overall_risk_scores.append(risk_score)
            
            # Ensure we have the same number of risk scores as transactions
            if len(overall_risk_scores) != len(transactions):
                logger.error(f"Mismatch in risk scores: {len(overall_risk_scores)} vs {len(transactions)} transactions")
                return False
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(X, overall_risk_scores, test_size=0.2, random_state=42)
            
            # Scale features
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_test_scaled = self.scaler.transform(X_test)
            
            # Train model
            self.model.fit(X_train_scaled, y_train)
            
            # Evaluate
            y_pred = self.model.predict(X_test_scaled)
            mse = np.mean((y_test - y_pred) ** 2)
            
            logger.info(f"Overall Analysis Model trained with MSE: {mse:.4f}")
            
            self.is_trained = True
            self.save_model()
            return True
            
        except Exception as e:
            logger.error(f"Error training Overall Analysis Model: {e}")
            return False
    
    def predict(self, transactions: List[SAPGLPosting], 
                general_results: List[Dict], 
                duplicate_results: List[Dict], 
                backdated_results: List[Dict],
                user_results: List[Dict],
                unusual_days_results: List[Dict] = None,
                holiday_results: List[Dict] = None,
                closing_entries_results: List[Dict] = None) -> List[Dict[str, Any]]:
        """Predict overall analysis results"""
        if not self.is_trained:
            self.load_model()
        
        if not self.is_trained:
            logger.warning("Overall model not trained, using default results")
            return self._generate_default_results(transactions)
        
        # Validate closing entries results
        if closing_entries_results and isinstance(closing_entries_results[0], (int, str)):
            logger.warning("Closing entries results contain transaction IDs instead of full objects. Skipping closing entries for prediction.")
            closing_entries_results = []
        
        # Validate unusual days results
        if unusual_days_results and isinstance(unusual_days_results[0], (int, str)):
            logger.warning("Unusual days results contain transaction IDs instead of full objects. Skipping unusual days for prediction.")
            unusual_days_results = []
        
        try:
            X = self.extract_features(transactions, general_results, duplicate_results, backdated_results, user_results, unusual_days_results, holiday_results, closing_entries_results)
            if X.empty:
                logger.warning("No features extracted for overall prediction, using default results")
                return self._generate_default_results(transactions)
            
            # Handle feature mismatch by ensuring all expected features are present
            expected_features = getattr(self.model, 'feature_names_in_', None)
            if expected_features is not None:
                missing_features = set(expected_features) - set(X.columns)
                extra_features = set(X.columns) - set(expected_features)
                
                if missing_features or extra_features:
                    logger.warning(f"Feature mismatch for overall analysis. Missing: {missing_features}, Extra: {extra_features}")
                    
                    # If there are significant feature mismatches, force retrain the model
                    if len(missing_features) > 2 or len(extra_features) > 2:
                        logger.warning("Significant feature mismatch detected. Forcing model retrain.")
                        self._force_retrain_with_new_features(transactions, general_results, duplicate_results, backdated_results, user_results, unusual_days_results, holiday_results, closing_entries_results)
                        # After retrain, try prediction again
                        X = self.extract_features(transactions, general_results, duplicate_results, backdated_results, user_results, unusual_days_results, holiday_results, closing_entries_results)
                        expected_features = getattr(self.model, 'feature_names_in_', None)
                        if expected_features is not None:
                            missing_features = set(expected_features) - set(X.columns)
                            extra_features = set(X.columns) - set(expected_features)
                    
                    # Add missing features with default values
                    for feature in missing_features:
                        X[feature] = 0.0
                    
                    # Remove extra features
                    for feature in extra_features:
                        X = X.drop(columns=[feature])
                
                # Ensure columns are in the same order as expected
                if expected_features is not None:
                    X = X.reindex(columns=expected_features, fill_value=0.0)
            
            # Scale features
            X_scaled = self.scaler.transform(X)
            
            # Make predictions
            predictions = self.model.predict(X_scaled)
            
            # Generate results
            results = []
            for i, t in enumerate(transactions):
                risk_score = float(predictions[i])
                risk_level = self._get_risk_level(risk_score)
                
                # Get risk factors from individual analyses
                risk_factors = {}
                
                # General analysis risk factors
                general_result = next((r for r in general_results if r['transaction_id'] == str(t.id)), {})
                if general_result:
                    risk_factors['general'] = {
                        'risk_score': general_result.get('risk_score', 0),
                        'anomaly_type': general_result.get('anomaly_type', 'none')
                    }
                
                # Duplicate analysis risk factors
                duplicate_result = next((r for r in duplicate_results if r['transaction_id'] == str(t.id)), {})
                if duplicate_result:
                    risk_factors['duplicate'] = {
                        'risk_score': duplicate_result.get('risk_score', 0),
                        'duplicate_type': duplicate_result.get('duplicate_type', 'none')
                    }
                
                # Backdated analysis risk factors
                backdated_result = next((r for r in backdated_results if r['transaction_id'] == str(t.id)), {})
                if backdated_result:
                    risk_factors['backdated'] = {
                        'risk_score': backdated_result.get('risk_score', 0),
                        'backdated_days': backdated_result.get('backdated_days', 0)
                    }
                
                # User analysis risk factors
                user_result = next((r for r in user_results if r['transaction_id'] == str(t.id)), {})
                if user_result:
                    risk_factors['user'] = {
                        'risk_score': user_result.get('risk_score', 0),
                        'user_anomaly_type': user_result.get('anomaly_type', 'none')
                    }
                
                results.append({
                    'transaction_id': str(t.id),
                    'overall_risk_score': risk_score,
                    'risk_level': risk_level,
                    'recommendations': self._get_recommendations(risk_score),
                    'risk_factors': risk_factors
                })
            
            logger.info(f"Overall analysis prediction completed for {len(results)} transactions")
            return results
            
        except Exception as e:
            logger.error(f"Error predicting with Overall Analysis Model: {e}")
            logger.warning("Falling back to default results")
            return self._generate_default_results(transactions)
    
    def _force_retrain_with_new_features(self, transactions: List[SAPGLPosting], 
                                       general_results: List[Dict], 
                                       duplicate_results: List[Dict], 
                                       backdated_results: List[Dict],
                                       user_results: List[Dict],
                                       unusual_days_results: List[Dict] = None,
                                       holiday_results: List[Dict] = None,
                                       closing_entries_results: List[Dict] = None):
        """Force retrain the model with new features"""
        try:
            # Generate synthetic risk scores for training
            overall_risk_scores = []
            for i, t in enumerate(transactions):
                # Calculate a simple risk score based on transaction properties
                risk_score = 30.0  # Base risk score
                
                # Increase risk for high-value transactions
                if t.is_high_value:
                    risk_score += 20.0
                
                # Increase risk for unusual transaction types
                if t.transaction_type == 'DEBIT':
                    risk_score += 10.0
                
                # Increase risk for month-end transactions
                if t.posting_date and t.posting_date.day >= 25:
                    risk_score += 15.0
                
                overall_risk_scores.append(risk_score)
            
            # Retrain the model
            success = self.train(transactions, general_results, duplicate_results, backdated_results, user_results, unusual_days_results, closing_entries_results, overall_risk_scores)
            
            if success:
                logger.info("Overall model successfully retrained with new features")
            else:
                logger.error("Failed to retrain overall model with new features")
                
        except Exception as e:
            logger.error(f"Error during forced retrain: {e}")
    
    def _generate_default_results(self, transactions: List[SAPGLPosting]) -> List[Dict[str, Any]]:
        """Generate default results when model prediction fails"""
        results = []
        for t in transactions:
            # Calculate a simple risk score based on transaction properties
            risk_score = 30.0  # Base risk score
            
            # Increase risk for high-value transactions
            if t.is_high_value:
                risk_score += 20.0
            
            # Increase risk for unusual transaction types
            if t.transaction_type == 'DEBIT':
                risk_score += 10.0
            
            # Increase risk for month-end transactions
            if t.posting_date and t.posting_date.day >= 25:
                risk_score += 15.0
            
            risk_level = self._get_risk_level(risk_score)
            
            results.append({
                'transaction_id': str(t.id),
                'overall_risk_score': risk_score,
                'risk_level': risk_level,
                'recommendations': self._get_recommendations(risk_score)
            })
        
        return results
    
    def _get_risk_level(self, risk_score: float) -> str:
        """Get risk level based on score"""
        if risk_score >= 80:
            return 'CRITICAL'
        elif risk_score >= 60:
            return 'HIGH'
        elif risk_score >= 30:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def _get_recommendations(self, risk_score: float) -> List[str]:
        """Get recommendations based on risk score"""
        recommendations = []
        
        if risk_score >= 80:
            recommendations.extend([
                'Immediate investigation required',
                'Review transaction details thoroughly',
                'Consider blocking similar transactions'
            ])
        elif risk_score >= 60:
            recommendations.extend([
                'Investigate transaction patterns',
                'Monitor user activity closely',
                'Review account usage patterns'
            ])
        elif risk_score >= 30:
            recommendations.extend([
                'Monitor for similar patterns',
                'Review transaction periodically',
                'Consider additional controls'
            ])
        else:
            recommendations.append('Normal transaction - no action required')
        
        return recommendations

class HolidayAnalysisModel(BaseAnalysisModel):
    """ML Model for Holiday Analysis - Holiday posting detection and classification"""
    
    def __init__(self):
        super().__init__('holiday_analysis')
        self.model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')
    
    def extract_features(self, transactions: List[SAPGLPosting]) -> pd.DataFrame:
        """Extract features for holiday analysis"""
        if not transactions:
            return pd.DataFrame()
        
        features = []
        for t in transactions:
            feature_dict = {
                'is_debit': 1 if t.transaction_type == 'DEBIT' else 0,
                'is_credit': 1 if t.transaction_type == 'CREDIT' else 0,
                'day_of_week': t.posting_date.weekday() if t.posting_date else 0,
                'day_of_month': t.posting_date.day if t.posting_date else 0,
                'month': t.posting_date.month if t.posting_date else 0,
                'quarter': (t.posting_date.month - 1) // 3 + 1 if t.posting_date else 0,
                'is_high_value': 1 if t.is_high_value else 0,
                'account_type_code': self._get_account_type_code(t.gl_account),
                'fiscal_year': t.fiscal_year,
                'posting_period': t.posting_period,
                'user_numeric': self._encode_user(t.user_name),
                'amount_log': np.log(float(t.amount_local_currency) + 1) if float(t.amount_local_currency) > 0 else 0
            }
            features.append(feature_dict)
        
        return pd.DataFrame(features)
    
    def _get_account_type_code(self, account_id: str) -> int:
        """Get account type code for ML features"""
        if not account_id:
            return 0
        try:
            account_num = int(account_id)
            if 1000 <= account_num <= 1999:
                return 1  # Asset
            elif 2000 <= account_num <= 2999:
                return 2  # Liability
            elif 3000 <= account_num <= 3999:
                return 3  # Equity
            elif 4000 <= account_num <= 4999:
                return 4  # Revenue
            elif 5000 <= account_num <= 5999:
                return 5  # Expense
            else:
                return 0  # Other
        except:
            return 0
    
    def _encode_user(self, user_name: str) -> int:
        """Encode user name to numeric for ML"""
        if not user_name:
            return 0
        return hash(user_name) % 10000  # Simple hash encoding
    
    def train(self, transactions: List[SAPGLPosting], labels: List[int]):
        """Train the holiday analysis model"""
        try:
            X = self.extract_features(transactions)
            if X.empty:
                logger.error("No features extracted for holiday analysis training")
                return False
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(X, labels, test_size=0.2, random_state=42)
            
            # Scale features
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_test_scaled = self.scaler.transform(X_test)
            
            # Train model
            self.model.fit(X_train_scaled, y_train)
            
            # Evaluate
            y_pred = self.model.predict(X_test_scaled)
            accuracy = accuracy_score(y_test, y_pred)
            
            logger.info(f"Holiday Analysis Model trained with accuracy: {accuracy:.4f}")
            
            self.is_trained = True
            self.save_model()
            return True
            
        except Exception as e:
            logger.error(f"Error training Holiday Analysis Model: {e}")
            return False
    
    def predict(self, transactions: List[SAPGLPosting]) -> List[Dict[str, Any]]:
        """Predict holiday analysis results"""
        if not self.is_trained:
            self.load_model()
        
        if not self.is_trained:
            return []
        
        try:
            X = self.extract_features(transactions)
            if X.empty:
                return []
            
            X_scaled = self.scaler.transform(X)
            predictions = self.model.predict(X_scaled)
            probabilities = self.model.predict_proba(X_scaled)
            
            results = []
            for i, t in enumerate(transactions):
                results.append({
                    'transaction_id': str(t.id),
                    'prediction': int(predictions[i]),
                    'confidence': float(max(probabilities[i])),
                    'risk_score': float(probabilities[i][1] * 100) if len(probabilities[i]) > 1 else 0.0
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Error predicting with Holiday Analysis Model: {e}")
            return []

class RiskAnalysisModel(BaseAnalysisModel):
    """ML Model for Risk Analysis - Risk scoring and classification"""
    
    def __init__(self):
        super().__init__('risk_analysis')
        self.model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')
    
    def extract_features(self, transactions: List[SAPGLPosting], 
                        overall_results: List[Dict]) -> pd.DataFrame:
        """Extract features for risk analysis"""
        if not transactions:
            return pd.DataFrame()
        
        # Create lookup for overall results
        overall_lookup = {r['transaction_id']: r for r in overall_results}
        
        features = []
        for t in transactions:
            t_id = str(t.id)
            overall_result = overall_lookup.get(t_id, {})
            
            feature_dict = {
                'is_high_value': 1 if t.is_high_value else 0,
                'is_cleared': 1 if t.is_cleared else 0,
                'account_type_code': self._get_account_type_code(t.gl_account),
                'user_numeric': self._encode_user(t.user_name),
                'fiscal_year': t.fiscal_year,
                'posting_period': t.posting_period,
                'day_of_week': t.posting_date.weekday() if t.posting_date else 0,
                'day_of_month': t.posting_date.day if t.posting_date else 0,
                'month': t.posting_date.month if t.posting_date else 0,
                'is_month_end': 1 if t.posting_date and t.posting_date.day >= 25 else 0,
                'is_quarter_end': 1 if t.posting_date and t.posting_date.month in [3, 6, 9, 12] and t.posting_date.day >= 25 else 0,
                'is_year_end': 1 if t.posting_date and t.posting_date.month == 12 and t.posting_date.day >= 25 else 0,
                
                # Overall analysis features
                'overall_risk_score': overall_result.get('overall_risk_score', 0.0),
                'risk_level_numeric': self._risk_level_to_numeric(overall_result.get('risk_level', 'LOW'))
            }
            features.append(feature_dict)
        
        return pd.DataFrame(features)
    
    def _get_account_type_code(self, account_id: str) -> int:
        """Get account type code for ML features"""
        if not account_id:
            return 0
        try:
            account_num = int(account_id)
            if 1000 <= account_num <= 1999:
                return 1  # Asset
            elif 2000 <= account_num <= 2999:
                return 2  # Liability
            elif 3000 <= account_num <= 3999:
                return 3  # Equity
            elif 4000 <= account_num <= 4999:
                return 4  # Revenue
            elif 5000 <= account_num <= 5999:
                return 5  # Expense
            else:
                return 0  # Other
        except:
            return 0
    
    def _encode_user(self, user_name: str) -> int:
        """Encode user name to numeric"""
        return hash(user_name) % 10000 if user_name else 0
    
    def _risk_level_to_numeric(self, risk_level: str) -> int:
        """Convert risk level to numeric"""
        risk_levels = {'LOW': 0, 'MEDIUM': 1, 'HIGH': 2, 'CRITICAL': 3}
        return risk_levels.get(risk_level, 0)
    
    def train(self, transactions: List[SAPGLPosting], 
              overall_results: List[Dict], 
              risk_labels: List[int]):
        """Train the risk analysis model"""
        try:
            X = self.extract_features(transactions, overall_results)
            if X.empty:
                logger.error("No features extracted for risk training")
                return False
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(X, risk_labels, test_size=0.2, random_state=42)
            
            # Scale features
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_test_scaled = self.scaler.transform(X_test)
            
            # Train model
            self.model.fit(X_train_scaled, y_train)
            
            # Evaluate
            y_pred = self.model.predict(X_test_scaled)
            accuracy = accuracy_score(y_test, y_pred)
            
            logger.info(f"Risk Analysis Model trained with accuracy: {accuracy:.4f}")
            
            self.is_trained = True
            self.save_model()
            return True
            
        except Exception as e:
            logger.error(f"Error training Risk Analysis Model: {e}")
            return False
    
    def predict(self, transactions: List[SAPGLPosting], 
                overall_results: List[Dict]) -> List[Dict[str, Any]]:
        """Predict risk analysis results"""
        if not self.is_trained:
            self.load_model()
        
        if not self.is_trained:
            logger.warning("Risk model not trained, using default results")
            return self._generate_default_risk_results(transactions, overall_results)
        
        try:
            X = self.extract_features(transactions, overall_results)
            if X.empty:
                logger.warning("No features extracted for risk prediction, using default results")
                return self._generate_default_risk_results(transactions, overall_results)
            
            # Handle feature mismatch
            expected_features = getattr(self.model, 'feature_names_in_', None)
            if expected_features is not None:
                missing_features = set(expected_features) - set(X.columns)
                extra_features = set(X.columns) - set(expected_features)
                
                if missing_features or extra_features:
                    logger.warning(f"Feature mismatch for risk analysis. Missing: {missing_features}, Extra: {extra_features}")
                    
                    # Add missing features with default values
                    for feature in missing_features:
                        X[feature] = 0.0
                    
                    # Remove extra features
                    for feature in extra_features:
                        X = X.drop(columns=[feature])
                
                # Ensure columns are in the same order as expected
                if expected_features is not None:
                    X = X.reindex(columns=expected_features, fill_value=0.0)
            
            # Scale features
            X_scaled = self.scaler.transform(X)
            
            # Make predictions
            predictions = self.model.predict(X_scaled)
            probabilities = self.model.predict_proba(X_scaled)
            
            # Generate results
            results = []
            for i, t in enumerate(transactions):
                risk_class = int(predictions[i])
                risk_probability = float(np.max(probabilities[i]))
                
                # Get overall risk score
                overall_result = next((r for r in overall_results if r['transaction_id'] == str(t.id)), {})
                overall_risk_score = overall_result.get('overall_risk_score', 30.0)
                
                # Determine risk level based on class and probability
                if risk_class == 3:  # Critical
                    risk_level = 'CRITICAL'
                    risk_score = 90.0 + (risk_probability * 10.0)
                elif risk_class == 2:  # High
                    risk_level = 'HIGH'
                    risk_score = 70.0 + (risk_probability * 20.0)
                elif risk_class == 1:  # Medium
                    risk_level = 'MEDIUM'
                    risk_score = 40.0 + (risk_probability * 30.0)
                else:  # Low
                    risk_level = 'LOW'
                    risk_score = 10.0 + (risk_probability * 30.0)
                
                results.append({
                    'transaction_id': str(t.id),
                    'risk_class': risk_class,
                    'risk_level': risk_level,
                    'risk_score': risk_score,
                    'risk_probability': risk_probability,
                    'overall_risk_score': overall_risk_score,
                    'recommendations': self._get_risk_recommendations(risk_level, risk_score)
                })
            
            logger.info(f"Risk analysis prediction completed for {len(results)} transactions")
            return results
            
        except Exception as e:
            logger.error(f"Error predicting with Risk Analysis Model: {e}")
            logger.warning("Falling back to default risk results")
            return self._generate_default_risk_results(transactions, overall_results)
    
    def _generate_default_risk_results(self, transactions: List[SAPGLPosting], overall_results: List[Dict]) -> List[Dict[str, Any]]:
        """Generate default risk results when model prediction fails"""
        results = []
        for t in transactions:
            # Get overall risk score
            overall_result = next((r for r in overall_results if r['transaction_id'] == str(t.id)), {})
            overall_risk_score = overall_result.get('overall_risk_score', 30.0)
            
            # Calculate risk based on overall score
            if overall_risk_score >= 80:
                risk_level = 'CRITICAL'
                risk_score = 95.0
                risk_class = 3
            elif overall_risk_score >= 60:
                risk_level = 'HIGH'
                risk_score = 75.0
                risk_class = 2
            elif overall_risk_score >= 30:
                risk_level = 'MEDIUM'
                risk_score = 50.0
                risk_class = 1
            else:
                risk_level = 'LOW'
                risk_score = 25.0
                risk_class = 0
            
            results.append({
                'transaction_id': str(t.id),
                'risk_class': risk_class,
                'risk_level': risk_level,
                'risk_score': risk_score,
                'risk_probability': 0.8,
                'overall_risk_score': overall_risk_score,
                'recommendations': self._get_risk_recommendations(risk_level, risk_score)
            })
        
        return results
    
    def _get_risk_recommendations(self, risk_level: str, risk_score: float) -> List[str]:
        """Get recommendations based on risk level and score"""
        recommendations = []
        
        if risk_level == 'CRITICAL':
            recommendations.extend([
                'Immediate investigation required',
                'Consider transaction blocking',
                'Review user access permissions',
                'Escalate to management'
            ])
        elif risk_level == 'HIGH':
            recommendations.extend([
                'Investigate transaction patterns',
                'Monitor user activity closely',
                'Review account usage',
                'Consider additional controls'
            ])
        elif risk_level == 'MEDIUM':
            recommendations.extend([
                'Monitor for similar patterns',
                'Review transaction periodically',
                'Consider enhanced monitoring'
            ])
        else:
            recommendations.append('Normal transaction - no action required')
        
        return recommendations

class AnalysisModelManager:
    """Manager for all analysis models with efficient training and caching"""
    
    def __init__(self):
        self.models = {
            'general': GeneralAnalysisModel(),
            'duplicate': DuplicateAnalysisModel(),
            'backdated': BackdatedAnalysisModel(),
            'user': UserAnalysisModel(),
            'holiday': HolidayAnalysisModel(),
            'overall': OverallAnalysisModel(),
            'risk': RiskAnalysisModel()
        }
        self._training_status = {}  # Track training status for each model
        self._model_cache = {}  # Cache for trained models
    
    def get_model(self, model_type: str):
        """Get a specific model"""
        return self.models.get(model_type)
    
    def ensure_model_trained(self, model_type: str, transactions: List[SAPGLPosting], 
                           labels: List[int] = None, force_retrain: bool = False):
        """
        Ensure a model is trained, training it only if necessary
        
        Args:
            model_type: Type of model ('general', 'duplicate', 'backdated', etc.)
            transactions: List of transactions for training
            labels: Training labels (for supervised models)
            force_retrain: Force retraining even if model exists
            
        Returns:
            bool: True if model is ready for prediction
        """
        model = self.models.get(model_type)
        if not model:
            logger.error(f"Model type {model_type} not found")
            return False
        
        # First try to load existing trained model
        if not force_retrain:
            if model.load_model():
                logger.info(f"Model {model_type} loaded from disk, skipping training")
                return True
        
        # Check if model is already trained in memory and not forcing retrain
        if not force_retrain and model.is_trained:
            logger.info(f"Model {model_type} already trained in memory, skipping training")
            return True
        
        # Check if we have enough data for training
        if len(transactions) < 10:
            logger.warning(f"Insufficient data for training {model_type} model. Need at least 10 transactions.")
            return False
        
        logger.info(f"Training {model_type} model with {len(transactions)} transactions")
        
        try:
            # Train model based on type
            if model_type == 'general':
                if labels is None:
                    # Create labels for general analysis
                    labels = []
                    for t in transactions:
                        is_anomaly = (
                            (t.posting_date and t.posting_date.day >= 25)  # Month end
                        )
                        labels.append(1 if is_anomaly else 0)
                success = model.train(transactions, labels)
                
            elif model_type == 'duplicate':
                success = model.train(transactions)
                
            elif model_type == 'backdated':
                if labels is None:
                    # Create labels for backdated analysis
                    labels = []
                    for t in transactions:
                        if t.document_date and t.posting_date and t.document_date > t.posting_date:
                            labels.append(1)  # Backdated
                        else:
                            labels.append(0)  # Normal
                success = model.train(transactions, labels)
                
            elif model_type == 'user':
                if labels is None:
                    # Create labels for user analysis
                    labels = []
                    for t in transactions:
                        # Simple heuristic for user anomalies
                        is_anomaly = (
                            (t.posting_date and t.posting_date.weekday() >= 5)  # Weekend
                        )
                        labels.append(1 if is_anomaly else 0)
                success = model.train(transactions, labels)
                
            elif model_type == 'holiday':
                if labels is None:
                    # Create labels for holiday analysis using holiday_utils
                    labels = []
                    try:
                        from .holiday_utils import is_holiday
                        # Default to Saudi Arabian holidays
                        country_code = 'saudiarabian'
                        for t in transactions:
                            if t.posting_date:
                                # Check if posting date is a holiday
                                is_holiday_posting = is_holiday(country_code, t.posting_date)
                                labels.append(1 if is_holiday_posting else 0)
                            else:
                                labels.append(0)
                        logger.info(f"Generated {sum(labels)} holiday labels from {len(transactions)} transactions")
                    except Exception as e:
                        logger.warning(f"Could not generate holiday labels using holiday_utils: {e}")
                        # Fallback to simple heuristic
                        for t in transactions:
                            # Simple heuristic: weekend postings as potential holiday indicators
                            is_anomaly = (
                                (t.posting_date and t.posting_date.weekday() >= 5)  # Weekend
                            )
                            labels.append(1 if is_anomaly else 0)
                        logger.info(f"Generated {sum(labels)} holiday labels using fallback heuristic")
                success = model.train(transactions, labels)
                
            elif model_type == 'overall':
                # Overall model needs results from other models to train
                # Get results from other analyses to create training data
                try:
                    logger.info("Training overall model with comprehensive analysis results")
                    
                    # Get results from other analyses
                    general_results = self.predict_with_model('general', transactions)
                    duplicate_results = self.predict_with_model('duplicate', transactions)
                    backdated_results = self.predict_with_model('backdated', transactions)
                    user_results = self.predict_with_model('user', transactions)
                    
                    # Get unusual days, holiday, and closing entries results if available
                    unusual_days_results = []
                    holiday_results = []
                    closing_entries_results = []
                    
                    try:
                        from .ml_analysis_orchestrator import MLAnalysisOrchestrator
                        orchestrator = MLAnalysisOrchestrator()
                        unusual_days_data = orchestrator.run_unusual_days_analysis(transactions)
                        unusual_days_results = unusual_days_data.get('unusual_transactions', [])
                    except Exception as e:
                        logger.warning(f"Could not get unusual days results for training: {e}")
                    
                    try:
                        holiday_data = orchestrator.run_holiday_analysis(transactions)
                        holiday_results = holiday_data.get('holiday_postings', [])
                    except Exception as e:
                        logger.warning(f"Could not get holiday results for training: {e}")
                    
                    try:
                        closing_entries_data = orchestrator.run_closing_entries_analysis(transactions)
                        closing_entries_results = closing_entries_data.get('closing_entries', [])
                        # Ensure we have valid closing entries (not just IDs)
                        if closing_entries_results and isinstance(closing_entries_results[0], (int, str)):
                            logger.warning("Closing entries analysis returned transaction IDs instead of full objects. Skipping closing entries for training.")
                            closing_entries_results = []
                    except Exception as e:
                        logger.warning(f"Could not get closing entries results for training: {e}")
                        closing_entries_results = []
                    
                    # Create comprehensive training data
                    success = model.train(transactions, general_results, duplicate_results, 
                                        backdated_results, user_results, unusual_days_results, 
                                        holiday_results, closing_entries_results)
                    
                    if success:
                        logger.info("Overall model trained successfully with comprehensive data")
                    else:
                        logger.warning("Overall model training failed, will use default results")
                        success = True  # Don't fail the process
                        
                except Exception as e:
                    logger.error(f"Error training overall model: {e}")
                    logger.info("Overall model training skipped - will use default results")
                    success = True  # Don't fail the process
                
            elif model_type == 'risk':
                # Risk model needs overall results to train
                # For now, we'll skip training and use default results
                logger.info("Risk model training skipped - will use default results")
                success = True
                
            else:
                logger.warning(f"Training not implemented for model type: {model_type}")
                return False
            
            if success:
                self._training_status[model_type] = 'TRAINED'
                logger.info(f"Model {model_type} trained successfully")
                return True
            else:
                logger.error(f"Failed to train model {model_type}")
                return False
                
        except Exception as e:
            logger.error(f"Error training {model_type} model: {e}")
            return False
    
    def predict_with_model(self, model_type: str, transactions: List[SAPGLPosting], 
                          **kwargs) -> List[Dict[str, Any]]:
        """
        Predict using a specific model, ensuring it's trained first
        
        Args:
            model_type: Type of model to use
            transactions: Transactions to predict on
            **kwargs: Additional arguments for prediction
            
        Returns:
            List of prediction results
        """
        model = self.models.get(model_type)
        if not model:
            logger.error(f"Model type {model_type} not found")
            return []
        
        # Ensure model is trained
        if not model.is_trained:
            logger.info(f"Model {model_type} not trained, training now...")
            if not self.ensure_model_trained(model_type, transactions):
                logger.error(f"Failed to train model {model_type}")
                return []
        
        # Make predictions
        try:
            if model_type == 'general':
                return model.predict(transactions)
            elif model_type == 'duplicate':
                return model.predict(transactions)
            elif model_type == 'backdated':
                return model.predict(transactions)
            elif model_type == 'user':
                return model.predict(transactions)
            elif model_type == 'overall':
                general_results = kwargs.get('general_results', [])
                duplicate_results = kwargs.get('duplicate_results', [])
                backdated_results = kwargs.get('backdated_results', [])
                user_results = kwargs.get('user_results', [])
                unusual_days_results = kwargs.get('unusual_days_results', [])
                closing_entries_results = kwargs.get('closing_entries_results', [])
                return model.predict(transactions, general_results, duplicate_results, backdated_results, user_results, unusual_days_results, closing_entries_results)
            elif model_type == 'risk':
                overall_results = kwargs.get('overall_results', [])
                return model.predict(transactions, overall_results)
            else:
                logger.error(f"Prediction not implemented for model type: {model_type}")
                return []
                
        except Exception as e:
            logger.error(f"Error predicting with {model_type} model: {e}")
            return []
    
    def train_all_models(self, transactions: List[SAPGLPosting], 
                        labels: Dict[str, List] = None):
        """Train all models efficiently"""
        results = {}
        
        # Train general model
        results['general'] = self.ensure_model_trained('general', transactions, 
                                                      labels.get('general') if labels else None)
        
        # Train duplicate model
        results['duplicate'] = self.ensure_model_trained('duplicate', transactions)
        
        # Train backdated model
        results['backdated'] = self.ensure_model_trained('backdated', transactions,
                                                        labels.get('backdated') if labels else None)
        
        # Train user model
        results['user'] = self.ensure_model_trained('user', transactions,
                                                   labels.get('user') if labels else None)
        
        # Train holiday model
        results['holiday'] = self.ensure_model_trained('holiday', transactions,
                                                      labels.get('holiday') if labels else None)
        
        # Note: Overall and Risk models need results from other models to train
        
        return results
    
    def get_training_status(self) -> Dict[str, str]:
        """Get training status of all models"""
        status = {}
        for model_type, model in self.models.items():
            status[model_type] = 'TRAINED' if model.is_trained else 'NOT_TRAINED'
        return status
    
    def clear_cache(self):
        """Clear model cache"""
        self._model_cache.clear()
        logger.info("Model cache cleared")
    
    def predict_all(self, transactions: List[SAPGLPosting]) -> Dict[str, List[Dict]]:
        """Run predictions with all models"""
        results = {}
        
        # Run general analysis
        results['general'] = self.models['general'].predict(transactions)
        
        # Run duplicate analysis
        results['duplicate'] = self.models['duplicate'].predict(transactions)
        
        # Run backdated analysis
        results['backdated'] = self.models['backdated'].predict(transactions)
        
        # Run user analysis
        results['user'] = self.models['user'].predict(transactions)
        
        # Run holiday analysis
        results['holiday'] = self.models['holiday'].predict(transactions)
        
        # Run unusual days analysis (if available)
        try:
            from .ml_analysis_orchestrator import MLAnalysisOrchestrator
            orchestrator = MLAnalysisOrchestrator()
            unusual_days_results = orchestrator.run_unusual_days_analysis(transactions)
            results['unusual_days'] = unusual_days_results.get('unusual_transactions', [])
        except Exception as e:
            logger.warning(f"Could not run unusual days analysis: {e}")
            results['unusual_days'] = []
        
        # Run closing entries analysis (if available)
        try:
            closing_entries_results = orchestrator.run_closing_entries_analysis(transactions)
            results['closing_entries'] = closing_entries_results.get('closing_entries', [])
        except Exception as e:
            logger.warning(f"Could not run closing entries analysis: {e}")
            results['closing_entries'] = []
        
        # Run overall analysis (needs results from other models)
        results['overall'] = self.models['overall'].predict(
            transactions, 
            results['general'], 
            results['duplicate'], 
            results['backdated'],
            results['user'],
            results['unusual_days'],
            results['holiday'],
            results['closing_entries']
        )
        
        # Run risk analysis (needs overall results)
        results['risk'] = self.models['risk'].predict(transactions, results['overall'])
        
        return results 

    def force_retrain_all_models(self, transactions: List[SAPGLPosting]):
        """Force retrain all models to handle feature mismatches"""
        logger.info("Forcing retrain of all models to handle feature mismatches")
        
        # Clear all model caches
        for model_type, model in self.models.items():
            model.is_trained = False
            # Remove saved model files to force retrain
            try:
                if os.path.exists(model.model_path):
                    os.remove(model.model_path)
                if os.path.exists(model.scaler_path):
                    os.remove(model.scaler_path)
            except Exception as e:
                logger.warning(f"Could not remove model files for {model_type}: {e}")
        
        # Clear cache
        self.clear_cache()
        
        # Retrain all models
        return self.train_all_models(transactions) 