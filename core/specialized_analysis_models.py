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
                'amount': float(t.amount_local_currency),
                'is_debit': 1 if t.transaction_type == 'DEBIT' else 0,
                'is_credit': 1 if t.transaction_type == 'CREDIT' else 0,
                'amount_log': np.log(float(t.amount_local_currency) + 1),
                'day_of_week': t.posting_date.weekday() if t.posting_date else 0,
                'day_of_month': t.posting_date.day if t.posting_date else 0,
                'month': t.posting_date.month if t.posting_date else 0,
                'quarter': (t.posting_date.month - 1) // 3 + 1 if t.posting_date else 0,
                'is_month_end': 1 if t.posting_date and t.posting_date.day >= 25 else 0,
                'is_quarter_end': 1 if t.posting_date and t.posting_date.month in [3, 6, 9, 12] and t.posting_date.day >= 25 else 0,
                'is_year_end': 1 if t.posting_date and t.posting_date.month == 12 and t.posting_date.day >= 25 else 0,
                'has_arabic_text': 1 if t.has_arabic_text else 0,
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
                'amount': float(t.amount_local_currency),
                'amount_log': np.log(float(t.amount_local_currency) + 1),
                'account_numeric': self._account_to_numeric(t.gl_account),
                'user_numeric': self._encode_user(t.user_name),
                'document_type_numeric': self._encode_document_type(t.document_type),
                'posting_date_numeric': self._date_to_numeric(t.posting_date),
                'document_date_numeric': self._date_to_numeric(t.document_date),
                'days_between_dates': self._days_between_dates(t.document_date, t.posting_date),
                'is_high_value': 1 if t.is_high_value else 0,
                'has_arabic_text': 1 if t.has_arabic_text else 0,
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
                'amount': float(t.amount_local_currency),
                'amount_log': np.log(float(t.amount_local_currency) + 1),
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
                'has_arabic_text': 1 if t.has_arabic_text else 0,
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

class OverallAnalysisModel(BaseAnalysisModel):
    """ML Model for Overall Analysis - Combined risk assessment"""
    
    def __init__(self):
        super().__init__('overall_analysis')
        self.model = RandomForestRegressor(n_estimators=100, random_state=42)
    
    def extract_features(self, transactions: List[SAPGLPosting], 
                        general_results: List[Dict], 
                        duplicate_results: List[Dict], 
                        backdated_results: List[Dict]) -> pd.DataFrame:
        """Extract features for overall analysis"""
        if not transactions:
            return pd.DataFrame()
        
        # Create lookup dictionaries for results
        general_lookup = {r['transaction_id']: r for r in general_results}
        duplicate_lookup = {r['transaction_id']: r for r in duplicate_results}
        backdated_lookup = {r['transaction_id']: r for r in backdated_results}
        
        features = []
        for t in transactions:
            t_id = str(t.id)
            
            # Get results from other analyses
            general_result = general_lookup.get(t_id, {})
            duplicate_result = duplicate_lookup.get(t_id, {})
            backdated_result = backdated_lookup.get(t_id, {})
            
            feature_dict = {
                'amount': float(t.amount_local_currency),
                'amount_log': np.log(float(t.amount_local_currency) + 1),
                'is_high_value': 1 if t.is_high_value else 0,
                'has_arabic_text': 1 if t.has_arabic_text else 0,
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
                
                # Combined risk indicators
                'total_flags': (duplicate_result.get('is_duplicate', 0) + 
                               backdated_result.get('is_backdated', 0)),
                'max_risk_score': max([
                    general_result.get('risk_score', 0.0),
                    duplicate_result.get('risk_score', 0.0),
                    backdated_result.get('risk_score', 0.0)
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
    
    def train(self, transactions: List[SAPGLPosting], 
              general_results: List[Dict], 
              duplicate_results: List[Dict], 
              backdated_results: List[Dict],
              overall_risk_scores: List[float]):
        """Train the overall analysis model"""
        try:
            X = self.extract_features(transactions, general_results, duplicate_results, backdated_results)
            if X.empty:
                logger.error("No features extracted for overall training")
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
                backdated_results: List[Dict]) -> List[Dict[str, Any]]:
        """Predict overall analysis results"""
        if not self.is_trained:
            self.load_model()
        
        if not self.is_trained:
            return []
        
        try:
            X = self.extract_features(transactions, general_results, duplicate_results, backdated_results)
            if X.empty:
                return []
            
            X_scaled = self.scaler.transform(X)
            predictions = self.model.predict(X_scaled)
            
            results = []
            for i, t in enumerate(transactions):
                risk_score = float(predictions[i])
                risk_level = self._get_risk_level(risk_score)
                
                results.append({
                    'transaction_id': str(t.id),
                    'overall_risk_score': risk_score,
                    'risk_level': risk_level,
                    'recommendations': self._get_recommendations(risk_score)
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Error predicting with Overall Analysis Model: {e}")
            return []
    
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
                'amount': float(t.amount_local_currency),
                'amount_log': np.log(float(t.amount_local_currency) + 1),
                'is_high_value': 1 if t.is_high_value else 0,
                'has_arabic_text': 1 if t.has_arabic_text else 0,
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
            return []
        
        try:
            X = self.extract_features(transactions, overall_results)
            if X.empty:
                return []
            
            X_scaled = self.scaler.transform(X)
            predictions = self.model.predict(X_scaled)
            probabilities = self.model.predict_proba(X_scaled)
            
            results = []
            for i, t in enumerate(transactions):
                risk_class = int(predictions[i])
                confidence = float(max(probabilities[i]))
                
                results.append({
                    'transaction_id': str(t.id),
                    'risk_class': risk_class,
                    'confidence': confidence,
                    'risk_probabilities': probabilities[i].tolist(),
                    'final_risk_score': float(risk_class * 25 + confidence * 25)  # Scale to 0-100
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Error predicting with Risk Analysis Model: {e}")
            return []

class AnalysisModelManager:
    """Manager for all analysis models with efficient training and caching"""
    
    def __init__(self):
        self.models = {
            'general': GeneralAnalysisModel(),
            'duplicate': DuplicateAnalysisModel(),
            'backdated': BackdatedAnalysisModel(),
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
                            t.amount_local_currency > 10000 or  # High value
                            (t.posting_date and t.posting_date.day >= 25) or  # Month end
                            t.has_arabic_text  # Arabic text
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
            elif model_type == 'overall':
                general_results = kwargs.get('general_results', [])
                duplicate_results = kwargs.get('duplicate_results', [])
                backdated_results = kwargs.get('backdated_results', [])
                return model.predict(transactions, general_results, duplicate_results, backdated_results)
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
        
        # Run overall analysis (needs results from other models)
        results['overall'] = self.models['overall'].predict(
            transactions, 
            results['general'], 
            results['duplicate'], 
            results['backdated']
        )
        
        # Run risk analysis (needs overall results)
        results['risk'] = self.models['risk'].predict(transactions, results['overall'])
        
        return results 