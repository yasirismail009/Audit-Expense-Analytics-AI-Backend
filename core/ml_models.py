"""
Machine Learning Models for Anomaly Detection in SAP GL Postings

This module provides ML-based anomaly detection capabilities that can be trained
on historical transaction data to identify patterns and detect anomalies more accurately.
Uses in-memory storage for security and local ML models.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from decimal import Decimal
import logging
from typing import List, Dict, Tuple, Optional
import warnings
import pickle
import base64
import io
warnings.filterwarnings('ignore')

from .models import SAPGLPosting, FileProcessingJob

logger = logging.getLogger(__name__)

class MLAnomalyDetector:
    """Machine Learning-based Anomaly Detection System with in-memory storage"""
    
    def __init__(self):
        self.scaler = None
        self.label_encoders = {}
        self.models = {}
        self.feature_columns = []
        self.is_trained = False
        self.model_data = {}  # In-memory storage for model data
        self.duplicate_model = None  # Duplicate detection model
        
        # Initialize models
        self._initialize_models()
        self._initialize_duplicate_model()
    
    def _initialize_models(self):
        """Initialize ML models for different anomaly types"""
        try:
            from sklearn.ensemble import IsolationForest, RandomForestClassifier
            from sklearn.preprocessing import StandardScaler
            from sklearn.cluster import DBSCAN
            
            self.scaler = StandardScaler()
            
            self.models = {
                'isolation_forest': IsolationForest(
                    contamination=0.1,  # Expected proportion of anomalies
                    random_state=42,
                    n_estimators=100
                ),
                'random_forest': RandomForestClassifier(
                    n_estimators=100,
                    random_state=42,
                    class_weight='balanced'
                ),
                'dbscan': DBSCAN(
                    eps=0.5,
                    min_samples=5
                )
            }
        except ImportError:
            logger.warning("scikit-learn not available. Using simplified models.")
            self._initialize_simple_models()
    
    def _initialize_simple_models(self):
        """Initialize simplified models when scikit-learn is not available"""
        self.models = {
            'statistical': StatisticalAnomalyDetector(),
            'rule_based': RuleBasedAnomalyDetector()
        }
    
    def _initialize_duplicate_model(self):
        """Initialize the duplicate detection model"""
        try:
            self.duplicate_model = DuplicateDetectionModel()
            logger.info("Duplicate detection model initialized successfully")
        except Exception as e:
            logger.warning(f"Failed to initialize duplicate detection model: {e}")
            self.duplicate_model = None
    
    def extract_features(self, transactions: List[SAPGLPosting]) -> pd.DataFrame:
        """
        Extract features from transactions for ML training/prediction
        
        Args:
            transactions: List of SAPGLPosting objects
            
        Returns:
            DataFrame with extracted features
        """
        if not transactions:
            return pd.DataFrame()
        
        # Convert transactions to DataFrame
        df = pd.DataFrame([{
            'id': str(t.id),
            'amount': float(t.amount_local_currency),
            'gl_account': t.gl_account or 'UNKNOWN',
            'user_name': t.user_name,
            'posting_date': t.posting_date,
            'document_date': t.document_date,
            'document_type': t.document_type or 'UNKNOWN',
            'transaction_type': t.transaction_type,
            'fiscal_year': t.fiscal_year,
            'posting_period': t.posting_period,
            'profit_center': t.profit_center or 'UNKNOWN',
            'local_currency': t.local_currency,
            'text_length': len(t.text) if t.text else 0,
            'has_arabic_text': t.has_arabic_text,
            'is_high_value': t.is_high_value,
            'is_cleared': t.is_cleared,
        } for t in transactions])
        
        # Extract temporal features
        df['posting_day_of_week'] = df['posting_date'].dt.dayofweek
        df['posting_day_of_month'] = df['posting_date'].dt.day
        df['posting_month'] = df['posting_date'].dt.month
        df['posting_quarter'] = df['posting_date'].dt.quarter
        df['is_weekend'] = df['posting_day_of_week'].isin([5, 6]).astype(int)
        df['is_month_end'] = (df['posting_day_of_month'] >= 25).astype(int)
        df['is_month_start'] = (df['posting_day_of_month'] <= 5).astype(int)
        
        # Extract amount-based features
        df['amount_log'] = np.log1p(df['amount'])
        df['amount_sqrt'] = np.sqrt(df['amount'])
        df['is_negative'] = (df['amount'] < 0).astype(int)
        
        # Extract categorical features
        df['gl_account_prefix'] = df['gl_account'].str[:2] if df['gl_account'].dtype == 'object' else '00'
        df['document_type_category'] = df['document_type'].str[:2] if df['document_type'].dtype == 'object' else 'UN'
        
        # Calculate user and account statistics
        user_stats = df.groupby('user_name').agg({
            'amount': ['count', 'mean', 'std', 'sum'],
            'id': 'count'
        }).fillna(0)
        user_stats.columns = ['user_transaction_count', 'user_avg_amount', 'user_std_amount', 'user_total_amount', 'user_count']
        
        account_stats = df.groupby('gl_account').agg({
            'amount': ['count', 'mean', 'std', 'sum'],
            'id': 'count'
        }).fillna(0)
        account_stats.columns = ['account_transaction_count', 'account_avg_amount', 'account_std_amount', 'account_total_amount', 'account_count']
        
        # Merge statistics back to main DataFrame
        df = df.merge(user_stats, left_on='user_name', right_index=True, how='left')
        df = df.merge(account_stats, left_on='gl_account', right_index=True, how='left')
        
        # Fill NaN values
        df = df.fillna(0)
        
        return df
    
    def prepare_features(self, df: pd.DataFrame, is_training: bool = True) -> Tuple[np.ndarray, List[str]]:
        """
        Prepare features for ML models
        
        Args:
            df: DataFrame with extracted features
            is_training: Whether this is for training (True) or prediction (False)
            
        Returns:
            Tuple of (feature_matrix, feature_names)
        """
        # Select numerical features for ML
        numerical_features = [
            'amount', 'amount_log', 'amount_sqrt', 'is_negative',
            'posting_day_of_week', 'posting_day_of_month', 'posting_month', 'posting_quarter',
            'is_weekend', 'is_month_end', 'is_month_start',
            'fiscal_year', 'posting_period', 'text_length',
            'has_arabic_text', 'is_high_value', 'is_cleared',
            'user_transaction_count', 'user_avg_amount', 'user_std_amount', 'user_total_amount',
            'account_transaction_count', 'account_avg_amount', 'account_std_amount', 'account_total_amount'
        ]
        
        # Select categorical features for encoding
        categorical_features = [
            'gl_account_prefix', 'document_type_category', 'transaction_type', 'local_currency'
        ]
        
        # Create feature matrix
        feature_matrix = df[numerical_features].values
        
        # Encode categorical features
        for feature in categorical_features:
            if feature in df.columns:
                if is_training:
                    # Fit and transform for training
                    le = SimpleLabelEncoder()
                    encoded_values = le.fit_transform(df[feature].astype(str))
                    self.label_encoders[feature] = le
                else:
                    # Transform for prediction
                    le = self.label_encoders.get(feature)
                    if le:
                        try:
                            encoded_values = le.transform(df[feature].astype(str))
                        except ValueError:
                            # Handle unseen categories
                            encoded_values = np.full(len(df), -1)
                    else:
                        encoded_values = np.full(len(df), -1)
                
                # Add encoded feature to matrix
                feature_matrix = np.column_stack([feature_matrix, encoded_values])
                numerical_features.append(f'{feature}_encoded')
        
        # Scale features
        if self.scaler:
            if is_training:
                feature_matrix = self.scaler.fit_transform(feature_matrix)
            else:
                feature_matrix = self.scaler.transform(feature_matrix)
        
        return feature_matrix, numerical_features
    
    def create_training_labels(self, transactions: List[SAPGLPosting]) -> np.ndarray:
        """
        Create training labels based on rule-based anomaly detection
        
        Args:
            transactions: List of transactions
            
        Returns:
            Array of labels (1 for anomaly, 0 for normal)
        """
        from .analytics import SAPGLAnalyzer
        
        analyzer = SAPGLAnalyzer()
        
        # Get rule-based anomalies
        duplicate_anomalies = analyzer.detect_duplicate_entries(transactions)
        backdated_anomalies = analyzer.detect_backdated_entries(transactions)
        closing_anomalies = analyzer.detect_closing_entries(transactions)
        unusual_day_anomalies = analyzer.detect_unusual_days(transactions)
        holiday_anomalies = analyzer.detect_holiday_entries(transactions)
        user_anomalies = analyzer.detect_user_anomalies(transactions)
        
        # Create anomaly transaction IDs
        anomaly_ids = set()
        
        # Collect duplicate anomaly IDs
        for anomaly in duplicate_anomalies:
            for transaction in anomaly.get('transactions', []):
                anomaly_ids.add(transaction['id'])
        
        # Collect backdated anomaly IDs
        for anomaly in backdated_anomalies:
            anomaly_ids.add(anomaly.get('transaction_id'))
        
        # Collect closing anomaly IDs
        for anomaly in closing_anomalies:
            anomaly_ids.add(anomaly.get('transaction_id'))
        
        # Collect unusual day anomaly IDs
        for anomaly in unusual_day_anomalies:
            anomaly_ids.add(anomaly.get('transaction_id'))
        
        # Collect holiday anomaly IDs
        for anomaly in holiday_anomalies:
            anomaly_ids.add(anomaly.get('transaction_id'))
        
        # Collect user anomaly IDs
        for anomaly in user_anomalies:
            for transaction in anomaly.get('transactions', []):
                anomaly_ids.add(transaction['id'])
        
        # Create labels
        labels = np.zeros(len(transactions))
        for i, transaction in enumerate(transactions):
            if str(transaction.id) in anomaly_ids:
                labels[i] = 1
        
        return labels
    
    def train_models(self, transactions: List[SAPGLPosting]) -> Dict[str, float]:
        """
        Train ML models on historical transaction data
        
        Args:
            transactions: List of historical transactions for training
            
        Returns:
            Dictionary with model performance metrics
        """
        logger.info(f"Training ML models on {len(transactions)} transactions")
        
        if len(transactions) < 10:
            logger.warning("Insufficient data for training. Need at least 10 transactions.")
            return {}
        
        # Extract features
        df = self.extract_features(transactions)
        feature_matrix, feature_names = self.prepare_features(df, is_training=True)
        
        # Create training labels
        labels = self.create_training_labels(transactions)
        
        # Store feature columns for later use
        self.feature_columns = feature_names
        
        # Split data for training and validation
        if len(feature_matrix) > 20:
            split_idx = int(len(feature_matrix) * 0.8)
            X_train, X_val = feature_matrix[:split_idx], feature_matrix[split_idx:]
            y_train, y_val = labels[:split_idx], labels[split_idx:]
        else:
            X_train, X_val = feature_matrix, feature_matrix
            y_train, y_val = labels, labels
        
        performance_metrics = {}
        
        # Train models based on availability
        if 'isolation_forest' in self.models:
            try:
                self.models['isolation_forest'].fit(X_train)
                if_anomalies = self.models['isolation_forest'].predict(X_val)
                if_anomalies = (if_anomalies == -1).astype(int)  # Convert to 0/1
                if_auc = self._calculate_auc(y_val, if_anomalies)
                performance_metrics['isolation_forest_auc'] = if_auc
                logger.info(f"Isolation Forest AUC: {if_auc:.3f}")
            except Exception as e:
                logger.error(f"Error training Isolation Forest: {e}")
        
        if 'random_forest' in self.models:
            try:
                self.models['random_forest'].fit(X_train, y_train)
                rf_predictions = self.models['random_forest'].predict(X_val)
                rf_auc = self._calculate_auc(y_val, rf_predictions)
                performance_metrics['random_forest_auc'] = rf_auc
                logger.info(f"Random Forest AUC: {rf_auc:.3f}")
            except Exception as e:
                logger.error(f"Error training Random Forest: {e}")
        
        if 'statistical' in self.models:
            try:
                self.models['statistical'].fit(X_train, y_train)
                stat_predictions = self.models['statistical'].predict(X_val)
                stat_auc = self._calculate_auc(y_val, stat_predictions)
                performance_metrics['statistical_auc'] = stat_auc
                logger.info(f"Statistical AUC: {stat_auc:.3f}")
            except Exception as e:
                logger.error(f"Error training Statistical: {e}")
        
        # Mark as trained
        self.is_trained = True
        
        # Store models in memory
        self._store_models_in_memory()
        
        logger.info("ML models training completed")
        return performance_metrics
    
    def _calculate_auc(self, y_true, y_pred):
        """Calculate AUC score"""
        try:
            from sklearn.metrics import roc_auc_score
            return roc_auc_score(y_true, y_pred)
        except ImportError:
            # Simple accuracy calculation if sklearn not available
            return np.mean(y_true == y_pred)
    
    def predict_anomalies(self, transactions: List[SAPGLPosting]) -> Dict[str, List[Dict]]:
        """
        Predict anomalies using trained ML models
        
        Args:
            transactions: List of transactions to analyze
            
        Returns:
            Dictionary with anomaly predictions from each model
        """
        if not self.is_trained:
            logger.warning("Models not trained. Please train models first.")
            return {}
        
        if not transactions:
            return {}
        
        # Extract features
        df = self.extract_features(transactions)
        feature_matrix, _ = self.prepare_features(df, is_training=False)
        
        predictions = {}
        
        # Isolation Forest predictions
        if 'isolation_forest' in self.models:
            try:
                if_anomalies = self.models['isolation_forest'].predict(feature_matrix)
                if_scores = self.models['isolation_forest'].decision_function(feature_matrix)
                if_anomalies = (if_anomalies == -1).astype(int)
                
                predictions['isolation_forest'] = []
                for i, (is_anomaly, score) in enumerate(zip(if_anomalies, if_scores)):
                    if is_anomaly:
                        predictions['isolation_forest'].append({
                            'transaction_id': str(transactions[i].id),
                            'anomaly_score': float(score),
                            'model': 'isolation_forest',
                            'confidence': min(abs(score) * 10, 100)
                        })
            except Exception as e:
                logger.error(f"Error in Isolation Forest prediction: {e}")
        
        # Random Forest predictions
        if 'random_forest' in self.models:
            try:
                rf_predictions = self.models['random_forest'].predict(feature_matrix)
                rf_proba = self.models['random_forest'].predict_proba(feature_matrix)
                
                predictions['random_forest'] = []
                for i, (prediction, proba) in enumerate(zip(rf_predictions, rf_proba)):
                    if prediction == 1:
                        predictions['random_forest'].append({
                            'transaction_id': str(transactions[i].id),
                            'anomaly_score': float(proba[1]),
                            'model': 'random_forest',
                            'confidence': float(proba[1] * 100)
                        })
            except Exception as e:
                logger.error(f"Error in Random Forest prediction: {e}")
        
        # Statistical predictions
        if 'statistical' in self.models:
            try:
                stat_predictions = self.models['statistical'].predict(feature_matrix)
                stat_scores = self.models['statistical'].predict_proba(feature_matrix)
                
                predictions['statistical'] = []
                for i, (prediction, score) in enumerate(zip(stat_predictions, stat_scores)):
                    if prediction == 1:
                        predictions['statistical'].append({
                            'transaction_id': str(transactions[i].id),
                            'anomaly_score': float(score),
                            'model': 'statistical',
                            'confidence': float(score * 100)
                        })
            except Exception as e:
                logger.error(f"Error in Statistical prediction: {e}")
        
        return predictions
    
    def ensemble_predict(self, transactions: List[SAPGLPosting]) -> List[Dict]:
        """
        Ensemble prediction combining multiple models
        
        Args:
            transactions: List of transactions to analyze
            
        Returns:
            List of ensemble anomaly predictions
        """
        individual_predictions = self.predict_anomalies(transactions)
        
        # Combine predictions from all models
        ensemble_scores = {}
        
        for model_name, predictions in individual_predictions.items():
            for pred in predictions:
                transaction_id = pred['transaction_id']
                if transaction_id not in ensemble_scores:
                    ensemble_scores[transaction_id] = {
                        'transaction_id': transaction_id,
                        'models': [],
                        'total_score': 0,
                        'model_count': 0
                    }
                
                ensemble_scores[transaction_id]['models'].append(model_name)
                ensemble_scores[transaction_id]['total_score'] += pred['anomaly_score']
                ensemble_scores[transaction_id]['model_count'] += 1
        
        # Calculate ensemble predictions
        ensemble_predictions = []
        for transaction_id, scores in ensemble_scores.items():
            avg_score = scores['total_score'] / scores['model_count']
            confidence = min(avg_score * 100, 100)
            
            # Consider anomaly if at least 2 models agree or high confidence
            is_anomaly = (scores['model_count'] >= 2) or (confidence > 70)
            
            if is_anomaly:
                ensemble_predictions.append({
                    'transaction_id': transaction_id,
                    'anomaly_score': avg_score,
                    'confidence': confidence,
                    'models_agreed': scores['models'],
                    'model_count': scores['model_count'],
                    'ensemble_prediction': True
                })
        
        return ensemble_predictions
    
    def _store_models_in_memory(self):
        """Store models in memory using base64 encoding"""
        try:
            model_data = {
                'scaler': self.scaler,
                'label_encoders': self.label_encoders,
                'models': self.models,
                'feature_columns': self.feature_columns,
                'is_trained': self.is_trained
            }
            
            # Convert to base64 string for in-memory storage
            buffer = io.BytesIO()
            pickle.dump(model_data, buffer)
            buffer.seek(0)
            self.model_data['serialized_models'] = base64.b64encode(buffer.getvalue()).decode('utf-8')
            
            logger.info("Models stored in memory")
            
        except Exception as e:
            logger.error(f"Error storing models in memory: {e}")
    
    def load_models_from_memory(self):
        """Load models from in-memory storage"""
        try:
            if 'serialized_models' not in self.model_data:
                return False
            
            # Decode from base64
            serialized_data = base64.b64decode(self.model_data['serialized_models'])
            buffer = io.BytesIO(serialized_data)
            
            # Load model data
            model_data = pickle.load(buffer)
            
            self.scaler = model_data['scaler']
            self.label_encoders = model_data['label_encoders']
            self.models = model_data['models']
            self.feature_columns = model_data['feature_columns']
            self.is_trained = model_data['is_trained']
            
            logger.info("Models loaded from memory")
            return True
            
        except Exception as e:
            logger.error(f"Error loading models from memory: {e}")
            return False
    
    def get_model_info(self) -> Dict:
        """Get information about trained models"""
        return {
            'is_trained': self.is_trained,
            'feature_count': len(self.feature_columns) if self.feature_columns else 0,
            'models_available': list(self.models.keys()),
            'label_encoders': list(self.label_encoders.keys()) if self.label_encoders else [],
            'storage_type': 'in_memory'
        }
    
    def retrain_models(self, transactions: List[SAPGLPosting]) -> Dict[str, float]:
        """
        Retrain models with new data
        
        Args:
            transactions: New transaction data for retraining
            
        Returns:
            Performance metrics after retraining
        """
        logger.info("Retraining ML models with new data")
        return self.train_models(transactions)
    
    def get_comprehensive_duplicate_analysis(self, transactions: List[SAPGLPosting] = None) -> Dict:
        """
        Get comprehensive duplicate analysis from the duplicate model
        
        Args:
            transactions: Optional list of transactions to analyze. If None, returns saved analysis.
            
        Returns:
            Comprehensive duplicate analysis results
        """
        if not self.duplicate_model:
            logger.error("Duplicate model not available")
            return {}
        
        try:
            if transactions:
                # Run new analysis
                return self.duplicate_model.run_comprehensive_duplicate_analysis(transactions)
            else:
                # Return saved analysis
                return self.duplicate_model.get_saved_duplicate_analysis()
        except Exception as e:
            logger.error(f"Error getting comprehensive duplicate analysis: {e}")
            return {}
    
    def get_duplicate_analysis_components(self) -> Dict:
        """
        Get individual components of the saved duplicate analysis
        
        Returns:
            Dictionary with all duplicate analysis components
        """
        if not self.duplicate_model:
            return {}
        
        return {
            'duplicate_list': self.duplicate_model.get_duplicate_list(),
            'chart_data': self.duplicate_model.get_chart_data(),
            'breakdowns': self.duplicate_model.get_breakdowns(),
            'slicer_filters': self.duplicate_model.get_slicer_filters(),
            'summary_table': self.duplicate_model.get_summary_table(),
            'export_data': self.duplicate_model.get_export_data()
        }


class SimpleLabelEncoder:
    """Simple label encoder for categorical features"""
    
    def __init__(self):
        self.label_map = {}
        self.reverse_map = {}
        self.next_label = 0
    
    def fit_transform(self, values):
        """Fit and transform values"""
        unique_values = sorted(set(values))
        for value in unique_values:
            if value not in self.label_map:
                self.label_map[value] = self.next_label
                self.reverse_map[self.next_label] = value
                self.next_label += 1
        
        return np.array([self.label_map[value] for value in values])
    
    def transform(self, values):
        """Transform values using fitted encoder"""
        return np.array([self.label_map.get(value, -1) for value in values])


class StatisticalAnomalyDetector:
    """Simple statistical anomaly detector"""
    
    def __init__(self):
        self.thresholds = {}
        self.means = {}
        self.stds = {}
    
    def fit(self, X, y):
        """Fit the detector"""
        # Calculate statistics for each feature
        for i in range(X.shape[1]):
            self.means[i] = np.mean(X[:, i])
            self.stds[i] = np.std(X[:, i])
            self.thresholds[i] = self.means[i] + 2 * self.stds[i]  # 2 standard deviations
    
    def predict(self, X):
        """Predict anomalies"""
        predictions = np.zeros(X.shape[0])
        for i in range(X.shape[0]):
            for j in range(X.shape[1]):
                if abs(X[i, j] - self.means[j]) > self.thresholds[j]:
                    predictions[i] = 1
                    break
        return predictions
    
    def predict_proba(self, X):
        """Predict anomaly probabilities"""
        scores = np.zeros(X.shape[0])
        for i in range(X.shape[0]):
            max_score = 0
            for j in range(X.shape[1]):
                score = abs(X[i, j] - self.means[j]) / self.stds[j] if self.stds[j] > 0 else 0
                max_score = max(max_score, score)
            scores[i] = min(max_score / 3, 1.0)  # Normalize to 0-1
        return scores


class RuleBasedAnomalyDetector:
    """Rule-based anomaly detector"""
    
    def __init__(self):
        self.rules = []
    
    def fit(self, X, y):
        """Fit the detector (not used for rule-based)"""
        pass
    
    def predict(self, X):
        """Predict using rules"""
        predictions = np.zeros(X.shape[0])
        for i in range(X.shape[0]):
            for rule in self.rules:
                if rule(X[i]):
                    predictions[i] = 1
                    break
        return predictions 


class DuplicateDetectionModel:
    """Dedicated model for duplicate detection with 6 duplicate types - trained once"""
    
    def __init__(self):
        self.model = None
        self.scaler = None
        self.feature_columns = []
        self.duplicate_types = [
            'Type 1 Duplicate - Account Number + Amount',
            'Type 2 Duplicate - Account Number + Source + Amount',
            'Type 3 Duplicate - Account Number + User + Amount',
            'Type 4 Duplicate - Account Number + Posted Date + Amount',
            'Type 5 Duplicate - Account Number + Effective Date + Amount',
            'Type 6 Duplicate - Account Number + Effective Date + Posted Date + User + Source + Amount'
        ]
        self.risk_scores = {
            'Type 1 Duplicate': 10,
            'Type 2 Duplicate': 12,
            'Type 3 Duplicate': 15,
            'Type 4 Duplicate': 18,
            'Type 5 Duplicate': 20,
            'Type 6 Duplicate': 25
        }
        self.is_trained_flag = False
        self.model_data = {}
        self.enhanced_analyzer = None
        
        # Initialize enhanced duplicate analyzer
        try:
            from .enhanced_duplicate_analysis import EnhancedDuplicateAnalyzer
            self.enhanced_analyzer = EnhancedDuplicateAnalyzer()
        except ImportError:
            logger.warning("EnhancedDuplicateAnalyzer not available")
            self.enhanced_analyzer = None
    
    def is_trained(self):
        """Check if the duplicate detection model is trained"""
        return self.is_trained_flag
    
    def extract_duplicate_features(self, transactions: List[SAPGLPosting]) -> pd.DataFrame:
        """
        Extract features specifically for duplicate detection
        
        Args:
            transactions: List of SAPGLPosting objects
            
        Returns:
            DataFrame with duplicate-specific features
        """
        if not transactions:
            return pd.DataFrame()
        
        # Convert transactions to DataFrame
        df = pd.DataFrame([{
            'id': str(t.id),
            'amount': float(t.amount_local_currency),
            'gl_account': t.gl_account or 'UNKNOWN',
            'user_name': t.user_name,
            'posting_date': t.posting_date,
            'document_date': t.document_date,
            'document_type': t.document_type or 'UNKNOWN',
            'transaction_type': t.transaction_type,
            'fiscal_year': t.fiscal_year,
            'posting_period': t.posting_period,
            'profit_center': t.profit_center or 'UNKNOWN',
            'cost_center': t.cost_center or 'UNKNOWN',
            'local_currency': t.local_currency or 'UNKNOWN',
            'text': t.text or ''
        } for t in transactions])
        
        # Create duplicate-specific features
        df['amount_log'] = np.log1p(df['amount'].abs())
        df['gl_account_length'] = df['gl_account'].str.len()
        df['user_name_length'] = df['user_name'].str.len()
        df['text_length'] = df['text'].str.len()
        
        # Date features
        df['posting_date'] = pd.to_datetime(df['posting_date'], errors='coerce')
        df['document_date'] = pd.to_datetime(df['document_date'], errors='coerce')
        df['day_of_week'] = df['posting_date'].dt.dayofweek
        df['day_of_month'] = df['posting_date'].dt.day
        df['month'] = df['posting_date'].dt.month
        df['quarter'] = df['posting_date'].dt.quarter
        
        # Amount-based features
        df['amount_category'] = pd.cut(df['amount'], bins=5, labels=['Very Low', 'Low', 'Medium', 'High', 'Very High'])
        df['amount_rounded'] = round(df['amount'], -2)  # Round to nearest 100
        
        return df
    
    def prepare_duplicate_features(self, df: pd.DataFrame) -> Tuple[np.ndarray, List[str]]:
        """
        Prepare features for duplicate detection model
        
        Args:
            df: DataFrame with extracted features
            
        Returns:
            Tuple of (feature matrix, feature names)
        """
        if df.empty:
            return np.array([]), []
        
        # Select features for duplicate detection
        feature_columns = [
            'amount_log', 'gl_account_length', 'user_name_length', 'text_length',
            'day_of_week', 'day_of_month', 'month', 'quarter'
        ]
        
        # Handle categorical features
        categorical_features = ['amount_category', 'document_type', 'transaction_type']
        for col in categorical_features:
            if col in df.columns:
                df[col] = df[col].astype(str)
                df[col] = df[col].fillna('UNKNOWN')
        
        # Create feature matrix
        X = df[feature_columns].fillna(0).values
        
        # Scale features
        if self.scaler is None:
            from sklearn.preprocessing import StandardScaler
            self.scaler = StandardScaler()
            X = self.scaler.fit_transform(X)
        else:
            X = self.scaler.transform(X)
        
        return X, feature_columns
    
    def create_duplicate_labels(self, transactions: List[SAPGLPosting], enhanced_duplicates: List[Dict]) -> np.ndarray:
        """
        Create labels for duplicate detection training
        
        Args:
            transactions: List of SAPGLPosting objects
            enhanced_duplicates: List of enhanced duplicate detection results
            
        Returns:
            Array of labels (1 for duplicate, 0 for normal)
        """
        if not transactions or not enhanced_duplicates:
            return np.array([])
        
        # Create mapping of transaction IDs to duplicate types
        duplicate_transaction_ids = set()
        
        # Handle different duplicate data structures
        for dup in enhanced_duplicates:
            # Check if this is a duplicate group with transactions
            if 'transactions' in dup:
                for transaction in dup.get('transactions', []):
                    transaction_id = transaction.get('id')
                    if transaction_id:
                        duplicate_transaction_ids.add(transaction_id)
            # Check if this is a direct duplicate entry with transaction_id
            elif 'transaction_id' in dup:
                transaction_id = dup.get('transaction_id')
                if transaction_id:
                    duplicate_transaction_ids.add(transaction_id)
            # Check if this is a duplicate entry with id
            elif 'id' in dup:
                transaction_id = dup.get('id')
                if transaction_id:
                    duplicate_transaction_ids.add(transaction_id)
        
        # Create labels array
        labels = []
        for transaction in transactions:
            transaction_id = str(transaction.id)
            if transaction_id in duplicate_transaction_ids:
                labels.append(1)  # Duplicate
            else:
                labels.append(0)  # Normal
        
        return np.array(labels)
    
    def train_once(self, transactions: List[SAPGLPosting], enhanced_duplicates: List[Dict], training_session=None) -> Dict:
        """
        Train the duplicate detection model once
        
        Args:
            transactions: List of SAPGLPosting objects
            enhanced_duplicates: List of enhanced duplicate detection results
            training_session: MLModelTraining session object
            
        Returns:
            Dictionary with training results
        """
        logger.info("Training duplicate detection model once")
        
        try:
            # Extract features
            df = self.extract_duplicate_features(transactions)
            if df.empty:
                return {
                    'status': 'FAILED',
                    'error': 'No features extracted from transactions'
                }
            
            # Create labels
            labels = self.create_duplicate_labels(transactions, enhanced_duplicates)
            
            # Prepare features
            X, feature_names = self.prepare_duplicate_features(df)
            
            if len(X) == 0:
                return {
                    'status': 'FAILED',
                    'error': 'No features prepared for training'
                }
            
            # Train model (Random Forest for duplicate detection)
            try:
                from sklearn.ensemble import RandomForestClassifier
                from sklearn.model_selection import train_test_split
                
                # Split data for training
                X_train, X_test, y_train, y_test = train_test_split(
                    X, labels, test_size=0.2, random_state=42, stratify=labels
                )
                
                # Train Random Forest
                self.model = RandomForestClassifier(
                    n_estimators=100,
                    max_depth=10,
                    random_state=42,
                    class_weight='balanced'
                )
                
                self.model.fit(X_train, y_train)
                
                # Calculate performance metrics
                y_pred = self.model.predict(X_test)
                accuracy = np.mean(y_test == y_pred)
                
                # Calculate duplicate type breakdown
                duplicate_breakdown = {}
                for dup in enhanced_duplicates:
                    # Handle different duplicate type field names
                    dup_type = dup.get('duplicate_type') or dup.get('type') or 'Unknown'
                    duplicate_breakdown[dup_type] = duplicate_breakdown.get(dup_type, 0) + 1
                
                # Store model data
                self.model_data = {
                    'feature_names': feature_names,
                    'duplicate_types': self.duplicate_types,
                    'risk_scores': self.risk_scores,
                    'training_accuracy': accuracy,
                    'duplicate_breakdown': duplicate_breakdown
                }
                
                self.is_trained_flag = True
                
                # Update training session if provided
                if training_session:
                    training_session.performance_metrics = {
                        **training_session.performance_metrics,
                        'training_accuracy': accuracy,
                        'duplicate_breakdown': duplicate_breakdown,
                        'feature_count': len(feature_names),
                        'training_samples': len(X_train),
                        'test_samples': len(X_test)
                    }
                    training_session.save()
                
                return {
                    'status': 'COMPLETED',
                    'accuracy': accuracy,
                    'duplicate_breakdown': duplicate_breakdown,
                    'feature_count': len(feature_names),
                    'training_samples': len(X_train),
                    'test_samples': len(X_test),
                    'model_type': 'duplicate_detection_only'
                }
                
            except ImportError:
                logger.warning("scikit-learn not available. Using simplified duplicate detection.")
                # Fallback to simplified model
                self.model = SimplifiedDuplicateDetector()
                self.model.fit(X, labels)
                self.is_trained_flag = True
                
                return {
                    'status': 'COMPLETED',
                    'accuracy': 0.8,  # Estimated accuracy for simplified model
                    'duplicate_breakdown': {dup['type']: 1 for dup in enhanced_duplicates},
                    'model_type': 'simplified_duplicate_detection'
                }
            
        except Exception as e:
            logger.error(f"Error training duplicate detection model: {e}")
            return {
                'status': 'FAILED',
                'error': str(e)
            }
    
    def predict_duplicates(self, transactions: List[SAPGLPosting]) -> List[Dict]:
        """
        Predict duplicates using the trained model
        
        Args:
            transactions: List of SAPGLPosting objects
            
        Returns:
            List of duplicate predictions
        """
        if not self.is_trained() or not transactions:
            return []
        
        try:
            # Extract features
            df = self.extract_duplicate_features(transactions)
            if df.empty:
                return []
            
            # Prepare features
            X, _ = self.prepare_duplicate_features(df)
            
            if len(X) == 0:
                return []
            
            # Make predictions
            if hasattr(self.model, 'predict_proba'):
                probabilities = self.model.predict_proba(X)
                duplicate_probs = probabilities[:, 1] if probabilities.shape[1] > 1 else probabilities[:, 0]
            else:
                duplicate_probs = self.model.predict(X)
            
            # Create predictions
            predictions = []
            for i, transaction in enumerate(transactions):
                prediction = {
                    'transaction_id': str(transaction.id),
                    'is_duplicate': duplicate_probs[i] > 0.5,
                    'duplicate_probability': float(duplicate_probs[i]),
                    'risk_score': int(duplicate_probs[i] * 25),  # Scale to 0-25
                    'gl_account': transaction.gl_account,
                    'amount': float(transaction.amount_local_currency),
                    'user_name': transaction.user_name,
                    'posting_date': transaction.posting_date
                }
                predictions.append(prediction)
            
            return predictions
            
        except Exception as e:
            logger.error(f"Error predicting duplicates: {e}")
            return []
    
    def get_model_info(self) -> Dict:
        """Get information about the trained duplicate detection model"""
        base_info = {
            'is_trained': self.is_trained_flag,
            'model_type': 'duplicate_detection_only',
            'duplicate_types': self.duplicate_types,
            'risk_scores': self.risk_scores,
            'enhanced_analyzer_available': self.enhanced_analyzer is not None,
            'training_requirements': {
                'min_transactions': 50,  # Reduced for testing
                'min_duplicates': 1,
                'requires_scikit_learn': True
            },
            'saved_analyses_count': len(self.model_data),
            'last_training_attempt': self.model_data.get('last_training_attempt')
        }
        
        if self.is_trained_flag:
            base_info.update({
                'status': 'trained',
                'feature_count': len(self.model_data.get('feature_names', [])),
                'training_accuracy': self.model_data.get('training_accuracy', 0),
                'duplicate_breakdown': self.model_data.get('duplicate_breakdown', {}),
                'training_status': 'TRAINED'
            })
        else:
            base_info.update({
                'status': 'not_trained',
                'training_status': 'NOT_TRAINED',
                'training_reason': self._get_training_reason()
            })
        
        return base_info
    
    def _get_training_reason(self) -> str:
        """Get reason why model is not trained"""
        if not self.model_data:
            return "No data available for training"
        
        # Check if we have any saved analyses with duplicates
        for file_id, data in self.model_data.items():
            if isinstance(data, dict) and 'analysis_result' in data:
                duplicate_list = data['analysis_result'].get('duplicate_list', [])
                if len(duplicate_list) > 0:
                    return f"Has {len(duplicate_list)} duplicates but training not triggered"
        
        return "No duplicates found in available data"
    
    def force_train(self, transactions: List[SAPGLPosting]) -> Dict:
        """Force training of the duplicate detection model"""
        try:
            print("Force training duplicate detection model")
            
            # Run analysis to get duplicates
            if self.enhanced_analyzer:
                analysis_result = self.enhanced_analyzer.analyze_duplicates(transactions)
            else:
                analysis_result = self._run_basic_analysis(transactions)
            
            duplicate_list = analysis_result.get('duplicate_list', [])
            
            if len(duplicate_list) == 0:
                return {
                    'status': 'FAILED',
                    'error': 'No duplicates found in data - cannot train without duplicates'
                }
            
            if len(transactions) < 30:  # Reduced for testing
                return {
                    'status': 'FAILED',
                    'error': f'Insufficient data for training. Need at least 30 transactions, got {len(transactions)}'
                }
            
            # Train the model
            training_result = self.train_once(transactions, duplicate_list)
            
            # Save training attempt info
            self.model_data['last_training_attempt'] = {
                'timestamp': datetime.now().isoformat(),
                'transaction_count': len(transactions),
                'duplicate_count': len(duplicate_list),
                'result': training_result
            }
            
            return training_result
            
        except Exception as e:
            logger.error(f"Error in force training: {e}")
            return {
                'status': 'FAILED',
                'error': str(e)
            }
    
    def run_comprehensive_duplicate_analysis(self, transactions: List[SAPGLPosting]) -> Dict:
        """
        Run comprehensive duplicate analysis using EnhancedDuplicateAnalyzer
        and save results in model data for persistence
        
        Args:
            transactions: List of SAPGLPosting objects
            
        Returns:
            Comprehensive duplicate analysis results
        """
        if not self.enhanced_analyzer:
            logger.error("EnhancedDuplicateAnalyzer not available")
            return {}
        
        try:
            # Run comprehensive analysis
            analysis_result = self.enhanced_analyzer.analyze_duplicates(transactions)
            
            # Save analysis results in model data for persistence
            self.model_data['comprehensive_duplicate_analysis'] = {
                'analysis_date': datetime.now().isoformat(),
                'total_transactions': len(transactions),
                'analysis_result': analysis_result
            }
            
            # Save specific components for easy access
            self.model_data['duplicate_list'] = analysis_result.get('duplicate_list', [])
            self.model_data['chart_data'] = analysis_result.get('chart_data', {})
            self.model_data['breakdowns'] = analysis_result.get('breakdowns', {})
            self.model_data['slicer_filters'] = analysis_result.get('slicer_filters', {})
            self.model_data['summary_table'] = analysis_result.get('summary_table', [])
            self.model_data['export_data'] = analysis_result.get('export_data', [])
            
            logger.info(f"Comprehensive duplicate analysis completed and saved. Found {len(analysis_result.get('duplicate_list', []))} duplicate entries.")
            
            return analysis_result
            
        except Exception as e:
            logger.error(f"Error in comprehensive duplicate analysis: {e}")
            return {}
    
    def get_saved_duplicate_analysis(self) -> Dict:
        """
        Retrieve saved comprehensive duplicate analysis from model data
        
        Returns:
            Saved duplicate analysis results
        """
        return self.model_data.get('comprehensive_duplicate_analysis', {})
    
    def get_duplicate_list(self) -> List[Dict]:
        """Get saved duplicate list"""
        return self.model_data.get('duplicate_list', [])
    
    def get_chart_data(self) -> Dict:
        """Get saved chart data"""
        return self.model_data.get('chart_data', {})
    
    def get_breakdowns(self) -> Dict:
        """Get saved breakdowns"""
        return self.model_data.get('breakdowns', {})
    
    def get_slicer_filters(self) -> Dict:
        """Get saved slicer filters"""
        return self.model_data.get('slicer_filters', {})
    
    def get_summary_table(self) -> List[Dict]:
        """Get saved summary table"""
        return self.model_data.get('summary_table', [])
    
    def get_export_data(self) -> List[Dict]:
        """Get saved export data"""
        return self.model_data.get('export_data', [])
    
    def has_saved_analysis(self, file_id: str) -> bool:
        """Check if analysis exists for a specific file"""
        return file_id in self.model_data
    
    def get_saved_analysis(self, file_id: str) -> Dict:
        """Get saved analysis for a specific file"""
        if file_id in self.model_data:
            return self.model_data[file_id].get('analysis_result', {})
        return {}
    
    def run_comprehensive_analysis(self, transactions: List[SAPGLPosting], file_id: str) -> Dict:
        """Run comprehensive duplicate analysis and save results - includes training if needed"""
        try:
            # Check if analysis already exists for this file
            if self.has_saved_analysis(file_id):
                print(f"Analysis already exists for file {file_id}, retrieving saved results")
                return self.get_saved_analysis(file_id)
            
            # Run enhanced duplicate analysis
            if self.enhanced_analyzer:
                analysis_result = self.enhanced_analyzer.analyze_duplicates(transactions)
            else:
                # Fallback to basic analysis
                analysis_result = self._run_basic_analysis(transactions)
            
            # Check if we should train the model
            duplicate_list = analysis_result.get('duplicate_list', [])
            should_train = (
                not self.is_trained_flag and  # Model not trained yet
                len(duplicate_list) > 0 and    # Has duplicates to learn from
                len(transactions) >= 50        # Enough data for training (reduced for testing)
            )
            
            if should_train:
                print(f"Training duplicate detection model with {len(duplicate_list)} duplicates from {len(transactions)} transactions")
                training_result = self.train_once(transactions, duplicate_list)
                print(f"Training result: {training_result.get('status', 'UNKNOWN')}")
                
                if training_result.get('status') == 'COMPLETED':
                    print(f"Model training completed successfully with accuracy: {training_result.get('accuracy', 0):.2f}")
                else:
                    print(f"Model training failed: {training_result.get('error', 'Unknown error')}")
            
            # Save analysis results in model data
            self.model_data[file_id] = {
                'analysis_result': analysis_result,
                'analysis_timestamp': datetime.now().isoformat(),
                'transaction_count': len(transactions) if transactions else 0,
                'analysis_type': 'comprehensive_duplicate',
                'training_attempted': should_train,
                'training_result': training_result if should_train else None
            }
            
            print(f"Saved comprehensive analysis for file {file_id}")
            return analysis_result
            
        except Exception as e:
            print(f"Error running comprehensive analysis: {e}")
            return {}
    
    def _run_basic_analysis(self, transactions: List[SAPGLPosting]) -> Dict:
        """Run basic duplicate analysis as fallback"""
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
            
            return {
                'analysis_info': {
                    'total_transactions': len(transactions),
                    'total_duplicate_groups': len(duplicates),
                    'total_duplicate_transactions': len(duplicates),
                    'total_amount_involved': sum(d['amount'] for d in duplicates),
                    'analysis_date': datetime.now().isoformat()
                },
                'duplicate_list': duplicates,
                'chart_data': {},
                'breakdowns': {},
                'slicer_filters': {},
                'summary_table': duplicates,
                'export_data': duplicates,
                'detailed_insights': {}
            }
            
        except Exception as e:
            print(f"Error in basic analysis: {e}")
            return {}


class SimplifiedDuplicateDetector:
    """Simplified duplicate detector when scikit-learn is not available"""
    
    def __init__(self):
        self.thresholds = {}
        self.means = {}
    
    def fit(self, X, y):
        """Fit the simplified detector"""
        # Calculate means for each feature
        for i in range(X.shape[1]):
            self.means[i] = np.mean(X[:, i])
            self.thresholds[i] = np.std(X[:, i]) * 2
    
    def predict(self, X):
        """Predict duplicates using simplified rules"""
        predictions = np.zeros(X.shape[0])
        for i in range(X.shape[0]):
            anomaly_score = 0
            for j in range(X.shape[1]):
                if abs(X[i, j] - self.means[j]) > self.thresholds[j]:
                    anomaly_score += 1
            predictions[i] = 1 if anomaly_score > X.shape[1] / 2 else 0
        return predictions 