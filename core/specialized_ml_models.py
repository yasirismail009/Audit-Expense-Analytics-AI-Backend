"""
Specialized Machine Learning Models for Each Anomaly Type

This module provides specific ML models tailored for each type of anomaly detection:
- Duplicate Detection Model
- Backdated Entry Model  
- User Anomaly Model
- Closing Entry Model
- Unusual Days Model
- Holiday Entry Model
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
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import holidays

warnings.filterwarnings('ignore')

from .models import SAPGLPosting

logger = logging.getLogger(__name__)

class SpecializedAnomalyDetector:
    """Specialized ML models for each anomaly type with in-memory storage"""
    
    def __init__(self):
        self.models = {}
        self.scalers = {}
        self.label_encoders = {}
        self.feature_columns = {}
        self.is_trained = {}
        self.model_data = {}  # In-memory storage
        
        # Initialize specialized models for each anomaly type
        self._initialize_specialized_models()
    
    def _initialize_specialized_models(self):
        """Initialize specific models for each anomaly type"""
        try:
            # Duplicate Detection Model
            self.models['duplicate'] = {
                'isolation_forest': IsolationForest(contamination=0.05, random_state=42),
                'random_forest': RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')
            }
            
            # Backdated Entry Model
            self.models['backdated'] = {
                'isolation_forest': IsolationForest(contamination=0.1, random_state=42),
                'random_forest': RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')
            }
            
            # User Anomaly Model
            self.models['user_anomalies'] = {
                'isolation_forest': IsolationForest(contamination=0.08, random_state=42),
                'random_forest': RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')
            }
            
            # Closing Entry Model
            self.models['closing'] = {
                'isolation_forest': IsolationForest(contamination=0.03, random_state=42),
                'random_forest': RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')
            }
            
            # Unusual Days Model
            self.models['unusual_days'] = {
                'isolation_forest': IsolationForest(contamination=0.15, random_state=42),
                'random_forest': RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')
            }
            
            # Holiday Entry Model
            self.models['holiday'] = {
                'isolation_forest': IsolationForest(contamination=0.02, random_state=42),
                'random_forest': RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')
            }
            
            # Initialize scalers and encoders for each model
            for anomaly_type in self.models.keys():
                self.scalers[anomaly_type] = StandardScaler()
                self.label_encoders[anomaly_type] = {}
                self.is_trained[anomaly_type] = False
                
        except ImportError:
            logger.warning("scikit-learn not available. Using simplified models.")
            self._initialize_simple_specialized_models()
    
    def _initialize_simple_specialized_models(self):
        """Initialize simplified specialized models"""
        self.models = {
            'duplicate': {'statistical': StatisticalDuplicateDetector()},
            'backdated': {'statistical': StatisticalBackdatedDetector()},
            'user_anomalies': {'statistical': StatisticalUserAnomalyDetector()},
            'closing': {'statistical': StatisticalClosingDetector()},
            'unusual_days': {'statistical': StatisticalUnusualDaysDetector()},
            'holiday': {'statistical': StatisticalHolidayDetector()}
        }
        
        for anomaly_type in self.models.keys():
            self.is_trained[anomaly_type] = False
    
    def extract_duplicate_features(self, transactions: List[SAPGLPosting]) -> pd.DataFrame:
        """Extract features specific to duplicate detection"""
        if not transactions:
            return pd.DataFrame()
        
        df = pd.DataFrame([{
            'amount': float(t.amount_local_currency),
            'gl_account': t.gl_account or 'UNKNOWN',
            'user_name': t.user_name,
            'posting_date': t.posting_date,
            'document_type': t.document_type or 'UNKNOWN',
            'transaction_type': t.transaction_type,
            'fiscal_year': t.fiscal_year,
            'posting_period': t.posting_period,
            'profit_center': t.profit_center or 'UNKNOWN',
            'cost_center': t.cost_center or 'UNKNOWN',
            'amount_rounded': round(float(t.amount_local_currency), -3),  # Round to thousands
            'amount_mod_1000': float(t.amount_local_currency) % 1000,
            'amount_mod_100': float(t.amount_local_currency) % 100,
            'day_of_week': t.posting_date.weekday() if t.posting_date else 0,
            'day_of_month': t.posting_date.day if t.posting_date else 1,
            'month': t.posting_date.month if t.posting_date else 1,
            'is_month_end': 1 if t.posting_date and t.posting_date.day >= 25 else 0,
            'is_quarter_end': 1 if t.posting_date and t.posting_date.month in [3, 6, 9, 12] and t.posting_date.day >= 25 else 0,
            'is_year_end': 1 if t.posting_date and t.posting_date.month == 12 and t.posting_date.day >= 25 else 0,
            'amount_log': np.log1p(abs(float(t.amount_local_currency))),
            'amount_sqrt': np.sqrt(abs(float(t.amount_local_currency))),
            'amount_reciprocal': 1 / (abs(float(t.amount_local_currency)) + 1),
        } for t in transactions])
        
        return df
    
    def extract_backdated_features(self, transactions: List[SAPGLPosting]) -> pd.DataFrame:
        """Extract enhanced features specific to backdated entry detection"""
        if not transactions:
            return pd.DataFrame()
        
        current_date = datetime.now().date()
        
        # Calculate user statistics for pattern analysis
        user_stats = {}
        account_stats = {}
        
        for t in transactions:
            # User statistics
            if t.user_name not in user_stats:
                user_stats[t.user_name] = {
                    'total_transactions': 0,
                    'total_amount': 0,
                    'backdated_count': 0,
                    'backdated_amount': 0,
                    'accounts_used': set(),
                    'avg_days_diff': 0,
                    'max_days_diff': 0
                }
            
            user_stats[t.user_name]['total_transactions'] += 1
            user_stats[t.user_name]['total_amount'] += float(t.amount_local_currency)
            user_stats[t.user_name]['accounts_used'].add(t.gl_account or 'UNKNOWN')
            
            # Account statistics
            if t.gl_account not in account_stats:
                account_stats[t.gl_account] = {
                    'total_transactions': 0,
                    'total_amount': 0,
                    'backdated_count': 0,
                    'backdated_amount': 0,
                    'users': set()
                }
            
            account_stats[t.gl_account]['total_transactions'] += 1
            account_stats[t.gl_account]['total_amount'] += float(t.amount_local_currency)
            account_stats[t.gl_account]['users'].add(t.user_name)
        
        # Calculate backdated statistics
        for t in transactions:
            if t.posting_date and t.document_date and t.posting_date > t.document_date:
                days_diff = (t.posting_date - t.document_date).days
                user_stats[t.user_name]['backdated_count'] += 1
                user_stats[t.user_name]['backdated_amount'] += float(t.amount_local_currency)
                user_stats[t.user_name]['avg_days_diff'] += days_diff
                user_stats[t.user_name]['max_days_diff'] = max(user_stats[t.user_name]['max_days_diff'], days_diff)
                
                account_stats[t.gl_account]['backdated_count'] += 1
                account_stats[t.gl_account]['backdated_amount'] += float(t.amount_local_currency)
        
        # Calculate averages
        for user in user_stats:
            if user_stats[user]['backdated_count'] > 0:
                user_stats[user]['avg_days_diff'] /= user_stats[user]['backdated_count']
            user_stats[user]['accounts_count'] = len(user_stats[user]['accounts_used'])
            user_stats[user]['backdated_rate'] = user_stats[user]['backdated_count'] / user_stats[user]['total_transactions']
        
        for account in account_stats:
            account_stats[account]['users_count'] = len(account_stats[account]['users'])
            account_stats[account]['backdated_rate'] = account_stats[account]['backdated_count'] / account_stats[account]['total_transactions']
        
        df = pd.DataFrame([{
            # Basic transaction features
            'amount': float(t.amount_local_currency),
            'amount_log': np.log1p(abs(float(t.amount_local_currency))),
            'amount_rounded': round(float(t.amount_local_currency), -3),
            'amount_mod_1000': float(t.amount_local_currency) % 1000,
            'is_credit': 1 if t.transaction_type == 'CREDIT' else 0,
            
            # Date features
            'posting_date': t.posting_date,
            'document_date': t.document_date,
            'entry_date': t.entry_date,
            'days_diff_posting_document': (t.posting_date - t.document_date).days if t.posting_date and t.document_date else 0,
            'days_diff_entry_posting': (t.entry_date - t.posting_date).days if t.entry_date and t.posting_date else 0,
            'days_diff_current_posting': (current_date - t.posting_date).days if t.posting_date else 0,
            
            # Backdated indicators
            'is_backdated': 1 if t.posting_date and t.document_date and t.posting_date > t.document_date else 0,
            'is_extreme_backdated': 1 if t.posting_date and t.document_date and (t.posting_date - t.document_date).days > 90 else 0,
            'is_significant_backdated': 1 if t.posting_date and t.document_date and 30 < (t.posting_date - t.document_date).days <= 90 else 0,
            'is_moderate_backdated': 1 if t.posting_date and t.document_date and 7 < (t.posting_date - t.document_date).days <= 30 else 0,
            
            # Temporal features
            'day_of_week': t.posting_date.weekday() if t.posting_date else 0,
            'month': t.posting_date.month if t.posting_date else 1,
            'quarter': ((t.posting_date.month - 1) // 3) + 1 if t.posting_date else 1,
            'is_month_end': 1 if t.posting_date and t.posting_date.day >= 25 else 0,
            'is_quarter_end': 1 if t.posting_date and t.posting_date.month in [3, 6, 9, 12] and t.posting_date.day >= 25 else 0,
            'is_year_end': 1 if t.posting_date and t.posting_date.month == 12 and t.posting_date.day >= 25 else 0,
            'is_weekend': 1 if t.posting_date and t.posting_date.weekday() >= 5 else 0,
            
            # Account features
            'gl_account': t.gl_account or 'UNKNOWN',
            'account_first_digit': int(t.gl_account[0]) if t.gl_account and t.gl_account[0].isdigit() else 0,
            'account_type': self._get_account_type_code(t.gl_account),
            'is_sensitive_account': 1 if t.gl_account in ['1000', '1100', '1200', '1300', '2000', '2100', '3000', '4000', '5000'] else 0,
            
            # User features
            'user_name': t.user_name,
            'user_total_transactions': user_stats[t.user_name]['total_transactions'],
            'user_total_amount': user_stats[t.user_name]['total_amount'],
            'user_backdated_count': user_stats[t.user_name]['backdated_count'],
            'user_backdated_amount': user_stats[t.user_name]['backdated_amount'],
            'user_backdated_rate': user_stats[t.user_name]['backdated_rate'],
            'user_avg_days_diff': user_stats[t.user_name]['avg_days_diff'],
            'user_max_days_diff': user_stats[t.user_name]['max_days_diff'],
            'user_accounts_count': user_stats[t.user_name]['accounts_count'],
            
            # Account pattern features
            'account_total_transactions': account_stats[t.gl_account]['total_transactions'],
            'account_total_amount': account_stats[t.gl_account]['total_amount'],
            'account_backdated_count': account_stats[t.gl_account]['backdated_count'],
            'account_backdated_amount': account_stats[t.gl_account]['backdated_amount'],
            'account_backdated_rate': account_stats[t.gl_account]['backdated_rate'],
            'account_users_count': account_stats[t.gl_account]['users_count'],
            
            # Document features
            'document_type': t.document_type or 'UNKNOWN',
            'is_manual_entry': 1 if t.document_type in ['SA', 'AB'] else 0,
            'has_text': 1 if t.text and len(t.text.strip()) > 0 else 0,
            'text_length': len(t.text) if t.text else 0,
            
            # Organizational features
            'profit_center': t.profit_center or 'UNKNOWN',
            'cost_center': t.cost_center or 'UNKNOWN',
            'segment': t.segment or 'UNKNOWN',
            
            # Amount-based features
            'is_high_value': 1 if float(t.amount_local_currency) > 1000000 else 0,
            'is_medium_value': 1 if 100000 < float(t.amount_local_currency) <= 1000000 else 0,
            'is_low_value': 1 if float(t.amount_local_currency) <= 100000 else 0,
            
            # Pattern features
            'amount_vs_user_avg': float(t.amount_local_currency) / (user_stats[t.user_name]['total_amount'] / user_stats[t.user_name]['total_transactions'] + 1),
            'amount_vs_account_avg': float(t.amount_local_currency) / (account_stats[t.gl_account]['total_amount'] / account_stats[t.gl_account]['total_transactions'] + 1),
            
            # Risk indicators
            'user_risk_indicator': user_stats[t.user_name]['backdated_rate'] * 100,
            'account_risk_indicator': account_stats[t.gl_account]['backdated_rate'] * 100,
            'timing_risk_indicator': 1 if t.posting_date and t.posting_date.day >= 25 else 0,
            'amount_risk_indicator': 1 if float(t.amount_local_currency) > 100000 else 0,
            
        } for t in transactions])
        
        return df
    
    def _get_account_type_code(self, account_id):
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
    
    def extract_user_anomaly_features(self, transactions: List[SAPGLPosting]) -> pd.DataFrame:
        """Extract features specific to user anomaly detection"""
        if not transactions:
            return pd.DataFrame()
        
        # Calculate user statistics
        user_stats = {}
        for t in transactions:
            user = t.user_name
            if user not in user_stats:
                user_stats[user] = {
                    'count': 0,
                    'total_amount': 0,
                    'accounts_used': set(),
                    'avg_amount': 0,
                    'max_amount': 0,
                    'min_amount': float('inf')
                }
            
            amount = float(t.amount_local_currency)
            user_stats[user]['count'] += 1
            user_stats[user]['total_amount'] += amount
            user_stats[user]['accounts_used'].add(t.gl_account or 'UNKNOWN')
            user_stats[user]['max_amount'] = max(user_stats[user]['max_amount'], amount)
            user_stats[user]['min_amount'] = min(user_stats[user]['min_amount'], amount)
        
        # Calculate averages
        for user in user_stats:
            user_stats[user]['avg_amount'] = user_stats[user]['total_amount'] / user_stats[user]['count']
            user_stats[user]['accounts_count'] = len(user_stats[user]['accounts_used'])
        
        df = pd.DataFrame([{
            'amount': float(t.amount_local_currency),
            'gl_account': t.gl_account or 'UNKNOWN',
            'user_name': t.user_name,
            'posting_date': t.posting_date,
            'user_total_transactions': user_stats[t.user_name]['count'],
            'user_total_amount': user_stats[t.user_name]['total_amount'],
            'user_avg_amount': user_stats[t.user_name]['avg_amount'],
            'user_max_amount': user_stats[t.user_name]['max_amount'],
            'user_min_amount': user_stats[t.user_name]['min_amount'],
            'user_accounts_count': user_stats[t.user_name]['accounts_count'],
            'amount_vs_user_avg': float(t.amount_local_currency) / (user_stats[t.user_name]['avg_amount'] + 1),
            'amount_vs_user_max': float(t.amount_local_currency) / (user_stats[t.user_name]['max_amount'] + 1),
            'day_of_week': t.posting_date.weekday() if t.posting_date else 0,
            'month': t.posting_date.month if t.posting_date else 1,
            'is_weekend': 1 if t.posting_date and t.posting_date.weekday() >= 5 else 0,
            'is_month_end': 1 if t.posting_date and t.posting_date.day >= 25 else 0,
            'amount_log': np.log1p(abs(float(t.amount_local_currency))),
            'amount_rounded': round(float(t.amount_local_currency), -3),
            'amount_mod_1000': float(t.amount_local_currency) % 1000,
        } for t in transactions])
        
        return df
    
    def extract_closing_features(self, transactions: List[SAPGLPosting]) -> pd.DataFrame:
        """Extract features specific to closing entry detection"""
        if not transactions:
            return pd.DataFrame()
        
        df = pd.DataFrame([{
            'amount': float(t.amount_local_currency),
            'gl_account': t.gl_account or 'UNKNOWN',
            'user_name': t.user_name,
            'posting_date': t.posting_date,
            'document_type': t.document_type or 'UNKNOWN',
            'day_of_month': t.posting_date.day if t.posting_date else 1,
            'days_from_month_end': 32 - t.posting_date.day if t.posting_date else 0,
            'is_last_3_days': 1 if t.posting_date and t.posting_date.day >= 28 else 0,
            'is_last_5_days': 1 if t.posting_date and t.posting_date.day >= 26 else 0,
            'is_last_week': 1 if t.posting_date and t.posting_date.day >= 24 else 0,
            'month': t.posting_date.month if t.posting_date else 1,
            'quarter': ((t.posting_date.month - 1) // 3) + 1 if t.posting_date else 1,
            'is_quarter_end': 1 if t.posting_date and t.posting_date.month in [3, 6, 9, 12] else 0,
            'is_year_end': 1 if t.posting_date and t.posting_date.month == 12 else 0,
            'amount_log': np.log1p(abs(float(t.amount_local_currency))),
            'amount_rounded': round(float(t.amount_local_currency), -3),
            'amount_mod_1000': float(t.amount_local_currency) % 1000,
            'is_round_amount': 1 if float(t.amount_local_currency) % 1000 == 0 else 0,
            'is_high_value': 1 if abs(float(t.amount_local_currency)) > 5000000 else 0,
        } for t in transactions])
        
        return df
    
    def extract_unusual_days_features(self, transactions: List[SAPGLPosting]) -> pd.DataFrame:
        """Extract features specific to unusual days detection"""
        if not transactions:
            return pd.DataFrame()
        
        df = pd.DataFrame([{
            'amount': float(t.amount_local_currency),
            'gl_account': t.gl_account or 'UNKNOWN',
            'user_name': t.user_name,
            'posting_date': t.posting_date,
            'day_of_week': t.posting_date.weekday() if t.posting_date else 0,
            'is_weekend': 1 if t.posting_date and t.posting_date.weekday() >= 5 else 0,
            'is_saturday': 1 if t.posting_date and t.posting_date.weekday() == 5 else 0,
            'is_sunday': 1 if t.posting_date and t.posting_date.weekday() == 6 else 0,
            'is_monday': 1 if t.posting_date and t.posting_date.weekday() == 0 else 0,
            'is_friday': 1 if t.posting_date and t.posting_date.weekday() == 4 else 0,
            'day_of_month': t.posting_date.day if t.posting_date else 1,
            'month': t.posting_date.month if t.posting_date else 1,
            'is_month_end': 1 if t.posting_date and t.posting_date.day >= 25 else 0,
            'is_month_start': 1 if t.posting_date and t.posting_date.day <= 5 else 0,
            'amount_log': np.log1p(abs(float(t.amount_local_currency))),
            'amount_rounded': round(float(t.amount_local_currency), -3),
            'is_high_value': 1 if abs(float(t.amount_local_currency)) > 500000 else 0,
            'is_urgent_amount': 1 if abs(float(t.amount_local_currency)) > 1000000 else 0,
        } for t in transactions])
        
        return df
    
    def extract_holiday_features(self, transactions: List[SAPGLPosting]) -> pd.DataFrame:
        """Extract features specific to holiday entry detection"""
        if not transactions:
            return pd.DataFrame()
        
        # Saudi Arabia holidays
        sa_holidays = holidays.SaudiArabia()
        
        df = pd.DataFrame([{
            'amount': float(t.amount_local_currency),
            'gl_account': t.gl_account or 'UNKNOWN',
            'user_name': t.user_name,
            'posting_date': t.posting_date,
            'is_holiday': 1 if t.posting_date and t.posting_date in sa_holidays else 0,
            'holiday_name': sa_holidays.get(t.posting_date, 'Not Holiday') if t.posting_date else 'Not Holiday',
            'day_of_week': t.posting_date.weekday() if t.posting_date else 0,
            'is_weekend': 1 if t.posting_date and t.posting_date.weekday() >= 5 else 0,
            'month': t.posting_date.month if t.posting_date else 1,
            'day_of_month': t.posting_date.day if t.posting_date else 1,
            'amount_log': np.log1p(abs(float(t.amount_local_currency))),
            'amount_rounded': round(float(t.amount_local_currency), -3),
            'is_high_value': 1 if abs(float(t.amount_local_currency)) > 5000000 else 0,
            'is_urgent_amount': 1 if abs(float(t.amount_local_currency)) > 10000000 else 0,
            'is_round_amount': 1 if float(t.amount_local_currency) % 1000 == 0 else 0,
        } for t in transactions])
        
        return df
    
    def prepare_features(self, df: pd.DataFrame, anomaly_type: str, is_training: bool = True) -> Tuple[np.ndarray, List[str]]:
        """Prepare features for specific anomaly type"""
        if df.empty:
            return np.array([]), []
        
        # Select numeric columns
        numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
        
        # Handle categorical columns
        categorical_columns = ['gl_account', 'user_name', 'document_type', 'profit_center', 'cost_center']
        if 'holiday_name' in df.columns:
            categorical_columns.append('holiday_name')
        
        feature_matrix = df[numeric_columns].copy()
        
        # Encode categorical variables
        for col in categorical_columns:
            if col in df.columns:
                if is_training:
                    le = LabelEncoder()
                    encoded_values = le.fit_transform(df[col].fillna('UNKNOWN'))
                    self.label_encoders[anomaly_type][col] = le
                else:
                    le = self.label_encoders[anomaly_type].get(col)
                    if le:
                        encoded_values = le.transform(df[col].fillna('UNKNOWN'))
                    else:
                        encoded_values = np.zeros(len(df))
                
                feature_matrix[f'{col}_encoded'] = encoded_values
        
        # Scale features
        if is_training:
            scaled_features = self.scalers[anomaly_type].fit_transform(feature_matrix)
        else:
            scaled_features = self.scalers[anomaly_type].transform(feature_matrix)
        
        return scaled_features, feature_matrix.columns.tolist()
    
    def create_training_labels(self, transactions: List[SAPGLPosting], anomaly_type: str) -> np.ndarray:
        """Create training labels for specific anomaly type"""
        if not transactions:
            return np.array([])
        
        # Use rule-based detection to create labels
        from .analytics import SAPGLAnalyzer
        analyzer = SAPGLAnalyzer()
        
        if anomaly_type == 'duplicate':
            anomalies = analyzer.detect_duplicate_entries(transactions)
            anomaly_ids = set()
            for anomaly in anomalies:
                for transaction in anomaly.get('transactions', []):
                    anomaly_ids.add(transaction['id'])
        elif anomaly_type == 'backdated':
            anomalies = analyzer.detect_backdated_entries(transactions)
            anomaly_ids = set()
            for anomaly in anomalies:
                for transaction in anomaly.get('transactions', []):
                    anomaly_ids.add(transaction['id'])
        elif anomaly_type == 'user_anomalies':
            anomalies = analyzer.detect_user_anomalies(transactions)
            anomaly_ids = set()
            for anomaly in anomalies:
                for transaction in anomaly.get('transactions', []):
                    anomaly_ids.add(transaction['id'])
        elif anomaly_type == 'closing':
            anomalies = analyzer.detect_closing_entries(transactions)
            anomaly_ids = set()
            for anomaly in anomalies:
                for transaction in anomaly.get('transactions', []):
                    anomaly_ids.add(transaction['id'])
        elif anomaly_type == 'unusual_days':
            anomalies = analyzer.detect_unusual_days(transactions)
            anomaly_ids = set()
            for anomaly in anomalies:
                for transaction in anomaly.get('transactions', []):
                    anomaly_ids.add(transaction['id'])
        elif anomaly_type == 'holiday':
            anomalies = analyzer.detect_holiday_entries(transactions)
            anomaly_ids = set()
            for anomaly in anomalies:
                for transaction in anomaly.get('transactions', []):
                    anomaly_ids.add(transaction['id'])
        else:
            return np.zeros(len(transactions))
        
        # Create labels
        labels = np.zeros(len(transactions))
        for i, transaction in enumerate(transactions):
            if str(transaction.id) in anomaly_ids:
                labels[i] = 1
        
        return labels
    
    def train_specialized_models(self, transactions: List[SAPGLPosting]) -> Dict[str, Dict[str, float]]:
        """Train specialized models for each anomaly type"""
        if not transactions:
            return {}
        
        results = {}
        
        # Train models for each anomaly type
        anomaly_types = ['duplicate', 'backdated', 'user_anomalies', 'closing', 'unusual_days', 'holiday']
        
        for anomaly_type in anomaly_types:
            try:
                logger.info(f"Training specialized model for {anomaly_type}")
                
                # Extract features for this anomaly type
                if anomaly_type == 'duplicate':
                    df = self.extract_duplicate_features(transactions)
                elif anomaly_type == 'backdated':
                    df = self.extract_backdated_features(transactions)
                elif anomaly_type == 'user_anomalies':
                    df = self.extract_user_anomaly_features(transactions)
                elif anomaly_type == 'closing':
                    df = self.extract_closing_features(transactions)
                elif anomaly_type == 'unusual_days':
                    df = self.extract_unusual_days_features(transactions)
                elif anomaly_type == 'holiday':
                    df = self.extract_holiday_features(transactions)
                else:
                    continue
                
                if df.empty:
                    logger.warning(f"No features extracted for {anomaly_type}")
                    continue
                
                # Prepare features
                feature_matrix, feature_columns = self.prepare_features(df, anomaly_type, is_training=True)
                self.feature_columns[anomaly_type] = feature_columns
                
                if feature_matrix.size == 0:
                    logger.warning(f"No features prepared for {anomaly_type}")
                    continue
                
                # Create labels
                labels = self.create_training_labels(transactions, anomaly_type)
                
                if len(np.unique(labels)) < 2:
                    logger.warning(f"Insufficient labels for {anomaly_type}")
                    continue
                
                # Split data
                X_train, X_test, y_train, y_test = train_test_split(
                    feature_matrix, labels, test_size=0.2, random_state=42, stratify=labels
                )
                
                # Train models
                model_results = {}
                
                for model_name, model in self.models[anomaly_type].items():
                    try:
                        if hasattr(model, 'fit'):
                            model.fit(X_train, y_train)
                            
                            # Predictions
                            if hasattr(model, 'predict_proba'):
                                y_pred_proba = model.predict_proba(X_test)[:, 1]
                                y_pred = (y_pred_proba > 0.5).astype(int)
                            else:
                                y_pred = model.predict(X_test)
                                y_pred_proba = y_pred
                            
                            # Metrics
                            accuracy = accuracy_score(y_test, y_pred)
                            precision = precision_score(y_test, y_pred, zero_division=0)
                            recall = recall_score(y_test, y_pred, zero_division=0)
                            f1 = f1_score(y_test, y_pred, zero_division=0)
                            
                            model_results[model_name] = {
                                'accuracy': accuracy,
                                'precision': precision,
                                'recall': recall,
                                'f1_score': f1
                            }
                            
                            logger.info(f"{anomaly_type} - {model_name}: Accuracy={accuracy:.3f}, F1={f1:.3f}")
                    
                    except Exception as e:
                        logger.error(f"Error training {model_name} for {anomaly_type}: {e}")
                        model_results[model_name] = {'error': str(e)}
                
                results[anomaly_type] = model_results
                self.is_trained[anomaly_type] = True
                
            except Exception as e:
                logger.error(f"Error training specialized model for {anomaly_type}: {e}")
                results[anomaly_type] = {'error': str(e)}
        
        # Store models in memory
        self._store_models_in_memory()
        
        return results
    
    def predict_specialized_anomalies(self, transactions: List[SAPGLPosting], anomaly_type: str) -> List[Dict]:
        """Predict anomalies using specialized model for specific type"""
        if not self.is_trained.get(anomaly_type, False):
            logger.warning(f"Model for {anomaly_type} not trained")
            return []
        
        if not transactions:
            return []
        
        try:
            # Extract features for this anomaly type
            if anomaly_type == 'duplicate':
                df = self.extract_duplicate_features(transactions)
            elif anomaly_type == 'backdated':
                df = self.extract_backdated_features(transactions)
            elif anomaly_type == 'user_anomalies':
                df = self.extract_user_anomaly_features(transactions)
            elif anomaly_type == 'closing':
                df = self.extract_closing_features(transactions)
            elif anomaly_type == 'unusual_days':
                df = self.extract_unusual_days_features(transactions)
            elif anomaly_type == 'holiday':
                df = self.extract_holiday_features(transactions)
            else:
                return []
            
            if df.empty:
                return []
            
            # Prepare features
            feature_matrix, _ = self.prepare_features(df, anomaly_type, is_training=False)
            
            if feature_matrix.size == 0:
                return []
            
            predictions = []
            
            # Get predictions from all models for this anomaly type
            for model_name, model in self.models[anomaly_type].items():
                try:
                    if hasattr(model, 'predict_proba'):
                        proba = model.predict_proba(feature_matrix)[:, 1]
                        predictions_proba = proba
                    else:
                        predictions_proba = model.predict(feature_matrix)
                    
                    # Add predictions for transactions with high anomaly scores
                    for i, score in enumerate(predictions_proba):
                        if score > 0.5:  # Threshold for anomaly detection
                            predictions.append({
                                'transaction_id': str(transactions[i].id),
                                'anomaly_type': anomaly_type,
                                'model': model_name,
                                'anomaly_score': float(score),
                                'confidence': float(score * 100),
                                'gl_account': transactions[i].gl_account,
                                'amount': float(transactions[i].amount_local_currency),
                                'user_name': transactions[i].user_name,
                                'posting_date': transactions[i].posting_date.isoformat() if transactions[i].posting_date else None
                            })
                
                except Exception as e:
                    logger.error(f"Error predicting with {model_name} for {anomaly_type}: {e}")
            
            return predictions
        
        except Exception as e:
            logger.error(f"Error predicting {anomaly_type} anomalies: {e}")
            return []
    
    def _store_models_in_memory(self):
        """Store all specialized models in memory"""
        try:
            model_data = {}
            
            for anomaly_type in self.models.keys():
                model_data[anomaly_type] = {
                    'models': {},
                    'scalers': {},
                    'label_encoders': {},
                    'feature_columns': self.feature_columns.get(anomaly_type, []),
                    'is_trained': self.is_trained.get(anomaly_type, False)
                }
                
                # Store models
                for model_name, model in self.models[anomaly_type].items():
                    if hasattr(model, 'fit'):
                        model_bytes = pickle.dumps(model)
                        model_data[anomaly_type]['models'][model_name] = base64.b64encode(model_bytes).decode('utf-8')
                
                # Store scaler
                if anomaly_type in self.scalers:
                    scaler_bytes = pickle.dumps(self.scalers[anomaly_type])
                    model_data[anomaly_type]['scalers']['standard'] = base64.b64encode(scaler_bytes).decode('utf-8')
                
                # Store label encoders
                if anomaly_type in self.label_encoders:
                    for col, encoder in self.label_encoders[anomaly_type].items():
                        encoder_bytes = pickle.dumps(encoder)
                        model_data[anomaly_type]['label_encoders'][col] = base64.b64encode(encoder_bytes).decode('utf-8')
            
            self.model_data = model_data
            logger.info("Specialized models stored in memory")
            
        except Exception as e:
            logger.error(f"Error storing specialized models in memory: {e}")
    
    def load_models_from_memory(self):
        """Load specialized models from memory"""
        try:
            if not self.model_data:
                logger.warning("No model data found in memory")
                return False
            
            for anomaly_type, data in self.model_data.items():
                # Load models
                for model_name, model_str in data.get('models', {}).items():
                    model_bytes = base64.b64decode(model_str.encode('utf-8'))
                    self.models[anomaly_type][model_name] = pickle.loads(model_bytes)
                
                # Load scaler
                if 'standard' in data.get('scalers', {}):
                    scaler_bytes = base64.b64decode(data['scalers']['standard'].encode('utf-8'))
                    self.scalers[anomaly_type] = pickle.loads(scaler_bytes)
                
                # Load label encoders
                for col, encoder_str in data.get('label_encoders', {}).items():
                    encoder_bytes = base64.b64decode(encoder_str.encode('utf-8'))
                    self.label_encoders[anomaly_type][col] = pickle.loads(encoder_bytes)
                
                # Load other data
                self.feature_columns[anomaly_type] = data.get('feature_columns', [])
                self.is_trained[anomaly_type] = data.get('is_trained', False)
            
            logger.info("Specialized models loaded from memory")
            return True
            
        except Exception as e:
            logger.error(f"Error loading specialized models from memory: {e}")
            return False
    
    def get_model_info(self) -> Dict:
        """Get information about all specialized models"""
        info = {
            'total_anomaly_types': len(self.models),
            'trained_models': {},
            'model_types': {}
        }
        
        for anomaly_type in self.models.keys():
            info['trained_models'][anomaly_type] = self.is_trained.get(anomaly_type, False)
            info['model_types'][anomaly_type] = list(self.models[anomaly_type].keys())
        
        return info

# Simplified specialized detectors for when scikit-learn is not available
class StatisticalDuplicateDetector:
    def fit(self, X, y): pass
    def predict(self, X): return np.zeros(len(X))
    def predict_proba(self, X): return np.zeros((len(X), 2))

class StatisticalBackdatedDetector:
    def fit(self, X, y): pass
    def predict(self, X): return np.zeros(len(X))
    def predict_proba(self, X): return np.zeros((len(X), 2))

class StatisticalUserAnomalyDetector:
    def fit(self, X, y): pass
    def predict(self, X): return np.zeros(len(X))
    def predict_proba(self, X): return np.zeros((len(X), 2))

class StatisticalClosingDetector:
    def fit(self, X, y): pass
    def predict(self, X): return np.zeros(len(X))
    def predict_proba(self, X): return np.zeros((len(X), 2))

class StatisticalUnusualDaysDetector:
    def fit(self, X, y): pass
    def predict(self, X): return np.zeros(len(X))
    def predict_proba(self, X): return np.zeros((len(X), 2))

class StatisticalHolidayDetector:
    def fit(self, X, y): pass
    def predict(self, X): return np.zeros(len(X))
    def predict_proba(self, X): return np.zeros((len(X), 2)) 