"""
Advanced AI Tasks for GL Transaction Prediction and Anomaly Detection

This module contains AI/ML tasks that learn from GL transaction patterns to:
1. Predict future transaction volumes and patterns
2. Detect unusual activities and anomalies
3. Forecast client engagement trends
4. Identify risk patterns and unusual user behavior
"""

from celery import shared_task
from django.utils import timezone
from django.db import transaction
from django.db.models import Count, Sum, Avg, Q, F
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, Any, List, Tuple
import joblib
import os
from django.conf import settings

from .models import (
    SAPGLPosting, DataFile, Engagement, Client, GLAccount,
    AICompletenessModel, CompletenessAIPrediction
)

logger = logging.getLogger(__name__)

# ============================================================================
# AI MODEL TYPES FOR GL PREDICTIONS
# ============================================================================

class GLPredictionModelTypes:
    """Model types for GL-based AI predictions"""
    TRANSACTION_VOLUME_PREDICTOR = 'TRANSACTION_VOLUME_PREDICTOR'
    UNUSUAL_ACTIVITY_DETECTOR = 'UNUSUAL_ACTIVITY_DETECTOR'
    AMOUNT_PATTERN_ANALYZER = 'AMOUNT_PATTERN_ANALYZER'
    USER_BEHAVIOR_CLASSIFIER = 'USER_BEHAVIOR_CLASSIFIER'
    ACCOUNT_USAGE_PREDICTOR = 'ACCOUNT_USAGE_PREDICTOR'
    SEASONAL_TREND_FORECASTER = 'SEASONAL_TREND_FORECASTER'
    RISK_PATTERN_DETECTOR = 'RISK_PATTERN_DETECTOR'

# ============================================================================
# FEATURE EXTRACTION FROM GL DATA
# ============================================================================

def extract_transaction_features_for_anomaly_detection(engagement: Engagement) -> List[List[float]]:
    """
    Extract transaction-level features for anomaly detection within a single engagement
    
    Args:
        engagement: Engagement to analyze
        
    Returns:
        List of feature vectors for each transaction
    """
    from .models import SAPGLPosting
    import pandas as pd
    import numpy as np
    
    # Get all GL transactions for this engagement
    gl_postings = SAPGLPosting.objects.filter(
        data_file__engagement=engagement
    ).select_related('gl_account_ref', 'data_file')
    
    if not gl_postings.exists():
        return []
    
    # Convert to DataFrame for efficient analysis
    df = pd.DataFrame(list(gl_postings.values(
        'document_number', 'document_type', 'amount_local_currency', 
        'gl_account', 'profit_center', 'user_name', 'posting_date',
        'document_date', 'entry_date', 'fiscal_year', 'posting_period',
        'text', 'local_currency'
    )))
    
    if len(df) == 0:
        return []
    
    # Convert dates and amounts
    df['posting_date'] = pd.to_datetime(df['posting_date'])
    df['document_date'] = pd.to_datetime(df['document_date'])
    df['entry_date'] = pd.to_datetime(df['entry_date'])
    df['amount_numeric'] = pd.to_numeric(df['amount_local_currency'], errors='coerce').fillna(0)
    
    # Extract features for each transaction
    transaction_features = []
    
    for idx, row in df.iterrows():
        features = []
        
        # Amount features
        amount = abs(row['amount_numeric'])
        features.extend([
            amount,  # absolute amount
            np.log10(amount + 1),  # log amount (to handle large values)
            1 if amount == 0 else 0,  # zero amount flag
            1 if amount % 1000 == 0 else 0,  # round number flag
            1 if amount > df['amount_numeric'].abs().quantile(0.95) else 0,  # large transaction flag
        ])
        
        # Temporal features
        if pd.notna(row['posting_date']):
            features.extend([
                row['posting_date'].day,  # day of month
                row['posting_date'].month,  # month
                row['posting_date'].quarter,  # quarter
                row['posting_date'].dayofweek,  # day of week (0=Monday)
                1 if row['posting_date'].dayofweek >= 5 else 0,  # weekend flag
                row['posting_date'].dayofyear,  # day of year
            ])
        else:
            features.extend([0, 0, 0, 0, 0, 0])  # default values for missing dates
        
        # Account features (categorical to numeric)
        gl_account = str(row['gl_account']) if pd.notna(row['gl_account']) else '0'
        features.extend([
            len(gl_account),  # account code length
            sum(c.isdigit() for c in gl_account),  # number of digits
            sum(c.isalpha() for c in gl_account),  # number of letters
        ])
        
        # User features
        user_name = str(row['user_name']) if pd.notna(row['user_name']) else 'UNKNOWN'
        features.extend([
            len(user_name),  # user name length
            1 if 'ADMIN' in user_name.upper() else 0,  # admin user flag
        ])
        
        # Document type features
        doc_type = str(row['document_type']) if pd.notna(row['document_type']) else 'UNKNOWN'
        features.extend([
            len(doc_type),  # document type length
            1 if 'INVOICE' in doc_type.upper() else 0,  # invoice flag
            1 if 'PAYMENT' in doc_type.upper() else 0,  # payment flag
        ])
        
        # Text features
        text = str(row['text']) if pd.notna(row['text']) else ''
        features.extend([
            len(text),  # text length
            text.count(' '),  # word count (spaces)
            1 if 'REFUND' in text.upper() else 0,  # refund flag
            1 if 'ADJUSTMENT' in text.upper() else 0,  # adjustment flag
        ])
        
        # Profit center features
        profit_center = str(row['profit_center']) if pd.notna(row['profit_center']) else '0'
        features.extend([
            len(profit_center),  # profit center length
            1 if profit_center == '0' else 0,  # default profit center flag
        ])
        
        transaction_features.append(features)
    
    return transaction_features


def extract_gl_features_for_engagement(engagement: Engagement) -> Dict[str, Any]:
    """
    Extract comprehensive features from ALL file types in an engagement (GL + TB + COA)
    
    Args:
        engagement: Engagement to analyze
        
    Returns:
        Dictionary of extracted features from GL, TB, and COA data
    """
    features = {}
    
    # 1. GET GL (General Ledger) DATA
    gl_postings = SAPGLPosting.objects.filter(
        data_file__engagement=engagement
    ).select_related('gl_account_ref', 'data_file')
    
    if not gl_postings.exists():
        return {}
    
    # Convert to DataFrame for efficient analysis
    df = pd.DataFrame(list(gl_postings.values(
        'document_number', 'document_type', 'amount_local_currency', 
        'gl_account', 'profit_center', 'user_name', 'posting_date',
        'document_date', 'entry_date', 'fiscal_year', 'posting_period',
        'text', 'local_currency'
    )))
    
    features = {}
    
    # Basic transaction statistics
    features.update({
        'total_transactions': len(df),
        'unique_documents': df['document_number'].nunique(),
        'unique_users': df['user_name'].nunique(),
        'unique_accounts': df['gl_account'].nunique(),
        'unique_profit_centers': df['profit_center'].nunique(),
        'date_range_days': int((pd.to_datetime(df['posting_date']).max() - pd.to_datetime(df['posting_date']).min()).days) if len(df) > 1 else 0,
    })
    
    # Amount analysis
    if 'amount_local_currency' in df.columns:
        # Convert Decimal to float for calculations
        amounts = pd.to_numeric(df['amount_local_currency'], errors='coerce').dropna()
        if len(amounts) > 0:
            features.update({
                'total_debit_amount': float(amounts[amounts > 0].sum()),
                'total_credit_amount': float(abs(amounts[amounts < 0].sum())),
                'average_transaction_amount': float(amounts.abs().mean()),
                'median_transaction_amount': float(amounts.abs().median()),
                'max_transaction_amount': float(amounts.abs().max()),
                'amount_std_deviation': float(amounts.abs().std()),
                'large_transactions_count': int(len(amounts[amounts.abs() > amounts.abs().quantile(0.95)])),
                'small_transactions_count': int(len(amounts[amounts.abs() < amounts.abs().quantile(0.05)])),
            })
    
    # Temporal patterns
    if 'posting_date' in df.columns:
        df['posting_date'] = pd.to_datetime(df['posting_date'])
        df['day_of_week'] = df['posting_date'].dt.dayofweek
        df['month'] = df['posting_date'].dt.month
        df['quarter'] = df['posting_date'].dt.quarter
        
        features.update({
            'transactions_per_day_avg': len(df) / max(1, features['date_range_days']),
            'weekend_transactions_ratio': len(df[df['day_of_week'].isin([5, 6])]) / len(df),
            'month_with_most_transactions': df['month'].mode().iloc[0] if len(df) > 0 else 0,
            'transactions_by_quarter': df['quarter'].value_counts().to_dict(),
        })
    
    # User behavior patterns
    # Convert amount to numeric before aggregation
    df['amount_numeric'] = pd.to_numeric(df['amount_local_currency'], errors='coerce')
    user_stats = df.groupby('user_name').agg({
        'document_number': 'count',
        'amount_numeric': ['sum', 'mean', 'std']
    }).round(2)
    
    if len(user_stats) > 0:
        features.update({
            'most_active_user_transactions': user_stats[('document_number', 'count')].max(),
            'least_active_user_transactions': user_stats[('document_number', 'count')].min(),
            'user_activity_variance': user_stats[('document_number', 'count')].std(),
            'power_users_count': len(user_stats[user_stats[('document_number', 'count')] > user_stats[('document_number', 'count')].quantile(0.8)]),
        })
    
    # Account usage patterns
    account_stats = df.groupby('gl_account').agg({
        'document_number': 'count',
        'amount_numeric': ['sum', 'mean']
    }).round(2)
    
    if len(account_stats) > 0:
        features.update({
            'most_used_account_frequency': account_stats[('document_number', 'count')].max(),
            'account_usage_concentration': (account_stats[('document_number', 'count')].max() / account_stats[('document_number', 'count')].sum()),
            'high_value_accounts_count': len(account_stats[account_stats[('amount_numeric', 'sum')].abs() > account_stats[('amount_numeric', 'sum')].abs().quantile(0.9)]),
        })
    
    # Document type patterns
    doc_type_stats = df['document_type'].value_counts()
    features.update({
        'document_types_count': len(doc_type_stats),
        'most_common_doc_type_ratio': doc_type_stats.iloc[0] / len(df) if len(doc_type_stats) > 0 else 0,
    })
    
    # Potential anomaly indicators
    zero_amount_count = int(len(amounts[amounts == 0])) if len(amounts) > 0 else 0
    round_number_count = int(len(amounts[amounts.astype(float) % 1000 == 0])) if len(amounts) > 0 else 0
    
    features.update({
        'zero_amount_transactions': zero_amount_count,
        'round_number_transactions': round_number_count,
        'weekend_large_transactions': 0,  # Will calculate after date processing
    })
    
    # 2. GET TRIAL BALANCE (TB) DATA
    from .models import TrialBalance
    tb_records = TrialBalance.objects.filter(data_file__engagement=engagement)
    
    if tb_records.exists():
        tb_df = pd.DataFrame(list(tb_records.values(
            'gl_account', 'short_text', 'opening_balance', 'closing_balance',
            'debit', 'credit', 'company_code'
        )))
        
        # Convert TB amounts to numeric
        tb_df['opening_balance'] = pd.to_numeric(tb_df['opening_balance'], errors='coerce').fillna(0)
        tb_df['closing_balance'] = pd.to_numeric(tb_df['closing_balance'], errors='coerce').fillna(0)
        tb_df['debit'] = pd.to_numeric(tb_df['debit'], errors='coerce').fillna(0)
        tb_df['credit'] = pd.to_numeric(tb_df['credit'], errors='coerce').fillna(0)
        
        # TB-specific features
        features.update({
            'tb_total_accounts': len(tb_df),
            'tb_total_opening_balance': float(tb_df['opening_balance'].sum()),
            'tb_total_closing_balance': float(tb_df['closing_balance'].sum()),
            'tb_total_debits': float(tb_df['debit'].sum()),
            'tb_total_credits': float(tb_df['credit'].sum()),
            'tb_balance_difference': float(abs(tb_df['debit'].sum() - tb_df['credit'].sum())),
            'tb_zero_balance_accounts': int(len(tb_df[tb_df['closing_balance'] == 0])),
            'tb_large_balance_accounts': int(len(tb_df[tb_df['closing_balance'].abs() > tb_df['closing_balance'].abs().quantile(0.9)])),
            'tb_company_codes_count': int(tb_df['company_code'].nunique()) if 'company_code' in tb_df.columns else 0,
        })
    else:
        # No TB data available
        features.update({
            'tb_total_accounts': 0,
            'tb_total_opening_balance': 0,
            'tb_total_closing_balance': 0,
            'tb_total_debits': 0,
            'tb_total_credits': 0,
            'tb_balance_difference': 0,
            'tb_zero_balance_accounts': 0,
            'tb_large_balance_accounts': 0,
            'tb_company_codes_count': 0,
        })
    
    # 3. GET CHART OF ACCOUNTS (COA) DATA
    from .models import GLAccount
    coa_accounts = GLAccount.objects.filter(engagement=engagement)
    
    if coa_accounts.exists():
        coa_df = pd.DataFrame(list(coa_accounts.values(
            'account_code', 'account_name', 'account_type', 'sub_type', 'sub_sub_type',
            'financial_statement_category', 'balance_sheet_category', 'income_statement_category',
            'is_active', 'opening_balance', 'closing_balance'
        )))
        
        # Convert COA amounts to numeric
        coa_df['opening_balance'] = pd.to_numeric(coa_df['opening_balance'], errors='coerce').fillna(0)
        coa_df['closing_balance'] = pd.to_numeric(coa_df['closing_balance'], errors='coerce').fillna(0)
        
        # COA-specific features
        features.update({
            'coa_total_accounts': len(coa_df),
            'coa_active_accounts': int(len(coa_df[coa_df['is_active'] == True])),
            'coa_inactive_accounts': int(len(coa_df[coa_df['is_active'] == False])),
            'coa_account_types': int(coa_df['account_type'].nunique()),
            'coa_sub_types': int(coa_df['sub_type'].nunique()),
            'coa_sub_sub_types': int(coa_df['sub_sub_type'].nunique()),
            'coa_fs_categories': int(coa_df['financial_statement_category'].nunique()),
            'coa_bs_categories': int(coa_df['balance_sheet_category'].nunique()),
            'coa_is_categories': int(coa_df['income_statement_category'].nunique()),
            'coa_accounts_with_balances': int(len(coa_df[coa_df['closing_balance'] != 0])),
            'coa_total_opening_balance': float(coa_df['opening_balance'].sum()),
            'coa_total_closing_balance': float(coa_df['closing_balance'].sum()),
        })
    else:
        # No COA data available
        features.update({
            'coa_total_accounts': 0,
            'coa_active_accounts': 0,
            'coa_inactive_accounts': 0,
            'coa_account_types': 0,
            'coa_sub_types': 0,
            'coa_sub_sub_types': 0,
            'coa_fs_categories': 0,
            'coa_bs_categories': 0,
            'coa_is_categories': 0,
            'coa_accounts_with_balances': 0,
            'coa_total_opening_balance': 0,
            'coa_total_closing_balance': 0,
        })
    
    # 4. CROSS-FILE ANALYSIS (GL vs TB vs COA)
    # Reconciliation and consistency features
    gl_accounts_used = set(gl_postings.values_list('gl_account', flat=True).distinct())
    
    if tb_records.exists():
        tb_accounts = set(tb_records.values_list('gl_account', flat=True))
        gl_tb_common_accounts = len(gl_accounts_used.intersection(tb_accounts))
        gl_tb_coverage_ratio = gl_tb_common_accounts / len(gl_accounts_used) if gl_accounts_used else 0
    else:
        gl_tb_common_accounts = 0
        gl_tb_coverage_ratio = 0
    
    if coa_accounts.exists():
        coa_accounts_set = set(coa_accounts.values_list('account_code', flat=True))
        gl_coa_common_accounts = len(gl_accounts_used.intersection(coa_accounts_set))
        gl_coa_coverage_ratio = gl_coa_common_accounts / len(gl_accounts_used) if gl_accounts_used else 0
    else:
        gl_coa_common_accounts = 0
        gl_coa_coverage_ratio = 0
    
    # Cross-file consistency features
    features.update({
        'gl_tb_common_accounts': gl_tb_common_accounts,
        'gl_tb_coverage_ratio': float(gl_tb_coverage_ratio),
        'gl_coa_common_accounts': gl_coa_common_accounts,
        'gl_coa_coverage_ratio': float(gl_coa_coverage_ratio),
        'file_completeness_score': float((gl_tb_coverage_ratio + gl_coa_coverage_ratio) / 2),
    })
    
    return features

def extract_user_behavior_features(user_name: str, engagement: Engagement) -> Dict[str, Any]:
    """
    Extract features for specific user behavior analysis
    
    Args:
        user_name: User to analyze
        engagement: Engagement context
        
    Returns:
        Dictionary of user-specific features
    """
    user_postings = SAPGLPosting.objects.filter(
        data_file__engagement=engagement,
        user_name=user_name
    )
    
    if not user_postings.exists():
        return {}
    
    df = pd.DataFrame(list(user_postings.values(
        'document_number', 'amount_local_currency', 'gl_account',
        'posting_date', 'document_type', 'posting_period'
    )))
    
    # Convert amounts to numeric for calculations
    if 'amount_local_currency' in df.columns:
        df['amount_numeric'] = pd.to_numeric(df['amount_local_currency'], errors='coerce')
    
    features = {
        'user_transaction_count': len(df),
        'user_unique_accounts': df['gl_account'].nunique(),
        'user_unique_doc_types': df['document_type'].nunique(),
        'user_avg_amount': float(df['amount_numeric'].abs().mean()) if 'amount_numeric' in df.columns else 0,
        'user_total_volume': float(df['amount_numeric'].abs().sum()) if 'amount_numeric' in df.columns else 0,
    }
    
    # Temporal patterns for this user
    if 'posting_date' in df.columns:
        df['posting_date'] = pd.to_datetime(df['posting_date'])
        df['hour'] = df['posting_date'].dt.hour
        df['day_of_week'] = df['posting_date'].dt.dayofweek
        
        features.update({
            'user_after_hours_ratio': len(df[(df['hour'] < 8) | (df['hour'] > 18)]) / len(df),
            'user_weekend_ratio': len(df[df['day_of_week'].isin([5, 6])]) / len(df),
            'user_activity_span_days': int((df['posting_date'].max() - df['posting_date'].min()).days),
        })
    
    return features

# ============================================================================
# AI TRAINING TASKS
# ============================================================================

@shared_task(bind=True, max_retries=2, default_retry_delay=300, time_limit=1800, soft_time_limit=1500)
def train_gl_anomaly_detection_model(self, engagement_id=None, client_name=None):
    """
    Train AI model to detect unusual activities in GL transactions
    
    Args:
        engagement_id: Specific engagement ID (optional)
        client_name: Specific client name for client-specific model (optional)
        
    Returns:
        Training results and model performance metrics
    """
    task_name = "train_gl_anomaly_detection_model"
    start_time = timezone.now()
    
    try:
        logger.info(f"Starting GL anomaly detection model training for client: {client_name or 'General'}")
        
        # Create or update model record
        model_name = f"gl_anomaly_detector_{client_name.lower().replace(' ', '_') if client_name else 'general'}"
        
        # Try to get existing model or create new one
        ai_model, created = AICompletenessModel.objects.get_or_create(
            model_name=model_name,
            model_version='1.0.0',
            client_name=client_name or '',
            defaults={
                'model_type': GLPredictionModelTypes.UNUSUAL_ACTIVITY_DETECTOR,
                'status': 'TRAINING',
                'training_started_at': start_time,
                'training_data_size': 0
            }
        )
        
        # If model already exists, update it for retraining
        if not created:
            ai_model.status = 'TRAINING'
            ai_model.training_started_at = start_time
            ai_model.training_data_size = 0
            ai_model.save()
            logger.info(f"Updating existing model: {model_name}")
        
        # Get specific engagement for training
        if not engagement_id:
            raise Exception("engagement_id is required for anomaly detection training")
            
        try:
            engagement = Engagement.objects.get(id=engagement_id)
        except Engagement.DoesNotExist:
            raise Exception(f"Engagement {engagement_id} not found")
        
        # Extract transaction-level features for anomaly detection
        training_features = extract_transaction_features_for_anomaly_detection(engagement)
        
        if len(training_features) < 10:
            raise Exception(f"Insufficient transaction data: {len(training_features)} transactions (minimum 10 required)")
        
        logger.info(f"📊 Training anomaly detection on {len(training_features)} transactions from engagement {engagement.engagement_id}")
        
        # Update training data size
        ai_model.training_data_size = len(training_features)
        ai_model.save()
        
        # Train anomaly detection model (Isolation Forest)
        from sklearn.ensemble import IsolationForest
        from sklearn.preprocessing import StandardScaler
        import numpy as np
        
        X = np.array(training_features)
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Train Isolation Forest for anomaly detection
        model = IsolationForest(
            contamination=0.05,  # Assume 5% anomalies (more conservative for single engagement)
            random_state=42,
            n_estimators=100
        )
        model.fit(X_scaled)
        
        # Predict anomalies on training data
        anomaly_predictions = model.predict(X_scaled)
        anomaly_scores = model.decision_function(X_scaled)
        
        # Calculate metrics
        normal_count = len(anomaly_predictions[anomaly_predictions == 1])
        anomaly_count = len(anomaly_predictions[anomaly_predictions == -1])
        
        logger.info(f"🔍 Anomaly Detection Results:")
        logger.info(f"  📊 Total transactions: {len(training_features)}")
        logger.info(f"  ✅ Normal transactions: {normal_count}")
        logger.info(f"  ⚠️ Anomalous transactions: {anomaly_count}")
        logger.info(f"  📈 Anomaly rate: {(anomaly_count/len(training_features)*100):.2f}%")
        
        # Save model files
        model_dir = os.path.join(settings.BASE_DIR, 'trained_models')
        os.makedirs(model_dir, exist_ok=True)
        
        model_path = os.path.join(model_dir, f'{model_name}_model.joblib')
        scaler_path = os.path.join(model_dir, f'{model_name}_scaler.joblib')
        
        joblib.dump(model, model_path)
        joblib.dump(scaler, scaler_path)
        
        # Update model record
        training_duration = (timezone.now() - start_time).total_seconds()
        feature_names = [
            'amount', 'log_amount', 'zero_amount', 'round_number', 'large_transaction',
            'day', 'month', 'quarter', 'dayofweek', 'weekend', 'dayofyear',
            'account_length', 'account_digits', 'account_letters',
            'user_length', 'admin_user',
            'doc_type_length', 'invoice_flag', 'payment_flag',
            'text_length', 'word_count', 'refund_flag', 'adjustment_flag',
            'profit_center_length', 'default_profit_center'
        ]
        
        ai_model.feature_set = feature_names
        ai_model.model_file_path = model_path
        ai_model.scaler_file_path = scaler_path
        ai_model.training_completed_at = timezone.now()
        ai_model.training_duration = training_duration
        ai_model.status = 'TRAINED'
        ai_model.model_parameters = {
            'model_type': 'IsolationForest',
            'contamination': 0.05,
            'n_estimators': 100,
            'normal_transactions': normal_count,
            'anomalous_transactions': anomaly_count,
            'engagement_id': str(engagement.id),
            'engagement_name': engagement.engagement_id
        }
        ai_model.save()
        
        logger.info(f"GL anomaly detection model training completed:")
        logger.info(f"  Model ID: {ai_model.id}")
        logger.info(f"  Training Duration: {training_duration:.2f} seconds")
        logger.info(f"  Training Data: {len(training_features)} transactions")
        logger.info(f"  Normal: {normal_count}, Anomalous: {anomaly_count}")
        logger.info(f"  Engagement: {engagement.engagement_id}")
        
        return {
            'success': True,
            'model_id': str(ai_model.id),
            'model_type': GLPredictionModelTypes.UNUSUAL_ACTIVITY_DETECTOR,
            'client_name': client_name or '',
            'engagement_id': str(engagement.id),
            'engagement_name': engagement.engagement_id,
            'training_duration': training_duration,
            'training_data_size': len(training_features),
            'normal_count': normal_count,
            'anomaly_count': anomaly_count,
            'feature_count': len(feature_names)
        }
        
    except Exception as e:
        # Update model status to failed
        if 'ai_model' in locals():
            ai_model.status = 'FAILED'
            ai_model.training_completed_at = timezone.now()
            ai_model.save()
        
        logger.error(f"GL anomaly detection model training failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'model_type': GLPredictionModelTypes.UNUSUAL_ACTIVITY_DETECTOR
        }

@shared_task(bind=True, max_retries=2, default_retry_delay=300, time_limit=1800, soft_time_limit=1500)
def train_gl_volume_prediction_model(self, engagement_id=None, client_name=None):
    """
    Train AI model to predict future transaction volumes and patterns
    
    Args:
        engagement_id: Specific engagement ID (optional)
        client_name: Specific client name for client-specific model (optional)
        
    Returns:
        Training results and model performance metrics
    """
    task_name = "train_gl_volume_prediction_model"
    start_time = timezone.now()
    
    try:
        logger.info(f"Starting GL volume prediction model training for client: {client_name or 'General'}")
        
        # Create or update model record
        model_name = f"gl_volume_predictor_{client_name.lower().replace(' ', '_') if client_name else 'general'}"
        
        # Try to get existing model or create new one
        ai_model, created = AICompletenessModel.objects.get_or_create(
            model_name=model_name,
            model_version='1.0.0',
            client_name=client_name or '',
            defaults={
                'model_type': GLPredictionModelTypes.TRANSACTION_VOLUME_PREDICTOR,
                'status': 'TRAINING',
                'training_started_at': start_time,
                'training_data_size': 0
            }
        )
        
        # If model already exists, update it for retraining
        if not created:
            ai_model.status = 'TRAINING'
            ai_model.training_started_at = start_time
            ai_model.training_data_size = 0
            ai_model.save()
            logger.info(f"Updating existing model: {model_name}")
        
        # Get training data
        query_filter = {}
        if engagement_id:
            query_filter['id'] = engagement_id
        elif client_name:
            query_filter['client__client_name__icontains'] = client_name
            
        engagements = Engagement.objects.filter(**query_filter)
        
        if not engagements.exists():
            raise Exception("No engagements found for training")
        
        # Extract time series data for volume prediction
        training_features = []
        training_targets = []
        
        for engagement in engagements:
            # Get monthly transaction volumes
            gl_postings = SAPGLPosting.objects.filter(data_file__engagement=engagement)
            
            if gl_postings.exists():
                # Group by month and calculate volumes
                monthly_data = gl_postings.extra(
                    select={'month': "DATE_TRUNC('month', posting_date)"}
                ).values('month').annotate(
                    transaction_count=Count('id'),
                    total_amount=Sum('amount_local_currency'),
                    unique_users=Count('user_name', distinct=True),
                    unique_accounts=Count('gl_account', distinct=True)
                ).order_by('month')
                
                monthly_list = list(monthly_data)
                if len(monthly_list) >= 3:  # Need at least 3 months for prediction
                    for i in range(len(monthly_list) - 1):
                        # Features: current month stats
                        current = monthly_list[i]
                        next_month = monthly_list[i + 1]
                        
                        # Calculate quarter from month (1-3=Q1, 4-6=Q2, 7-9=Q3, 10-12=Q4)
                        month_num = current['month'].month
                        quarter = ((month_num - 1) // 3) + 1
                        
                        features = [
                            int(current['transaction_count']),
                            float(current['total_amount'] or 0),
                            int(current['unique_users']),
                            int(current['unique_accounts']),
                            int(month_num),  # Month of year
                            int(quarter),  # Quarter
                        ]
                        
                        # Target: next month's transaction count
                        target = int(next_month['transaction_count'])
                        
                        training_features.append(features)
                        training_targets.append(target)
        
        if len(training_features) < 2:
            raise Exception(f"Insufficient training data: {len(training_features)} samples (minimum 2 required)")
        
        # Update training data size
        ai_model.training_data_size = len(training_features)
        ai_model.save()
        
        # Train volume prediction model (Random Forest Regressor)
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.preprocessing import StandardScaler
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import mean_squared_error, r2_score
        
        X = np.array(training_features)
        y = np.array(training_targets)
        
        # Split data (use smaller test size for small datasets)
        test_size = min(0.2, 1.0 / len(X)) if len(X) > 2 else 0.0
        if test_size > 0:
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=42)
        else:
            # Use all data for training when very small dataset
            X_train, X_test, y_train, y_test = X, X, y, y
        
        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # Train model
        model = RandomForestRegressor(
            n_estimators=100,
            random_state=42,
            max_depth=10
        )
        model.fit(X_train_scaled, y_train)
        
        # Evaluate model
        train_predictions = model.predict(X_train_scaled)
        test_predictions = model.predict(X_test_scaled)
        
        train_r2 = r2_score(y_train, train_predictions)
        test_r2 = r2_score(y_test, test_predictions)
        train_mse = mean_squared_error(y_train, train_predictions)
        test_mse = mean_squared_error(y_test, test_predictions)
        
        # Save model files
        model_dir = os.path.join(settings.BASE_DIR, 'trained_models')
        os.makedirs(model_dir, exist_ok=True)
        
        model_path = os.path.join(model_dir, f'{model_name}_model.joblib')
        scaler_path = os.path.join(model_dir, f'{model_name}_scaler.joblib')
        
        joblib.dump(model, model_path)
        joblib.dump(scaler, scaler_path)
        
        # Update model record
        training_duration = (timezone.now() - start_time).total_seconds()
        feature_names = ['transaction_count', 'total_amount', 'unique_users', 'unique_accounts', 'month', 'quarter']
        
        ai_model.feature_set = feature_names
        ai_model.model_file_path = model_path
        ai_model.scaler_file_path = scaler_path
        # Handle NaN values for JSON serialization
        def safe_float(value):
            """Convert value to float, handling NaN and None"""
            if value is None or (isinstance(value, float) and (value != value)):  # NaN check
                return 0.0
            return float(value)
        
        ai_model.training_completed_at = timezone.now()
        ai_model.training_duration = training_duration
        ai_model.training_accuracy = safe_float(train_r2) * 100
        ai_model.validation_accuracy = safe_float(test_r2) * 100
        ai_model.status = 'TRAINED'
        
        ai_model.model_parameters = {
            'model_type': 'RandomForestRegressor',
            'n_estimators': 100,
            'train_r2': safe_float(train_r2),
            'test_r2': safe_float(test_r2),
            'train_mse': safe_float(train_mse),
            'test_mse': safe_float(test_mse)
        }
        ai_model.save()
        
        logger.info(f"GL volume prediction model training completed:")
        logger.info(f"  Model ID: {ai_model.id}")
        logger.info(f"  Training R²: {train_r2:.3f}")
        logger.info(f"  Test R²: {test_r2:.3f}")
        logger.info(f"  Training Duration: {training_duration:.2f} seconds")
        
        return {
            'success': True,
            'model_id': str(ai_model.id),
            'model_type': GLPredictionModelTypes.TRANSACTION_VOLUME_PREDICTOR,
            'client_name': client_name or '',
            'training_duration': training_duration,
            'training_data_size': len(training_features),
            'train_r2': train_r2,
            'test_r2': test_r2,
            'feature_count': len(feature_names)
        }
        
    except Exception as e:
        # Update model status to failed
        if 'ai_model' in locals():
            ai_model.status = 'FAILED'
            ai_model.training_completed_at = timezone.now()
            ai_model.save()
        
        logger.error(f"GL volume prediction model training failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'model_type': GLPredictionModelTypes.TRANSACTION_VOLUME_PREDICTOR
        }

# ============================================================================
# AI PREDICTION TASKS
# ============================================================================

@shared_task(bind=True, max_retries=2, default_retry_delay=60, time_limit=300, soft_time_limit=240)
def predict_gl_anomalies(self, engagement_id, model_type=None):
    """
    Predict anomalies in GL transactions for a specific engagement
    
    Args:
        engagement_id: Engagement to analyze
        model_type: Type of model to use (optional)
        
    Returns:
        Anomaly prediction results
    """
    task_name = "predict_gl_anomalies"
    start_time = timezone.now()
    
    try:
        engagement = Engagement.objects.get(id=engagement_id)
        client_name = engagement.client.client_name
        
        logger.info(f"Starting GL anomaly prediction for engagement: {engagement.engagement_name}")
        
        # Find best anomaly detection model
        ai_model = AICompletenessModel.objects.filter(
            model_type=GLPredictionModelTypes.UNUSUAL_ACTIVITY_DETECTOR,
            status='TRAINED'
        ).order_by('-validation_accuracy').first()
        
        if not ai_model:
            logger.warning("No trained anomaly detection model available. Using rule-based detection.")
            return _fallback_anomaly_detection(engagement)
        
        # Extract features for this engagement
        features = extract_gl_features_for_engagement(engagement)
        if not features:
            raise Exception("No GL data found for this engagement")
        
        # Load model and scaler
        model = joblib.load(ai_model.model_file_path)
        scaler = joblib.load(ai_model.scaler_file_path)
        
        # Prepare features
        feature_values = [features.get(fname, 0) for fname in ai_model.feature_set]
        features_array = np.array([feature_values])
        features_scaled = scaler.transform(features_array)
        
        # Make prediction
        anomaly_prediction = model.predict(features_scaled)[0]
        anomaly_score = model.decision_function(features_scaled)[0]
        
        # Interpret results
        is_anomalous = anomaly_prediction == -1
        confidence = abs(anomaly_score)
        risk_level = 'HIGH' if is_anomalous and confidence > 0.5 else 'MEDIUM' if is_anomalous else 'LOW'
        
        # Save prediction
        prediction = CompletenessAIPrediction.objects.create(
            data_file=engagement.data_files.first(),  # Use first data file
            ai_model=ai_model,
            prediction_confidence=round(confidence * 100, 2),
            predicted_completeness_score=50 if is_anomalous else 85,  # Lower score for anomalies
            predicted_status='FAIL' if is_anomalous else 'PASS',
            predicted_step_results={
                'anomaly_detected': is_anomalous,
                'anomaly_score': float(anomaly_score),
                'risk_level': risk_level,
                'analyzed_features': features
            },
            predicted_processing_time=30.0,
            input_features=features
        )
        
        logger.info(f"GL anomaly prediction completed:")
        logger.info(f"  Anomalous: {is_anomalous}")
        logger.info(f"  Risk Level: {risk_level}")
        logger.info(f"  Confidence: {confidence:.3f}")
        
        return {
            'success': True,
            'prediction_id': str(prediction.id),
            'model_used': ai_model.model_name,
            'engagement_id': str(engagement_id),
            'is_anomalous': is_anomalous,
            'risk_level': risk_level,
            'confidence': confidence,
            'anomaly_score': float(anomaly_score),
            'features_analyzed': features
        }
        
    except Exception as e:
        logger.error(f"GL anomaly prediction failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'engagement_id': str(engagement_id)
        }

def _fallback_anomaly_detection(engagement: Engagement) -> Dict[str, Any]:
    """
    Fallback rule-based anomaly detection when no trained model is available
    
    Args:
        engagement: Engagement to analyze
        
    Returns:
        Anomaly detection results
    """
    features = extract_gl_features_for_engagement(engagement)
    
    anomaly_indicators = []
    
    # Rule-based anomaly detection
    if features.get('weekend_transactions_ratio', 0) > 0.3:
        anomaly_indicators.append("High weekend activity")
    
    if features.get('round_number_transactions', 0) > features.get('total_transactions', 1) * 0.5:
        anomaly_indicators.append("Many round number transactions")
    
    if features.get('power_users_count', 0) == 1:
        anomaly_indicators.append("Single power user dominance")
    
    if features.get('account_usage_concentration', 0) > 0.8:
        anomaly_indicators.append("High account usage concentration")
    
    is_anomalous = len(anomaly_indicators) >= 2
    confidence = len(anomaly_indicators) * 0.3
    
    return {
        'success': True,
        'prediction_id': None,
        'model_used': 'Rule-based Fallback',
        'is_anomalous': is_anomalous,
        'risk_level': 'HIGH' if is_anomalous else 'LOW',
        'confidence': confidence,
        'anomaly_indicators': anomaly_indicators,
        'features_analyzed': features
    }
