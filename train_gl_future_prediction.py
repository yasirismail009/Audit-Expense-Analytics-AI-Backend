#!/usr/bin/env python
"""
GL Future Prediction Model Training Script
Trains a model to predict future GL transaction patterns and volumes
"""
import os
import sys
import django
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
import joblib
import warnings
warnings.filterwarnings('ignore')

# Setup Django
sys.path.insert(0, '.')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import SAPGLPosting, Engagement, AICompletenessModel
from django.utils import timezone

def extract_gl_features(engagement_id):
    """
    Extract features from GL data for future prediction
    """
    print(f"🔍 Extracting features for engagement: {engagement_id}")
    
    # Get GL transactions
    gl_transactions = SAPGLPosting.objects.filter(
        data_file__engagement_id=engagement_id
    ).select_related('data_file__engagement')
    
    if not gl_transactions.exists():
        print("❌ No GL transactions found")
        return None, None
    
    # Convert to DataFrame
    data = []
    for gl in gl_transactions:
        # Calculate debit/credit from amount_local_currency (positive=debit, negative=credit)
        amount = float(gl.amount_local_currency or 0)
        debit_amount = amount if amount > 0 else 0
        credit_amount = abs(amount) if amount < 0 else 0
        
        data.append({
            'posting_date': gl.posting_date,
            'gl_account': gl.gl_account or '',
            'debit_amount': debit_amount,
            'credit_amount': credit_amount,
            'total_amount': abs(amount),
            'user_name': gl.user_name or 'Unknown',
            'document_number': gl.document_number or '',
            'fiscal_year': gl.data_file.engagement.fiscal_year,
            'month': gl.posting_date.month if gl.posting_date else 1,
            'quarter': ((gl.posting_date.month - 1) // 3) + 1 if gl.posting_date else 1,
            'day_of_week': gl.posting_date.weekday() if gl.posting_date else 0,
            'is_weekend': 1 if gl.posting_date and gl.posting_date.weekday() >= 5 else 0,
        })
    
    df = pd.DataFrame(data)
    print(f"📊 Loaded {len(df):,} GL transactions")
    
    # Feature Engineering
    features = []
    targets = []
    
    # Group by date to create time series features
    daily_data = df.groupby('posting_date').agg({
        'total_amount': ['sum', 'mean', 'count'],
        'debit_amount': 'sum',
        'credit_amount': 'sum',
        'gl_account': 'nunique',
        'user_name': 'nunique',
        'document_number': 'nunique'
    }).reset_index()
    
    # Flatten column names
    daily_data.columns = ['posting_date', 'total_amount_sum', 'total_amount_mean', 'transaction_count',
                         'debit_sum', 'credit_sum', 'unique_accounts', 'unique_users', 'unique_documents']
    
    # Sort by date
    daily_data = daily_data.sort_values('posting_date')
    
    # Create lag features (previous days)
    for lag in [1, 2, 3, 7, 14, 30]:
        daily_data[f'total_amount_lag_{lag}'] = daily_data['total_amount_sum'].shift(lag)
        daily_data[f'transaction_count_lag_{lag}'] = daily_data['transaction_count'].shift(lag)
        daily_data[f'debit_sum_lag_{lag}'] = daily_data['debit_sum'].shift(lag)
        daily_data[f'credit_sum_lag_{lag}'] = daily_data['credit_sum'].shift(lag)
    
    # Create rolling averages
    for window in [3, 7, 14, 30]:
        daily_data[f'total_amount_ma_{window}'] = daily_data['total_amount_sum'].rolling(window=window).mean()
        daily_data[f'transaction_count_ma_{window}'] = daily_data['transaction_count'].rolling(window=window).mean()
    
    # Add time-based features
    daily_data['month'] = daily_data['posting_date'].dt.month
    daily_data['quarter'] = daily_data['posting_date'].dt.quarter
    daily_data['day_of_week'] = daily_data['posting_date'].dt.dayofweek
    daily_data['is_weekend'] = (daily_data['day_of_week'] >= 5).astype(int)
    daily_data['day_of_month'] = daily_data['posting_date'].dt.day
    daily_data['is_month_end'] = (daily_data['day_of_month'] >= 28).astype(int)
    
    # Create targets (next day predictions)
    daily_data['next_day_total_amount'] = daily_data['total_amount_sum'].shift(-1)
    daily_data['next_day_transaction_count'] = daily_data['transaction_count'].shift(-1)
    daily_data['next_day_debit_sum'] = daily_data['debit_sum'].shift(-1)
    daily_data['next_day_credit_sum'] = daily_data['credit_sum'].shift(-1)
    
    # Remove rows with NaN values
    daily_data = daily_data.dropna()
    
    if len(daily_data) < 30:
        print(f"❌ Insufficient data for training: {len(daily_data)} days (minimum 30 required)")
        return None, None
    
    # Prepare features and targets
    feature_columns = [col for col in daily_data.columns if col not in [
        'posting_date', 'next_day_total_amount', 'next_day_transaction_count', 
        'next_day_debit_sum', 'next_day_credit_sum'
    ]]
    
    X = daily_data[feature_columns].values
    y_amount = daily_data['next_day_total_amount'].values
    y_count = daily_data['next_day_transaction_count'].values
    y_debit = daily_data['next_day_debit_sum'].values
    y_credit = daily_data['next_day_credit_sum'].values
    
    print(f"✅ Feature extraction complete: {X.shape[0]} samples, {X.shape[1]} features")
    
    return {
        'features': X,
        'targets': {
            'amount': y_amount,
            'count': y_count,
            'debit': y_debit,
            'credit': y_credit
        },
        'feature_names': feature_columns,
        'dates': daily_data['posting_date'].values
    }, daily_data

def train_gl_future_prediction_model(engagement_id, client_name=''):
    """
    Train GL Future Prediction Model
    """
    print("🚀 Starting GL Future Prediction Model Training")
    print("=" * 60)
    
    # Extract features
    feature_data, daily_data = extract_gl_features(engagement_id)
    if feature_data is None:
        return {'success': False, 'error': 'Insufficient data for training'}
    
    X = feature_data['features']
    targets = feature_data['targets']
    feature_names = feature_data['feature_names']
    
    # Create model record
    model_name = f"gl_future_predictor_{client_name.lower().replace(' ', '_') if client_name else 'general'}"
    
    ai_model, created = AICompletenessModel.objects.get_or_create(
        model_name=model_name,
        model_version='1.0.0',
        client_name=client_name or '',
        defaults={
            'model_type': 'TRANSACTION_VOLUME_PREDICTOR',
            'status': 'TRAINING',
            'training_started_at': timezone.now(),
            'training_data_size': len(X)
        }
    )
    
    if not created:
        ai_model.status = 'TRAINING'
        ai_model.training_started_at = timezone.now()
        ai_model.training_data_size = len(X)
        ai_model.save()
        print(f"🔄 Updating existing model: {model_name}")
    else:
        print(f"🆕 Creating new model: {model_name}")
    
    # Train multiple models for different predictions
    models = {}
    results = {}
    
    for target_name, y in targets.items():
        print(f"\n📈 Training model for {target_name} prediction...")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, shuffle=False
        )
        
        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # Train model
        model = GradientBoostingRegressor(
            n_estimators=100,
            learning_rate=0.1,
            max_depth=6,
            random_state=42
        )
        
        model.fit(X_train_scaled, y_train)
        
        # Make predictions
        y_pred_train = model.predict(X_train_scaled)
        y_pred_test = model.predict(X_test_scaled)
        
        # Calculate metrics
        train_r2 = r2_score(y_train, y_pred_train)
        test_r2 = r2_score(y_test, y_pred_test)
        train_rmse = np.sqrt(mean_squared_error(y_train, y_pred_train))
        test_rmse = np.sqrt(mean_squared_error(y_test, y_pred_test))
        train_mae = mean_absolute_error(y_train, y_pred_train)
        test_mae = mean_absolute_error(y_test, y_pred_test)
        
        models[target_name] = {
            'model': model,
            'scaler': scaler,
            'train_r2': train_r2,
            'test_r2': test_r2,
            'train_rmse': train_rmse,
            'test_rmse': test_rmse,
            'train_mae': train_mae,
            'test_mae': test_mae
        }
        
        results[target_name] = {
            'train_r2': train_r2,
            'test_r2': test_r2,
            'train_rmse': train_rmse,
            'test_rmse': test_rmse,
            'train_mae': train_mae,
            'test_mae': test_mae
        }
        
        print(f"   ✅ {target_name} - Train R²: {train_r2:.3f}, Test R²: {test_r2:.3f}")
        print(f"   📊 RMSE - Train: {train_rmse:.2f}, Test: {test_rmse:.2f}")
    
    # Save models
    model_dir = os.path.join('.', 'trained_models')
    os.makedirs(model_dir, exist_ok=True)
    
    model_files = {}
    for target_name, model_data in models.items():
        model_path = os.path.join(model_dir, f'{model_name}_{target_name}_model.joblib')
        scaler_path = os.path.join(model_dir, f'{model_name}_{target_name}_scaler.joblib')
        
        joblib.dump(model_data['model'], model_path)
        joblib.dump(model_data['scaler'], scaler_path)
        
        model_files[target_name] = {
            'model_path': model_path,
            'scaler_path': scaler_path
        }
    
    # Update model record
    training_duration = (timezone.now() - ai_model.training_started_at).total_seconds()
    
    ai_model.feature_set = feature_names
    ai_model.model_file_path = str(model_files)
    ai_model.training_accuracy = np.mean([r['test_r2'] for r in results.values()]) * 100
    ai_model.validation_accuracy = np.mean([r['train_r2'] for r in results.values()]) * 100
    ai_model.status = 'TRAINED'
    ai_model.training_completed_at = timezone.now()
    ai_model.training_duration = training_duration
    ai_model.save()
    
    print("\n" + "=" * 60)
    print("🎉 GL Future Prediction Model Training Complete!")
    print(f"📊 Model: {model_name}")
    print(f"⏱️  Training Duration: {training_duration:.1f} seconds")
    print(f"📈 Average Test R²: {ai_model.training_accuracy:.1f}%")
    print(f"💾 Models saved to: {model_dir}")
    
    return {
        'success': True,
        'model_name': model_name,
        'training_duration': training_duration,
        'results': results,
        'model_files': model_files,
        'feature_count': len(feature_names),
        'training_samples': len(X)
    }

if __name__ == "__main__":
    # Get ENG-008 UUID
    from core.models import Engagement
    eng = Engagement.objects.get(engagement_id='ENG-008')
    engagement_id = str(eng.id)
    client_name = eng.client.client_name
    
    print(f"🎯 Training GL Future Prediction Model")
    print(f"📊 Engagement: {eng.engagement_name}")
    print(f"👤 Client: {client_name}")
    print(f"🆔 Engagement ID: {engagement_id}")
    
    result = train_gl_future_prediction_model(engagement_id, client_name)
    
    if result['success']:
        print(f"\n✅ Training completed successfully!")
        print(f"📈 Model can predict: Amount, Count, Debit, Credit")
        print(f"🎯 Average accuracy: {result['results']['amount']['test_r2']:.1%}")
    else:
        print(f"\n❌ Training failed: {result['error']}")
