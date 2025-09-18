#!/usr/bin/env python
"""
Completeness Test Recommendation Model Training Script
Trains a model to generate recommendations based on completeness test results
"""
import os
import sys
import django
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import joblib
import warnings
warnings.filterwarnings('ignore')

# Setup Django
sys.path.insert(0, '.')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import CompletenessTestResult, Engagement, AICompletenessModel
from django.utils import timezone

def extract_completeness_features():
    """
    Extract features from completeness test results for recommendation generation
    """
    print("🔍 Extracting features from completeness test results...")
    
    # Get all completeness test results
    test_results = CompletenessTestResult.objects.all().select_related('engagement__client')
    
    if not test_results.exists():
        print("❌ No completeness test results found")
        return None
    
    # Convert to DataFrame
    data = []
    for test in test_results:
        # Extract basic features
        data.append({
            'engagement_id': test.engagement.engagement_id,
            'client_name': test.engagement.client.client_name,
            'fiscal_year': test.engagement.fiscal_year,
            'overall_status': test.overall_status,
            'completeness_score': test.completeness_score,
            'tests_passed': test.tests_passed,
            'total_tests': test.total_tests,
            'critical_issues_count': test.critical_issues_count,
            'total_gl_records': test.total_gl_records,
            'total_tb_records': test.total_tb_records,
            'test_timestamp': test.test_timestamp,
        })
    
    df = pd.DataFrame(data)
    print(f"📊 Loaded {len(df)} completeness test results")
    
    if len(df) < 5:
        print("❌ Insufficient data for training (minimum 5 tests required)")
        return None
    
    # Feature Engineering
    features = []
    recommendations = []
    
    for _, row in df.iterrows():
        # Basic features
        feature_vector = {
            'completeness_score': row['completeness_score'],
            'tests_passed': row['tests_passed'],
            'total_tests': row['total_tests'],
            'critical_issues_count': row['critical_issues_count'],
            'total_gl_records': row['total_gl_records'],
            'total_tb_records': row['total_tb_records'],
            'fiscal_year': row['fiscal_year'],
            'pass_rate': row['tests_passed'] / row['total_tests'] if row['total_tests'] > 0 else 0,
            'gl_tb_ratio': row['total_gl_records'] / row['total_tb_records'] if row['total_tb_records'] > 0 else 0,
            'issues_per_test': row['critical_issues_count'] / row['total_tests'] if row['total_tests'] > 0 else 0,
        }
        
        # Generate recommendations based on test results
        recommendation_vector = generate_recommendations(row)
        
        features.append(feature_vector)
        recommendations.append(recommendation_vector)
    
    # Convert to DataFrame
    features_df = pd.DataFrame(features)
    recommendations_df = pd.DataFrame(recommendations)
    
    print(f"✅ Feature extraction complete: {len(features_df)} samples, {len(features_df.columns)} features")
    print(f"📋 Generated {len(recommendations_df.columns)} recommendation categories")
    
    return {
        'features': features_df,
        'recommendations': recommendations_df,
        'feature_names': list(features_df.columns),
        'recommendation_names': list(recommendations_df.columns)
    }

def generate_recommendations(test_row):
    """
    Generate recommendations based on completeness test results
    """
    recommendations = {}
    
    # Data Quality Recommendations
    if test_row['completeness_score'] < 70:
        recommendations['improve_data_quality'] = 1
        recommendations['review_source_systems'] = 1
    else:
        recommendations['improve_data_quality'] = 0
        recommendations['review_source_systems'] = 0
    
    # Process Improvement Recommendations
    if test_row['critical_issues_count'] > 5:
        recommendations['enhance_validation_rules'] = 1
        recommendations['implement_automated_checks'] = 1
    else:
        recommendations['enhance_validation_rules'] = 0
        recommendations['implement_automated_checks'] = 0
    
    # Volume-based Recommendations
    if test_row['total_gl_records'] > 100000:
        recommendations['consider_data_sampling'] = 1
        recommendations['optimize_processing_performance'] = 1
    else:
        recommendations['consider_data_sampling'] = 0
        recommendations['optimize_processing_performance'] = 0
    
    # Balance Recommendations
    if test_row['gl_tb_ratio'] > 10 or test_row['gl_tb_ratio'] < 0.1:
        recommendations['review_balance_reconciliation'] = 1
        recommendations['check_data_mapping'] = 1
    else:
        recommendations['review_balance_reconciliation'] = 0
        recommendations['check_data_mapping'] = 0
    
    # Testing Strategy Recommendations
    if test_row['pass_rate'] < 0.8:
        recommendations['increase_testing_frequency'] = 1
        recommendations['expand_test_coverage'] = 1
    else:
        recommendations['increase_testing_frequency'] = 0
        recommendations['expand_test_coverage'] = 0
    
    # Risk-based Recommendations
    if test_row['critical_issues_count'] > 0:
        recommendations['implement_risk_monitoring'] = 1
        recommendations['create_escalation_procedures'] = 1
    else:
        recommendations['implement_risk_monitoring'] = 0
        recommendations['create_escalation_procedures'] = 0
    
    return recommendations

def train_completeness_recommendation_model(client_name=''):
    """
    Train Completeness Test Recommendation Model
    """
    print("🚀 Starting Completeness Test Recommendation Model Training")
    print("=" * 60)
    
    # Extract features
    feature_data = extract_completeness_features()
    if feature_data is None:
        return {'success': False, 'error': 'Insufficient data for training'}
    
    features_df = feature_data['features']
    recommendations_df = feature_data['recommendations']
    feature_names = feature_data['feature_names']
    recommendation_names = feature_data['recommendation_names']
    
    # Create model record
    model_name = f"completeness_recommendation_{client_name.lower().replace(' ', '_') if client_name else 'general'}"
    
    ai_model, created = AICompletenessModel.objects.get_or_create(
        model_name=model_name,
        model_version='1.0.0',
        client_name=client_name or '',
        defaults={
            'model_type': 'CLIENT_PATTERN_LEARNER',
            'status': 'TRAINING',
            'training_started_at': timezone.now(),
            'training_data_size': len(features_df)
        }
    )
    
    if not created:
        ai_model.status = 'TRAINING'
        ai_model.training_started_at = timezone.now()
        ai_model.training_data_size = len(features_df)
        ai_model.save()
        print(f"🔄 Updating existing model: {model_name}")
    else:
        print(f"🆕 Creating new model: {model_name}")
    
    # Prepare data
    X = features_df.values
    y = recommendations_df.values
    
    # Train models for each recommendation category
    models = {}
    results = {}
    
    for i, rec_name in enumerate(recommendation_names):
        print(f"\n📋 Training recommendation model for: {rec_name}")
        
        y_rec = y[:, i]
        
        # Skip if all values are the same
        if len(np.unique(y_rec)) < 2:
            print(f"   ⚠️  Skipping {rec_name} - insufficient variation")
            continue
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y_rec, test_size=0.3, random_state=42
        )
        
        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # Train model
        model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42
        )
        
        model.fit(X_train_scaled, y_train)
        
        # Make predictions
        y_pred_train = model.predict(X_train_scaled)
        y_pred_test = model.predict(X_test_scaled)
        
        # Calculate metrics
        train_accuracy = accuracy_score(y_train, y_pred_train)
        test_accuracy = accuracy_score(y_test, y_pred_test)
        
        models[rec_name] = {
            'model': model,
            'scaler': scaler,
            'train_accuracy': train_accuracy,
            'test_accuracy': test_accuracy
        }
        
        results[rec_name] = {
            'train_accuracy': train_accuracy,
            'test_accuracy': test_accuracy
        }
        
        print(f"   ✅ {rec_name} - Train Accuracy: {train_accuracy:.3f}, Test Accuracy: {test_accuracy:.3f}")
    
    # Save models
    model_dir = os.path.join('.', 'trained_models')
    os.makedirs(model_dir, exist_ok=True)
    
    model_files = {}
    for rec_name, model_data in models.items():
        model_path = os.path.join(model_dir, f'{model_name}_{rec_name}_model.joblib')
        scaler_path = os.path.join(model_dir, f'{model_name}_{rec_name}_scaler.joblib')
        
        joblib.dump(model_data['model'], model_path)
        joblib.dump(model_data['scaler'], scaler_path)
        
        model_files[rec_name] = {
            'model_path': model_path,
            'scaler_path': scaler_path
        }
    
    # Update model record
    training_duration = (timezone.now() - ai_model.training_started_at).total_seconds()
    
    ai_model.feature_set = feature_names
    ai_model.model_file_path = str(model_files)
    ai_model.training_accuracy = np.mean([r['test_accuracy'] for r in results.values()]) * 100
    ai_model.validation_accuracy = np.mean([r['train_accuracy'] for r in results.values()]) * 100
    ai_model.status = 'TRAINED'
    ai_model.training_completed_at = timezone.now()
    ai_model.training_duration = training_duration
    ai_model.save()
    
    print("\n" + "=" * 60)
    print("🎉 Completeness Test Recommendation Model Training Complete!")
    print(f"📊 Model: {model_name}")
    print(f"⏱️  Training Duration: {training_duration:.1f} seconds")
    print(f"📈 Average Test Accuracy: {ai_model.training_accuracy:.1f}%")
    print(f"💾 Models saved to: {model_dir}")
    print(f"📋 Recommendation Categories: {len(results)}")
    
    return {
        'success': True,
        'model_name': model_name,
        'training_duration': training_duration,
        'results': results,
        'model_files': model_files,
        'feature_count': len(feature_names),
        'recommendation_count': len(results),
        'training_samples': len(features_df)
    }

if __name__ == "__main__":
    print("🎯 Training Completeness Test Recommendation Model")
    
    result = train_completeness_recommendation_model()
    
    if result['success']:
        print(f"\n✅ Training completed successfully!")
        print(f"📋 Recommendation categories: {result['recommendation_count']}")
        print(f"🎯 Average accuracy: {result['results'][list(result['results'].keys())[0]]['test_accuracy']:.1%}")
    else:
        print(f"\n❌ Training failed: {result['error']}")
