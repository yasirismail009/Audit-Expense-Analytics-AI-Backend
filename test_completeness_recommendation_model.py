#!/usr/bin/env python
"""
Test Script for Completeness Recommendation ML Model

This script demonstrates the completeness recommendation model functionality
including training and prediction capabilities.
"""

import os
import sys
import django
import json
import logging
from datetime import datetime, timedelta

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.ml_models import CompletenessRecommendationModel

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_sample_historical_data():
    """Create sample historical data for model training"""
    
    sample_data = [
        # Successful completeness test
        {
            'file_size_mb': 15.2,
            'gl_records_count': 5000,
            'tb_records_count': 120,
            'total_documents': 450,
            'unique_documents': 445,
            'duplicate_ratio': 0.01,
            'missing_dates_ratio': 0.0,
            'missing_amounts_ratio': 0.0,
            'invalid_accounts_ratio': 0.0,
            'zero_amount_ratio': 0.05,
            'user_count': 8,
            'account_count': 120,
            'months_covered': 12,
            'transaction_types_count': 4,
            'currency_count': 1,
            'trial_balance_matches': 1,
            'opening_balance_available': 1,
            'closing_balance_calculated': 1,
            'balance_discrepancy_ratio': 0.001,
            'previous_completeness_score': 88,
            'client_avg_score': 85,
            'processing_duration_minutes': 15,
            'unrecognized_accounts_count': 0,
            'inactive_accounts_used': 0,
            'account_hierarchy_issues': 0,
            'final_completeness_score': 92,
            'critical_issues_count': 0,
            'primary_issue_category': 'data_quality'
        },
        
        # Failed completeness test - missing data issues
        {
            'file_size_mb': 8.5,
            'gl_records_count': 3200,
            'tb_records_count': 0,  # Missing trial balance
            'total_documents': 280,
            'unique_documents': 275,
            'duplicate_ratio': 0.02,
            'missing_dates_ratio': 0.08,  # High missing dates
            'missing_amounts_ratio': 0.03,  # Missing amounts
            'invalid_accounts_ratio': 0.0,
            'zero_amount_ratio': 0.12,
            'user_count': 5,
            'account_count': 95,
            'months_covered': 9,
            'transaction_types_count': 3,
            'currency_count': 1,
            'trial_balance_matches': 0,  # No trial balance
            'opening_balance_available': 0,  # Missing opening balances
            'closing_balance_calculated': 1,
            'balance_discrepancy_ratio': 0.15,  # High discrepancy
            'previous_completeness_score': 65,
            'client_avg_score': 70,
            'processing_duration_minutes': 45,
            'unrecognized_accounts_count': 0,
            'inactive_accounts_used': 0,
            'account_hierarchy_issues': 0,
            'final_completeness_score': 58,
            'critical_issues_count': 3,
            'primary_issue_category': 'missing_data'
        },
        
        # Failed completeness test - account verification issues
        {
            'file_size_mb': 22.1,
            'gl_records_count': 7800,
            'tb_records_count': 180,
            'total_documents': 620,
            'unique_documents': 610,
            'duplicate_ratio': 0.015,
            'missing_dates_ratio': 0.01,
            'missing_amounts_ratio': 0.0,
            'invalid_accounts_ratio': 0.08,  # High invalid accounts
            'zero_amount_ratio': 0.04,
            'user_count': 12,
            'account_count': 180,
            'months_covered': 12,
            'transaction_types_count': 5,
            'currency_count': 2,
            'trial_balance_matches': 1,
            'opening_balance_available': 1,
            'closing_balance_calculated': 1,
            'balance_discrepancy_ratio': 0.005,
            'previous_completeness_score': 75,
            'client_avg_score': 78,
            'processing_duration_minutes': 35,
            'unrecognized_accounts_count': 15,  # Unrecognized accounts
            'inactive_accounts_used': 3,
            'account_hierarchy_issues': 2,
            'final_completeness_score': 64,
            'critical_issues_count': 2,
            'primary_issue_category': 'account_verification'
        },
        
        # Failed completeness test - balance reconciliation issues
        {
            'file_size_mb': 18.7,
            'gl_records_count': 6500,
            'tb_records_count': 150,
            'total_documents': 520,
            'unique_documents': 515,
            'duplicate_ratio': 0.008,
            'missing_dates_ratio': 0.0,
            'missing_amounts_ratio': 0.0,
            'invalid_accounts_ratio': 0.01,
            'zero_amount_ratio': 0.06,
            'user_count': 10,
            'account_count': 150,
            'months_covered': 12,
            'transaction_types_count': 4,
            'currency_count': 1,
            'trial_balance_matches': 0,  # Trial balance doesn't match
            'opening_balance_available': 1,
            'closing_balance_calculated': 1,
            'balance_discrepancy_ratio': 0.25,  # Very high discrepancy
            'previous_completeness_score': 72,
            'client_avg_score': 76,
            'processing_duration_minutes': 55,
            'unrecognized_accounts_count': 2,
            'inactive_accounts_used': 0,
            'account_hierarchy_issues': 1,
            'final_completeness_score': 45,
            'critical_issues_count': 4,
            'primary_issue_category': 'balance_reconciliation'
        },
        
        # Successful completeness test - large dataset
        {
            'file_size_mb': 45.8,
            'gl_records_count': 15000,
            'tb_records_count': 250,
            'total_documents': 1200,
            'unique_documents': 1195,
            'duplicate_ratio': 0.003,
            'missing_dates_ratio': 0.0,
            'missing_amounts_ratio': 0.0,
            'invalid_accounts_ratio': 0.0,
            'zero_amount_ratio': 0.02,
            'user_count': 25,
            'account_count': 250,
            'months_covered': 12,
            'transaction_types_count': 6,
            'currency_count': 3,
            'trial_balance_matches': 1,
            'opening_balance_available': 1,
            'closing_balance_calculated': 1,
            'balance_discrepancy_ratio': 0.0005,
            'previous_completeness_score': 90,
            'client_avg_score': 88,
            'processing_duration_minutes': 25,
            'unrecognized_accounts_count': 0,
            'inactive_accounts_used': 0,
            'account_hierarchy_issues': 0,
            'final_completeness_score': 95,
            'critical_issues_count': 0,
            'primary_issue_category': 'data_quality'
        }
    ]
    
    # Duplicate the data to have enough training samples
    extended_data = []
    for i in range(20):  # Create 100 samples (20 * 5)
        for sample in sample_data:
            # Add some variation to the data
            varied_sample = sample.copy()
            varied_sample['file_size_mb'] *= (0.8 + 0.4 * (i / 20))
            varied_sample['gl_records_count'] = int(varied_sample['gl_records_count'] * (0.9 + 0.2 * (i / 20)))
            varied_sample['processing_duration_minutes'] *= (0.7 + 0.6 * (i / 20))
            extended_data.append(varied_sample)
    
    return extended_data

def test_model_training():
    """Test the model training functionality"""
    print("\n" + "="*80)
    print("🤖 TESTING COMPLETENESS RECOMMENDATION MODEL TRAINING")
    print("="*80)
    
    # Create sample data
    historical_data = create_sample_historical_data()
    print(f"📊 Created {len(historical_data)} historical records for training")
    
    # Initialize and train the model
    model = CompletenessRecommendationModel()
    print("🚀 Starting model training...")
    
    training_results = model.train_completeness_recommendation_model(historical_data)
    
    print("\n📈 TRAINING RESULTS:")
    print("-" * 50)
    print(f"Failure Prediction Accuracy: {training_results['failure_prediction_metrics']['accuracy']:.3f}")
    print(f"Failure Prediction F1-Score: {training_results['failure_prediction_metrics']['f1']:.3f}")
    print(f"Recommendation Accuracy: {training_results['recommendation_accuracy']:.3f}")
    print(f"Best Failure Model: {training_results['best_failure_model']}")
    print(f"Best Recommendation Model: {training_results['best_recommendation_model']}")
    print(f"Training Data Size: {training_results['training_data_size']}")
    print(f"Feature Count: {training_results['feature_count']}")
    
    print("\n🎯 FEATURE IMPORTANCE (Top 10):")
    print("-" * 50)
    if training_results['feature_importance'].get('failure'):
        sorted_features = sorted(
            training_results['feature_importance']['failure'].items(),
            key=lambda x: x[1],
            reverse=True
        )[:10]
        for feature, importance in sorted_features:
            print(f"{feature:<30}: {importance:.4f}")
    
    return model, training_results

def test_model_prediction(model):
    """Test the model prediction functionality"""
    print("\n" + "="*80)
    print("🔮 TESTING COMPLETENESS PREDICTION AND RECOMMENDATIONS")
    print("="*80)
    
    # Test scenarios
    test_scenarios = [
        {
            'name': 'High-Risk Scenario (Missing Trial Balance + Account Issues)',
            'features': {
                'file_size_mb': 12.5,
                'gl_records_count': 4500,
                'tb_records_count': 0,  # Missing trial balance
                'total_documents': 380,
                'unique_documents': 375,
                'duplicate_ratio': 0.08,  # High duplicates
                'missing_dates_ratio': 0.05,
                'missing_amounts_ratio': 0.03,
                'invalid_accounts_ratio': 0.06,  # Invalid accounts
                'zero_amount_ratio': 0.10,
                'user_count': 6,
                'account_count': 110,
                'months_covered': 8,
                'transaction_types_count': 3,
                'currency_count': 1,
                'trial_balance_matches': 0,
                'opening_balance_available': 0,
                'closing_balance_calculated': 1,
                'balance_discrepancy_ratio': 0.20,  # High discrepancy
                'previous_completeness_score': 60,
                'client_avg_score': 65,
                'processing_duration_minutes': 0,
                'unrecognized_accounts_count': 12,  # Many unrecognized accounts
                'inactive_accounts_used': 2,
                'account_hierarchy_issues': 3
            }
        },
        {
            'name': 'Medium-Risk Scenario (Minor Data Quality Issues)',
            'features': {
                'file_size_mb': 20.3,
                'gl_records_count': 7200,
                'tb_records_count': 160,
                'total_documents': 580,
                'unique_documents': 575,
                'duplicate_ratio': 0.04,  # Some duplicates
                'missing_dates_ratio': 0.01,
                'missing_amounts_ratio': 0.0,
                'invalid_accounts_ratio': 0.02,
                'zero_amount_ratio': 0.05,
                'user_count': 11,
                'account_count': 160,
                'months_covered': 12,
                'transaction_types_count': 4,
                'currency_count': 1,
                'trial_balance_matches': 1,
                'opening_balance_available': 1,
                'closing_balance_calculated': 1,
                'balance_discrepancy_ratio': 0.02,  # Minor discrepancy
                'previous_completeness_score': 80,
                'client_avg_score': 82,
                'processing_duration_minutes': 0,
                'unrecognized_accounts_count': 3,
                'inactive_accounts_used': 0,
                'account_hierarchy_issues': 1
            }
        },
        {
            'name': 'Low-Risk Scenario (Clean Data)',
            'features': {
                'file_size_mb': 35.7,
                'gl_records_count': 12000,
                'tb_records_count': 220,
                'total_documents': 950,
                'unique_documents': 948,
                'duplicate_ratio': 0.005,  # Very low duplicates
                'missing_dates_ratio': 0.0,
                'missing_amounts_ratio': 0.0,
                'invalid_accounts_ratio': 0.0,
                'zero_amount_ratio': 0.02,
                'user_count': 18,
                'account_count': 220,
                'months_covered': 12,
                'transaction_types_count': 5,
                'currency_count': 2,
                'trial_balance_matches': 1,
                'opening_balance_available': 1,
                'closing_balance_calculated': 1,
                'balance_discrepancy_ratio': 0.001,  # Very low discrepancy
                'previous_completeness_score': 92,
                'client_avg_score': 90,
                'processing_duration_minutes': 0,
                'unrecognized_accounts_count': 0,
                'inactive_accounts_used': 0,
                'account_hierarchy_issues': 0
            }
        }
    ]
    
    for i, scenario in enumerate(test_scenarios, 1):
        print(f"\n📋 SCENARIO {i}: {scenario['name']}")
        print("-" * 60)
        
        # Get predictions
        results = model.predict_completeness_issues(scenario['features'])
        
        # Display failure prediction
        failure_pred = results['failure_prediction']
        print(f"🚨 Failure Prediction:")
        print(f"   Will Fail: {failure_pred['will_fail']}")
        print(f"   Failure Probability: {failure_pred['failure_probability']:.3f}")
        print(f"   Confidence: {failure_pred['confidence']:.3f}")
        
        # Display risk assessment
        risk = results['risk_assessment']
        print(f"\n⚠️  Risk Assessment:")
        print(f"   Overall Risk Score: {risk['overall_risk_score']:.3f}")
        print(f"   Risk Level: {risk['risk_level']}")
        print(f"   Mitigation Urgency: {risk['mitigation_urgency']}")
        
        # Display top priority actions
        print(f"\n📝 Top Priority Actions:")
        for j, action in enumerate(results['priority_actions'][:3], 1):
            print(f"   {j}. [{action['priority']}] {action['recommendation']}")
            print(f"      Time: {action['estimated_time_hours']:.1f}h | Success: {action['success_probability']:.0%}")
        
        # Display completion time estimate
        time_est = results['estimated_completion_time']
        print(f"\n⏱️  Time Estimates:")
        print(f"   Total Hours: {time_est['total_hours']:.1f}")
        print(f"   Critical Path: {time_est['critical_path_hours']:.1f}")
        print(f"   Estimated Days: {time_est['estimated_days']:.1f}")
        
        # Display success potential
        success_pot = results['success_improvement_potential']
        print(f"\n📊 Success Potential:")
        print(f"   Current Score: {success_pot['current_estimated_score']:.0f}%")
        print(f"   Potential Final Score: {success_pot['estimated_final_score']:.0f}%")
        print(f"   Success Probability: {success_pot['success_probability']:.0%}")

def test_account_verification_scenarios(model):
    """Test specific account verification failure scenarios"""
    print("\n" + "="*80)
    print("🏦 TESTING ACCOUNT VERIFICATION SCENARIOS")
    print("="*80)
    
    verification_scenarios = [
        {
            'name': 'Many Unrecognized Accounts',
            'features': {
                'file_size_mb': 25.0,
                'gl_records_count': 8000,
                'tb_records_count': 180,
                'total_documents': 650,
                'unique_documents': 645,
                'duplicate_ratio': 0.02,
                'missing_dates_ratio': 0.0,
                'missing_amounts_ratio': 0.0,
                'invalid_accounts_ratio': 0.12,  # High invalid account ratio
                'zero_amount_ratio': 0.04,
                'user_count': 15,
                'account_count': 180,
                'months_covered': 12,
                'transaction_types_count': 4,
                'currency_count': 1,
                'trial_balance_matches': 1,
                'opening_balance_available': 1,
                'closing_balance_calculated': 1,
                'balance_discrepancy_ratio': 0.01,
                'previous_completeness_score': 75,
                'client_avg_score': 78,
                'processing_duration_minutes': 0,
                'unrecognized_accounts_count': 25,  # Many unrecognized accounts
                'inactive_accounts_used': 5,
                'account_hierarchy_issues': 8
            }
        },
        {
            'name': 'Account Hierarchy Issues',
            'features': {
                'file_size_mb': 18.5,
                'gl_records_count': 6200,
                'tb_records_count': 140,
                'total_documents': 480,
                'unique_documents': 475,
                'duplicate_ratio': 0.015,
                'missing_dates_ratio': 0.0,
                'missing_amounts_ratio': 0.0,
                'invalid_accounts_ratio': 0.05,
                'zero_amount_ratio': 0.06,
                'user_count': 12,
                'account_count': 140,
                'months_covered': 10,
                'transaction_types_count': 4,
                'currency_count': 1,
                'trial_balance_matches': 1,
                'opening_balance_available': 1,
                'closing_balance_calculated': 1,
                'balance_discrepancy_ratio': 0.008,
                'previous_completeness_score': 78,
                'client_avg_score': 80,
                'processing_duration_minutes': 0,
                'unrecognized_accounts_count': 8,
                'inactive_accounts_used': 3,
                'account_hierarchy_issues': 15  # Many hierarchy issues
            }
        }
    ]
    
    for i, scenario in enumerate(verification_scenarios, 1):
        print(f"\n🔍 VERIFICATION SCENARIO {i}: {scenario['name']}")
        print("-" * 60)
        
        results = model.predict_completeness_issues(scenario['features'])
        
        # Focus on account verification recommendations
        account_recs = [rec for rec in results['recommendations'] if rec['category'] == 'account_verification']
        
        print(f"🏦 Account Verification Recommendations ({len(account_recs)} found):")
        for j, rec in enumerate(account_recs, 1):
            print(f"   {j}. [{rec['priority']}] {rec['recommendation']}")
            print(f"      Estimated Time: {rec['estimated_time_hours']:.1f} hours")
            print(f"      Success Probability: {rec['success_probability']:.0%}")
            print(f"      Implementation Difficulty: {rec['implementation_difficulty']}")
            if rec.get('dependencies'):
                print(f"      Dependencies: {', '.join(rec['dependencies'])}")
            if rec.get('required_resources'):
                print(f"      Required Resources: {', '.join(rec['required_resources'])}")
            print()

def main():
    """Main test function"""
    print("🚀 STARTING COMPLETENESS RECOMMENDATION MODEL TESTING")
    print("=" * 80)
    
    try:
        # Test model training
        model, training_results = test_model_training()
        
        # Test predictions
        test_model_prediction(model)
        
        # Test account verification scenarios
        test_account_verification_scenarios(model)
        
        print("\n" + "="*80)
        print("✅ ALL TESTS COMPLETED SUCCESSFULLY!")
        print("="*80)
        
        print("\n📋 SUMMARY:")
        print(f"• Model trained with {training_results['training_data_size']} records")
        print(f"• Failure prediction accuracy: {training_results['failure_prediction_metrics']['accuracy']:.1%}")
        print(f"• Recommendation accuracy: {training_results['recommendation_accuracy']:.1%}")
        print(f"• Features analyzed: {training_results['feature_count']}")
        print(f"• Model confidence: {training_results['model_confidence']['failure_prediction']:.1%}")
        
        print("\n🎯 KEY CAPABILITIES DEMONSTRATED:")
        print("• Completeness failure prediction before full testing")
        print("• Specific account verification issue detection")
        print("• Prioritized action recommendations with time estimates")
        print("• Risk assessment and mitigation urgency calculation")
        print("• Success probability estimation after remediation")
        print("• Resource and dependency identification for recommendations")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
