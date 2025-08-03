#!/usr/bin/env python
"""
Script to train the Holiday Analysis ML model using rule-based results as training data
"""

import os
import sys
import django
from datetime import datetime
import numpy as np
import pandas as pd

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import SAPGLPosting, DataFile, HolidayAnalysisResult
from core.ml_analysis_orchestrator import MLAnalysisOrchestrator
from core.specialized_analysis_models import AnalysisModelManager
from core.holiday_utils import get_holidays, is_holiday
from django.utils import timezone

def generate_training_labels_from_rules(transactions):
    """
    Generate training labels using rule-based holiday detection
    
    Args:
        transactions: List of SAPGLPosting objects
        
    Returns:
        List of binary labels (0 = not holiday, 1 = holiday)
    """
    print("Generating training labels using rule-based logic...")
    
    # Get data file information
    data_file = None
    if transactions:
        data_file = transactions[0].data_file
    
    # Default to Saudi Arabian holidays
    country_code = 'saudiarabian'
    start_date = None
    end_date = None
    
    if data_file:
        # Use fiscal year dates from data file
        if data_file.audit_start_date and data_file.audit_end_date:
            start_date = data_file.audit_start_date
            end_date = data_file.audit_end_date
        else:
            # Fallback to fiscal year
            start_date = datetime(data_file.fiscal_year, 1, 1).date()
            end_date = datetime(data_file.fiscal_year, 12, 31).date()
    
    # Get holidays for the date range
    try:
        holidays = get_holidays(country_code, start_date, end_date, include_observances=True)
        holiday_dates = {h.date for h in holidays}
        print(f"Retrieved {len(holidays)} holidays for {country_code} from {start_date} to {end_date}")
    except Exception as e:
        print(f"Error retrieving holidays: {e}")
        holiday_dates = set()
    
    # Generate labels based on rule-based logic
    labels = []
    holiday_count = 0
    
    for transaction in transactions:
        if transaction.posting_date and transaction.posting_date in holiday_dates:
            labels.append(1)  # Holiday posting
            holiday_count += 1
        else:
            labels.append(0)  # Not a holiday posting
    
    print(f"Generated labels: {len(labels)} total, {holiday_count} holiday postings ({holiday_count/len(labels)*100:.2f}%)")
    
    return labels

def calculate_enhanced_risk_scores(transactions, labels):
    """
    Calculate enhanced risk scores for training data
    
    Args:
        transactions: List of SAPGLPosting objects
        labels: List of binary labels
        
    Returns:
        List of enhanced risk scores
    """
    print("Calculating enhanced risk scores...")
    
    risk_scores = []
    
    for i, transaction in enumerate(transactions):
        base_score = 0.0
        
        # Base score from label
        if labels[i] == 1:
            base_score += 30.0  # Base holiday risk
        
        # Risk based on transaction amount
        amount = float(transaction.amount_local_currency)
        if amount > 1000000:  # Over 1M SAR
            base_score += 30.0
        elif amount > 100000:  # Over 100K SAR
            base_score += 15.0
        elif amount > 10000:  # Over 10K SAR
            base_score += 5.0
        
        # Risk based on account type
        if transaction.is_expense_account:
            base_score += 10.0  # Higher risk for expense accounts
        
        # Risk based on transaction type
        if transaction.transaction_type == 'DEBIT':
            base_score += 5.0  # Slightly higher risk for debits
        
        # Risk based on posting period (month-end)
        if transaction.posting_period in [12, 6, 3, 9]:  # Quarter ends
            base_score += 5.0
        
        # Risk based on day of week (weekend-like patterns)
        if transaction.posting_date:
            day_of_week = transaction.posting_date.weekday()
            if day_of_week >= 5:  # Weekend
                base_score += 10.0
            elif day_of_week == 4:  # Friday
                base_score += 5.0
        
        # Cap the score at 100
        risk_scores.append(min(base_score, 100.0))
    
    print(f"Calculated risk scores: min={min(risk_scores):.1f}, max={max(risk_scores):.1f}, avg={np.mean(risk_scores):.1f}")
    
    return risk_scores

def train_holiday_ml_model(transactions, labels, risk_scores):
    """
    Train the Holiday Analysis ML model
    
    Args:
        transactions: List of SAPGLPosting objects
        labels: List of binary labels
        risk_scores: List of risk scores
        
    Returns:
        bool: Success status
    """
    print("Training Holiday Analysis ML model...")
    
    try:
        # Initialize model manager
        model_manager = AnalysisModelManager()
        
        # Get the holiday model
        holiday_model = model_manager.get_model('holiday')
        
        if not holiday_model:
            print("Error: Could not get holiday model")
            return False
        
        # Train the model
        print(f"Training with {len(transactions)} transactions, {sum(labels)} positive cases")
        
        success = holiday_model.train(transactions, labels)
        
        if success:
            print("✅ Holiday ML model trained successfully!")
            
            # Test the model
            print("Testing trained model...")
            predictions = holiday_model.predict(transactions)
            
            if predictions:
                # Calculate accuracy
                correct_predictions = 0
                for i, pred in enumerate(predictions):
                    if pred['prediction'] == labels[i]:
                        correct_predictions += 1
                
                accuracy = correct_predictions / len(predictions) if predictions else 0
                print(f"Model accuracy: {accuracy:.4f} ({correct_predictions}/{len(predictions)})")
                
                # Show some sample predictions
                print("\nSample predictions:")
                for i in range(min(10, len(predictions))):
                    pred = predictions[i]
                    actual = labels[i]
                    status = "✅" if pred['prediction'] == actual else "❌"
                    print(f"  {status} Transaction {i+1}: Predicted={pred['prediction']}, Actual={actual}, Confidence={pred['confidence']:.3f}")
            
            return True
        else:
            print("❌ Failed to train holiday ML model")
            return False
            
    except Exception as e:
        print(f"❌ Error training holiday ML model: {e}")
        import traceback
        traceback.print_exc()
        return False

def compare_rule_vs_ml_results(transactions, labels, risk_scores):
    """
    Compare rule-based vs ML-based results
    
    Args:
        transactions: List of SAPGLPosting objects
        labels: List of binary labels (rule-based)
        risk_scores: List of risk scores
    """
    print("\n" + "="*60)
    print("COMPARING RULE-BASED vs ML-BASED RESULTS")
    print("="*60)
    
    try:
        # Get ML predictions
        model_manager = AnalysisModelManager()
        holiday_model = model_manager.get_model('holiday')
        
        if not holiday_model or not holiday_model.is_trained:
            print("❌ ML model not available or not trained")
            return
        
        ml_predictions = holiday_model.predict(transactions)
        
        if not ml_predictions:
            print("❌ No ML predictions available")
            return
        
        # Compare results
        rule_holidays = sum(labels)
        ml_holidays = sum(pred['prediction'] for pred in ml_predictions)
        
        print(f"Rule-based detected: {rule_holidays} holiday postings")
        print(f"ML-based detected: {ml_holidays} holiday postings")
        
        # Find agreements and disagreements
        agreements = 0
        rule_only = 0
        ml_only = 0
        
        for i, pred in enumerate(ml_predictions):
            rule_label = labels[i]
            ml_label = pred['prediction']
            
            if rule_label == ml_label:
                agreements += 1
            elif rule_label == 1 and ml_label == 0:
                rule_only += 1
            elif rule_label == 0 and ml_label == 1:
                ml_only += 1
        
        print(f"\nAgreement Analysis:")
        print(f"  Agreements: {agreements} ({agreements/len(labels)*100:.1f}%)")
        print(f"  Rule-only detections: {rule_only}")
        print(f"  ML-only detections: {ml_only}")
        
        # Show high-confidence ML predictions that differ from rules
        print(f"\nHigh-confidence ML predictions that differ from rules:")
        high_conf_differences = []
        
        for i, pred in enumerate(ml_predictions):
            if pred['confidence'] > 0.8 and pred['prediction'] != labels[i]:
                high_conf_differences.append({
                    'index': i,
                    'transaction': transactions[i],
                    'rule_label': labels[i],
                    'ml_prediction': pred['prediction'],
                    'confidence': pred['confidence'],
                    'risk_score': risk_scores[i]
                })
        
        for diff in high_conf_differences[:5]:  # Show top 5
            t = diff['transaction']
            print(f"  Transaction {diff['index']}: Rule={diff['rule_label']}, ML={diff['ml_prediction']}, "
                  f"Confidence={diff['confidence']:.3f}, Amount={t.amount_local_currency}, "
                  f"Date={t.posting_date}, User={t.user_name}")
        
        # Calculate correlation between risk scores and ML confidence
        ml_confidences = [pred['confidence'] for pred in ml_predictions]
        correlation = np.corrcoef(risk_scores, ml_confidences)[0, 1]
        print(f"\nCorrelation between risk scores and ML confidence: {correlation:.3f}")
        
    except Exception as e:
        print(f"❌ Error comparing results: {e}")
        import traceback
        traceback.print_exc()

def save_training_results(transactions, labels, risk_scores, success):
    """
    Save training results and metadata
    
    Args:
        transactions: List of SAPGLPosting objects
        labels: List of binary labels
        risk_scores: List of risk scores
        success: Training success status
    """
    print("\nSaving training results...")
    
    try:
        # Create training metadata
        training_metadata = {
            'training_date': timezone.now().isoformat(),
            'total_transactions': len(transactions),
            'positive_cases': sum(labels),
            'negative_cases': len(labels) - sum(labels),
            'positive_percentage': sum(labels) / len(labels) * 100 if labels else 0,
            'risk_score_stats': {
                'min': min(risk_scores) if risk_scores else 0,
                'max': max(risk_scores) if risk_scores else 0,
                'mean': np.mean(risk_scores) if risk_scores else 0,
                'std': np.std(risk_scores) if risk_scores else 0
            },
            'training_success': success,
            'model_type': 'HolidayAnalysisModel',
            'algorithm': 'RandomForestClassifier',
            'features_used': [
                'transaction_type', 'day_of_week', 'day_of_month', 'month', 'quarter',
                'is_high_value', 'account_type_code', 'fiscal_year', 'posting_period',
                'user_numeric', 'amount_log'
            ]
        }
        
        # Save to file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"holiday_ml_training_results_{timestamp}.json"
        
        import json
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(training_metadata, f, indent=2, default=str)
        
        print(f"✅ Training results saved to: {filename}")
        
        # Print summary
        print(f"\n📊 Training Summary:")
        print(f"   Total Transactions: {training_metadata['total_transactions']}")
        print(f"   Holiday Postings: {training_metadata['positive_cases']} ({training_metadata['positive_percentage']:.2f}%)")
        print(f"   Non-Holiday Postings: {training_metadata['negative_cases']}")
        print(f"   Training Success: {'✅ Yes' if success else '❌ No'}")
        print(f"   Average Risk Score: {training_metadata['risk_score_stats']['mean']:.1f}")
        
    except Exception as e:
        print(f"❌ Error saving training results: {e}")

def main():
    """Main function to train the Holiday Analysis ML model"""
    print("HOLIDAY ANALYSIS ML MODEL TRAINING")
    print("="*50)
    
    # Get transactions from database
    print("Loading transactions from database...")
    transactions = list(SAPGLPosting.objects.all())
    
    if not transactions:
        print("❌ No transactions found in database")
        return
    
    print(f"Loaded {len(transactions)} transactions")
    
    # Generate training labels using rule-based logic
    labels = generate_training_labels_from_rules(transactions)
    
    if not labels or len(labels) != len(transactions):
        print("❌ Error generating training labels")
        return
    
    # Calculate enhanced risk scores
    risk_scores = calculate_enhanced_risk_scores(transactions, labels)
    
    # Train the ML model
    success = train_holiday_ml_model(transactions, labels, risk_scores)
    
    # Compare rule-based vs ML results
    if success:
        compare_rule_vs_ml_results(transactions, labels, risk_scores)
    
    # Save training results
    save_training_results(transactions, labels, risk_scores, success)
    
    print("\n" + "="*50)
    if success:
        print("✅ Holiday ML model training completed successfully!")
        print("The model is now ready for use in holiday analysis.")
    else:
        print("❌ Holiday ML model training failed.")
        print("The system will continue using rule-based logic.")

if __name__ == "__main__":
    main() 