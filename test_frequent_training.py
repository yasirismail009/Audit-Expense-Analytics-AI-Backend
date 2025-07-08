#!/usr/bin/env python3
"""
Test script for frequent model training functionality

This script demonstrates:
1. Auto-training when sheets are uploaded
2. Auto-training before analysis
3. Scheduled training command
4. Performance-based training triggers
"""

import os
import sys
import django
import requests
import time
from datetime import datetime, timedelta

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import ExpenseSheet, Expense
from core.analytics import ExpenseSheetAnalyzer
from django.core.management import call_command

def test_auto_training_on_upload():
    """Test auto-training when sheets are uploaded"""
    print("=== Testing Auto-Training on Sheet Upload ===")
    
    # Create test CSV data
    csv_data = """Date,Category,Subcategory,Description,Employee,Department,Amount,Currency,Payment Method,Vendor/Supplier,Receipt Number,Status,Approved By,Notes
01/15/2024,Travel,Flight,Airline ticket to conference,John Doe,Sales,500.00,USD,Credit Card,Delta Airlines,RC001,Approved,Manager A,Conference travel
01/16/2024,Meals,Lunch,Client lunch meeting,Jane Smith,Marketing,75.50,USD,Credit Card,Restaurant ABC,RC002,Approved,Manager B,Client meeting
01/17/2024,Office Supplies,Equipment,Printer ink cartridges,Bob Johnson,Engineering,120.00,USD,Credit Card,Office Depot,RC003,Approved,Manager C,Office supplies
01/18/2024,Software,License,Adobe Creative Suite,Alice Brown,Design,299.99,USD,Credit Card,Adobe Inc,RC004,Approved,Manager D,Design software
01/19/2024,Travel,Hotel,Conference hotel stay,John Doe,Sales,250.00,USD,Credit Card,Hilton Hotel,RC005,Approved,Manager A,Conference accommodation"""
    
    # Save to temporary file
    with open('test_sheet_upload.csv', 'w') as f:
        f.write(csv_data)
    
    # Upload via API
    base_url = 'http://localhost:8000'
    with open('test_sheet_upload.csv', 'rb') as f:
        files = {'file': ('test_sheet_upload.csv', f, 'text/csv')}
        response = requests.post(f'{base_url}/expenses/upload/', files=files)
    
    if response.status_code == 201:
        result = response.json()
        print(f"✅ Sheet uploaded successfully")
        print(f"   Training Status: {result.get('training_status', 'Unknown')}")
        print(f"   Sheet Info: {result.get('sheet_info', {})}")
    else:
        print(f"❌ Upload failed: {response.text}")
    
    # Cleanup
    os.remove('test_sheet_upload.csv')

def test_auto_training_before_analysis():
    """Test auto-training before sheet analysis"""
    print("\n=== Testing Auto-Training Before Analysis ===")
    
    # Get the first sheet
    try:
        sheet = ExpenseSheet.objects.first()
        if sheet:
            base_url = 'http://localhost:8000'
            response = requests.post(f'{base_url}/sheets/{sheet.id}/analyze/')
            
            if response.status_code == 200:
                result = response.json()
                print(f"✅ Analysis completed successfully")
                print(f"   Training Status: {result.get('training_status', 'Unknown')}")
                print(f"   Fraud Score: {result.get('analysis_summary', {}).get('overall_fraud_score', 0)}")
            else:
                print(f"❌ Analysis failed: {response.text}")
        else:
            print("ℹ️  No sheets available for testing")
    except Exception as e:
        print(f"❌ Error testing analysis: {e}")

def test_scheduled_training():
    """Test scheduled training command"""
    print("\n=== Testing Scheduled Training Command ===")
    
    try:
        # Run scheduled training command
        call_command('scheduled_training', '--days', '7')
        print("✅ Scheduled training command executed")
    except Exception as e:
        print(f"❌ Scheduled training failed: {e}")

def test_force_training():
    """Test force training command"""
    print("\n=== Testing Force Training Command ===")
    
    try:
        # Run force training command
        call_command('scheduled_training', '--force')
        print("✅ Force training command executed")
    except Exception as e:
        print(f"❌ Force training failed: {e}")

def test_performance_based_training():
    """Test performance-based training triggers"""
    print("\n=== Testing Performance-Based Training ===")
    
    try:
        analyzer = ExpenseSheetAnalyzer()
        
        # Check if performance suggests retraining
        should_retrain = analyzer.evaluate_model_performance()
        print(f"Performance check suggests retraining: {should_retrain}")
        
        if should_retrain:
            print("🔄 Performance issues detected, retraining models...")
            success = analyzer.train_models()
            if success:
                print("✅ Models retrained due to performance issues")
            else:
                print("❌ Performance-based retraining failed")
        else:
            print("✅ Model performance is acceptable")
            
    except Exception as e:
        print(f"❌ Performance check failed: {e}")

def test_training_configuration():
    """Test training configuration and thresholds"""
    print("\n=== Testing Training Configuration ===")
    
    try:
        analyzer = ExpenseSheetAnalyzer()
        
        print("Training Configuration:")
        for key, value in analyzer.training_config.items():
            print(f"   {key}: {value}")
        
        # Test should_retrain logic
        should_retrain = analyzer.should_retrain()
        print(f"Should retrain based on new data: {should_retrain}")
        
        # Check model readiness
        models_ready = analyzer.ensure_models_ready()
        print(f"Models ready for prediction: {models_ready}")
        
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")

def test_bulk_analysis_with_training():
    """Test bulk analysis with auto-training"""
    print("\n=== Testing Bulk Analysis with Auto-Training ===")
    
    try:
        base_url = 'http://localhost:8000'
        response = requests.post(f'{base_url}/analysis/bulk/')
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Bulk analysis completed")
            print(f"   Training Status: {result.get('training_status', 'Unknown')}")
            print(f"   Summary: {result.get('summary', {})}")
        else:
            print(f"❌ Bulk analysis failed: {response.text}")
            
    except Exception as e:
        print(f"❌ Bulk analysis test failed: {e}")

def main():
    """Run all tests"""
    print("🚀 Testing Frequent Model Training Functionality")
    print("=" * 60)
    
    # Test 1: Auto-training on upload
    test_auto_training_on_upload()
    
    # Test 2: Auto-training before analysis
    test_auto_training_before_analysis()
    
    # Test 3: Scheduled training
    test_scheduled_training()
    
    # Test 4: Force training
    test_force_training()
    
    # Test 5: Performance-based training
    test_performance_based_training()
    
    # Test 6: Training configuration
    test_training_configuration()
    
    # Test 7: Bulk analysis with training
    test_bulk_analysis_with_training()
    
    print("\n" + "=" * 60)
    print("✅ All tests completed!")

if __name__ == '__main__':
    main() 