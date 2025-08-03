#!/usr/bin/env python3
"""
Test script for holiday model training integration
"""

import os
import sys
import django
from datetime import datetime, date

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, SAPGLPosting, MLModelTraining
from core.specialized_analysis_models import AnalysisModelManager
from core.holiday_utils import is_holiday
from django.utils import timezone

def test_holiday_model_training():
    """Test holiday model training integration"""
    print("HOLIDAY MODEL TRAINING INTEGRATION TEST")
    print("=" * 50)
    
    # Get transactions from database
    print("Loading transactions from database...")
    transactions = list(SAPGLPosting.objects.all())
    
    if not transactions:
        print("❌ No transactions found in database")
        return False
    
    print(f"✅ Loaded {len(transactions)} transactions")
    
    # Test 1: Generate holiday labels
    print("\n1. Testing holiday label generation...")
    labels = []
    try:
        # Default to Saudi Arabian holidays
        country_code = 'saudiarabian'
        for t in transactions:
            if t.posting_date:
                is_holiday_posting = is_holiday(country_code, t.posting_date)
                labels.append(1 if is_holiday_posting else 0)
            else:
                labels.append(0)
        print(f"✅ Generated {sum(labels)} holiday labels from {len(transactions)} transactions")
        print(f"   Holiday rate: {(sum(labels) / len(transactions)) * 100:.2f}%")
    except Exception as e:
        print(f"❌ Error generating holiday labels: {e}")
        return False
    
    # Test 2: Initialize model manager
    print("\n2. Testing model manager initialization...")
    try:
        model_manager = AnalysisModelManager()
        print("✅ Model manager initialized successfully")
    except Exception as e:
        print(f"❌ Error initializing model manager: {e}")
        return False
    
    # Test 3: Get holiday model
    print("\n3. Testing holiday model retrieval...")
    try:
        holiday_model = model_manager.get_model('holiday')
        if holiday_model:
            print("✅ Holiday model retrieved successfully")
            print(f"   Model name: {holiday_model.model_name}")
            print(f"   Is trained: {holiday_model.is_trained}")
        else:
            print("❌ Could not retrieve holiday model")
            return False
    except Exception as e:
        print(f"❌ Error retrieving holiday model: {e}")
        return False
    
    # Test 4: Train holiday model
    print("\n4. Testing holiday model training...")
    try:
        success = model_manager.ensure_model_trained('holiday', transactions, labels)
        if success:
            print("✅ Holiday model trained successfully")
            
            # Check if model is now trained
            holiday_model = model_manager.get_model('holiday')
            print(f"   Is trained after training: {holiday_model.is_trained}")
            
            # Test prediction
            print("\n5. Testing holiday model prediction...")
            predictions = holiday_model.predict(transactions[:10])  # Test with first 10 transactions
            print(f"✅ Generated {len(predictions)} predictions")
            
            # Show sample predictions
            for i, pred in enumerate(predictions[:3]):
                print(f"   Prediction {i+1}: {pred}")
                
        else:
            print("❌ Holiday model training failed")
            return False
    except Exception as e:
        print(f"❌ Error training holiday model: {e}")
        return False
    
    # Test 6: Test train_all_models integration
    print("\n6. Testing train_all_models integration...")
    try:
        results = model_manager.train_all_models(transactions)
        print("✅ train_all_models completed")
        print("   Results:")
        for model_type, success in results.items():
            print(f"     {model_type}: {'✅' if success else '❌'}")
        
        # Check if holiday is included
        if 'holiday' in results:
            print("✅ Holiday model training is included in train_all_models")
        else:
            print("❌ Holiday model training is NOT included in train_all_models")
            return False
            
    except Exception as e:
        print(f"❌ Error in train_all_models: {e}")
        return False
    
    # Test 7: Test predict_all integration
    print("\n7. Testing predict_all integration...")
    try:
        all_predictions = model_manager.predict_all(transactions[:5])  # Test with first 5 transactions
        print("✅ predict_all completed")
        print("   Available predictions:")
        for model_type, predictions in all_predictions.items():
            print(f"     {model_type}: {len(predictions)} predictions")
        
        # Check if holiday is included
        if 'holiday' in all_predictions:
            print("✅ Holiday predictions are included in predict_all")
        else:
            print("❌ Holiday predictions are NOT included in predict_all")
            return False
            
    except Exception as e:
        print(f"❌ Error in predict_all: {e}")
        return False
    
    print("\n" + "=" * 50)
    print("✅ ALL TESTS PASSED! Holiday model training is fully integrated.")
    print("The holiday model will now be trained automatically as part of the ML training process.")
    
    return True

def test_holiday_analysis_integration():
    """Test holiday analysis integration with ML models"""
    print("\nHOLIDAY ANALYSIS ML INTEGRATION TEST")
    print("=" * 50)
    
    # Get transactions
    transactions = list(SAPGLPosting.objects.all())
    if not transactions:
        print("❌ No transactions found")
        return False
    
    # Test holiday analysis with ML integration
    try:
        from core.ml_analysis_orchestrator import MLAnalysisOrchestrator
        orchestrator = MLAnalysisOrchestrator()
        
        print("1. Testing holiday analysis with ML models...")
        holiday_result = orchestrator.run_holiday_analysis(transactions)
        
        if holiday_result:
            print("✅ Holiday analysis completed successfully")
            print(f"   Holiday postings found: {len(holiday_result.get('holiday_postings', []))}")
            print(f"   Analysis info: {holiday_result.get('analysis_info', {})}")
        else:
            print("❌ Holiday analysis failed")
            return False
            
    except Exception as e:
        print(f"❌ Error in holiday analysis: {e}")
        return False
    
    print("\n✅ Holiday analysis ML integration test passed!")
    return True

if __name__ == "__main__":
    print("Starting holiday model training integration tests...")
    
    # Run tests
    success1 = test_holiday_model_training()
    success2 = test_holiday_analysis_integration()
    
    if success1 and success2:
        print("\n🎉 ALL INTEGRATION TESTS PASSED!")
        print("Holiday model training is now fully integrated into the system.")
        print("\nNext steps:")
        print("1. Upload and process a data file")
        print("2. Run ML model training (holiday model will be included)")
        print("3. Or use the specific holiday model training endpoint: POST /api/ml-model-training/train_holiday_model/")
    else:
        print("\n❌ Some tests failed. Please check the implementation.")
        sys.exit(1) 