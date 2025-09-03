#!/usr/bin/env python3
"""
Direct test of ML functions without Celery connection
"""

import sys
import os

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Set Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')

import django
django.setup()

from core.models import SAPGLPosting, MLModelTraining
from core.ml_models import MLModelTrainer

def test_ml_model_trainer_direct():
    """Test ML model trainer directly"""
    print("🔍 Testing ML Model Trainer Directly")
    print("=" * 50)
    
    try:
        # Initialize ML trainer
        ml_trainer = MLModelTrainer()
        print("✅ ML Model Trainer initialized successfully")
        
        # Get some transactions for testing
        transactions = list(SAPGLPosting.objects.all()[:10])
        print(f"✅ Retrieved {len(transactions)} transactions for testing")
        
        if transactions:
            # Test duplicate prediction
            print("\n🔍 Testing duplicate prediction...")
            duplicate_results = ml_trainer.predict_duplicates(transactions)
            print(f"✅ Duplicate prediction completed: {duplicate_results.get('duplicate_count', 0)} duplicates found")
            
            # Test backdated prediction
            print("\n🔍 Testing backdated prediction...")
            backdated_results = ml_trainer.predict_backdated(transactions)
            print(f"✅ Backdated prediction completed: {backdated_results.get('backdated_count', 0)} backdated found")
            
            # Test user anomaly prediction
            print("\n🔍 Testing user anomaly prediction...")
            user_results = ml_trainer.predict_user_anomalies(transactions)
            print(f"✅ User anomaly prediction completed: {user_results.get('anomaly_count', 0)} anomalies found")
            
            return True
        else:
            print("⚠️  No transactions found for testing")
            return False
            
    except Exception as e:
        print(f"❌ Error testing ML Model Trainer: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_ml_model_training_records():
    """Test ML model training records"""
    print("\n🔍 Testing ML Model Training Records")
    print("=" * 50)
    
    try:
        # Check existing ML training records
        training_records = MLModelTraining.objects.all()
        print(f"✅ Found {len(training_records)} ML training records")
        
        if training_records:
            for record in training_records[:3]:  # Show first 3
                print(f"   📊 {record.session_name} - {record.model_type} - {record.status}")
                print(f"      📅 Created: {record.created_at}")
                print(f"      📈 Performance: {record.performance_metrics}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error testing ML training records: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_ml_recommendations_generation():
    """Test ML recommendations generation"""
    print("\n🔍 Testing ML Recommendations Generation")
    print("=" * 50)
    
    try:
        # Test ML-specific recommendation generation
        from core.views import DuplicateAnalysisView
        
        # Create a mock analysis object
        class MockAnalysis:
            def __init__(self):
                self.duplicate_list = [{'transaction1': {'amount': 1000000}}]
                self.detection_method = 'trained_model'
                self.confidence_scores = [0.85, 0.92, 0.78]
                self.false_positive_indicators = [{'transaction_id': '123', 'confidence': 0.65}]
                self.model_accuracy = 0.87
        
        mock_analysis = MockAnalysis()
        
        # Test recommendation generation
        view = DuplicateAnalysisView()
        recommendations = view._generate_ml_recommendations(mock_analysis)
        
        print(f"✅ Generated {len(recommendations)} ML-specific recommendations")
        for i, rec in enumerate(recommendations[:3]):
            print(f"   {i+1}. {rec}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error testing ML recommendations: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main test function"""
    print("🎯 DIRECT ML FUNCTIONS TEST")
    print("=" * 70)
    
    # Test 1: ML Model Trainer
    ml_trainer_test = test_ml_model_trainer_direct()
    
    # Test 2: ML Training Records
    ml_records_test = test_ml_model_training_records()
    
    # Test 3: ML Recommendations
    ml_recommendations_test = test_ml_recommendations_generation()
    
    # Summary
    print("\n" + "=" * 70)
    print("🎯 DIRECT ML FUNCTIONS TEST SUMMARY")
    print("=" * 70)
    
    print(f"✅ ML Model Trainer: {'PASSED' if ml_trainer_test else 'FAILED'}")
    print(f"✅ ML Training Records: {'PASSED' if ml_records_test else 'FAILED'}")
    print(f"✅ ML Recommendations: {'PASSED' if ml_recommendations_test else 'FAILED'}")
    
    overall_success = ml_trainer_test and ml_records_test and ml_recommendations_test
    
    if overall_success:
        print("\n🎉 DIRECT ML FUNCTIONS WORKING EXCELLENTLY!")
        print("✅ ML model trainer is functional")
        print("✅ ML training records are accessible")
        print("✅ ML recommendations generation is working")
        print("✅ Core ML functionality is operational")
    else:
        print("\n⚠️  SOME ML FUNCTIONS NEED ATTENTION")
        print("Please review the failed tests above")
    
    return overall_success

if __name__ == "__main__":
    main()
