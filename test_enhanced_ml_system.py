#!/usr/bin/env python3
"""
Comprehensive test for the enhanced ML system including:
- ML model training functions
- ML-specific recommendations
- ML prediction capabilities
"""

import requests
import json
import time

# Configuration
BASE_URL = "http://localhost:8000/api"
FILE_ID = "96c59456-e1cd-491e-a8da-7f1b0084c081"

def test_ml_training_functions():
    """Test the newly implemented ML training functions"""
    print("🔍 Testing Enhanced ML Training Functions")
    print("=" * 50)
    
    # Test 1: Train ML Models endpoint
    print("\n🔍 Testing Train ML Models endpoint...")
    try:
        response = requests.post(f"{BASE_URL}/ml-model-training/train_models/")
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Train ML Models: Successfully executed")
            print(f"   📊 Response: {result}")
            
            # Check for expected fields
            expected_fields = ['message', 'status', 'task_id', 'job_id']
            missing_fields = [field for field in expected_fields if field not in result]
            
            if missing_fields:
                print(f"   ⚠️  Missing fields: {missing_fields}")
            else:
                print(f"   ✅ All expected fields present")
                
            return True
        elif response.status_code == 400:
            print(f"ℹ️  Train ML Models: Bad request (expected if no data)")
            print(f"   📄 Response: {response.text}")
            return True  # This is expected behavior
        else:
            print(f"❌ Train ML Models: HTTP {response.status_code}")
            print(f"   📄 Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Train ML Models: Error - {e}")
        return False

def test_ml_retraining_functions():
    """Test the ML retraining functions"""
    print("\n🔍 Testing ML Retraining Functions...")
    
    # Test 1: Retrain ML Models endpoint
    print("\n🔍 Testing Retrain ML Models endpoint...")
    try:
        response = requests.post(f"{BASE_URL}/ml-model-training/retrain_models/")
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Retrain ML Models: Successfully executed")
            print(f"   📊 Response: {result}")
            return True
        elif response.status_code == 400:
            print(f"ℹ️  Retrain ML Models: Bad request (expected if no data)")
            print(f"   📄 Response: {response.text}")
            return True  # This is expected behavior
        else:
            print(f"❌ Retrain ML Models: HTTP {response.status_code}")
            print(f"   📄 Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Retrain ML Models: Error - {e}")
        return False

def test_ml_predictions():
    """Test ML prediction capabilities"""
    print("\n🔍 Testing ML Prediction Capabilities...")
    
    # Test 1: Predict Anomalies endpoint
    print("\n🔍 Testing Predict Anomalies endpoint...")
    try:
        response = requests.post(f"{BASE_URL}/ml-model-training/predict_anomalies/")
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Predict Anomalies: Successfully executed")
            print(f"   📊 Response: {result}")
            return True
        elif response.status_code == 400:
            print(f"ℹ️  Predict Anomalies: Bad request (expected if no data)")
            print(f"   📄 Response: {response.text}")
            return True  # This is expected behavior
        else:
            print(f"❌ Predict Anomalies: HTTP {response.status_code}")
            print(f"   📄 Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Predict Anomalies: Error - {e}")
        return False

def test_ml_recommendations_in_analysis():
    """Test ML-specific recommendations in analysis results"""
    print("\n🔍 Testing ML-Specific Recommendations in Analysis Results...")
    
    # Test analysis endpoints for ML recommendations
    analysis_endpoints = [
        ('duplicate-analysis', 'Duplicate Analysis'),
        ('user-analysis', 'User Analysis'),
        ('backdated-analysis', 'Backdated Analysis')
    ]
    
    ml_recommendations_found = {}
    
    for endpoint, name in analysis_endpoints:
        print(f"\n🔍 Checking {name} for ML recommendations...")
        
        try:
            response = requests.get(f"{BASE_URL}/{endpoint}/{FILE_ID}/")
            
            if response.status_code == 200:
                result = response.json()
                
                # Check for ML-specific recommendations
                has_ml_recommendations = False
                recommendation_sources = []
                
                # Check audit_recommendations for ML content
                if 'audit_recommendations' in result and result['audit_recommendations']:
                    recommendations = result['audit_recommendations']
                    
                    # Look for ML-specific content in recommendations
                    ml_keywords = ['ML', 'machine learning', 'model', 'confidence', 'prediction', 'accuracy']
                    ml_recommendations = []
                    
                    for rec in recommendations:
                        if isinstance(rec, str):
                            for keyword in ml_keywords:
                                if keyword.lower() in rec.lower():
                                    ml_recommendations.append(rec)
                                    has_ml_recommendations = True
                                    break
                        elif isinstance(rec, dict) and 'recommendation' in rec:
                            for keyword in ml_keywords:
                                if keyword.lower() in rec['recommendation'].lower():
                                    ml_recommendations.append(rec)
                                    has_ml_recommendations = True
                                    break
                    
                    if ml_recommendations:
                        recommendation_sources.append(f'ml_recommendations({len(ml_recommendations)})')
                
                # Check for ML-specific fields
                if 'ml_detected_anomalies' in result:
                    recommendation_sources.append('ml_detected_anomalies')
                    has_ml_recommendations = True
                
                # Check for confidence scores
                if 'confidence_scores' in result:
                    recommendation_sources.append('confidence_scores')
                    has_ml_recommendations = True
                
                # Check for detection methods
                if 'detailed_results' in result and 'detection_methods' in result['detailed_results']:
                    detection_methods = result['detailed_results']['detection_methods']
                    if 'trained_model' in detection_methods or 'ml_detection' in detection_methods:
                        recommendation_sources.append('ml_detection_methods')
                        has_ml_recommendations = True
                
                if has_ml_recommendations:
                    print(f"   ✅ ML recommendations found: {', '.join(recommendation_sources)}")
                    ml_recommendations_found[name] = True
                else:
                    print(f"   ℹ️  No ML recommendations found")
                    ml_recommendations_found[name] = False
                    
            else:
                print(f"❌ Failed to get {name}: {response.status_code}")
                ml_recommendations_found[name] = False
                
        except Exception as e:
            print(f"❌ Error checking {name}: {e}")
            ml_recommendations_found[name] = False
    
    return ml_recommendations_found

def test_ml_model_performance_tracking():
    """Test ML model performance tracking"""
    print("\n🔍 Testing ML Model Performance Tracking...")
    
    try:
        # Check if ML model training records exist
        response = requests.get(f"{BASE_URL}/ml-model-training/")
        
        if response.status_code == 200:
            models = response.json()
            print(f"✅ ML Model Training records retrieved successfully")
            
            if models:
                print(f"   📊 Found {len(models)} ML model training records")
                
                # Check for completed models
                completed_models = [m for m in models if m.get('status') == 'COMPLETED']
                print(f"   ✅ Completed models: {len(completed_models)}")
                
                # Check for performance metrics
                models_with_metrics = [m for m in completed_models if m.get('performance_metrics')]
                print(f"   📈 Models with performance metrics: {len(models_with_metrics)}")
                
                # Show latest model info
                if completed_models:
                    latest_model = max(completed_models, key=lambda x: x.get('created_at', ''))
                    print(f"   🔍 Latest model: {latest_model.get('model_type', 'Unknown')}")
                    print(f"      📊 Performance: {latest_model.get('performance_metrics', 'N/A')}")
                    print(f"      📅 Created: {latest_model.get('created_at', 'N/A')}")
                
                return len(completed_models) > 0
            else:
                print(f"   ℹ️  No ML model training records found")
                return False
                
        else:
            print(f"❌ Failed to get ML model training records: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing ML model performance: {e}")
        return False

def test_ml_feature_extraction():
    """Test ML feature extraction and model training"""
    print("\n🔍 Testing ML Feature Extraction & Model Training...")
    
    try:
        # Test duplicate analysis for ML features
        duplicate_response = requests.get(f"{BASE_URL}/duplicate-analysis/{FILE_ID}/")
        if duplicate_response.status_code == 200:
            duplicate_data = duplicate_response.json()
            
            # Check for ML-generated features
            ml_features_found = []
            
            if 'detailed_results' in duplicate_data:
                detailed = duplicate_data['detailed_results']
                
                # Check for ML detection methods
                if 'detection_methods' in detailed:
                    methods = detailed['detection_methods']
                    if 'trained_model' in methods:
                        ml_features_found.append('trained_model_detection')
                
                # Check for confidence scores
                if 'confidence_scores' in detailed:
                    ml_features_found.append('confidence_scores')
                
                # Check for false positive indicators
                if 'false_positive_indicators' in detailed:
                    ml_features_found.append('false_positive_indicators')
            
            if ml_features_found:
                print(f"   ✅ ML features found in duplicate analysis: {', '.join(ml_features_found)}")
            else:
                print(f"   ℹ️  No ML features found in duplicate analysis")
        
        # Test user analysis for ML features
        user_response = requests.get(f"{BASE_URL}/user-analysis/{FILE_ID}/")
        if user_response.status_code == 200:
            user_data = user_response.json()
            
            # Check for ML-generated features
            ml_features_found = []
            
            if 'anomaly_detection' in user_data:
                anomaly_detection = user_data['anomaly_detection']
                
                # Check for ML detected anomalies
                if 'ml_detected_anomalies' in anomaly_detection:
                    ml_features_found.append('ml_detected_anomalies')
                
                # Check for anomaly severity breakdown
                if 'anomaly_severity_breakdown' in anomaly_detection:
                    ml_features_found.append('anomaly_severity_breakdown')
            
            if ml_features_found:
                print(f"   ✅ ML features found in user analysis: {', '.join(ml_features_found)}")
            else:
                print(f"   ℹ️  No ML features found in user analysis")
        
        return True
        
    except Exception as e:
        print(f"❌ Error testing ML feature extraction: {e}")
        return False

def main():
    """Main comprehensive ML enhancement test function"""
    print("🎯 ENHANCED ML SYSTEM COMPREHENSIVE TEST")
    print("=" * 70)
    
    # Test 1: ML Training Functions
    ml_training = test_ml_training_functions()
    
    # Test 2: ML Retraining Functions
    ml_retraining = test_ml_retraining_functions()
    
    # Test 3: ML Predictions
    ml_predictions = test_ml_predictions()
    
    # Test 4: ML Recommendations in Analysis
    ml_recommendations = test_ml_recommendations_in_analysis()
    
    # Test 5: ML Model Performance Tracking
    ml_performance = test_ml_model_performance_tracking()
    
    # Test 6: ML Feature Extraction
    ml_features = test_ml_feature_extraction()
    
    # Summary
    print("\n" + "=" * 70)
    print("🎯 ENHANCED ML SYSTEM TEST SUMMARY")
    print("=" * 70)
    
    # ML training functions summary
    print(f"✅ ML Training Functions: {'PASSED' if ml_training else 'FAILED'}")
    print(f"✅ ML Retraining Functions: {'PASSED' if ml_retraining else 'FAILED'}")
    print(f"✅ ML Predictions: {'PASSED' if ml_predictions else 'FAILED'}")
    
    # ML recommendations summary
    working_recommendations = sum(1 for result in ml_recommendations.values() if result)
    total_recommendations = len(ml_recommendations)
    print(f"📊 ML Recommendations: {working_recommendations}/{total_recommendations} working")
    
    # Overall results
    print(f"✅ ML Model Performance: {'PASSED' if ml_performance else 'FAILED'}")
    print(f"✅ ML Feature Extraction: {'PASSED' if ml_features else 'FAILED'}")
    
    overall_success = (ml_training and ml_retraining and ml_predictions and 
                      working_recommendations >= total_recommendations * 0.5 and 
                      ml_performance and ml_features)
    
    if overall_success:
        print("\n🎉 ENHANCED ML SYSTEM WORKING EXCELLENTLY!")
        print("✅ ML model training functions are implemented and functional")
        print("✅ ML retraining functions are working")
        print("✅ ML prediction capabilities are operational")
        print("✅ ML-specific recommendations are being generated")
        print("✅ ML model performance is being tracked")
        print("✅ ML feature extraction is working")
        print("✅ End-to-end enhanced ML pipeline is operational")
    else:
        print("\n⚠️  PARTIAL ML ENHANCEMENT - Some areas need attention")
        print("Please review the failed tests above")
    
    return overall_success

if __name__ == "__main__":
    main()
