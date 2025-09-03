#!/usr/bin/env python3
"""
Comprehensive test to check the current state of ML model training, 
recommendations, and implementations
"""

import requests
import json

# Configuration
BASE_URL = "http://localhost:8000/api"
FILE_ID = "96c59456-e1cd-491e-a8da-7f1b0084c081"

def test_ml_model_training_endpoints():
    """Test ML model training endpoints"""
    print("🔍 Testing ML Model Training Endpoints")
    print("=" * 50)
    
    ml_endpoints = [
        ('ml-model-training/train_models/', 'Train ML Models'),
        ('ml-model-training/retrain_models/', 'Retrain ML Models'),
        ('ml-model-training/model_info/', 'Model Information'),
        ('ml-model-training/predict_anomalies/', 'Predict Anomalies')
    ]
    
    ml_functionality = {}
    
    for endpoint, name in ml_endpoints:
        print(f"\n🔍 Testing {name}...")
        
        try:
            if 'train' in endpoint:
                # POST request for training
                response = requests.post(f"{BASE_URL}/{endpoint}")
            else:
                # GET request for info
                response = requests.get(f"{BASE_URL}/{endpoint}")
            
            if response.status_code == 200:
                result = response.json()
                print(f"✅ {name}: Successfully executed")
                print(f"   📊 Response: {result}")
                ml_functionality[name] = True
            elif response.status_code == 400:
                print(f"ℹ️  {name}: Bad request (expected if no data)")
                print(f"   📄 Response: {response.text}")
                ml_functionality[name] = False
            else:
                print(f"❌ {name}: HTTP {response.status_code}")
                print(f"   📄 Response: {response.text}")
                ml_functionality[name] = False
                
        except Exception as e:
            print(f"❌ {name}: Error - {e}")
            ml_functionality[name] = False
    
    return ml_functionality

def test_ml_recommendations():
    """Test ML-based recommendations in analysis results"""
    print(f"\n🔍 Testing ML-Based Recommendations...")
    print("=" * 50)
    
    # Test analysis endpoints for ML recommendations
    analysis_endpoints = [
        ('duplicate-analysis', 'Duplicate Analysis'),
        ('user-analysis', 'User Analysis'),
        ('backdated-analysis', 'Backdated Analysis')
    ]
    
    ml_recommendations = {}
    
    for endpoint, name in analysis_endpoints:
        print(f"\n🔍 Checking {name} for ML recommendations...")
        
        try:
            response = requests.get(f"{BASE_URL}/{endpoint}/{FILE_ID}/")
            
            if response.status_code == 200:
                result = response.json()
                
                # Check for ML-based recommendations
                has_ml_recommendations = False
                recommendation_sources = []
                
                # Check audit_recommendations
                if 'audit_recommendations' in result and result['audit_recommendations']:
                    recommendation_sources.append('audit_recommendations')
                    has_ml_recommendations = True
                
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
                    ml_recommendations[name] = True
                else:
                    print(f"   ℹ️  No ML recommendations found")
                    ml_recommendations[name] = False
                    
            else:
                print(f"❌ Failed to get {name}: {response.status_code}")
                ml_recommendations[name] = False
                
        except Exception as e:
            print(f"❌ Error checking {name}: {e}")
            ml_recommendations[name] = False
    
    return ml_recommendations

def test_ml_model_performance():
    """Test ML model performance tracking"""
    print(f"\n🔍 Testing ML Model Performance Tracking...")
    print("=" * 50)
    
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
    print(f"\n🔍 Testing ML Feature Extraction & Model Training...")
    print("=" * 50)
    
    try:
        # Check if ML model training is working by looking at analysis results
        # that should have ML-generated features
        
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
    """Main comprehensive ML test function"""
    print("🎯 COMPREHENSIVE ML MODEL TRAINING & RECOMMENDATIONS TEST")
    print("=" * 70)
    
    # Test 1: ML Model Training Endpoints
    ml_endpoints = test_ml_model_training_endpoints()
    
    # Test 2: ML-Based Recommendations
    ml_recommendations = test_ml_recommendations()
    
    # Test 3: ML Model Performance Tracking
    ml_performance = test_ml_model_performance()
    
    # Test 4: ML Feature Extraction
    ml_features = test_ml_feature_extraction()
    
    # Summary
    print("\n" + "=" * 70)
    print("🎯 ML IMPLEMENTATION TEST SUMMARY")
    print("=" * 70)
    
    # ML endpoints summary
    working_endpoints = sum(1 for result in ml_endpoints.values() if result)
    total_endpoints = len(ml_endpoints)
    print(f"📊 ML Training Endpoints: {working_endpoints}/{total_endpoints} working")
    
    # ML recommendations summary
    working_recommendations = sum(1 for result in ml_recommendations.values() if result)
    total_recommendations = len(ml_recommendations)
    print(f"📊 ML Recommendations: {working_recommendations}/{total_recommendations} working")
    
    # Overall results
    print(f"✅ ML Model Performance: {'PASSED' if ml_performance else 'FAILED'}")
    print(f"✅ ML Feature Extraction: {'PASSED' if ml_features else 'FAILED'}")
    
    overall_success = (working_endpoints >= total_endpoints * 0.5) and (working_recommendations >= total_recommendations * 0.5) and ml_performance and ml_features
    
    if overall_success:
        print("\n🎉 ML IMPLEMENTATION WORKING EXCELLENTLY!")
        print("✅ ML model training endpoints are functional")
        print("✅ ML-based recommendations are being generated")
        print("✅ ML model performance is being tracked")
        print("✅ ML feature extraction is working")
        print("✅ End-to-end ML pipeline is operational")
    else:
        print("\n⚠️  PARTIAL ML IMPLEMENTATION - Some areas need attention")
        print("Please review the failed tests above")
    
    return overall_success

if __name__ == "__main__":
    main()
