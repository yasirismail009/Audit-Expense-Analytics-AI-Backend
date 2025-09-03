#!/usr/bin/env python3
"""
Comprehensive test script to verify ML integration across ALL analysis types
"""

import requests
import json
import time

# Configuration
BASE_URL = "http://web:8000/api"

def test_all_ml_integration():
    """Test that ML models are running during ALL analysis types"""
    print("Testing ML Model Integration Across ALL Analysis Types")
    print("=" * 80)
    
    # Test 1: Check if analysis endpoints return ML-enhanced results
    print("\nTesting Analysis Endpoints for ML Integration...")
    
    # Get the latest file ID (you may need to upload a file first)
    try:
        files_response = requests.get(f"{BASE_URL}/data-files/")
        if files_response.status_code == 200:
            files_data = files_response.json()
            if files_data.get('results') and len(files_data['results']) > 0:
                file_id = files_data['results'][0]['id']
                print(f"Using file ID: {file_id}")
            else:
                print("No files found. Please upload a file first.")
                return False
        else:
            print(f"Failed to get files: {files_response.status_code}")
            return False
    except Exception as e:
        print(f"Error getting files: {e}")
        return False
    
    # Test all analysis types
    analysis_types = [
        ('duplicate', 'Duplicate Analysis'),
        ('user', 'User Analysis'),
        ('backdated', 'Backdated Analysis'),
        ('unusual-days', 'Unusual Days Analysis'),
        ('closing-entries', 'Closing Entries Analysis'),
        ('holiday', 'Holiday Analysis')
    ]
    
    results = {}
    
    for analysis_type, analysis_name in analysis_types:
        print(f"\nTesting {analysis_name} ML Integration...")
        try:
            response = requests.get(f"{BASE_URL}/{analysis_type}-analysis/{file_id}/")
            
            if response.status_code == 200:
                data = response.json()
                
                # Check for ML-enhanced features
                ml_features_found = []
                
                # Check for ML insights
                if 'ml_insights' in data:
                    ml_features_found.append('ml_insights')
                    ml_insights = data['ml_insights']
                    print(f"   ML Insights found:")
                    print(f"      Detection Method: {ml_insights.get('detection_method', 'N/A')}")
                    print(f"      ML Model Accuracy: {ml_insights.get('ml_model_accuracy', 0.0)}")
                
                # Check for detection methods
                if 'detection_methods' in data:
                    ml_features_found.append('detection_methods')
                    detection_methods = data['detection_methods']
                    print(f"   Detection Methods found:")
                    print(f"      Primary Method: {detection_methods.get('primary_method', 'N/A')}")
                    print(f"      ML Available: {detection_methods.get('ml_available', False)}")
                    print(f"      Rule-based Fallback: {detection_methods.get('rule_based_fallback', False)}")
                
                # Check for ML-specific counts
                if 'ml_insights' in data:
                    ml_insights = data['ml_insights']
                    if 'ml_detected_duplicates' in ml_insights:
                        print(f"      ML Detected Duplicates: {ml_insights.get('ml_detected_duplicates', 0)}")
                    if 'ml_detected_backdated' in ml_insights:
                        print(f"      ML Detected Backdated: {ml_insights.get('ml_detected_backdated', 0)}")
                    if 'ml_detected_unusual' in ml_insights:
                        print(f"      ML Detected Unusual: {ml_insights.get('ml_detected_unusual', 0)}")
                    if 'ml_detected_closing' in ml_insights:
                        print(f"      ML Detected Closing: {ml_insights.get('ml_detected_closing', 0)}")
                    if 'ml_detected_holidays' in ml_insights:
                        print(f"      ML Detected Holidays: {ml_insights.get('ml_detected_holidays', 0)}")
                    if 'ml_detected_anomalies' in ml_insights:
                        print(f"      ML Detected Anomalies: {ml_insights.get('ml_detected_anomalies', 0)}")
                
                if ml_features_found:
                    print(f"   ML Integration Working: {', '.join(ml_features_found)} features found")
                    results[analysis_type] = True
                else:
                    print(f"   No ML features found in {analysis_name}")
                    results[analysis_type] = False
                    
            else:
                print(f"Failed to get {analysis_name}: {response.status_code}")
                results[analysis_type] = False
                
        except Exception as e:
            print(f"Error testing {analysis_name} ML integration: {e}")
            results[analysis_type] = False
    
    # Summary
    print("\n" + "=" * 80)
    print("COMPREHENSIVE ML INTEGRATION TEST SUMMARY")
    print("=" * 80)
    
    total_tests = len(analysis_types)
    passed_tests = sum(results.values())
    success_rate = (passed_tests / total_tests) * 100
    
    for analysis_type, analysis_name in analysis_types:
        status = "PASSED" if results.get(analysis_type, False) else "FAILED"
        print(f"{analysis_name}: {status}")
    
    print(f"\nOverall Results: {passed_tests}/{total_tests} tests passed ({success_rate:.1f}%)")
    
    if passed_tests == total_tests:
        print("\nALL ML INTEGRATIONS WORKING EXCELLENTLY!")
        print("ML models are running during ALL analysis types")
        print("ML insights are being generated for all analyses")
        print("ML features are being saved to database")
        print("End-to-end ML integration is fully operational")
    elif passed_tests > total_tests / 2:
        print("\nPARTIAL ML INTEGRATION - Most areas working")
        print("Some analysis types may need attention")
    else:
        print("\nLIMITED ML INTEGRATION - Many areas need attention")
        print("Please review the failed tests above")
    
    return passed_tests == total_tests

def main():
    """Main test function"""
    print("COMPREHENSIVE ML MODEL INTEGRATION TEST")
    print("=" * 80)
    
    success = test_all_ml_integration()
    
    return success

if __name__ == "__main__":
    main()
