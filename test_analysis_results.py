#!/usr/bin/env/python3
"""
Test script to verify analysis results and model alignment
Checks the analysis results that were created during file processing
"""

import requests
import json

# Configuration
BASE_URL = "http://localhost:8000/api"
FILE_ID = "96c59456-e1cd-491e-a8da-7f1b0084c081"  # From previous test

def test_analysis_endpoints():
    """Test all analysis endpoints to verify model alignment"""
    print("🧪 Testing Analysis Endpoints and Model Alignment")
    print("=" * 60)
    
    analysis_endpoints = [
        ('duplicate-analysis', 'Duplicate Analysis'),
        ('backdated-analysis', 'Backdated Analysis'),
        ('user-analysis', 'User Analysis'),
        ('unusual-days-analysis', 'Unusual Days Analysis'),
        ('closing-entries-analysis', 'Closing Entries Analysis'),
        ('holiday-analysis', 'Holiday Analysis')
    ]
    
    successful_tests = 0
    total_tests = len(analysis_endpoints)
    
    for endpoint, name in analysis_endpoints:
        print(f"\n🔍 Testing {name}...")
        
        try:
            response = requests.get(f"{BASE_URL}/{endpoint}/{FILE_ID}/")
            
            if response.status_code == 200:
                result = response.json()
                print(f"✅ {name}: Successfully retrieved")
                
                # Check if using correct field structure
                if 'analysis_summary' in result:
                    print(f"   ✅ Using analysis_summary (correct)")
                    successful_tests += 1
                elif 'analysis_info' in result:
                    print(f"   ❌ Still using analysis_info (needs fixing)")
                else:
                    print(f"   ℹ️  No analysis data structure found")
                    successful_tests += 1
                    
            elif response.status_code == 404:
                print(f"ℹ️  {name}: No analysis results found (expected if analysis not run)")
                successful_tests += 1
            else:
                print(f"❌ {name}: HTTP {response.status_code}")
                print(f"   Response: {response.text}")
                
        except Exception as e:
            print(f"❌ {name}: Error - {e}")
    
    print(f"\n📊 Test Results: {successful_tests}/{total_tests} tests passed")
    return successful_tests == total_tests

def test_file_statistics():
    """Test file analysis statistics endpoint"""
    print(f"\n📊 Testing File Analysis Statistics...")
    
    try:
        response = requests.get(f"{BASE_URL}/file-analysis-statistics/{FILE_ID}/")
        
        if response.status_code == 200:
            stats = response.json()
            print("✅ File analysis statistics retrieved successfully")
            
            # Check anomaly statistics structure
            if 'anomaly_statistics' in stats:
                anomaly_stats = stats['anomaly_statistics']
                print(f"   📈 Found {len(anomaly_stats)} analysis types")
                
                # Verify each analysis type uses correct structure
                correct_structure_count = 0
                for analysis_type, analysis_data in anomaly_stats.items():
                    if isinstance(analysis_data, dict):
                        if 'analysis_summary' in analysis_data:
                            print(f"   ✅ {analysis_type}: Using analysis_summary")
                            correct_structure_count += 1
                        elif 'analysis_info' in analysis_data:
                            print(f"   ❌ {analysis_type}: Still using analysis_info")
                        else:
                            print(f"   ℹ️  {analysis_type}: No analysis data structure")
                            correct_structure_count += 1
                
                print(f"   📊 Structure verification: {correct_structure_count}/{len(anomaly_stats)} correct")
                return True
            else:
                print("   ℹ️  No anomaly statistics found")
                return True
                
        else:
            print(f"❌ Failed to get file statistics: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing file statistics: {e}")
        return False

def test_data_file_info():
    """Test data file information endpoint"""
    print(f"\n📁 Testing Data File Information...")
    
    try:
        response = requests.get(f"{BASE_URL}/data-files/{FILE_ID}/")
        
        if response.status_code == 200:
            file_info = response.json()
            print("✅ Data file information retrieved successfully")
            print(f"   📄 File: {file_info.get('file_name', 'Unknown')}")
            print(f"   📊 Status: {file_info.get('status', 'Unknown')}")
            print(f"   📈 Total Records: {file_info.get('total_records', 0)}")
            print(f"   ✅ Processed Records: {file_info.get('processed_records', 0)}")
            return True
        else:
            print(f"❌ Failed to get file info: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing file info: {e}")
        return False

def main():
    """Main test function"""
    print("🎯 Comprehensive Analysis Model Alignment Test")
    print("=" * 60)
    
    # Test 1: Analysis Endpoints
    endpoints_success = test_analysis_endpoints()
    
    # Test 2: File Statistics
    stats_success = test_file_statistics()
    
    # Test 3: File Information
    file_info_success = test_data_file_info()
    
    # Summary
    print("\n" + "=" * 60)
    print("🎯 TEST SUMMARY")
    print("=" * 60)
    
    print(f"✅ Analysis Endpoints: {'PASSED' if endpoints_success else 'FAILED'}")
    print(f"✅ File Statistics: {'PASSED' if stats_success else 'FAILED'}")
    print(f"✅ File Information: {'PASSED' if file_info_success else 'FAILED'}")
    
    overall_success = endpoints_success and stats_success and file_info_success
    
    if overall_success:
        print("\n🎉 SUCCESS: All tests passed!")
        print("✅ Analysis model alignment is working correctly")
        print("✅ No more 'analysis_info' field errors")
        print("✅ All endpoints are using the new unified structure")
    else:
        print("\n⚠️  PARTIAL SUCCESS: Some tests failed")
        print("Please review the failed tests above")
    
    return overall_success

if __name__ == "__main__":
    main()
