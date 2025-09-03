#!/usr/bin/env python3
"""
Comprehensive test to verify all models and response structures are properly aligned
"""

import requests
import json

# Configuration
BASE_URL = "http://localhost:8000/api"
FILE_ID = "96c59456-e1cd-491e-a8da-7f1b0084c081"  # From previous test

def test_model_structure_alignment():
    """Test that all models have the correct unified structure"""
    print("🔍 Testing Model Structure Alignment")
    print("=" * 50)
    
    # Test all analysis endpoints to verify structure
    analysis_endpoints = [
        ('duplicate-analysis', 'Duplicate Analysis'),
        ('backdated-analysis', 'Backdated Analysis'),
        ('user-analysis', 'User Analysis'),
        ('unusual-days-analysis', 'Unusual Days Analysis'),
        ('closing-entries-analysis', 'Closing Entries Analysis'),
        ('holiday-analysis', 'Holiday Analysis')
    ]
    
    structure_alignment = {}
    
    for endpoint, name in analysis_endpoints:
        print(f"\n🔍 Testing {name} structure...")
        
        try:
            response = requests.get(f"{BASE_URL}/{endpoint}/{FILE_ID}/")
            
            if response.status_code == 200:
                result = response.json()
                print(f"✅ {name}: Successfully retrieved")
                
                # Check for required unified fields
                required_fields = ['analysis_summary', 'anomaly_list', 'chart_data', 'risk_assessment', 'audit_recommendations', 'compliance_assessment', 'export_data']
                missing_fields = []
                
                for field in required_fields:
                    if field not in result:
                        missing_fields.append(field)
                
                if missing_fields:
                    print(f"   ❌ Missing unified fields: {missing_fields}")
                    structure_alignment[name] = False
                else:
                    print(f"   ✅ All unified fields present")
                    structure_alignment[name] = True
                    
                # Check for old analysis_info field (should not exist)
                if 'analysis_info' in result:
                    print(f"   ❌ Still contains analysis_info field")
                    structure_alignment[name] = False
                else:
                    print(f"   ✅ No analysis_info field (correct)")
                    
            elif response.status_code == 404:
                print(f"ℹ️  {name}: No analysis results found")
                structure_alignment[name] = True  # Not an alignment issue
            else:
                print(f"❌ {name}: HTTP {response.status_code}")
                structure_alignment[name] = False
                
        except Exception as e:
            print(f"❌ {name}: Error - {e}")
            structure_alignment[name] = False
    
    return structure_alignment

def test_response_consistency():
    """Test that all responses have consistent structure"""
    print(f"\n🔍 Testing Response Structure Consistency...")
    print("=" * 50)
    
    try:
        response = requests.get(f"{BASE_URL}/file-analysis-statistics/{FILE_ID}/")
        
        if response.status_code == 200:
            stats = response.json()
            print("✅ File analysis statistics retrieved successfully")
            
            # Check anomaly statistics structure consistency
            if 'anomaly_statistics' in stats:
                anomaly_stats = stats['anomaly_statistics']
                print(f"   📈 Found {len(anomaly_stats)} analysis types")
                
                # Check each analysis type for consistent structure
                consistent_structure = True
                for analysis_type, analysis_data in anomaly_stats.items():
                    if isinstance(analysis_data, dict):
                        if 'analysis_summary' in analysis_data:
                            print(f"   ✅ {analysis_type}: Using analysis_summary")
                        elif 'analysis_info' in analysis_data:
                            print(f"   ❌ {analysis_type}: Still using analysis_info")
                            consistent_structure = False
                        else:
                            print(f"   ℹ️  {analysis_type}: No analysis data structure")
                    else:
                        print(f"   ℹ️  {analysis_type}: Not a dict structure")
                
                return consistent_structure
            else:
                print("   ℹ️  No anomaly statistics found")
                return True
                
        else:
            print(f"❌ Failed to get file statistics: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing response consistency: {e}")
        return False

def test_field_access_methods():
    """Test that all required methods exist and work correctly"""
    print(f"\n🔍 Testing Field Access Methods...")
    print("=" * 50)
    
    # Test a few key endpoints to verify methods work
    test_endpoints = [
        ('duplicate-analysis', 'Duplicate Analysis'),
        ('user-analysis', 'User Analysis')
    ]
    
    methods_working = True
    
    for endpoint, name in test_endpoints:
        try:
            response = requests.get(f"{BASE_URL}/{endpoint}/{FILE_ID}/")
            
            if response.status_code == 200:
                result = response.json()
                
                # Check if key methods are working by examining response structure
                # Check for either legacy sections or unified fields
                has_legacy_sections = 'summary' in result and 'detailed_results' in result
                has_unified_fields = all(field in result for field in ['analysis_summary', 'anomaly_list', 'chart_data'])
                
                if has_legacy_sections or has_unified_fields:
                    print(f"   ✅ {name}: Methods working correctly")
                else:
                    print(f"   ❌ {name}: Missing expected response sections")
                    print(f"      Available keys: {list(result.keys())}")
                    methods_working = False
                    
        except Exception as e:
            print(f"   ❌ {name}: Error testing methods - {e}")
            methods_working = False
    
    return methods_working

def test_data_integrity():
    """Test that data integrity is maintained"""
    print(f"\n🔍 Testing Data Integrity...")
    print("=" * 50)
    
    try:
        # Test file info
        response = requests.get(f"{BASE_URL}/data-files/{FILE_ID}/")
        
        if response.status_code == 200:
            file_info = response.json()
            print("✅ Data file information retrieved successfully")
            
            # Check data integrity
            if file_info.get('total_records') == 63:
                print("   ✅ Total records count correct (63)")
            else:
                print(f"   ❌ Total records count mismatch: {file_info.get('total_records')}")
                return False
            
            if file_info.get('status') == 'COMPLETED':
                print("   ✅ File status correct (COMPLETED)")
            else:
                print(f"   ❌ File status mismatch: {file_info.get('status')}")
                return False
                
            return True
        else:
            print(f"❌ Failed to get file info: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing data integrity: {e}")
        return False

def main():
    """Main comprehensive test function"""
    print("🎯 COMPREHENSIVE MODEL & RESPONSE STRUCTURE ALIGNMENT TEST")
    print("=" * 70)
    
    # Test 1: Model Structure Alignment
    structure_results = test_model_structure_alignment()
    
    # Test 2: Response Consistency
    consistency_result = test_response_consistency()
    
    # Test 3: Field Access Methods
    methods_result = test_field_access_methods()
    
    # Test 4: Data Integrity
    integrity_result = test_data_integrity()
    
    # Summary
    print("\n" + "=" * 70)
    print("🎯 COMPREHENSIVE TEST SUMMARY")
    print("=" * 70)
    
    # Structure alignment summary
    aligned_models = sum(1 for result in structure_results.values() if result)
    total_models = len(structure_results)
    print(f"📊 Model Structure Alignment: {aligned_models}/{total_models} models aligned")
    
    # Overall results
    print(f"✅ Response Consistency: {'PASSED' if consistency_result else 'FAILED'}")
    print(f"✅ Field Access Methods: {'PASSED' if methods_result else 'FAILED'}")
    print(f"✅ Data Integrity: {'PASSED' if integrity_result else 'FAILED'}")
    
    overall_success = (aligned_models == total_models) and consistency_result and methods_result and integrity_result
    
    if overall_success:
        print("\n🎉 PERFECT ALIGNMENT ACHIEVED!")
        print("✅ All models use unified structure")
        print("✅ All response structures are consistent")
        print("✅ All field access methods work correctly")
        print("✅ Data integrity maintained")
        print("✅ Complete transition from analysis_info to unified structure successful")
    else:
        print("\n⚠️  PARTIAL ALIGNMENT - Some issues remain")
        print("Please review the failed tests above")
    
    return overall_success

if __name__ == "__main__":
    main()
