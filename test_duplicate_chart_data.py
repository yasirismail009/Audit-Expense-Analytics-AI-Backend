#!/usr/bin/env python3
"""
Test script to verify duplicate analysis chart data is working correctly
"""

import requests
import json
import sys
from datetime import datetime

# Configuration
BASE_URL = "http://localhost:8000/api"
HEADERS = {
    'Content-Type': 'application/json',
}

def get_available_files():
    """Get list of available files"""
    try:
        response = requests.get(f"{BASE_URL}/all-files/", headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict) and 'files' in data:
                return data['files']
            else:
                print(f"❌ Unexpected response structure: {type(data)}")
                return []
        else:
            print(f"❌ Error getting files: {response.status_code}")
            return []
    except Exception as e:
        print(f"❌ Error connecting to API: {e}")
        return []

def test_duplicate_chart_data(file_id):
    """Test duplicate analysis chart data for a specific file"""
    print(f"\n🔍 Testing Duplicate Chart Data for File ID: {file_id}")
    print("=" * 60)
    
    # Test 1: Check comprehensive analytics endpoint
    print("\n📊 Test 1: Comprehensive Analytics Endpoint")
    print("-" * 40)
    
    try:
        response = requests.get(f"{BASE_URL}/db-comprehensive-analytics/file/{file_id}/", headers=HEADERS)
        
        if response.status_code == 200:
            data = response.json()
            
            # Check if duplicate_summary exists
            if 'duplicate_summary' in data:
                duplicate_summary = data['duplicate_summary']
                print(f"✅ Duplicate summary found")
                print(f"   - Total transactions: {duplicate_summary.get('total_transactions', 0)}")
                print(f"   - Total duplicates: {duplicate_summary.get('total_duplicate_transactions', 0)}")
                print(f"   - Risk score: {duplicate_summary.get('risk_score', 0)}")
                print(f"   - Risk level: {duplicate_summary.get('risk_level', 'N/A')}")
            else:
                print("❌ Duplicate summary not found")
            
            # Check if charts section exists
            if 'charts' in data:
                charts = data['charts']
                print(f"✅ Charts section found with {len(charts)} chart types")
                for chart_type, chart_data in charts.items():
                    print(f"   - {chart_type}: {type(chart_data)}")
            else:
                print("❌ Charts section not found")
                
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"❌ Error testing comprehensive analytics: {e}")
    
    # Test 2: Check duplicate analysis endpoint directly
    print("\n📊 Test 2: Direct Duplicate Analysis Endpoint")
    print("-" * 40)
    
    try:
        response = requests.get(f"{BASE_URL}/db-comprehensive-duplicate-analysis/file/{file_id}/", headers=HEADERS)
        
        if response.status_code == 200:
            data = response.json()
            
            # Check for chart data in the response
            chart_data_found = False
            
            # Look for chart data in various possible locations
            if 'duplicate_charts' in data:
                chart_data = data['duplicate_charts']
                print(f"✅ Duplicate charts found: {len(chart_data)} chart types")
                for chart_type, chart_info in chart_data.items():
                    print(f"   - {chart_type}: {type(chart_info)}")
                    if isinstance(chart_info, list):
                        print(f"     Items: {len(chart_info)}")
                    elif isinstance(chart_info, dict):
                        print(f"     Keys: {list(chart_info.keys())}")
                chart_data_found = True
            
            if 'chart_data' in data:
                chart_data = data['chart_data']
                print(f"✅ Chart data found: {len(chart_data)} chart types")
                for chart_type, chart_info in chart_data.items():
                    print(f"   - {chart_type}: {type(chart_info)}")
                chart_data_found = True
            
            if 'charts' in data:
                charts = data['charts']
                print(f"✅ Charts found: {len(charts)} chart types")
                for chart_type, chart_info in charts.items():
                    print(f"   - {chart_type}: {type(chart_info)}")
                chart_data_found = True
            
            if not chart_data_found:
                print("❌ No chart data found in response")
                print("Available keys:", list(data.keys()))
            
            # Check for duplicate statistics
            if 'duplicate_stats' in data:
                stats = data['duplicate_stats']
                print(f"\n📈 Duplicate Statistics:")
                print(f"   - Total transactions: {stats.get('total_transactions', 0)}")
                print(f"   - Total duplicates: {stats.get('total_duplicate_transactions', 0)}")
                print(f"   - Total groups: {stats.get('total_duplicate_groups', 0)}")
                print(f"   - Total amount: {stats.get('total_amount_involved', 0)}")
            
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"❌ Error testing duplicate analysis: {e}")
    
    # Test 3: Check database structure
    print("\n📊 Test 3: Database Structure Check")
    print("-" * 40)
    
    try:
        # Check analytics database check endpoint
        response = requests.get(f"{BASE_URL}/analytics-db-check/file/{file_id}/", headers=HEADERS)
        
        if response.status_code == 200:
            data = response.json()
            
            if 'duplicate_analysis' in data:
                duplicate_info = data['duplicate_analysis']
                print(f"✅ Duplicate analysis found in database")
                print(f"   - Analysis ID: {duplicate_info.get('analysis_id', 'N/A')}")
                print(f"   - Status: {duplicate_info.get('status', 'N/A')}")
                print(f"   - Created: {duplicate_info.get('created_at', 'N/A')}")
                
                # Check if chart data exists in database
                if 'has_chart_data' in duplicate_info:
                    print(f"   - Has chart data: {duplicate_info['has_chart_data']}")
                else:
                    print(f"   - Chart data info not available")
                    
            else:
                print("❌ No duplicate analysis found in database")
                
        else:
            print(f"❌ Error checking database: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error checking database structure: {e}")

def main():
    """Main test function"""
    print("🔍 Duplicate Analysis Chart Data Test")
    print("=" * 50)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Get available files
    print("\n📁 Getting available files...")
    files = get_available_files()
    
    if not files:
        print("❌ No files available for testing")
        return
    
    print(f"✅ Found {len(files)} files")
    
    # Display files
    print("\n📋 Available Files:")
    for i, file_info in enumerate(files[:5] if isinstance(files, list) else []):  # Show first 5 files
        file_id = file_info.get('file_id', file_info.get('id', 'N/A'))
        file_name = file_info.get('file_name', 'Unknown')
        status = file_info.get('status', 'Unknown')
        print(f"   {i+1}. {file_name} (ID: {file_id}, Status: {status})")
    
    if isinstance(files, list) and len(files) > 5:
        print(f"   ... and {len(files) - 5} more files")
    
    # Test with the first file that has been processed
    test_file = None
    if isinstance(files, list):
        for file_info in files:
            if file_info.get('status') in ['PROCESSED', 'COMPLETED']:
                test_file = file_info
                break
    
    if not test_file:
        print("\n❌ No processed files found for testing")
        return
    
    file_id = test_file.get('file_id', test_file.get('id', 'N/A'))
    file_name = test_file['file_name']
    
    print(f"\n🎯 Testing with file: {file_name}")
    print(f"File ID: {file_id}")
    
    # Run the tests
    test_duplicate_chart_data(file_id)
    
    print("\n✅ Test completed!")

if __name__ == "__main__":
    main() 