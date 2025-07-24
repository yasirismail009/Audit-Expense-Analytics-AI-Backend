#!/usr/bin/env python3
"""
Test script to verify chart data is retrieved from duplicate_result.chart_data
"""

import requests
import json
from datetime import datetime

# Configuration
BASE_URL = "http://localhost:8000/api"
HEADERS = {'Content-Type': 'application/json'}

def get_available_files():
    """Get list of available files"""
    try:
        response = requests.get(f"{BASE_URL}/all-files/", headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict) and 'files' in data:
                return data['files']
            else:
                return []
        else:
            return []
    except Exception as e:
        return []

def test_chart_data_fix(file_id):
    """Test that chart data is retrieved from duplicate_result.chart_data"""
    print(f"\n🔍 Testing Chart Data Fix for File ID: {file_id}")
    print("=" * 60)
    
    try:
        response = requests.get(f"{BASE_URL}/db-comprehensive-duplicate-analysis/file/{file_id}/", headers=HEADERS)
        
        if response.status_code == 200:
            data = response.json()
            
            print("📊 Chart Data Analysis:")
            print("-" * 30)
            
            # Check chart_data_available
            chart_available = data.get('chart_data_available', False)
            print(f"Chart Data Available: {chart_available}")
            
            # Check chart_data
            chart_data = data.get('chart_data', {})
            if chart_data:
                print(f"✅ Chart Data Found: {len(chart_data)} chart types")
                for chart_type, chart_info in chart_data.items():
                    if isinstance(chart_info, list):
                        print(f"   - {chart_type}: {len(chart_info)} items")
                    elif isinstance(chart_info, dict):
                        print(f"   - {chart_type}: {len(chart_info)} keys")
                    else:
                        print(f"   - {chart_type}: {type(chart_info)}")
            else:
                print("❌ Chart Data: Empty")
            
            # Summary
            print(f"\n🎯 Result:")
            if chart_available and chart_data:
                print("✅ SUCCESS: Chart data is now being retrieved from duplicate_result.chart_data")
            elif chart_available and not chart_data:
                print("⚠️  WARNING: Chart data available flag is True but chart_data is empty")
            else:
                print("❌ FAILED: Chart data is not available")
                
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

def main():
    """Main function"""
    print("🔍 Chart Data Fix Test")
    print("=" * 30)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Get available files
    files = get_available_files()
    
    if not files:
        print("❌ No files available for testing")
        return
    
    # Test with the first completed file
    test_file = None
    for file_info in files:
        if file_info.get('status') in ['PROCESSED', 'COMPLETED']:
            test_file = file_info
            break
    
    if not test_file:
        print("❌ No processed files found for testing")
        return
    
    file_id = test_file.get('file_id', test_file.get('id', 'N/A'))
    file_name = test_file['file_name']
    
    print(f"Testing with file: {file_name}")
    print(f"File ID: {file_id}")
    
    # Run the test
    test_chart_data_fix(file_id)
    
    print("\n✅ Test completed!")

if __name__ == "__main__":
    main() 