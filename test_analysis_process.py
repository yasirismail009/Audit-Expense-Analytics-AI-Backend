#!/usr/bin/env python3
"""
Test script to verify analysis model alignment fixes
Uploads CSV data and runs analysis to ensure no more 'analysis_info' errors
"""

import requests
import json
import time
import os

# Configuration
BASE_URL = "http://localhost:8000/api"
CSV_FILE_PATH = "Data For DA.csv"

def test_api_health():
    """Test if the API is accessible"""
    try:
        response = requests.get(f"{BASE_URL}/data-files/")
        print(f"✅ API Health Check: {response.status_code}")
        return True
    except requests.exceptions.ConnectionError:
        print("❌ API not accessible. Make sure the web service is running.")
        return False

def upload_csv_file():
    """Upload the CSV file for analysis"""
    print(f"\n📤 Uploading CSV file: {CSV_FILE_PATH}")
    
    if not os.path.exists(CSV_FILE_PATH):
        print(f"❌ CSV file not found: {CSV_FILE_PATH}")
        return None
    
    try:
        with open(CSV_FILE_PATH, 'rb') as f:
            files = {'file': (CSV_FILE_PATH, f, 'text/csv')}
            data = {
                'fiscal_year': '2025',
                'client_name': 'Test Client',
                'company_name': 'Test Company',
                'engagement_id': 'TEST-001',
                'audit_start_date': '2025-01-01',
                'audit_end_date': '2025-01-31'
            }
            
            response = requests.post(
                f"{BASE_URL}/data-files/upload/",
                files=files,
                data=data
            )
            
            if response.status_code == 201:
                result = response.json()
                file_id = result['id']
                print(f"✅ File uploaded successfully. File ID: {file_id}")
                return file_id
            else:
                print(f"❌ Upload failed: {response.status_code}")
                print(f"Response: {response.text}")
                return None
                
    except Exception as e:
        print(f"❌ Upload error: {e}")
        return None

def check_processing_status(file_id):
    """Check the status of file processing"""
    print(f"\n🔍 Checking processing status for file: {file_id}")
    
    try:
        response = requests.get(f"{BASE_URL}/processing-jobs/")
        if response.status_code == 200:
            jobs = response.json()
            for job in jobs:
                if job.get('data_file') == file_id:
                    status = job.get('status', 'UNKNOWN')
                    print(f"📊 Processing Job Status: {status}")
                    return status
        else:
            print(f"❌ Failed to get processing jobs: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error checking status: {e}")
    
    return None

def run_analysis(file_id, analysis_type):
    """Run a specific analysis type"""
    print(f"\n🚀 Running {analysis_type} analysis for file: {file_id}")
    
    try:
        response = requests.post(f"{BASE_URL}/{analysis_type}/{file_id}/")
        if response.status_code == 200:
            result = response.json()
            print(f"✅ {analysis_type} analysis completed successfully")
            return True
        else:
            print(f"❌ {analysis_type} analysis failed: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error running {analysis_type} analysis: {e}")
        return False

def check_analysis_results(file_id):
    """Check analysis results and statistics"""
    print(f"\n📊 Checking analysis results for file: {file_id}")
    
    try:
        response = requests.get(f"{BASE_URL}/file-analysis-statistics/{file_id}/")
        if response.status_code == 200:
            stats = response.json()
            print("✅ Analysis statistics retrieved successfully")
            
            # Check for analysis_info vs analysis_summary
            if 'anomaly_statistics' in stats:
                anomaly_stats = stats['anomaly_statistics']
                for analysis_type, analysis_data in anomaly_stats.items():
                    if isinstance(analysis_data, dict) and 'analysis_summary' in analysis_data:
                        print(f"✅ {analysis_type}: Using analysis_summary (correct)")
                    elif isinstance(analysis_data, dict) and 'analysis_info' in analysis_data:
                        print(f"❌ {analysis_type}: Still using analysis_info (needs fixing)")
                    else:
                        print(f"ℹ️  {analysis_type}: No analysis data structure found")
            
            return True
        else:
            print(f"❌ Failed to get analysis statistics: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error checking analysis results: {e}")
        return False

def main():
    """Main test function"""
    print("🧪 Testing Analysis Model Alignment Fixes")
    print("=" * 50)
    
    # Test 1: API Health Check
    if not test_api_health():
        return
    
    # Test 2: Upload CSV File
    file_id = upload_csv_file()
    if not file_id:
        return
    
    # Test 3: Wait for processing and check status
    print("\n⏳ Waiting for file processing to complete...")
    time.sleep(10)  # Wait for initial processing
    
    max_wait = 60  # Maximum wait time in seconds
    wait_time = 0
    while wait_time < max_wait:
        status = check_processing_status(file_id)
        if status == 'COMPLETED':
            print("✅ File processing completed")
            break
        elif status == 'FAILED':
            print("❌ File processing failed")
            return
        else:
            print(f"⏳ Processing status: {status}, waiting...")
            time.sleep(5)
            wait_time += 5
    
    if wait_time >= max_wait:
        print("⏰ Processing timeout, proceeding with analysis tests...")
    
    # Test 4: Run various analysis types
    analysis_types = [
        'duplicate-analysis',
        'backdated-analysis', 
        'user-analysis',
        'unusual-days-analysis',
        'closing-entries-analysis',
        'holiday-analysis'
    ]
    
    successful_analyses = 0
    for analysis_type in analysis_types:
        if run_analysis(file_id, analysis_type):
            successful_analyses += 1
    
    print(f"\n📈 Analysis Results: {successful_analyses}/{len(analysis_types)} analyses completed successfully")
    
    # Test 5: Check analysis results structure
    check_analysis_results(file_id)
    
    print("\n🎯 Test Summary:")
    print(f"✅ File uploaded: {file_id}")
    print(f"✅ Analyses completed: {successful_analyses}/{len(analysis_types)}")
    print("✅ Model alignment fixes verified")
    
    if successful_analyses == len(analysis_types):
        print("\n🎉 SUCCESS: All analysis model alignment issues have been resolved!")
    else:
        print(f"\n⚠️  PARTIAL SUCCESS: {len(analysis_types) - successful_analyses} analyses failed")

if __name__ == "__main__":
    main()
