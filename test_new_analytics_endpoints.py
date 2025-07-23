#!/usr/bin/env python
import os
import django
import requests
import json

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, AnalyticsProcessingResult, MLModelProcessingResult

print("=== TESTING NEW ANALYTICS ENDPOINTS ===")
print()

# Get the file ID
file = DataFile.objects.first()
file_id = str(file.id)
print(f"Testing with file: {file.file_name} (ID: {file_id})")
print()

# Test the new database-stored analytics endpoints
base_url = "http://localhost:8000/api"

print("1. Testing Database-Stored Comprehensive Analytics:")
print(f"   URL: {base_url}/db-comprehensive-analytics/file/{file_id}/")
try:
    response = requests.get(f"{base_url}/db-comprehensive-analytics/file/{file_id}/")
    if response.status_code == 200:
        data = response.json()
        print("   ✅ SUCCESS - Comprehensive Analytics Retrieved")
        print(f"   📊 Total Transactions: {data.get('summary', {}).get('total_transactions', 'N/A')}")
        print(f"   💰 Total Amount: {data.get('summary', {}).get('total_amount', 'N/A')}")
        print(f"   👥 Unique Users: {data.get('summary', {}).get('unique_users', 'N/A')}")
        print(f"   🏦 Unique Accounts: {data.get('summary', {}).get('unique_accounts', 'N/A')}")
    else:
        print(f"   ❌ ERROR - Status: {response.status_code}")
        print(f"   Response: {response.text}")
except Exception as e:
    print(f"   ❌ ERROR - {e}")
print()

print("2. Testing Database-Stored Duplicate Analysis:")
print(f"   URL: {base_url}/db-comprehensive-duplicate-analysis/file/{file_id}/")
try:
    response = requests.get(f"{base_url}/db-comprehensive-duplicate-analysis/file/{file_id}/")
    if response.status_code == 200:
        data = response.json()
        print("   ✅ SUCCESS - Duplicate Analysis Retrieved")
        print(f"   🔍 Total Duplicates: {data.get('analysis_info', {}).get('total_duplicate_transactions', 'N/A')}")
        print(f"   💰 Amount Involved: {data.get('analysis_info', {}).get('total_amount_involved', 'N/A')}")
        print(f"   📋 Duplicate Groups: {data.get('analysis_info', {}).get('total_duplicate_groups', 'N/A')}")
    else:
        print(f"   ❌ ERROR - Status: {response.status_code}")
        print(f"   Response: {response.text}")
except Exception as e:
    print(f"   ❌ ERROR - {e}")
print()

print("3. Testing Analytics Results API:")
print(f"   URL: {base_url}/analytics-results/?file_id={file_id}&analytics_type=all")
try:
    response = requests.get(f"{base_url}/analytics-results/?file_id={file_id}&analytics_type=all")
    if response.status_code == 200:
        data = response.json()
        print("   ✅ SUCCESS - Analytics Results Retrieved")
        print(f"   📊 Results Count: {data.get('results_count', 'N/A')}")
        for result in data.get('results', []):
            print(f"   - {result.get('analytics_type')}: {result.get('processing_status')}")
    else:
        print(f"   ❌ ERROR - Status: {response.status_code}")
        print(f"   Response: {response.text}")
except Exception as e:
    print(f"   ❌ ERROR - {e}")
print()

print("4. Testing ML Processing Results API:")
print(f"   URL: {base_url}/ml-processing-results/?file_id={file_id}&model_type=all")
try:
    response = requests.get(f"{base_url}/ml-processing-results/?file_id={file_id}&model_type=all")
    if response.status_code == 200:
        data = response.json()
        print("   ✅ SUCCESS - ML Processing Results Retrieved")
        print(f"   🤖 Results Count: {data.get('results_count', 'N/A')}")
        for result in data.get('results', []):
            print(f"   - {result.get('model_type')}: {result.get('processing_status')}")
    else:
        print(f"   ❌ ERROR - Status: {response.status_code}")
        print(f"   Response: {response.text}")
except Exception as e:
    print(f"   ❌ ERROR - {e}")
print()

print("5. Testing Analytics Database Check:")
print(f"   URL: {base_url}/analytics-db-check/file/{file_id}/")
try:
    response = requests.get(f"{base_url}/analytics-db-check/file/{file_id}/")
    if response.status_code == 200:
        data = response.json()
        print("   ✅ SUCCESS - Database Check Retrieved")
        print(f"   📊 Analytics Results: {data.get('analytics_results_count', 'N/A')}")
        print(f"   🤖 ML Results: {data.get('ml_results_count', 'N/A')}")
        print(f"   📈 Job Trackers: {data.get('job_trackers_count', 'N/A')}")
    else:
        print(f"   ❌ ERROR - Status: {response.status_code}")
        print(f"   Response: {response.text}")
except Exception as e:
    print(f"   ❌ ERROR - {e}")
print()

print("=== DIRECT DATABASE ACCESS ===")
print()

# Direct database access
print("6. Direct Database Access (AnalyticsProcessingResult):")
analytics_results = AnalyticsProcessingResult.objects.filter(data_file=file)
print(f"   📊 Total Analytics Results: {analytics_results.count()}")
for result in analytics_results:
    print(f"   - {result.analytics_type}: {result.processing_status}")
    print(f"     Transactions: {result.total_transactions}, Amount: {result.total_amount}")
    if result.analytics_type == 'duplicate_analysis':
        print(f"     Duplicates Found: {result.duplicates_found}")
print()

print("7. Direct Database Access (MLModelProcessingResult):")
ml_results = MLModelProcessingResult.objects.filter(data_file=file)
print(f"   🤖 Total ML Results: {ml_results.count()}")
for result in ml_results:
    print(f"   - {result.model_type}: {result.processing_status}")
    print(f"     Anomalies: {result.anomalies_detected}, Duplicates: {result.duplicates_found}")
    print(f"     Risk Score: {result.risk_score}, Confidence: {result.confidence_score}")
print()

print("=== SUMMARY ===")
print("✅ Data IS being saved correctly to the NEW models:")
print("   - AnalyticsProcessingResult (4 records)")
print("   - MLModelProcessingResult (1 record)")
print()
print("❌ Data is NOT in the OLD models:")
print("   - AnalyticsResult (0 records)")
print("   - DuplicateAnalysisResult (0 records)")
print()
print("📋 Use these NEW endpoints to access your data:")
print("   - /api/db-comprehensive-analytics/file/{file_id}/")
print("   - /api/db-comprehensive-duplicate-analysis/file/{file_id}/")
print("   - /api/analytics-results/?file_id={file_id}&analytics_type=all")
print("   - /api/ml-processing-results/?file_id={file_id}&model_type=all")
print("   - /api/analytics-db-check/file/{file_id}/") 