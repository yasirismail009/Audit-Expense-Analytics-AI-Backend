#!/usr/bin/env python
"""
Script to test the Holiday Analysis API
"""

import os
import sys
import django
import requests
import json
from datetime import datetime

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

from core.models import DataFile, HolidayAnalysisResult

def test_holiday_api():
    """Test the Holiday Analysis API"""
    print("=== HOLIDAY ANALYSIS API TEST ===")
    
    # Get the first data file
    data_file = DataFile.objects.first()
    if not data_file:
        print("No data file found. Please upload a file first.")
        return
    
    file_id = str(data_file.id)
    print(f"Testing with file ID: {file_id}")
    print(f"File name: {data_file.file_name}")
    
    # Check if holiday analysis exists
    holiday_analysis = HolidayAnalysisResult.objects.filter(
        data_file=data_file, status='COMPLETED'
    ).first()
    
    if not holiday_analysis:
        print("No holiday analysis found. Running analysis first...")
        # Run the analysis script first
        os.system("python check_existing_data.py")
        holiday_analysis = HolidayAnalysisResult.objects.filter(
            data_file=data_file, status='COMPLETED'
        ).first()
    
    if not holiday_analysis:
        print("Failed to create holiday analysis.")
        return
    
    print(f"Holiday analysis found with ID: {holiday_analysis.id}")
    
    # Test API endpoint
    base_url = "http://localhost:8000"
    api_url = f"{base_url}/api/core/holiday-analysis/{file_id}/"
    
    print(f"\nTesting API endpoint: {api_url}")
    
    try:
        # Make the API request
        response = requests.get(api_url, timeout=30)
        
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("\n✅ API Response Success!")
            
            # Display key information
            print(f"\n📊 Analysis Summary:")
            summary = data.get('summary', {})
            print(f"   Total Holiday Postings: {summary.get('total_holiday_postings', 0)}")
            print(f"   Holiday Percentage: {summary.get('holiday_percentage', 0):.2f}%")
            print(f"   Unique Holidays: {summary.get('unique_holidays', 0)}")
            print(f"   Risk Level: {summary.get('risk_level', 'Unknown')}")
            print(f"   Overall Risk Score: {summary.get('overall_risk_score', 0):.2f}/100")
            
            # Display analysis info
            analysis_info = data.get('analysis_info', {})
            print(f"\n📋 Analysis Info:")
            print(f"   Analysis ID: {analysis_info.get('analysis_id', 'N/A')}")
            print(f"   Analysis Date: {analysis_info.get('analysis_date', 'N/A')}")
            print(f"   Processing Duration: {analysis_info.get('processing_duration', 0):.2f} seconds")
            print(f"   Status: {analysis_info.get('status', 'N/A')}")
            print(f"   Version: {analysis_info.get('analysis_version', 'N/A')}")
            
            # Display detailed results
            detailed_results = data.get('detailed_results', {})
            print(f"\n🔍 Detailed Results:")
            print(f"   Holiday Postings: {len(detailed_results.get('holiday_postings', []))}")
            print(f"   Holiday by FS Line: {len(detailed_results.get('holiday_by_fs_line', []))}")
            print(f"   Holiday by Account: {len(detailed_results.get('holiday_by_account', []))}")
            print(f"   Holiday by User: {len(detailed_results.get('holiday_by_user', []))}")
            
            # Display visualizations
            visualizations = data.get('visualizations', {})
            print(f"\n📈 Visualizations:")
            chart_data = visualizations.get('chart_data', {})
            print(f"   Chart Data Available: {len(chart_data)} charts")
            
            slicer_filters = visualizations.get('slicer_filters', {})
            print(f"   Slicer Filters:")
            for filter_name, filter_values in slicer_filters.items():
                if isinstance(filter_values, list):
                    print(f"     {filter_name}: {len(filter_values)} options")
                else:
                    print(f"     {filter_name}: {filter_values}")
            
            # Display export data
            export_data = data.get('export_data', {})
            print(f"\n📋 Export Data:")
            print(f"   Summary Table: {len(export_data.get('summary_table', []))} records")
            print(f"   Detailed Export: {len(export_data.get('detailed_export', []))} records")
            
            # Display compliance and financial impact if available
            if 'compliance' in data:
                compliance = data['compliance']
                print(f"\n📋 Compliance Assessment:")
                for key, value in compliance.items():
                    print(f"   {key}: {value}")
            
            if 'financial_impact' in data:
                financial_impact = data['financial_impact']
                print(f"\n💰 Financial Impact:")
                for key, value in financial_impact.items():
                    print(f"   {key}: {value}")
            
            # Save response to file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            response_file = f"holiday_api_response_{timestamp}.json"
            with open(response_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, default=str)
            print(f"\n📄 API response saved to: {response_file}")
            
        else:
            print(f"❌ API Error: {response.status_code}")
            try:
                error_data = response.json()
                print(f"Error details: {error_data}")
            except:
                print(f"Error text: {response.text}")
                
    except requests.exceptions.ConnectionError:
        print("❌ Connection Error: Make sure the Django server is running on localhost:8000")
        print("   Run: python manage.py runserver")
    except requests.exceptions.Timeout:
        print("❌ Timeout Error: Request took too long")
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")

def test_api_without_server():
    """Test the API using Django test client (no server needed)"""
    print("\n=== TESTING API WITH DJANGO TEST CLIENT ===")
    
    from django.test import Client
    from django.urls import reverse
    
    client = Client()
    
    # Get the first data file
    data_file = DataFile.objects.first()
    if not data_file:
        print("No data file found.")
        return
    
    file_id = str(data_file.id)
    print(f"Testing with file ID: {file_id}")
    
    # Check if holiday analysis exists
    holiday_analysis = HolidayAnalysisResult.objects.filter(
        data_file=data_file, status='COMPLETED'
    ).first()
    
    if not holiday_analysis:
        print("No holiday analysis found. Running analysis first...")
        os.system("python check_existing_data.py")
        holiday_analysis = HolidayAnalysisResult.objects.filter(
            data_file=data_file, status='COMPLETED'
        ).first()
    
    if not holiday_analysis:
        print("Failed to create holiday analysis.")
        return
    
    try:
        # Make the API request using Django test client
        url = reverse('holiday-analysis', kwargs={'file_id': file_id})
        response = client.get(url)
        
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("✅ API Test Success!")
            
            # Display summary
            summary = data.get('summary', {})
            print(f"\n📊 Summary:")
            print(f"   Total Holiday Postings: {summary.get('total_holiday_postings', 0)}")
            print(f"   Holiday Percentage: {summary.get('holiday_percentage', 0):.2f}%")
            print(f"   Risk Level: {summary.get('risk_level', 'Unknown')}")
            
            # Save response to file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            response_file = f"holiday_api_test_response_{timestamp}.json"
            with open(response_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, default=str)
            print(f"📄 Test response saved to: {response_file}")
            
        else:
            print(f"❌ API Error: {response.status_code}")
            print(f"Response: {response.content.decode()}")
            
    except Exception as e:
        print(f"❌ Test Error: {e}")

def main():
    """Main function"""
    print("HOLIDAY ANALYSIS API TESTING")
    print("="*50)
    
    # Test with Django test client (no server needed)
    test_api_without_server()
    
    # Test with HTTP requests (requires server)
    print("\n" + "="*50)
    print("To test with HTTP requests, start the server with:")
    print("python manage.py runserver")
    print("Then run this script again.")
    
    # Uncomment the line below to test with HTTP requests when server is running
    # test_holiday_api()

if __name__ == "__main__":
    main() 